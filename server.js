// Divine Chat server (enhanced)
// - Persistent storage (PERSIST_PATH)
// - Per-room identity token (authorizes edits/deletes + locked-room rejoin)
// - Room lock/unlock (host only)
// - Host reassignment (host only)
// - Message IDs + edit/delete rules
// - History cap: 100 non-system messages (tombstones count; system messages do NOT)
//
// ENV:
//   PORT=3000
//   PERSIST_PATH=/var/data/chat-state.json
// Optional:
//   PERSIST_INTERVAL_MS=15000
//   PERSIST_DEBOUNCE_MS=2000

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const bcrypt = require('bcryptjs');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;

// Serve static files from public
app.use(express.static('public'));

// --------------------
// Persistence settings
// --------------------
const PERSIST_PATH =
  process.env.PERSIST_PATH || path.join(process.cwd(), 'data', 'chat-state.json');

const PERSIST_INTERVAL_MS = parseInt(process.env.PERSIST_INTERVAL_MS || '15000', 10);
const PERSIST_DEBOUNCE_MS = parseInt(process.env.PERSIST_DEBOUNCE_MS || '2000', 10);

// Desired history size (non-system)
const HISTORY_MAX = 100;

// Edit rules
const EDIT_WINDOW_MS = 60 * 1000;

// --------------------
// In-memory room store
// --------------------
// room = {
//   code,
//   ownerId,             // socket.id (NOT persisted)
//   passwordHash,        // persisted
//   maxUsers,            // persisted
//   users: { socketId: { username, joinedAt, tokenId } }, // NOT persisted
//   messages: [ Message ],                                  // persisted (non-system only)
//   isGlobal: boolean,    // persisted
//   safe: boolean,        // persisted
//   locked: boolean,      // persisted
//   tokens: { tokenId: { createdAt, lastUsername } } // persisted (per-room identity)
// }
//
// Message = {
//   id: string,
//   username: string,
//   text: string,
//   ts: number,
//   type?: string,
//   deleted?: boolean,
//   deletedAt?: number,
//   edited?: boolean,
//   editedAt?: number,
//   editedOnce?: boolean,
//   authorTokenId?: string
// }
const rooms = {};

// Global room key
const GLOBAL_CODE = 'GLOBAL_CHAT_DIVINE';

// --------------------
// Helpers
// --------------------
function sanitizeText(text) {
  if (!text) return '';
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function nowMs() {
  return Date.now();
}

function genId(prefix) {
  return `${prefix}_${crypto.randomBytes(10).toString('hex')}_${Date.now()}`;
}

function ensureDirForFile(filePath) {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
  } catch {}
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function atomicWriteFileSync(filePath, data) {
  const dir = path.dirname(filePath);
  const tmp = path.join(dir, `.tmp-${path.basename(filePath)}-${process.pid}-${Date.now()}`);
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filePath);
}

function trimHistory(room) {
  if (!room || !Array.isArray(room.messages)) return;
  if (room.messages.length <= HISTORY_MAX) return;
  room.messages = room.messages.slice(-HISTORY_MAX);
}

function buildUserList(room) {
  const users = Object.entries(room.users || {}).map(([sid, u]) => ({
    username: u.username,
    socketId: sid,
    joinedAt: u.joinedAt
  }));
  const ownerUsername =
    room.ownerId && room.users && room.users[room.ownerId] ? room.users[room.ownerId].username : null;
  return { users, ownerId: room.ownerId, ownerUsername };
}

function reassignOwnerIfNeeded(room) {
  if (!room) return;

  if (room.ownerId && room.users && room.users[room.ownerId]) return;

  const sids = Object.keys(room.users || {});
  if (sids.length === 0) {
    if (!room.isGlobal) {
      delete rooms[room.code];
      markDirty();
      console.log(`Room ${room.code} deleted because empty.`);
    } else {
      room.ownerId = null;
    }
    return;
  }

  room.ownerId = sids[0];
  const newOwnerName = room.users[room.ownerId] ? room.users[room.ownerId].username : null;
  io.to(room.code).emit('systemMessage', { text: `${newOwnerName || 'Someone'} is now the host.` });

  const userList = buildUserList(room);
  io.to(room.code).emit('userList', {
    users: userList.users,
    ownerId: userList.ownerId,
    ownerUsername: userList.ownerUsername
  });
}

function isHost(room, socket) {
  return !!(room && socket && room.ownerId && room.ownerId === socket.id);
}

// --------------------
// Persistence
// --------------------
let persistDirty = false;
let persistTimer = null;

function markDirty() {
  persistDirty = true;
  if (persistTimer) return;
  persistTimer = setTimeout(() => {
    persistTimer = null;
    saveIfDirty();
  }, PERSIST_DEBOUNCE_MS);
}

function serializeRoomsForDisk() {
  const out = { version: 2, savedAt: nowMs(), rooms: {} };

  for (const [code, room] of Object.entries(rooms)) {
    out.rooms[code] = {
      code: room.code,
      passwordHash: room.passwordHash || null,
      maxUsers: typeof room.maxUsers === 'number' ? room.maxUsers : 10,
      isGlobal: !!room.isGlobal,
      safe: !!room.safe,
      locked: !!room.locked,
      tokens: room.tokens || {},
      messages: Array.isArray(room.messages) ? room.messages.slice(-HISTORY_MAX) : []
    };
  }

  return out;
}

function hydrateRoomsFromDiskState(state) {
  if (!state || typeof state !== 'object') return;
  const stateRooms = state.rooms && typeof state.rooms === 'object' ? state.rooms : {};

  for (const [code, r] of Object.entries(stateRooms)) {
    if (!r || typeof r !== 'object') continue;
    const roomCode = String(r.code || code);

    rooms[roomCode] = {
      code: roomCode,
      ownerId: null, // re-established by first join after restart
      passwordHash: r.passwordHash || null,
      maxUsers: typeof r.maxUsers === 'number' ? r.maxUsers : 10,
      users: {},
      messages: Array.isArray(r.messages) ? r.messages.slice(-HISTORY_MAX) : [],
      isGlobal: !!r.isGlobal,
      safe: r.safe === undefined ? true : !!r.safe,
      locked: !!r.locked,
      tokens: (r.tokens && typeof r.tokens === 'object') ? r.tokens : {}
    };

    trimHistory(rooms[roomCode]);
  }
}

function ensureGlobalRoom() {
  if (!rooms[GLOBAL_CODE]) {
    rooms[GLOBAL_CODE] = {
      code: GLOBAL_CODE,
      ownerId: null,
      passwordHash: null,
      maxUsers: Infinity,
      users: {},
      messages: [],
      isGlobal: true,
      safe: false,
      locked: false,
      tokens: {}
    };
    markDirty();
  } else {
    rooms[GLOBAL_CODE].code = GLOBAL_CODE;
    rooms[GLOBAL_CODE].isGlobal = true;
    rooms[GLOBAL_CODE].safe = false;
    rooms[GLOBAL_CODE].passwordHash = null;
    rooms[GLOBAL_CODE].maxUsers = Infinity;
    rooms[GLOBAL_CODE].users = rooms[GLOBAL_CODE].users || {};
    rooms[GLOBAL_CODE].messages = Array.isArray(rooms[GLOBAL_CODE].messages) ? rooms[GLOBAL_CODE].messages : [];
    rooms[GLOBAL_CODE].locked = false;
    rooms[GLOBAL_CODE].tokens = rooms[GLOBAL_CODE].tokens || {};
  }
}

function loadStateFromDisk() {
  ensureDirForFile(PERSIST_PATH);
  try {
    if (!fs.existsSync(PERSIST_PATH)) return;
    const raw = fs.readFileSync(PERSIST_PATH, 'utf8');
    const parsed = safeJsonParse(raw);
    if (!parsed) return;
    hydrateRoomsFromDiskState(parsed);
  } catch (err) {
    console.error('Failed to load persisted state:', err);
  }
}

function saveStateToDisk() {
  ensureDirForFile(PERSIST_PATH);
  const state = serializeRoomsForDisk();
  const json = JSON.stringify(state, null, 2);
  atomicWriteFileSync(PERSIST_PATH, json);
}

function saveIfDirty() {
  if (!persistDirty) return;
  try {
    for (const r of Object.values(rooms)) trimHistory(r);
    saveStateToDisk();
    persistDirty = false;
  } catch (err) {
    console.error('Failed to persist state:', err);
  }
}

setInterval(() => saveIfDirty(), Math.max(2000, PERSIST_INTERVAL_MS)).unref();

loadStateFromDisk();
ensureGlobalRoom();

// --------------------
// Token + permissions
// --------------------
function ensureToken(room, tokenIdMaybe, username) {
  if (!room.tokens) room.tokens = {};
  let tokenId = tokenIdMaybe && String(tokenIdMaybe).trim() ? String(tokenIdMaybe).trim() : null;

  if (!tokenId) {
    tokenId = genId('t');
    room.tokens[tokenId] = { createdAt: nowMs(), lastUsername: username || '' };
    markDirty();
    return tokenId;
  }

  if (!room.tokens[tokenId]) {
    room.tokens[tokenId] = { createdAt: nowMs(), lastUsername: username || '' };
    markDirty();
    return tokenId;
  }

  room.tokens[tokenId].lastUsername = username || room.tokens[tokenId].lastUsername || '';
  markDirty();
  return tokenId;
}

function canEditMessage(room, socket, msg) {
  if (!room || !socket || !msg) return { ok: false, reason: 'Invalid.' };
  if (msg.deleted) return { ok: false, reason: 'Message already deleted.' };
  if (msg.editedOnce) return { ok: false, reason: 'Message already edited once.' };

  const tokenId = socket.data.tokenId;
  if (!tokenId || msg.authorTokenId !== tokenId) return { ok: false, reason: 'Not your message.' };

  const age = nowMs() - (msg.ts || 0);
  if (age > EDIT_WINDOW_MS) return { ok: false, reason: 'Edit window expired.' };

  // Option 1: once you send any new message, you cannot edit older ones.
  const cutoff = socket.data.lastSentMessageTs || 0;
  if (cutoff && msg.ts < cutoff) return { ok: false, reason: 'You already sent another message.' };

  return { ok: true };
}

function canDeleteMessage(room, socket, msg) {
  if (!room || !socket || !msg) return { ok: false, reason: 'Invalid.' };
  if (msg.deleted) return { ok: false, reason: 'Already deleted.' };

  const tokenId = socket.data.tokenId;
  const isSender = tokenId && msg.authorTokenId === tokenId;
  if (isSender) return { ok: true };
  if (isHost(room, socket)) return { ok: true };
  return { ok: false, reason: 'No permission.' };
}

// --------------------
// Socket.IO
// --------------------
io.on('connection', (socket) => {
  socket.data.currentRoom = null;
  socket.data.username = null;
  socket.data.tokenId = null;
  socket.data.lastMessageTs = 0;
  socket.data.lastSentMessageTs = 0;

  socket.on('createRoom', async (payload, cb) => {
    try {
      const { username, code, password, maxUsers, safe, tokenId } = payload || {};
      if (!username || !code) return cb && cb({ ok: false, message: 'Username and code required.' });

      const key = String(code).trim();
      if (rooms[key]) return cb && cb({ ok: false, message: 'Room code already exists.' });

      const parsedMax = parseInt(maxUsers, 10);
      const finalMax = Number.isInteger(parsedMax) ? Math.max(2, Math.min(parsedMax, 200)) : 10;

      const saltRounds = 10;
      const passwordHash = password ? await bcrypt.hash(String(password), saltRounds) : null;

      rooms[key] = {
        code: key,
        ownerId: socket.id,
        passwordHash,
        maxUsers: finalMax,
        users: {},
        messages: [],
        isGlobal: false,
        safe: safe === undefined ? true : !!safe,
        locked: false,
        tokens: {}
      };

      const safeName = sanitizeText(username);
      const assignedTokenId = ensureToken(rooms[key], tokenId, safeName);

      rooms[key].users[socket.id] = { username: safeName, joinedAt: nowMs(), tokenId: assignedTokenId };
      socket.join(key);
      socket.data.currentRoom = key;
      socket.data.username = safeName;
      socket.data.tokenId = assignedTokenId;
      socket.data.lastSentMessageTs = 0;

      const userList = buildUserList(rooms[key]);

      cb && cb({
        ok: true,
        room: { code: key, maxUsers: finalMax, isGlobal: false, safe: rooms[key].safe, locked: rooms[key].locked },
        messages: rooms[key].messages,
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername,
        tokenId: assignedTokenId
      });

      io.to(key).emit('userList', {
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername
      });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('joinRoom', async (payload, cb) => {
    try {
      const { username, code, password, tokenId } = payload || {};
      if (!username || !code) return cb && cb({ ok: false, message: 'Username and code required.' });

      const key = String(code).trim();
      const room = rooms[key];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });

      const safeName = sanitizeText(username);

      // Locked-room rule: ONLY users with an existing token for this room may join.
      if (!room.isGlobal && room.locked) {
        const incomingToken = tokenId && String(tokenId).trim() ? String(tokenId).trim() : '';
        if (!incomingToken || !room.tokens || !room.tokens[incomingToken]) {
          return cb && cb({ ok: false, message: 'Room locked.' });
        }
      }

      // username uniqueness (case-insensitive)
      const unameLower = String(safeName).trim().toLowerCase();
      const nameTaken = Object.values(room.users || {}).some(
        (u) => (u.username || '').toLowerCase() === unameLower
      );
      if (nameTaken) return cb && cb({ ok: false, message: 'Username already taken in this room.' });

      if (!room.isGlobal && room.passwordHash) {
        const ok = await bcrypt.compare(String(password || ''), room.passwordHash);
        if (!ok) return cb && cb({ ok: false, message: 'Incorrect password.' });
      }

      const userCount = Object.keys(room.users || {}).length;
      if (userCount >= room.maxUsers) return cb && cb({ ok: false, message: 'Room is full.' });

      const assignedTokenId = ensureToken(room, tokenId, safeName);

      room.users[socket.id] = { username: safeName, joinedAt: nowMs(), tokenId: assignedTokenId };
      socket.join(key);
      socket.data.currentRoom = key;
      socket.data.username = safeName;
      socket.data.tokenId = assignedTokenId;
      socket.data.lastSentMessageTs = 0;

      if (!room.isGlobal && !room.ownerId) {
        room.ownerId = socket.id;
        io.to(key).emit('systemMessage', { text: `${safeName} is now the host.` });
      }

      const userList = buildUserList(room);

      cb && cb({
        ok: true,
        room: { code: key, maxUsers: room.maxUsers, isGlobal: room.isGlobal, safe: room.safe, locked: room.locked },
        messages: room.messages,
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername,
        tokenId: assignedTokenId
      });

      io.to(key).emit('userList', {
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername
      });
      io.to(key).emit('systemMessage', { text: `${safeName} joined the room.` });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('joinGlobal', (payload, cb) => {
    try {
      const { username } = payload || {};
      if (!username) return cb && cb({ ok: false, message: 'Username required.' });

      ensureGlobalRoom();
      const room = rooms[GLOBAL_CODE];

      const safeName = sanitizeText(username);

      const unameLower = String(safeName).trim().toLowerCase();
      const nameTaken = Object.values(room.users || {}).some(
        (u) => (u.username || '').toLowerCase() === unameLower
      );
      if (nameTaken) return cb && cb({ ok: false, message: 'Username already taken in global chat.' });

      const assignedTokenId = ensureToken(room, null, safeName);

      room.users[socket.id] = { username: safeName, joinedAt: nowMs(), tokenId: assignedTokenId };
      socket.join(GLOBAL_CODE);
      socket.data.currentRoom = GLOBAL_CODE;
      socket.data.username = safeName;
      socket.data.tokenId = assignedTokenId;
      socket.data.lastSentMessageTs = 0;

      const userList = buildUserList(room);

      cb && cb({
        ok: true,
        room: { code: GLOBAL_CODE, isGlobal: true, safe: room.safe, locked: room.locked },
        messages: room.messages,
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername,
        tokenId: assignedTokenId
      });

      io.to(GLOBAL_CODE).emit('userList', {
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername
      });
      io.to(GLOBAL_CODE).emit('systemMessage', { text: `${safeName} joined Global Chat.` });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('sendMessage', (payload, cb) => {
    try {
      const { text, type } = payload || {};
      const roomKey = socket.data.currentRoom;
      const username = socket.data.username || 'Unknown';
      const tokenId = socket.data.tokenId || null;

      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });
      if (!text || !String(text).trim()) return cb && cb({ ok: false, message: 'Empty message.' });

      const now = nowMs();
      if (now - socket.data.lastMessageTs < 200) {
        return cb && cb({ ok: false, message: 'You are sending messages too quickly.' });
      }
      socket.data.lastMessageTs = now;

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });

      const safeText = sanitizeText(String(text).slice(0, 2000));
      const msg = {
        id: genId('m'),
        username,
        text: safeText,
        ts: now,
        authorTokenId: tokenId
      };
      if (type) msg.type = String(type);

      room.messages.push(msg);
      trimHistory(room);

      // Option 1: sending any new message ends edit ability for all older ones (per socket session)
      socket.data.lastSentMessageTs = now;

      io.to(roomKey).emit('newMessage', msg);
      cb && cb({ ok: true, message: msg });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('editMessage', (payload, cb) => {
    try {
      const { messageId, newText } = payload || {};
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });

      if (!messageId) return cb && cb({ ok: false, message: 'Missing message id.' });

      const msg = (room.messages || []).find((m) => m && m.id === messageId);
      if (!msg) return cb && cb({ ok: false, message: 'Message not found.' });

      const allowed = canEditMessage(room, socket, msg);
      if (!allowed.ok) return cb && cb({ ok: false, message: allowed.reason || 'Cannot edit.' });

      const safeText = sanitizeText(String(newText || '').slice(0, 2000));
      if (!safeText.trim()) return cb && cb({ ok: false, message: 'Empty message.' });

      msg.text = safeText;
      msg.edited = true;
      msg.editedAt = nowMs();
      msg.editedOnce = true;

      io.to(roomKey).emit('messageEdited', { id: msg.id, text: msg.text, editedAt: msg.editedAt });
      cb && cb({ ok: true });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('deleteMessage', (payload, cb) => {
    try {
      const { messageId } = payload || {};
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });

      if (!messageId) return cb && cb({ ok: false, message: 'Missing message id.' });

      const msg = (room.messages || []).find((m) => m && m.id === messageId);
      if (!msg) return cb && cb({ ok: false, message: 'Message not found.' });

      const allowed = canDeleteMessage(room, socket, msg);
      if (!allowed.ok) return cb && cb({ ok: false, message: allowed.reason || 'Cannot delete.' });

      msg.text = 'Message deleted';
      msg.deleted = true;
      msg.deletedAt = nowMs();

      io.to(roomKey).emit('messageDeleted', { id: msg.id, deletedAt: msg.deletedAt });
      cb && cb({ ok: true });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('setRoomLocked', (payload, cb) => {
    try {
      const { locked } = payload || {};
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });
      if (room.isGlobal) return cb && cb({ ok: false, message: 'Global chat cannot be locked.' });
      if (!isHost(room, socket)) return cb && cb({ ok: false, message: 'Only host can lock/unlock.' });

      room.locked = !!locked;

      io.to(roomKey).emit('roomLockedChanged', { locked: room.locked });
      io.to(roomKey).emit('systemMessage', { text: room.locked ? 'Room locked.' : 'Room unlocked.' });

      cb && cb({ ok: true, locked: room.locked });
      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('reassignHost', (payload, cb) => {
    try {
      const { targetSocketId } = payload || {};
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });
      if (room.isGlobal) return cb && cb({ ok: false, message: 'Global chat has no host.' });
      if (!isHost(room, socket)) return cb && cb({ ok: false, message: 'Only host can reassign.' });

      if (!targetSocketId || !room.users || !room.users[targetSocketId]) {
        return cb && cb({ ok: false, message: 'Target user not found in room.' });
      }

      room.ownerId = targetSocketId;

      const newOwnerName = room.users[targetSocketId] ? room.users[targetSocketId].username : 'Someone';
      io.to(roomKey).emit('systemMessage', { text: `${newOwnerName} is now the host.` });

      const userList = buildUserList(room);
      io.to(roomKey).emit('userList', {
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername
      });

    cb && cb({ ok: true });
      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('typing', (payload) => {
    try {
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return;
      const username = socket.data.username || 'Unknown';
      const typing = !!(payload && payload.typing);
      socket.to(roomKey).emit('typing', { username, socketId: socket.id, typing });
    } catch {}
  });

  socket.on('clearRoom', (payload, cb) => {
    try {
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });
      if (room.isGlobal) return cb && cb({ ok: false, message: 'Cannot clear global chat.' });
      if (!isHost(room, socket)) return cb && cb({ ok: false, message: 'Only the host can clear the chat.' });

      room.messages = [];
      io.to(roomKey).emit('roomCleared', { by: socket.data.username || 'host' });
      cb && cb({ ok: true });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  socket.on('client-refresh', () => {
    try {
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return;

      const room = rooms[roomKey];
      if (!room || room.isGlobal) return;

      if (room.safe) {
        io.to(roomKey).emit('kicked', { reason: 'Room closed due to page refresh (safe room).' });

        const socketsToLeave = Object.keys(room.users || {});
        socketsToLeave.forEach((sid) => {
          const s = io.sockets.sockets.get(sid);
          if (s) {
            s.leave(roomKey);
            s.data.currentRoom = null;
          }
        });

        delete rooms[roomKey];
        markDirty();
        console.log(`Room ${roomKey} deleted due to client refresh (safe room).`);
      } else {
        console.log(`Client refresh in non-safe room ${roomKey} — no room deletion.`);
      }
    } catch (err) {
      console.error(err);
    }
  });

  socket.on('leaveRoom', () => {
    const roomKey = socket.data.currentRoom;
    if (!roomKey) return;

    const room = rooms[roomKey];
    if (!room) {
      socket.data.currentRoom = null;
      return;
    }
    delete room.users[socket.id];
    socket.leave(roomKey);
    socket.data.currentRoom = null;

    if (room.ownerId === socket.id) {
      reassignOwnerIfNeeded(room);
    } else {
      const userList = buildUserList(room);
      io.to(roomKey).emit('userList', {
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername
      });
    }

    markDirty();
  });

  socket.on('disconnect', () => {
    const roomKey = socket.data.currentRoom;
    if (!roomKey) return;

    const room = rooms[roomKey];
    if (!room) return;

    delete room.users[socket.id];

    if (room.ownerId === socket.id) {
      reassignOwnerIfNeeded(room);
    } else {
      const userList = buildUserList(room);
      io.to(roomKey).emit('userList', {
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername
      });
    }

    markDirty();
  });
});

// Best-effort flush on shutdown
function shutdownHandler(signal) {
  try {
    console.log(`Received ${signal}. Flushing persistence...`);
    saveIfDirty();
  } catch {}
  process.exit(0);
}
process.on('SIGINT', () => shutdownHandler('SIGINT'));
process.on('SIGTERM', () => shutdownHandler('SIGTERM'));

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
  console.log(`Persistence path: ${PERSIST_PATH}`);
});
