// NOTE: This is a full replacement server.js for the "Divine Chat" style app (App B),
// with persistence to a JSON file on a persistent disk.
//
// Configure via ENV (you can change later without editing code):
//   PERSIST_PATH=/var/data/chat-state.json   (recommended on a persistent disk mount)
// Optional:
//   PERSIST_INTERVAL_MS=15000
//   PERSIST_DEBOUNCE_MS=2000
//   PERSIST_MAX_MESSAGES=500
//   PORT=3000
//
// What gets persisted:
//   - rooms: code, passwordHash, maxUsers, messages, isGlobal, safe
// What does NOT get persisted:
//   - live connected users (socket ids) and typing state
// On restart:
//   - rooms/messages restore
//   - users rejoin and the first user to join becomes host (ownerId) if ownerId is null

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const bcrypt = require('bcryptjs');
const fs = require('fs');
const path = require('path');

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
  process.env.PERSIST_PATH ||
  path.join(process.cwd(), 'data', 'chat-state.json'); // default local path (NOT persistent unless you mount it)

const PERSIST_INTERVAL_MS = parseInt(process.env.PERSIST_INTERVAL_MS || '15000', 10);
const PERSIST_DEBOUNCE_MS = parseInt(process.env.PERSIST_DEBOUNCE_MS || '2000', 10);
const PERSIST_MAX_MESSAGES = parseInt(process.env.PERSIST_MAX_MESSAGES || '500', 10);

function ensureDirForFile(filePath) {
  try {
    const dir = path.dirname(filePath);
    fs.mkdirSync(dir, { recursive: true });
  } catch {
    // ignore
  }
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function atomicWriteFileSync(filePath, data) {
  // Write to temp then rename to avoid half-written JSON on crashes.
  const dir = path.dirname(filePath);
  const tmp = path.join(dir, `.tmp-${path.basename(filePath)}-${process.pid}-${Date.now()}`);
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filePath);
}

// --------------------
// In-memory room store
// room = {
//   code,
//   ownerId,          // socket.id (NOT persisted)
//   passwordHash,     // persisted
//   maxUsers,         // persisted
//   users: { socketId: { username, joinedAt } }, // NOT persisted
//   messages: [ { username, text, ts, type? } ], // persisted (trimmed)
//   isGlobal: boolean, // persisted
//   safe: boolean      // persisted
// }
// --------------------
const rooms = {};

// Global room key
const GLOBAL_CODE = 'GLOBAL_CHAT_DIVINE';

// --------------------
// Sanitization helpers
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

// --------------------
// Persistence (load/save)
// --------------------
let persistDirty = false;
let persistTimer = null;
let lastPersistOkAt = 0;

function markDirty() {
  persistDirty = true;
  // Debounce: schedule a save soon, but not on every single event.
  if (persistTimer) return;
  persistTimer = setTimeout(() => {
    persistTimer = null;
    // We don't force save here; we call saveIfDirty which will do it.
    saveIfDirty();
  }, PERSIST_DEBOUNCE_MS);
}

function serializeRoomsForDisk() {
  const out = { version: 1, savedAt: Date.now(), rooms: {} };

  for (const [code, room] of Object.entries(rooms)) {
    // Persist only stable fields.
    const messages = Array.isArray(room.messages) ? room.messages.slice(-PERSIST_MAX_MESSAGES) : [];
    out.rooms[code] = {
      code: room.code,
      passwordHash: room.passwordHash || null,
      maxUsers: typeof room.maxUsers === 'number' ? room.maxUsers : 10,
      messages,
      isGlobal: !!room.isGlobal,
      safe: !!room.safe
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
      ownerId: null, // must be re-established by first join after restart
      passwordHash: r.passwordHash || null,
      maxUsers: typeof r.maxUsers === 'number' ? r.maxUsers : 10,
      users: {}, // live only
      messages: Array.isArray(r.messages) ? r.messages.slice(-PERSIST_MAX_MESSAGES) : [],
      isGlobal: !!r.isGlobal,
      safe: r.safe === undefined ? true : !!r.safe
    };
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
      safe: false
    };
    markDirty();
  } else {
    // Ensure global invariants
    rooms[GLOBAL_CODE].code = GLOBAL_CODE;
    rooms[GLOBAL_CODE].isGlobal = true;
    rooms[GLOBAL_CODE].safe = false;
    rooms[GLOBAL_CODE].passwordHash = null;
    rooms[GLOBAL_CODE].maxUsers = Infinity;
    rooms[GLOBAL_CODE].users = rooms[GLOBAL_CODE].users || {};
    rooms[GLOBAL_CODE].messages = Array.isArray(rooms[GLOBAL_CODE].messages) ? rooms[GLOBAL_CODE].messages : [];
  }
}

function loadStateFromDisk() {
  ensureDirForFile(PERSIST_PATH);

  try {
    if (!fs.existsSync(PERSIST_PATH)) {
      // nothing to load
      return;
    }
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
  lastPersistOkAt = Date.now();
}

function saveIfDirty() {
  if (!persistDirty) return;
  try {
    // Always trim messages before writing.
    for (const r of Object.values(rooms)) {
      if (Array.isArray(r.messages) && r.messages.length > PERSIST_MAX_MESSAGES) {
        r.messages = r.messages.slice(-PERSIST_MAX_MESSAGES);
      }
    }

    saveStateToDisk();
    persistDirty = false;
  } catch (err) {
    console.error('Failed to persist state:', err);
    // Leave dirty = true so we try again later.
  }
}

// Periodic flush: protects you if traffic stops before debounce fires
setInterval(() => {
  saveIfDirty();
}, Math.max(2000, PERSIST_INTERVAL_MS)).unref();

// Load persisted state at boot and ensure global room exists
loadStateFromDisk();
ensureGlobalRoom();

// --------------------
// Room helper functions
// --------------------
function buildUserList(room) {
  const users = Object.entries(room.users).map(([sid, u]) => ({
    username: u.username,
    socketId: sid,
    joinedAt: u.joinedAt
  }));
  const ownerUsername =
    room.ownerId && room.users[room.ownerId] ? room.users[room.ownerId].username : null;
  return { users, ownerId: room.ownerId, ownerUsername };
}

function reassignOwnerIfNeeded(room) {
  if (!room) return;

  // If owner is missing or disconnected, pick a new owner if there are users
  if (room.ownerId && room.users[room.ownerId]) return; // owner still present

  const sids = Object.keys(room.users);
  if (sids.length === 0) {
    // No users; delete non-global rooms to free memory
    if (!room.isGlobal) {
      delete rooms[room.code];
      markDirty();
      console.log(`Room ${room.code} deleted because empty.`);
    } else {
      room.ownerId = null;
    }
    return;
  }

  // pick first user as new owner
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

// --------------------
// Socket.IO
// --------------------
io.on('connection', (socket) => {
  socket.data.currentRoom = null;
  socket.data.username = null;
  socket.data.lastMessageTs = 0;

  // Create a room
  socket.on('createRoom', async (payload, cb) => {
    try {
      const { username, code, password, maxUsers, safe } = payload || {};
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
        safe: safe === undefined ? true : !!safe
      };

      // Add creator to room
      const safeName = sanitizeText(username);
      rooms[key].users[socket.id] = { username: safeName, joinedAt: Date.now() };
      socket.join(key);
      socket.data.currentRoom = key;
      socket.data.username = safeName;

      const userList = buildUserList(rooms[key]);

      cb &&
        cb({
          ok: true,
          room: { code: key, maxUsers: finalMax, isGlobal: false, safe: rooms[key].safe },
          messages: rooms[key].messages,
          users: userList.users,
          ownerId: userList.ownerId,
          ownerUsername: userList.ownerUsername
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

  // Join a room
  socket.on('joinRoom', async (payload, cb) => {
    try {
      const { username, code, password } = payload || {};
      if (!username || !code) return cb && cb({ ok: false, message: 'Username and code required.' });

      const key = String(code).trim();
      const room = rooms[key];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });

      // username uniqueness (case-insensitive)
      const unameLower = String(username).trim().toLowerCase();
      const nameTaken = Object.values(room.users).some(
        (u) => (u.username || '').toLowerCase() === unameLower
      );
      if (nameTaken) return cb && cb({ ok: false, message: 'Username already taken in this room.' });

      if (!room.isGlobal && room.passwordHash) {
        const ok = await bcrypt.compare(String(password || ''), room.passwordHash);
        if (!ok) return cb && cb({ ok: false, message: 'Incorrect password.' });
      }

      const userCount = Object.keys(room.users).length;
      if (userCount >= room.maxUsers) return cb && cb({ ok: false, message: 'Room is full.' });

      const safeName = sanitizeText(username);
      room.users[socket.id] = { username: safeName, joinedAt: Date.now() };
      socket.join(key);
      socket.data.currentRoom = key;
      socket.data.username = safeName;

      // If room was restored from disk, ownerId is null; set first joiner as owner.
      if (!room.isGlobal && !room.ownerId) {
        room.ownerId = socket.id;
        io.to(key).emit('systemMessage', { text: `${safeName} is now the host.` });
      }

      const userList = buildUserList(room);

      cb &&
        cb({
          ok: true,
          room: { code: key, maxUsers: room.maxUsers, isGlobal: room.isGlobal, safe: room.safe },
          messages: room.messages,
          users: userList.users,
          ownerId: userList.ownerId,
          ownerUsername: userList.ownerUsername
        });

      io.to(key).emit('userList', {
        users: userList.users,
        ownerId: userList.ownerId,
        ownerUsername: userList.ownerUsername
      });

      io.to(key).emit('systemMessage', { text: `${safeName} joined the room.` });

      // You can choose whether join/leave messages should persist; if you want them to persist,
      // you'd store system messages in room.messages too. For now, we only persist actual chat messages.
      // markDirty is still useful because room ownership might have changed.
      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  // Join global (convenience)
  socket.on('joinGlobal', (payload, cb) => {
    try {
      const { username } = payload || {};
      if (!username) return cb && cb({ ok: false, message: 'Username required.' });

      const room = rooms[GLOBAL_CODE];
      if (!room) {
        ensureGlobalRoom();
      }

      const globalRoom = rooms[GLOBAL_CODE];

      const unameLower = String(username).trim().toLowerCase();
      const nameTaken = Object.values(globalRoom.users).some(
        (u) => (u.username || '').toLowerCase() === unameLower
      );
      if (nameTaken) return cb && cb({ ok: false, message: 'Username already taken in global chat.' });

      const safeName = sanitizeText(username);
      globalRoom.users[socket.id] = { username: safeName, joinedAt: Date.now() };
      socket.join(GLOBAL_CODE);
      socket.data.currentRoom = GLOBAL_CODE;
      socket.data.username = safeName;

      const userList = buildUserList(globalRoom);

      cb &&
        cb({
          ok: true,
          room: { code: GLOBAL_CODE, isGlobal: true, safe: globalRoom.safe },
          messages: globalRoom.messages,
          users: userList.users,
          ownerId: userList.ownerId,
          ownerUsername: userList.ownerUsername
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

  // Send message
  socket.on('sendMessage', (payload, cb) => {
    try {
      const { text, type } = payload || {};
      const roomKey = socket.data.currentRoom;
      const username = socket.data.username || 'Unknown';

      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });
      if (!text || !String(text).trim()) return cb && cb({ ok: false, message: 'Empty message.' });

      const now = Date.now();
      if (now - socket.data.lastMessageTs < 200) {
        return cb && cb({ ok: false, message: 'You are sending messages too quickly.' });
      }
      socket.data.lastMessageTs = now;

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });

      const safeText = sanitizeText(String(text).slice(0, 2000));
      const msg = { username, text: safeText, ts: now };
      if (type) msg.type = String(type);

      room.messages.push(msg);
      if (room.messages.length > PERSIST_MAX_MESSAGES) room.messages.shift();

      io.to(roomKey).emit('newMessage', msg);
      cb && cb({ ok: true });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  // Typing indicator (simple broadcast to room)
  socket.on('typing', (payload) => {
    try {
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return;
      const username = socket.data.username || 'Unknown';
      const typing = !!(payload && payload.typing);
      socket.to(roomKey).emit('typing', { username, socketId: socket.id, typing });
    } catch {
      // ignore
    }
  });

  // Host clear chat
  socket.on('clearRoom', (payload, cb) => {
    try {
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return cb && cb({ ok: false, message: 'Not in a room.' });

      const room = rooms[roomKey];
      if (!room) return cb && cb({ ok: false, message: 'Room not found.' });
      if (room.isGlobal) return cb && cb({ ok: false, message: 'Cannot clear global chat.' });
      if (room.ownerId !== socket.id) {
        return cb && cb({ ok: false, message: 'Only the host can clear the chat.' });
      }

      room.messages = [];
      io.to(roomKey).emit('roomCleared', { by: socket.data.username || 'host' });
      cb && cb({ ok: true });

      markDirty();
    } catch (err) {
      console.error(err);
      cb && cb({ ok: false, message: 'Server error' });
    }
  });

  // Client explicitly signals refresh/kill action (private rooms only)
  socket.on('client-refresh', () => {
    try {
      const roomKey = socket.data.currentRoom;
      if (!roomKey) return;

      const room = rooms[roomKey];
      if (!room || room.isGlobal) return;

      // Only delete room if it's a safe room
      if (room.safe) {
        io.to(roomKey).emit('kicked', { reason: 'Room closed due to page refresh (safe room).' });

        // Remove users from room and close it
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

    // If room got deleted, reassignOwnerIfNeeded already handled it.
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

    // Don’t delete room on disconnect; deletion happens when empty in reassignOwnerIfNeeded
    markDirty();
  });
});

// Best-effort flush on shutdown (platform may SIGTERM on deploy)
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
  console.log(
    `Persistence: interval=${PERSIST_INTERVAL_MS}ms debounce=${PERSIST_DEBOUNCE_MS}ms maxMessages=${PERSIST_MAX_MESSAGES} lastOk=${lastPersistOkAt}`
  );
});
