"use strict";
const path = require("path");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: process.env.CORS_ORIGIN || "*" }, maxHttpBufferSize: 64 * 1024 });
app.use(express.static(path.join(__dirname, "public")));
const PORT = process.env.PORT || 3000;
const rooms = new Map();
const MAX_BUFFER_PER_ROOM = 50;
const MAX_MEMBERS_PER_ROOM = 60;
const RATE_LIMIT_WINDOW_MS = 10000;
const RATE_LIMIT_MAX_EVENTS = 40;
function getRoom(roomId) { if (!rooms.has(roomId)) rooms.set(roomId, { members: new Map(), buffer: [] }); return rooms.get(roomId); }
function presenceList(room) { return Array.from(room.members.values()); }
function cleanupRoomIfEmpty(roomId) { const room = rooms.get(roomId); if (room && room.members.size === 0) rooms.delete(roomId); }
io.on("connection", (socket) => {
  socket.data.roomId = null; socket.data.username = null; socket.data.eventTimestamps = [];
  function withinRateLimit() { const now = Date.now(); socket.data.eventTimestamps = socket.data.eventTimestamps.filter((t) => now - t < RATE_LIMIT_WINDOW_MS); socket.data.eventTimestamps.push(now); return socket.data.eventTimestamps.length <= RATE_LIMIT_MAX_EVENTS; }
  socket.on("join", ({ roomId, username }, ack) => {
    if (typeof roomId !== "string" || typeof username !== "string") return ack && ack({ ok: false, error: "invalid_payload" });
    const cleanRoomId = roomId.slice(0, 128); const cleanUsername = username.trim().slice(0, 24);
    if (!cleanRoomId || !cleanUsername) return ack && ack({ ok: false, error: "invalid_payload" });
    const room = getRoom(cleanRoomId);
    if (room.members.size >= MAX_MEMBERS_PER_ROOM) return ack && ack({ ok: false, error: "room_full" });
    socket.data.roomId = cleanRoomId; socket.data.username = cleanUsername;
    socket.join(cleanRoomId); room.members.set(socket.id, cleanUsername);
    socket.to(cleanRoomId).emit("presence", { members: presenceList(room) });
    ack && ack({ ok: true, members: presenceList(room), recent: room.buffer });
  });
  socket.on("message", (payload) => {
    if (!withinRateLimit()) return;
    const roomId = socket.data.roomId; if (!roomId) return;
    const { iv, ct } = payload || {};
    if (typeof iv !== "string" || typeof ct !== "string") return;
    if (iv.length > 64 || ct.length > 20000) return;
    const room = getRoom(roomId);
    const entry = { id: Date.now() + "-" + Math.random().toString(36).slice(2, 8), iv, ct };
    room.buffer.push(entry);
    if (room.buffer.length > MAX_BUFFER_PER_ROOM) room.buffer.shift();
    io.to(roomId).emit("message", entry);
  });
  socket.on("leave", () => handleLeave());
  socket.on("disconnect", () => handleLeave());
  function handleLeave() {
    const roomId = socket.data.roomId; if (!roomId) return;
    const room = rooms.get(roomId);
    if (room) { room.members.delete(socket.id); socket.to(roomId).emit("presence", { members: presenceList(room) }); cleanupRoomIfEmpty(roomId); }
    socket.leave(roomId); socket.data.roomId = null;
  }
});
server.listen(PORT, () => { console.log("CipherRoom relay listening on port " + PORT); });