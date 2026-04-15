const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const fetch = (...args) =>
  import("node-fetch").then(({ default: f }) => f(...args));
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { 
  cors: { origin: "*" },
  transports: ['websocket', 'polling']
});

app.use(express.static("public"));
app.use(express.json());

// ── Translation ───────────────────────────────────────────────────────────────
app.post("/translate", async (req, res) => {
  const { text, targetLang } = req.body;
  if (!text || !targetLang) return res.json({ translation: text });
  try {
    const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=${encodeURIComponent(targetLang)}&dt=t&q=${encodeURIComponent(text)}`;
    const r = await fetch(url);
    const data = await r.json();
    const translation = data[0]?.map((chunk) => chunk[0]).join("") || text;
    res.json({ translation });
  } catch (e) {
    console.error("Translate error:", e);
    res.json({ translation: text });
  }
});

// ── Deepgram key endpoint ─────────────────────────────────────────────────────
app.get("/deepgram-key", (req, res) => {
  res.json({ key: process.env.DEEPGRAM_API_KEY || "" });
});

// ── Room management ───────────────────────────────────────────────────────────
const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, []);
  return rooms.get(roomId);
}

// ── WebRTC signaling + Deepgram proxy ─────────────────────────────────────────
io.on("connection", (socket) => {
  console.log(`[connect] socket=${socket.id}`);

  socket.on("join-room", (roomId) => {
    const room = getRoom(roomId);
    console.log(`[join-room] socket=${socket.id} room=${roomId} current occupants=${room.length}`);

    if (room.length >= 2) {
      console.log(`[join-room] room=${roomId} FULL`);
      socket.emit("room-full");
      return;
    }

    room.push(socket.id);
    socket.join(roomId);
    socket.data.roomId = roomId;

    console.log(`[join-room] after push room=${roomId} occupants=${room.length} members=${JSON.stringify(room)}`);

    if (room.length === 2) {
      const [first, second] = room;
      const firstSocket  = io.sockets.sockets.get(first);
      const secondSocket = io.sockets.sockets.get(second);
      if (firstSocket)  firstSocket.data.peerId  = second;
      if (secondSocket) secondSocket.data.peerId = first;
      console.log(`[ready] room=${roomId} initiator=${first} receiver=${second}`);
      io.to(first).emit("ready",  { initiator: true });
      io.to(second).emit("ready", { initiator: false });
    } else {
      console.log(`[waiting] room=${roomId} has 1 peer`);
      socket.emit("waiting");
    }
  });

  // ── Deepgram proxy ──────────────────────────────────────────────────────────
  socket.on("start-transcription", ({ lang }) => {
    const key = process.env.DEEPGRAM_API_KEY;
    if (!key) {
      socket.emit("transcription-closed");
      console.warn("[deepgram] no API key");
      return;
    }

    const dgUrl = `wss://api.deepgram.com/v1/listen?encoding=linear16&sample_rate=48000&language=${lang}&punctuate=true&interim_results=true`;
    console.log(`[deepgram] opening for socket=${socket.id} lang=${lang}`);

    const dg = new WebSocket(dgUrl, {
      headers: { Authorization: `Token ${key}` }
    });

    dg.on("open", () => {
      console.log(`[deepgram] open for socket=${socket.id}`);
      socket.emit("transcription-ready");
    });

    dg.on("message", (data) => {
      try {
        const parsed     = JSON.parse(data);
        const transcript = parsed.channel?.alternatives?.[0]?.transcript || "";
        const isFinal    = parsed.is_final;
        if (!transcript) return;
        console.log(`[deepgram] transcript="${transcript}" final=${isFinal}`);
        socket.emit("transcript", { transcript, isFinal });
      } catch(e) {}
    });

    dg.on("close", (code) => {
      console.log(`[deepgram] closed code=${code} socket=${socket.id}`);
      socket.emit("transcription-closed");
    });

    dg.on("error", (e) => {
      console.error(`[deepgram] error socket=${socket.id}:`, e.message);
      socket.emit("transcription-closed");
    });

    const keepAlive = setInterval(() => {
      if (dg.readyState === WebSocket.OPEN) {
        dg.send(JSON.stringify({ type: "KeepAlive" }));
      }
    }, 8000);

    socket.on("audio-chunk", (chunk) => {
      if (dg.readyState === WebSocket.OPEN) {
        dg.send(chunk);
      }
    });

    socket.on("stop-transcription", () => {
      clearInterval(keepAlive);
      if (dg.readyState === WebSocket.OPEN) dg.close();
    });

    socket.on("disconnect", () => {
      clearInterval(keepAlive);
      if (dg.readyState === WebSocket.OPEN) dg.close();
    });
  });

  // ── Relay ───────────────────────────────────────────────────────────────────
  function relay(event, data) {
    const peerId = socket.data.peerId;
    if (!peerId) { 
      console.warn(`[${event}] no peerId for socket=${socket.id}, dropping`); 
      return; 
    }
    console.log(`[${event}] from=${socket.id} → peer=${peerId}`);
    io.to(peerId).emit(event, data);
  }

  socket.on("offer",    (data) => relay("offer",    data));
  socket.on("answer",   (data) => relay("answer",   data));
  socket.on("ice",      (data) => relay("ice",      data));
  socket.on("subtitle", (data) => relay("subtitle", data));

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId;
    console.log(`[disconnect] socket=${socket.id} room=${roomId}`);
    if (roomId && rooms.has(roomId)) {
      const room = rooms.get(roomId);
      const updated = room.filter((id) => id !== socket.id);
      if (updated.length === 0) {
        rooms.delete(roomId);
        console.log(`[disconnect] room=${roomId} deleted`);
      } else {
        rooms.set(roomId, updated);
        console.log(`[disconnect] room=${roomId} now has ${updated.length} members`);
      }
      io.to(roomId).emit("peer-disconnected");
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`LinguaCall running on port ${PORT}`));
