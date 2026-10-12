const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const page = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
  res.end(page);
});

const rooms = new Map();
let nid = 0;
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8192 });

wss.on('connection', (ws) => {
  const c = { id: 'p' + (++nid).toString(36) + Math.random().toString(36).slice(2, 5), ws, room: null, pres: {}, bud: 80, t: Date.now() };
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.send(JSON.stringify({ t: 'hi', id: c.id }));

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;
    const now = Date.now();                       // token bucket: ~60 msgs/s per client
    c.bud = Math.min(80, c.bud + (now - c.t) * 0.06); c.t = now;
    if (c.bud < 1) return; c.bud--;

    if (m.t === 'join' && typeof m.room === 'string' && !c.room) {
      const n = m.room.slice(0, 24);
      let r = rooms.get(n);
      if (!r) { r = { n, m: new Map(), dirty: false }; rooms.set(n, r); }
      if (r.m.size >= 16) return ws.send(JSON.stringify({ t: 'full' }));
      c.room = r; r.m.set(c.id, c); r.dirty = true;
    } else if (!c.room) {
      return;
    } else if (m.t === 'pres' && m.p && typeof m.p === 'object') {
      c.pres = m.p; c.room.dirty = true;
    } else if (m.t === 'emit' && typeof m.topic === 'string' && /^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic)) {
      if (m.topic === 'hit' && !(m.data && m.data.d <= 100)) return;   // basic damage clamp
      const s = JSON.stringify({ t: 'ev', topic: m.topic, data: m.data, peer: c.id });
      for (const o of c.room.m.values()) if (o.ws.readyState === 1) o.ws.send(s);
    }
  });

  ws.on('close', () => {
    if (!c.room) return;
    c.room.m.delete(c.id); c.room.dirty = true;
    if (!c.room.m.size) rooms.delete(c.room.n);
  });
});

// broadcast presence snapshots ~15x/s per room (only when something changed)
setInterval(() => {
  for (const r of rooms.values()) {
    if (!r.dirty) continue; r.dirty = false;
    const s = JSON.stringify({ t: 'peers', list: [...r.m.values()].map((c) => ({ peer: c.id, presence: c.pres })) });
    for (const c of r.m.values()) if (c.ws.readyState === 1) c.ws.send(s);
  }
}, 66);

// drop dead connections
setInterval(() => wss.clients.forEach((w) => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); }), 30000);

server.listen(PORT, () => console.log('Airsoft Arena on :' + PORT));
