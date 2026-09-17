'use strict';
/*
 * 协作服务器：零依赖（仅 Node 内置模块）。
 * - HTTP：托管前端静态文件与共享 CRDT 模块
 * - WebSocket（手写 RFC6455）：接收/广播操作
 * - 每个操作分配全局递增 seq => 所有客户端看到一致的修订时间线
 * - 每个操作批次形成一个修订（含快照）；恢复 = 生成一批普通的删除/插入/格式操作，
 *   作为新修订进入操作流，与他人的并发编辑按 CRDT 规则自然合并
 * - 操作日志落盘（data/<doc>.json），重启后重放恢复
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const CRDT = require('../shared/crdt.js');

const PORT = Number(process.env.PORT || 8080);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const MAX_REVS = 1000;
fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------------- WebSocket（RFC6455 最小实现） ----------------
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function wsAccept(key) {
  return crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
}
function wsEncode(str) {
  const p = Buffer.from(str, 'utf8');
  const l = p.length;
  if (l < 126) return Buffer.concat([Buffer.from([0x81, l]), p]);
  if (l < 65536) { const h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(l, 2); return Buffer.concat([h, p]); }
  const h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(l), 2); return Buffer.concat([h, p]);
}
class WSConn {
  constructor(socket) {
    this.sock = socket;
    this.buf = Buffer.alloc(0);
    this.frags = [];
    this.alive = true;
    this.onmessage = null;
    this.onclose = null;
    socket.on('data', (d) => this._feed(d));
    const bye = () => { if (this.alive) { this.alive = false; this.onclose && this.onclose(); } };
    socket.on('close', bye); socket.on('error', bye); socket.on('end', bye);
  }
  send(obj) {
    if (!this.alive) return;
    try { this.sock.write(wsEncode(typeof obj === 'string' ? obj : JSON.stringify(obj))); } catch (e) { /* ignore */ }
  }
  close() { this.alive = false; try { this.sock.end(); } catch (e) { /* ignore */ } }
  _feed(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      const maskOff = off;
      if (masked) off += 4;
      if (this.buf.length < off + len) return;
      let payload = this.buf.slice(off, off + len);
      if (masked) {
        const mask = this.buf.slice(maskOff, maskOff + 4);
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      this.buf = this.buf.slice(off + len);
      if (op === 8) { this.close(); return; }
      if (op === 9) { try { this.sock.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); } catch (e) { /* ignore */ } continue; }
      if (op === 0 || op === 1 || op === 2) {
        this.frags.push(payload);
        if (fin) {
          const full = Buffer.concat(this.frags);
          this.frags = [];
          if (this.onmessage) { try { this.onmessage(full.toString('utf8')); } catch (e) { console.error('[ws] handler error:', e); } }
        }
      }
    }
  }
}

// ---------------- 文档存储 ----------------
function clip(s, n) { return typeof s === 'string' ? s.slice(0, n) : s; }
function sanitizeOp(o) {
  if (!o || typeof o !== 'object') return null;
  const by = clip(String(o.by || '?'), 64);
  if (o.t === 'ins' && typeof o.id === 'string' && typeof o.ch === 'string' && o.ch.length > 0 && o.ch.length <= 8 && typeof o.after === 'string') {
    return { t: 'ins', id: clip(o.id, 160), after: clip(o.after, 160), ch: o.ch, by, opId: clip(o.opId, 200) };
  }
  if (o.t === 'del' && typeof o.id === 'string') return { t: 'del', id: clip(o.id, 160), by, opId: clip(o.opId, 200) };
  if (o.t === 'mark' && typeof o.id === 'string' && o.start && o.end && o.attrs && typeof o.attrs === 'object') {
    const attrs = {};
    for (const k of ['bold', 'italic', 'underline']) if (k in o.attrs) attrs[k] = !!o.attrs[k];
    return { t: 'mark', id: clip(o.id, 160), start: o.start, end: o.end, attrs, ts: +o.ts || Date.now(), by, deleted: !!o.deleted, opId: clip(o.opId, 200) };
  }
  if (o.t === 'com' && typeof o.id === 'string' && o.start && o.end) {
    return { t: 'com', id: clip(o.id, 160), start: o.start, end: o.end, text: clip(String(o.text || ''), 4000), quote: clip(String(o.quote || ''), 4000), ts: +o.ts || Date.now(), by, resolved: !!o.resolved, opId: clip(o.opId, 200) };
  }
  return null;
}

class Store {
  constructor(name) {
    this.name = name;
    this.doc = new CRDT.Doc('server');
    this.seq = 0;
    this.ops = [];          // [{ seq, op }]
    this.revs = [];         // [{ n, seq, ts, by, kind, summary, target?, snapshot }]
    this.seenOpIds = new Set();
    this.dirty = false;
    this.file = path.join(DATA_DIR, encodeURIComponent(name) + '.json');
    this._load();
  }
  _load() {
    try {
      const j = JSON.parse(fs.readFileSync(this.file, 'utf8'));
      this.seq = j.seq || 0;
      this.ops = j.ops || [];
      this.revs = j.revs || [];
      for (const { op } of this.ops) { this.doc.apply(op); if (op.opId) this.seenOpIds.add(op.opId); }
      console.log(`[store] "${this.name}" 载入 ${this.ops.length} 个操作，${this.revs.length} 个修订`);
    } catch (e) { /* 新文档 */ }
  }
  save() {
    if (!this.dirty) return;
    this.dirty = false;
    const tmp = this.file + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ seq: this.seq, ops: this.ops, revs: this.revs }));
    fs.renameSync(tmp, this.file);
  }
  _snapshot() {
    return { text: this.doc.text(), marks: this.doc.activeMarks().map(m => ({ s: m.s, e: m.e, attrs: m.attrs })) };
  }
  _summarize(ops) {
    let ins = 0, del = 0, mark = 0, com = 0;
    for (const o of ops) {
      if (o.t === 'ins') ins++; else if (o.t === 'del') del++;
      else if (o.t === 'mark') mark++; else if (o.t === 'com') com++;
    }
    const parts = [];
    if (ins) parts.push('+' + ins);
    if (del) parts.push('−' + del);
    if (mark) parts.push(mark + ' 处格式');
    if (com) parts.push(com + ' 条评论');
    return parts.join('  ') || '（无文本变化）';
  }
  applyBatch(rawOps, by, kind, extra) {
    const ops = [];
    for (const raw of rawOps) {
      const op = sanitizeOp(raw);
      if (!op) continue;
      if (op.opId && this.seenOpIds.has(op.opId)) continue; // 重连补发的重复操作
      if (op.opId) this.seenOpIds.add(op.opId);
      ops.push(op);
    }
    if (!ops.length) return null;
    const applied = [];
    for (const op of ops) {
      this.seq++;
      this.ops.push({ seq: this.seq, op });
      this.doc.apply(op);
      applied.push({ seq: this.seq, op });
    }
    const rev = Object.assign({
      n: this.revs.length ? this.revs[this.revs.length - 1].n + 1 : 1,
      seq: this.seq, ts: Date.now(), by: clip(String(by || '?'), 64),
      kind: kind || 'edit', summary: this._summarize(ops), snapshot: this._snapshot(),
    }, extra || {});
    this.revs.push(rev);
    if (this.revs.length > MAX_REVS) this.revs.splice(0, this.revs.length - MAX_REVS);
    this.dirty = true;
    return { applied, rev };
  }
  // 恢复历史版本：生成"删除当前可见字符 + 插入快照文本 + 重建格式"的普通操作批次。
  // 这些操作与其他参与者的并发编辑按 CRDT 规则合并——恢复本身也是一个新修订。
  restore(targetN, by) {
    const rev = this.revs.find(r => r.n === targetN);
    if (!rev || !rev.snapshot) return null;
    const snap = rev.snapshot;
    const ops = [];
    for (const id of this.doc.visibleIds()) ops.push({ t: 'del', id, by });
    const actor = 'srv-' + clip(String(by), 32) + '-r' + targetN + '-' + Date.now();
    let c = 0, after = '';
    const newIds = [];
    for (const ch of snap.text) {
      const id = actor + ':' + (++c);
      ops.push({ t: 'ins', id, after, ch, by });
      after = id;
      newIds.push(id);
    }
    const anchor = (pos, edge) => {
      if (!newIds.length) return { id: null, edge };
      if (edge === 's') return pos <= 0 ? { id: null, edge: 's' } : { id: newIds[Math.min(pos, newIds.length - 1)], edge: 's' };
      return pos >= newIds.length ? { id: null, edge: 'e' } : { id: newIds[Math.max(pos - 1, 0)], edge: 'e' };
    };
    for (const m of snap.marks || []) {
      if (m.e <= m.s) continue;
      ops.push({ t: 'mark', id: actor + ':m:' + (++c), start: anchor(m.s, 's'), end: anchor(m.e, 'e'), attrs: m.attrs, ts: Date.now(), by, deleted: false });
    }
    return this.applyBatch(ops, by, 'restore', { target: targetN });
  }
  publicRevs() {
    return this.revs.map(r => ({ n: r.n, seq: r.seq, ts: r.ts, by: r.by, kind: r.kind, summary: r.summary, target: r.target }));
  }
}

// ---------------- HTTP 静态服务 ----------------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8' };
function serveFile(res, file) {
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
}
const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/') p = '/index.html';
  if (p === '/shared/crdt.js') return serveFile(res, path.join(__dirname, '..', 'shared', 'crdt.js'));
  if (p === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  const file = path.normalize(path.join(PUBLIC_DIR, p));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  serveFile(res, file);
});

// ---------------- 房间与广播 ----------------
const stores = new Map();
const rooms = new Map(); // docName -> Set<WSConn>
function getStore(name) {
  let s = stores.get(name);
  if (!s) { s = new Store(name); stores.set(name, s); }
  return s;
}
function broadcast(room, msg) {
  const set = rooms.get(room);
  if (!set) return;
  for (const c of set) c.send(msg);
}
function pubRev(r) { return { n: r.n, seq: r.seq, ts: r.ts, by: r.by, kind: r.kind, summary: r.summary, target: r.target }; }

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + wsAccept(key) + '\r\n\r\n'
  );
  socket.setNoDelay(true);
  const conn = new WSConn(socket);
  let room = null, clientId = '?';

  conn.onmessage = (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (msg.t === 'hello') {
      clientId = String(msg.clientId || '?').slice(0, 64);
      const docName = String(msg.doc || 'default').slice(0, 64) || 'default';
      const store = getStore(docName);
      room = docName;
      if (!rooms.has(room)) rooms.set(room, new Set());
      rooms.get(room).add(conn);
      const lastSeq = +msg.lastSeq || 0;
      conn.send({ t: 'welcome', seq: store.seq, ops: store.ops.filter(o => o.seq > lastSeq), revs: store.publicRevs(), you: clientId });
      broadcast(room, { t: 'presence', count: rooms.get(room).size });
    } else if (msg.t === 'ops' && room) {
      const store = getStore(room);
      const r = store.applyBatch(Array.isArray(msg.ops) ? msg.ops.slice(0, 5000) : [], clientId, 'edit');
      if (r) broadcast(room, { t: 'ops', applied: r.applied, rev: pubRev(r.rev) });
    } else if (msg.t === 'restore' && room) {
      const store = getStore(room);
      const r = store.restore(+msg.rev, clientId);
      if (r) broadcast(room, { t: 'ops', applied: r.applied, rev: pubRev(r.rev) });
      else conn.send({ t: 'error', message: '找不到该修订版本' });
    }
  };
  conn.onclose = () => {
    if (room && rooms.has(room)) {
      rooms.get(room).delete(conn);
      broadcast(room, { t: 'presence', count: rooms.get(room).size });
    }
  };
});

setInterval(() => { for (const s of stores.values()) s.save(); }, 2000).unref();
function shutdown() { for (const s of stores.values()) { s.dirty = true; s.save(); } process.exit(0); }
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(PORT, () => console.log(`[server] 协作文档服务已启动: http://0.0.0.0:${PORT}`));
