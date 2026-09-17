'use strict';
/*
 * 集成测试：启动真实服务器，用模拟客户端验证：
 *  1. 在线双向同步
 *  2. 断网编辑 + 重连合并（插入/删除不丢失、不覆盖他人已确认内容）
 *  3. 评论锚定文字被删除 -> 悬空；重新挂接 -> 恢复锚定
 *  4. 格式标记并发合并
 *  5. 恢复历史版本：恢复作为新修订参与合并，他人并发内容不丢失
 *  6. 服务器重启后数据仍在
 */
const { spawn } = require('child_process');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const assert = require('assert');
const CRDT = require('../shared/crdt.js');

const PORT = 18099;
const DOC = 'test-' + Date.now();
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'collab-test-'));

// ---------- 最小 WebSocket 客户端 ----------
function wsConnect() {
  return new Promise((resolve, reject) => {
    const key = crypto.randomBytes(16).toString('base64');
    const req = http.request({
      port: PORT, path: '/ws',
      headers: { Connection: 'Upgrade', Upgrade: 'websocket', 'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': 13 },
    });
    req.on('upgrade', (res, socket) => resolve(new WSClient(socket)));
    req.on('error', reject);
    req.end();
  });
}
class WSClient {
  constructor(sock) {
    this.sock = sock;
    this.buf = Buffer.alloc(0);
    this.handlers = [];
    sock.on('data', (d) => this._feed(d));
  }
  onMessage(fn) { this.handlers.push(fn); }
  send(obj) {
    const p = Buffer.from(JSON.stringify(obj));
    const mask = crypto.randomBytes(4);
    const l = p.length;
    let h;
    if (l < 126) h = Buffer.from([0x81, 0x80 | l]);
    else if (l < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 0x80 | 126; h.writeUInt16BE(l, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 0x80 | 127; h.writeBigUInt64BE(BigInt(l), 2); }
    const masked = Buffer.from(p);
    for (let i = 0; i < masked.length; i++) masked[i] ^= mask[i & 3];
    this.sock.write(Buffer.concat([h, mask, masked]));
  }
  _feed(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (this.buf.length < off + len) return;
      const payload = this.buf.slice(off, off + len);
      this.buf = this.buf.slice(off + len);
      if ((b0 & 0x0f) === 1) {
        const msg = JSON.parse(payload.toString());
        for (const fn of this.handlers) fn(msg);
      }
    }
  }
  close() { try { this.sock.destroy(); } catch (e) { /* ignore */ } }
}

// ---------- 模拟客户端（复刻浏览器端同步逻辑） ----------
class SimClient {
  constructor(id) {
    this.id = id;
    this.doc = new CRDT.Doc(id);
    this.lastSeq = 0;
    this.opSeq = 0;
    this.pending = [];
    this.revs = [];
    this.ws = null;
  }
  async connect() {
    this.ws = await wsConnect();
    this.ws.onMessage((m) => this._handle(m));
    this.ws.send({ t: 'hello', clientId: this.id, doc: DOC, lastSeq: this.lastSeq });
  }
  disconnect() { if (this.ws) this.ws.close(); this.ws = null; }
  _handle(m) {
    if (m.t === 'welcome') {
      const seen = new Set();
      for (const { seq, op } of m.ops) {
        this.doc.apply(op);
        this.lastSeq = Math.max(this.lastSeq, seq);
        if (op.opId) seen.add(op.opId);
      }
      this.pending = this.pending.filter(p => !seen.has(p.opId));
      this.revs = m.revs || [];
      if (this.pending.length) this.ws.send({ t: 'ops', ops: this.pending });
    } else if (m.t === 'ops') {
      for (const { seq, op } of m.applied) {
        this.doc.apply(op);
        this.lastSeq = Math.max(this.lastSeq, seq);
        if (op.opId) {
          const i = this.pending.findIndex(p => p.opId === op.opId);
          if (i >= 0) this.pending.splice(i, 1);
        }
      }
      if (m.rev && !this.revs.some(r => r.n === m.rev.n)) this.revs.push(m.rev);
    }
  }
  op(o) {
    if (!o) return;
    o.opId = this.id + '#' + (++this.opSeq);
    this.doc.apply(o);
    this.pending.push(o);
    if (this.ws) this.ws.send({ t: 'ops', ops: [o] });
  }
  insertText(pos, str) {
    const ids = this.doc.visibleIds();
    let after = pos > 0 ? ids[pos - 1] : null;
    for (const ch of str) { const o = this.doc.insert(after, ch); after = o.id; this.op(o); }
  }
  deleteRange(pos, len) {
    const ids = this.doc.visibleIds();
    for (let i = 0; i < len; i++) this.op(this.doc.remove(ids[pos + i]));
  }
  anchors(s, e) {
    const ids = this.doc.visibleIds();
    return [
      s <= 0 ? { id: null, edge: 's' } : { id: ids[s], edge: 's' },
      e >= ids.length ? { id: null, edge: 'e' } : { id: ids[e - 1], edge: 'e' },
    ];
  }
  addComment(s, e, text) {
    const [st, en] = this.anchors(s, e);
    const quote = this.doc.text().slice(s, e);
    const op = this.doc.comment(st, en, text, quote);
    this.op(op);
    return op.id;
  }
  reattach(cid, s, e) {
    const [st, en] = this.anchors(s, e);
    this.op(this.doc.updateComment(cid, { start: st, end: en, quote: this.doc.text().slice(s, e) }));
  }
  mark(s, e, attrs) {
    const [st, en] = this.anchors(s, e);
    this.op(this.doc.mark(st, en, attrs));
  }
  restore(rev) { if (this.ws) this.ws.send({ t: 'restore', rev }); }
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function waitFor(fn, label, timeout = 8000) {
  return new Promise((resolve, reject) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      let ok = false;
      try { ok = fn(); } catch (e) { /* retry */ }
      if (ok) { clearInterval(iv); resolve(); }
      else if (Date.now() - t0 > timeout) { clearInterval(iv); reject(new Error('超时: ' + label)); }
    }, 25);
  });
}

let server = null;
function startServer() {
  return new Promise((resolve, reject) => {
    server = spawn(process.execPath, [path.join(__dirname, '..', 'server', 'server.js')], {
      env: Object.assign({}, process.env, { PORT: String(PORT), DATA_DIR }),
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    server.stdout.on('data', (d) => { if (String(d).includes('http://')) resolve(); });
    server.on('exit', (c) => reject(new Error('服务器退出 code=' + c)));
    setTimeout(() => reject(new Error('服务器启动超时')), 8000);
  });
}
function stopServer() { return new Promise((res) => { if (!server) return res(); server.on('exit', res); server.kill('SIGTERM'); setTimeout(res, 2000); }); }

let passed = 0;
function ok(name) { passed++; console.log('  ✓ ' + name); }

(async () => {
  await startServer();
  console.log('服务器已启动 (port ' + PORT + ', doc=' + DOC + ')\n');

  // ---- 1. 在线双向同步 ----
  const A = new SimClient('alice');
  const B = new SimClient('bob');
  await A.connect(); await B.connect();
  A.insertText(0, 'hello');
  await waitFor(() => B.doc.text() === 'hello', 'B 收到 A 的插入');
  B.insertText(5, ' world');
  await waitFor(() => A.doc.text() === 'hello world', 'A 收到 B 的插入');
  assert.strictEqual(A.doc.text(), B.doc.text());
  ok('在线双向同步');

  // ---- 2. 断网编辑 + 重连合并：较晚同步不覆盖他人已确认内容 ----
  B.disconnect();
  await sleep(200);
  A.insertText(11, '!');            // A 在线追加
  B.insertText(0, '>>');            // B 离线插入
  B.deleteRange(6, 1);              // B 离线删除 'o'（B 视角 ">>hello world" 中索引 6）
  assert.strictEqual(A.doc.text(), 'hello world!'); // A 不受离线者影响
  await B.connect();                // B 重连，补发离线操作
  await waitFor(() => A.doc.text() === B.doc.text() && A.doc.text().includes('world!'), '重连后双方收敛');
  assert.strictEqual(A.doc.text(), '>>hell world!'); // 'o' 被删、A 的 " world!" 完整保留
  assert.ok(A.doc.text().includes('world!'), 'A 已确认的内容未被 B 的迟到同步覆盖');
  ok('断网编辑重连合并，已确认内容不被覆盖');

  // ---- 3. 评论：锚定文字被删除 -> 悬空 -> 重新挂接 ----
  const text0 = A.doc.text(); // '>>hell world!'
  const wStart = text0.indexOf('world');
  const cid = A.addComment(wStart, wStart + 5, '这里用词再斟酌一下');
  await waitFor(() => B.doc.comments.has(cid), '评论同步到 B');
  assert.strictEqual(B.doc.resolveComment(B.doc.comments.get(cid)).status, 'anchored');
  B.deleteRange(wStart, 5); // B 删除被评论的 "world"
  await waitFor(() => {
    const c = A.doc.comments.get(cid);
    return c && A.doc.resolveComment(c).status === 'orphan-deleted';
  }, '评论进入悬空状态');
  const cA = A.doc.comments.get(cid);
  assert.strictEqual(A.doc.resolveComment(cA).cur, '');
  assert.strictEqual(cA.quote, 'world'); // 原文保留，可解释
  ok('锚定文字被删除后评论进入可解释的悬空状态');
  const hStart = A.doc.text().indexOf('hell');
  A.reattach(cid, hStart, hStart + 4); // 重新挂接到 "hell"
  await waitFor(() => {
    const c = B.doc.comments.get(cid);
    return c && B.doc.resolveComment(c).status === 'anchored' && c.quote === 'hell';
  }, '重新挂接同步到 B');
  ok('悬空评论可重新挂接并同步');

  // ---- 4. 并发格式合并 ----
  const t1 = A.doc.text();
  A.mark(2, 6, { bold: true });
  B.mark(4, 8, { italic: true });
  await waitFor(() => A.doc.activeMarks().length === 2 && B.doc.activeMarks().length === 2, '格式标记合并');
  const mA = A.doc.activeMarks().map(m => [m.s, m.e]).sort().join(';');
  const mB = B.doc.activeMarks().map(m => [m.s, m.e]).sort().join(';');
  assert.strictEqual(mA, mB);
  assert.strictEqual(A.doc.text(), t1); // 格式不改变文本
  ok('并发格式变化合并且文本不变');

  // ---- 5. 恢复历史版本：恢复作为新修订参与合并 ----
  const snapshotText = A.doc.text();
  const revBefore = Math.max(...A.revs.map(r => r.n));
  A.insertText(0, 'XXX'); // 制造一个之后要被"恢复掉"的编辑
  await waitFor(() => B.doc.text() === 'XXX' + snapshotText, 'B 收到 XXX');
  B.restore(revBefore);   // B 恢复到加 XXX 之前的修订
  await waitFor(() => A.doc.text() === snapshotText && B.doc.text() === snapshotText, '恢复后双方文本一致');
  const restoreRev = A.revs[A.revs.length - 1];
  assert.strictEqual(restoreRev.kind, 'restore');
  assert.strictEqual(restoreRev.target, revBefore);
  ok('恢复历史版本生效，且恢复本身是新修订 #' + restoreRev.n);
  // 恢复之后他人的新编辑继续正常合并
  A.insertText(snapshotText.length, ' [A追加]');
  B.insertText(0, '[B插入] ');
  await waitFor(() =>
    A.doc.text() === B.doc.text() &&
    A.doc.text().includes('[A追加]') && A.doc.text().includes('[B插入]'),
    '恢复后的并发编辑合并');
  ok('恢复后的并发编辑正常合并，互不覆盖');

  // ---- 6. 服务器重启后数据保留 ----
  const finalText = A.doc.text();
  await sleep(2500); // 等待落盘
  await stopServer();
  await startServer();
  const C = new SimClient('carol');
  await C.connect();
  await waitFor(() => C.doc.text() === finalText, '重启后新客户端拿到完整文档');
  assert.ok(C.revs.length >= restoreRev.n, '修订时间线在重启后保留');
  ok('服务器重启后文档与修订时间线保留');

  A.disconnect(); B.disconnect(); C.disconnect();
  await stopServer();
  console.log('\n全部 ' + passed + ' 项测试通过 ✅');
  process.exit(0);
})().catch(async (e) => {
  console.error('\n测试失败 ❌', e);
  await stopServer();
  process.exit(1);
});
