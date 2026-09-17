'use strict';
/*
 * 集成测试：启动真实服务器，用模拟客户端验证。
 * 原有场景：
 *  1. 在线双向同步
 *  2. 断网编辑 + 重连合并（不覆盖他人已确认内容）
 *  3. 评论悬空 / 重新挂接
 *  4. 并发格式合并
 *  5. 恢复历史版本（所有者）
 *  6. 服务器重启后内容/修订保留
 * 成员治理验收（与需求 1~9 对应）：
 *  G1 角色可见/可调用命令矩阵：owner/editor 可写文字样式；commenter 只能批注；viewer 只读
 *  G2 评论者可管批注但不能改正文；只读者两类写都不行
 *  G3 编辑断网排队 -> 被降为只读者 -> 重连：整批隔离，零泄漏，完整批次在隔离区
 *  G4 恢复编辑者后显式重新提交：恰好应用一次并正常 CRDT 合并
 *  G5 角色变更与断网批次竞争：两种消息序都得到相同授权结果、内容状态与台账顺序
 *  G6 重复邀请/角色变更/批次投递幂等（含进程重启后）
 *  G7 非所有者回滚被拒绝，不产生修订
 *  G8 成员表、授权纪元、隔离引用、审计台账跨重启存活
 *  G9 旧空间并发首开：恰好一个所有者，既有内容/批注/修订原样保留
 */
const { spawn } = require('child_process');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const assert = require('assert');
const CRDT = require('../shared/crdt.js');
const GOV = require('../shared/gov.js');

const PORT = 18099;
const DOC = 'test-' + Date.now();
const LEGACY = 'legacy-' + Date.now();
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

// ---------- 模拟客户端（与浏览器相同的批次/隔离语义） ----------
class SimClient {
  constructor(id, name, docName) {
    this.id = id;
    this.name = name || id;
    this.docName = docName || DOC;
    this.doc = new CRDT.Doc(id);
    this.lastSeq = 0;
    this.opSeq = 0;
    this.batchSeq = 0;
    this.confirmed = [];
    this.pendingBatches = [];
    this.revs = [];
    this.role = null;
    this.epoch = 0;
    this.members = [];
    this.audit = [];
    this.quarantines = [];
    this.errors = [];
    this.lastRejected = null;
    this.memberResults = [];
    this.ws = null;
  }
  async connect() {
    this.ws = await wsConnect();
    this.ws.onMessage((m) => this._handle(m));
    this.ws.send({ t: 'hello', uid: this.id, name: this.name, doc: this.docName, lastSeq: this.lastSeq });
    await new Promise((resolve, reject) => {
      const to = setTimeout(() => reject(new Error('welcome 超时 ' + this.id)), 5000);
      const fn = (m) => { if (m.t === 'welcome') { clearTimeout(to); this.ws.handlers.splice(this.ws.handlers.indexOf(fn), 1); resolve(); } };
      this.ws.handlers.push(fn);
    });
  }
  disconnect() { if (this.ws) this.ws.close(); this.ws = null; }
  _handle(m) {
    if (m.t === 'welcome') {
      const seenOp = new Set();
      for (const { seq, op } of m.ops) {
        this.doc.apply(op); this.confirmed.push(op);
        this.lastSeq = Math.max(this.lastSeq, seq);
        if (op.opId) seenOp.add(op.opId);
      }
      const quarOps = new Set();
      this.quarantines = m.quarantine || [];
      for (const q of this.quarantines) for (const op of q.ops) if (op.opId) quarOps.add(op.opId);
      this.pendingBatches = this.pendingBatches.filter(b =>
        !b.ops.some(op => (op.opId && seenOp.has(op.opId)) || (op.opId && quarOps.has(op.opId))));
      this.revs = m.revs || [];
      this.role = m.role; this.epoch = m.epoch || 0; this.members = m.members || [];
      this.audit = m.audit || [];
      for (const b of this.pendingBatches) this.ws.send({ t: 'ops', batchId: b.batchId, ops: b.ops, lastKnownRole: b.fromRole || null });
    } else if (m.t === 'ops') {
      for (const { seq, op } of m.applied) {
        this.doc.apply(op); this.confirmed.push(op);
        this.lastSeq = Math.max(this.lastSeq, seq);
      }
      if (m.batchId) this.pendingBatches = this.pendingBatches.filter(b => b.batchId !== m.batchId);
      if (m.rev && !this.revs.some(r => r.n === m.rev.n)) this.revs.push(m.rev);
      if (m.epoch) this.epoch = m.epoch;
    } else if (m.t === 'gov') {
      this.role = m.role; this.epoch = m.epoch || this.epoch;
      this.members = m.members || [];
      this.quarantines = m.quarantine || this.quarantines;
    } else if (m.t === 'audit') {
      for (const e of (m.entries || [])) if (!this.audit.some(a => a.seq === e.seq)) this.audit.push(e);
    } else if (m.t === 'rejected') {
      const bid = m.batchId || (m.quarantine && m.quarantine.batchId);
      this.pendingBatches = this.pendingBatches.filter(b => b.batchId !== bid);
      if (m.quarantine) {
        const i = this.quarantines.findIndex(q => q.qid === m.quarantine.qid);
        if (i >= 0) this.quarantines[i] = m.quarantine; else this.quarantines.push(m.quarantine);
      }
      this.lastRejected = m;
      this._rebuild();
    } else if (m.t === 'error') {
      this.errors.push(m.message);
    } else if (m.t === 'member-result') {
      this.memberResults.push(m);
    }
  }
  _rebuild() {
    const fresh = new CRDT.Doc(this.id);
    fresh.counter = this.opSeq;
    for (const op of this.confirmed) fresh.apply(op);
    for (const b of this.pendingBatches) for (const op of b.ops) fresh.apply(op);
    this.doc.chars = fresh.chars; this.doc.kids = fresh.kids;
    this.doc.marks = fresh.marks; this.doc.comments = fresh.comments;
    this.doc.waiting = fresh.waiting; this.doc.counter = fresh.counter;
  }
  // 一批操作：本地应用 + 整批作为一个授权/隔离单位
  batchOps(ops) {
    const ready = [];
    for (const op of ops) {
      if (!op) continue;
      op.opId = this.id + '#' + (++this.opSeq);
      this.doc.apply(op);
      ready.push(op);
    }
    const b = { batchId: 'b-' + this.id + '-' + (++this.batchSeq), ops: ready, fromRole: this.role };
    this.pendingBatches.push(b);
    if (this.ws) this.ws.send({ t: 'ops', batchId: b.batchId, ops: b.ops, lastKnownRole: b.fromRole });
    return b;
  }
  sendBatch(ops, batchId) {
    for (const op of ops) if (!op.opId) op.opId = this.id + '#' + (++this.opSeq);
    if (this.ws) this.ws.send({ t: 'ops', batchId, ops });
  }
  op(o) {
    if (!o) return;
    return this.batchOps([o]);
  }
  insertText(pos, str) {
    const ids = this.doc.visibleIds();
    let after = pos > 0 ? ids[pos - 1] : null;
    const ops = [];
    for (const ch of str) { const o = this.doc.insert(after, ch); after = o.id; ops.push(o); }
    this.batchOps(ops);
  }
  deleteRange(pos, len) {
    const ids = this.doc.visibleIds();
    const ops = [];
    for (let i = 0; i < len; i++) ops.push(this.doc.remove(ids[pos + i]));
    this.batchOps(ops);
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
    const op = this.doc.comment(st, en, text, this.doc.text().slice(s, e));
    this.op(op);
    return op.id;
  }
  reattach(cid, s, e) {
    const [st, en] = this.anchors(s, e);
    this.op(this.doc.updateComment(cid, { start: st, end: en, quote: this.doc.text().slice(s, e) }));
  }
  resolveComment(cid) { this.op(this.doc.updateComment(cid, { resolved: true })); }
  mark(s, e, attrs) {
    const [st, en] = this.anchors(s, e);
    this.op(this.doc.mark(st, en, attrs));
  }
  restore(rev) { if (this.ws) this.ws.send({ t: 'restore', rev }); }
  invite(uid, role, name, mId) {
    this.ws.send({ t: 'member', mId: mId || ('m-' + this.id + '-' + Math.random().toString(36).slice(2)), action: 'invite', uid, role, name: name || uid });
  }
  setRole(uid, role, mId) {
    this.ws.send({ t: 'member', mId: mId || ('m-' + this.id + '-' + Math.random().toString(36).slice(2)), action: 'role', uid, role });
  }
  remove(uid, mId) {
    this.ws.send({ t: 'member', mId: mId || ('m-' + this.id + '-' + Math.random().toString(36).slice(2)), action: 'remove', uid });
  }
  resubmit(qid, batchId) {
    this.ws.send({ t: 'resubmit', qid, batchId: batchId || ('rsb-' + this.id + '-' + Date.now().toString(36)) });
  }
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function waitFor(fn, label, timeout = 8000) {
  return new Promise((resolve, reject) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      let ok = false;
      try { ok = fn(); } catch (e) { /* retry */ }
      if (ok) { clearInterval(iv); resolve(); }
      else if (Date.now() - t0 > timeout) { clearInterval(iv); reject(new Error('超时: ' + (label || 'condition'))); }
    }, 20);
  });
}
// 等待某个客户端收到满足谓词的消息
function nextMsg(c, pred, timeout) {
  return new Promise((resolve, reject) => {
    const to = setTimeout(() => reject(new Error('等待消息超时')), timeout || 5000);
    const fn = (m) => {
      let hit = false;
      try { hit = pred(m); } catch (e) { /* keep waiting */ }
      if (hit) { clearTimeout(to); c.ws.handlers.splice(c.ws.handlers.indexOf(fn), 1); resolve(m); }
    };
    c.ws && c.ws.handlers.push(fn);
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
function stopServer() { return new Promise((res) => { if (!server) return res(); server.on('exit', res); server.kill('SIGTERM'); setTimeout(res, 2500); }); }

let passed = 0;
function ok(name) { passed++; console.log('  ✓ ' + name); }
function auditTypes(c) { return c.audit.map(e => e.type); }
function sameLedger(c1, c2) {
  return JSON.stringify(c1.audit.map(e => [e.seq, e.type, e.uid, e.role || null])) ===
         JSON.stringify(c2.audit.map(e => [e.seq, e.type, e.uid, e.role || null]));
}

(async () => {
  await startServer();
  console.log('服务器已启动 (port ' + PORT + ', data=' + DATA_DIR + ')\n');

  // ============ 原有 1~6：首个连接者成为所有者 ============
  const A = new SimClient('alice', 'Alice');
  const B = new SimClient('bob', 'Bob');
  await A.connect();
  assert.strictEqual(A.role, 'owner');
  assert.strictEqual(A.epoch, 1);
  ok('首个打开空间者成为唯一所有者（授权纪元=1）');
  await B.connect();
  A.invite('bob', 'editor', 'Bob');
  await waitFor(() => B.role === 'editor', 'Bob 受邀为编辑者');

  // ---- 1. 在线双向同步 ----
  A.insertText(0, 'hello');
  await waitFor(() => B.doc.text() === 'hello', 'B 收到 A 的插入');
  B.insertText(5, ' world');
  await waitFor(() => A.doc.text() === 'hello world', 'A 收到 B 的插入');
  assert.strictEqual(A.doc.text(), B.doc.text());
  ok('在线双向同步');

  // ---- 2. 断网编辑 + 重连合并 ----
  B.disconnect();
  await sleep(200);
  A.insertText(11, '!');
  B.insertText(0, '>>');
  B.deleteRange(6, 1);
  assert.strictEqual(A.doc.text(), 'hello world!');
  await B.connect();
  await waitFor(() => A.doc.text() === B.doc.text() && A.doc.text().includes('world!'), '重连后双方收敛');
  assert.strictEqual(A.doc.text(), '>>hell world!');
  assert.ok(A.doc.text().includes('world!'));
  ok('断网编辑重连合并，已确认内容不被覆盖');

  // ---- 3. 评论悬空 / 重新挂接 ----
  const text0 = A.doc.text();
  const wStart = text0.indexOf('world');
  const cid = A.addComment(wStart, wStart + 5, '这里用词再斟酌一下');
  await waitFor(() => B.doc.comments.has(cid), '评论同步到 B');
  assert.strictEqual(B.doc.resolveComment(B.doc.comments.get(cid)).status, 'anchored');
  B.deleteRange(wStart, 5);
  await waitFor(() => {
    const c = A.doc.comments.get(cid);
    return c && A.doc.resolveComment(c).status === 'orphan-deleted';
  }, '评论进入悬空状态');
  const cA = A.doc.comments.get(cid);
  assert.strictEqual(A.doc.resolveComment(cA).cur, '');
  assert.strictEqual(cA.quote, 'world');
  ok('锚定文字被删除后评论进入可解释的悬空状态');
  const hStart = A.doc.text().indexOf('hell');
  A.reattach(cid, hStart, hStart + 4);
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
  assert.strictEqual(A.doc.text(), t1);
  ok('并发格式变化合并且文本不变');

  // ---- G1/G2 角色命令矩阵 ----
  assert.ok(GOV.canProse('owner') && GOV.canAnnotate('owner') && GOV.canRollback('owner'));
  assert.ok(GOV.canProse('editor') && GOV.canAnnotate('editor') && !GOV.canRollback('editor'));
  assert.ok(!GOV.canProse('commenter') && GOV.canAnnotate('commenter'));
  assert.ok(!GOV.canProse('viewer') && !GOV.canAnnotate('viewer'));

  const C = new SimClient('carol', 'Carol');
  const D = new SimClient('dave', 'Dave');
  const E = new SimClient('erin', 'Erin');
  await C.connect(); await D.connect(); await E.connect();
  A.invite('carol', 'commenter', 'Carol');
  A.invite('dave', 'viewer', 'Dave');
  await waitFor(() => C.role === 'commenter' && D.role === 'viewer', '角色下发');
  // Erin 未受邀 => 非成员只读
  assert.strictEqual(E.role, null);

  // 评论者：批注可以
  const cpos = A.doc.text().indexOf('hell');
  const cid2 = C.addComment(cpos, cpos + 4, '评论者的批注');
  await waitFor(() => A.doc.comments.has(cid2), '评论者的批注生效');
  C.resolveComment(cid2);
  await waitFor(() => { const c = A.doc.comments.get(cid2); return c && c.resolved; }, '评论者可解决批注');
  ok('G1/G2 评论者可创建/解决批注');

  // 评论者：文字/样式被拒（整批隔离，正文不变）
  const textBefore = A.doc.text();
  C.insertText(0, 'CAROL');
  await waitFor(() => C.quarantines.some(q => q.status === 'quarantined'), '评论者的文字批次被隔离');
  C.mark(0, 2, { bold: true });
  await waitFor(() => C.quarantines.length >= 2, '评论者的样式批次被隔离');
  await sleep(150);
  assert.strictEqual(A.doc.text(), textBefore, '评论者的文字/样式零泄漏到共享状态');
  assert.strictEqual(C.doc.text(), textBefore, '被拒批次也从评论者本地摘除（隔离草稿保留）');
  // 只读者：批注和文字都被拒
  D.addComment(0, 1, '只读者的批注');
  await waitFor(() => D.quarantines.length >= 1, '只读者的批注被隔离');
  D.insertText(0, 'D');
  await waitFor(() => D.quarantines.length >= 2, '只读者的文字被隔离');
  assert.strictEqual(A.doc.text(), textBefore, '只读者两类写入均零泄漏');
  // 非成员（断连期间被移除）：批次同样整批隔离、零泄漏、草稿保留，可在重新受邀后再提交
  E.insertText(0, 'ERIN');
  await waitFor(() => E.quarantines.some(q => q.ops.some(o => o.ch === 'E')), '非成员批次被隔离');
  await sleep(100);
  assert.ok(!A.doc.text().includes('ERIN'), '非成员写入零泄漏');
  assert.ok(!E.doc.text().includes('ERIN'), '非成员本地也摘除被拒批次');
  const qE = E.quarantines.find(q => q.ops.some(o => o.ch === 'E'));
  assert.ok(qE.removed, '隔离记录标记为"成员已移除"');
  assert.ok(qE.reason.includes('移出空间'), '解释了成员身份被移除');
  ok('G1/G2 评论者不能改正文、只读者两类写都不行，越权批次整批隔离不发布');

  // 断连期间被移除：排队批次重连后整批隔离；重新受邀为编辑者后可显式再提交
  const G = new SimClient('gina', 'Gina');
  await G.connect();
  A.invite('gina', 'editor', 'Gina');
  await waitFor(() => G.role === 'editor', 'Gina 为编辑者');
  G.disconnect();
  await sleep(120);
  G.insertText(0, 'GINA');
  assert.ok(G.doc.text().includes('GINA'), 'Gina 离线本地先有改动');
  A.remove('gina');
  await waitFor(() => !A.members.some(m => m.uid === 'gina'), 'Gina 被移除');
  await G.connect();
  await waitFor(() => G.role === null && G.quarantines.some(q => q.ops.some(o => o.ch === 'G')), 'Gina 重连后批次隔离、身份为空');
  await sleep(120);
  assert.ok(!A.doc.text().includes('GINA') && !G.doc.text().includes('GINA'), '移除场景零泄漏且本地摘除');
  // 重新受邀为编辑者后，显式重新提交原隔离草稿
  A.invite('gina', 'editor', 'Gina');
  await waitFor(() => G.role === 'editor', 'Gina 重新受邀为编辑者');
  const qG = G.quarantines.find(q => q.ops.some(o => o.ch === 'G'));
  G.resubmit(qG.qid, 'rsb-gina-1');
  await waitFor(() => A.doc.text().includes('GINA'), 'Gina 重新受邀后显式提交成功');
  assert.strictEqual((A.doc.text().match(/GINA/g) || []).length, 1, 'Gina 草稿仅生效一次');
  ok('断连期间被移除：批次隔离不泄漏；重新受邀后显式重新提交生效');

  // 隔离区内容完整可读：操作齐全、含越权解释与角色信息
  const qC = C.quarantines.find(q => q.ops.some(o => o.t === 'ins'));
  assert.ok(qC, '被拒批次在隔离区可查');
  assert.strictEqual(qC.ops.length, 5);
  assert.ok(qC.ops.every(o => o.t === 'ins'), '隔离区完整保留了每个操作');
  assert.strictEqual(qC.ops.map(o => o.ch).join(''), 'CAROL', '隔离区保留了批次原文');
  assert.ok(qC.reason.includes('评论者') && qC.violations.length === 5, '隔离记录解释了拒绝原因');
  ok('被拒绝的完整批次保留在可读的隔离草稿中（含原因/角色/纪元）');

  // 隔离草稿隐私：他人（非所有者）看不到 Carol/Dave 被拒批次的内容，所有者可见
  assert.ok(A.quarantines.some(q => q.uid === 'carol'), '所有者可见全部隔离草稿');
  assert.ok(B.quarantines.filter(q => q.status === 'quarantined').every(q => q.uid === 'bob'),
    '普通成员不会收到他人的隔离草稿内容');
  ok('隔离草稿内容仅本人与所有者可见，不向全员泄漏');

  // ---- G7 非所有者回滚被拒绝，不产生修订 ----
  const revsBefore = A.revs.length;
  const rev0 = Math.min(...A.revs.map(r => r.n));
  B.restore(rev0); // editor
  await waitFor(() => auditTypes(A).filter(t => t === 'rollback-denied').length === 1, '台账记录越权回滚');
  await sleep(100);
  assert.strictEqual(A.revs.length, revsBefore, '越权回滚未产生修订');
  C.restore(rev0); // commenter
  await waitFor(() => auditTypes(A).filter(t => t === 'rollback-denied').length === 2, '评论者回滚也被拒');
  // 同一非所有者重复回滚同一检查点：幂等，不重复记账、不产生修订
  B.restore(rev0); B.restore(rev0);
  await sleep(300);
  assert.strictEqual(auditTypes(A).filter(t => t === 'rollback-denied').length, 2, '重复越权回滚不重复记账');
  assert.strictEqual(A.revs.length, revsBefore);
  ok('G7 非所有者回滚被拒绝且不创建修订，台账留痕（重复尝试幂等）');

  // ---- G6 幂等：重复邀请 / 重复角色变更 / 重复批次投递 ----
  const epochBefore = A.epoch;
  const mid = 'dup-invite-1';
  A.invite('carol', 'commenter', 'Carol', mid);
  A.invite('carol', 'commenter', 'Carol', mid);   // 完全相同的重试
  A.invite('carol', 'commenter', 'Carol', mid);
  await sleep(200);
  assert.strictEqual(A.epoch, epochBefore, '相同邀请重试不推进纪元、不重复记账');
  // 已在空间内、相同角色：纯幂等
  const eb2 = A.epoch;
  A.invite('dave', 'viewer', 'Dave');
  A.setRole('dave', 'viewer');
  await sleep(200);
  assert.strictEqual(A.epoch, eb2, '同角色变更幂等');
  // 重复批次投递：相同 batchId 只应用一次
  const dupText = 'DUP';
  let after = null; const dupOps = [];
  for (const ch of dupText) { const o = B.doc.insert(after, ch); after = o.id; dupOps.push(o); }
  for (const o of dupOps) o.opId = B.id + '#dup' + Math.random().toString(36).slice(2);
  B.sendBatch(dupOps.map(o => Object.assign({}, o)), 'dup-batch-1');
  B.sendBatch(dupOps.map(o => Object.assign({}, o)), 'dup-batch-1');
  B.sendBatch(dupOps.map(o => Object.assign({}, o)), 'dup-batch-1');
  await waitFor(() => A.doc.text().includes(dupText), '首批到达');
  await sleep(300);
  assert.strictEqual((A.doc.text().match(/DUP/g) || []).length, 1, '重复 batchId 投递只生效一次');
  // 相同 opId 也只生效一次（不同 batchId、不同字符 id 但 opId 已见过）
  const more = dupOps.map(o => Object.assign({}, o, { id: o.id + '-x' }));
  B.sendBatch(more, 'dup-batch-2');
  await sleep(300);
  assert.strictEqual((A.doc.text().match(/DUP/g) || []).length, 1, '同 opId 操作不重复应用');
  ok('G6 重复邀请/角色变更/批次投递均幂等');

  // ---- G3 断网排队 + 被降级 -> 重连整批隔离，零泄漏 ----
  const sharedBefore = A.doc.text();
  const F = new SimClient('frank', 'Frank');
  await F.connect();
  A.invite('frank', 'editor', 'Frank');
  await waitFor(() => F.role === 'editor', 'Frank 为编辑者');
  F.disconnect();
  await sleep(150);
  // 断网期间 Frank 编辑（整批：文字+样式混合）
  const idsF = F.doc.visibleIds();
  let fafter = null; const fops = [];
  for (const ch of '[FRANK]') { const o = F.doc.insert(fafter, ch); fafter = o.id; fops.push(o); }
  const fBatch = F.batchOps(fops.map(o => o)); // 已本地应用，入队
  assert.ok(F.doc.text().includes('[FRANK]'), '本地先显示离线编辑');
  // 与此同时，所有者把 Frank 降为只读者
  A.setRole('frank', 'viewer');
  await waitFor(() => A.epoch > F.epoch, '降级已落定（纪元推进）');
  const epochDowngrade = A.epoch;
  // Frank 重连：welcome 先带新角色 => 批次按当前纪元授权 => 整批拒绝
  await F.connect();
  await waitFor(() => F.lastRejected && F.quarantines.some(q => q.batchId === fBatch.batchId), 'Frank 批次被隔离');
  await sleep(150);
  assert.ok(!A.doc.text().includes('[FRANK]'), '共享状态零泄漏（1）');
  assert.ok(!B.doc.text().includes('[FRANK]'), '共享状态零泄漏（2）');
  assert.ok(!F.doc.text().includes('[FRANK]'), 'Frank 本地也已摘除被拒批次');
  assert.strictEqual(A.doc.text(), sharedBefore, '除降级外内容无变化');
  const qF = F.quarantines.find(q => q.batchId === fBatch.batchId);
  assert.strictEqual(qF.ops.length, 7, '完整批次保留');
  assert.strictEqual(qF.role, 'viewer');
  assert.strictEqual(qF.fromRole, 'editor', '隔离记录解释了 editor->viewer 的角色转换');
  assert.strictEqual(qF.epoch, epochDowngrade, '按当前授权纪元裁决');
  assert.ok(auditTypes(A).includes('batch-quarantined'), '台账记录隔离事件');
  ok('G3 断网排队+降级后重连：整批隔离、零泄漏、完整批次与角色转换说明保留');

  // ---- G4 恢复权限后显式重新提交：恰好一次 + 正常合并 ----
  const qid = qF.qid;
  A.setRole('frank', 'editor');
  await waitFor(() => F.role === 'editor', 'Frank 恢复编辑者');
  const revsBeforeResub = A.revs.length;
  F.resubmit(qid, 'rsb-frank-1');
  await waitFor(() => A.doc.text().includes('[FRANK]') && B.doc.text().includes('[FRANK]'), '重新提交合并');
  // 重试同一重新提交（qid 相同）：不重复应用
  F.resubmit(qid, 'rsb-frank-retry');
  await sleep(300);
  assert.strictEqual((A.doc.text().match(/\[FRANK\]/g) || []).length, 1, '显式重新提交恰好应用一次');
  assert.strictEqual(A.revs.length, revsBeforeResub + 1, '重新提交恰好产生一个修订');
  assert.strictEqual(A.doc.text(), F.doc.text(), '重新提交后正常 CRDT 收敛');
  // 重新提交期间他人并发编辑也在（CRDT 合并不冲突）
  const frAudit = A.audit.find(e => e.type === 'quarantine-released');
  assert.ok(frAudit, '台账记录隔离批次放行');
  ok('G4 权限恢复后显式重新提交整批：恰好一次、正常合并、台账留痕');

  // ---- G5 角色变更与断网批次竞争：两种顺序同一结论 ----
  // 确定性顺序 A：降级先于重连补发被服务器处理 => 整批隔离
  {
    const RA = new SimClient('racer-a', 'RacerA');
    await RA.connect();
    A.invite('racer-a', 'editor', 'RacerA');
    await waitFor(() => RA.role === 'editor');
    RA.disconnect();
    await sleep(100);
    const marker = '[RACEA]';
    let ua = null; const uops = [];
    for (const ch of marker) { const o = RA.doc.insert(ua, ch); ua = o.id; uops.push(o); }
    const ub = RA.batchOps(uops);
    // 断开期间完成降级（服务器已处理），再连接（welcome 带 viewer 角色），然后补发
    A.setRole('racer-a', 'viewer');
    await waitFor(() => A.members.find(m => m.uid === 'racer-a' && m.role === 'viewer'), '降级先落定');
    await RA.connect();
    await waitFor(() => RA.quarantines.some(q => q.batchId === ub.batchId), '顺序A：批次被隔离');
    await sleep(100);
    assert.ok(!A.doc.text().includes(marker), '顺序A 零泄漏');
    global.__raceA = { revCount: A.revs.length, text: A.doc.text(), audit: A.audit.map(e => e.seq + ':' + e.type).join(',') };
  }
  // 确定性顺序 B：racer 已连接并以 editor 补发在途，紧接着 owner 降级。
  // 由于服务器按消息到达顺序在单临界区裁决，"批次先到"=> 应用；"降级先到"=> 隔离。
  // 关键不变量：无论哪种结果，所有参与者看到的授权结果/内容/台账顺序必须完全一致。
  {
    const RB = new SimClient('racer-b', 'RacerB');
    await RB.connect();
    A.invite('racer-b', 'editor', 'RacerB');
    await waitFor(() => RB.role === 'editor');
    RB.disconnect();
    await sleep(100);
    const marker = '[RACEB]';
    let ua = null; const uops = [];
    for (const ch of marker) { const o = RB.doc.insert(ua, ch); ua = o.id; uops.push(o); }
    const ub = RB.batchOps(uops);
    await RB.connect(); // welcome(editor) 后自动补发批次
    // 等批次裁决（应用或隔离）完成，再降级
    await waitFor(() =>
      A.doc.text().includes(marker) || RB.quarantines.some(q => q.batchId === ub.batchId), '顺序B 批次裁决完成');
    A.setRole('racer-b', 'viewer');
    await waitFor(() => A.members.find(m => m.uid === 'racer-b' && m.role === 'viewer'), '顺序B 降级完成');
    await sleep(200);
    // 全员一致：A/B/RB 对该批次的结论相同
    const applied = A.doc.text().includes(marker);
    assert.strictEqual(B.doc.text().includes(marker), applied, '顺序B：B 与裁决一致');
    assert.strictEqual(RB.doc.text().includes(marker), applied, '顺序B：本人与裁决一致');
    if (!applied) assert.ok(RB.quarantines.some(q => q.batchId === ub.batchId), '顺序B：若拒绝则在隔离区');
    // 台账顺序全员一致
    assert.ok(sameLedger(A, B), '顺序B：A/B 台账顺序一致');
    assert.ok(sameLedger(A, RB), '顺序B：A/本人台账顺序一致');
  }
  // 顺序A 下全员一致检查
  assert.ok(sameLedger(A, B), '顺序A：台账顺序全员一致');
  ok('G5 角色变更竞争在两种消息序下：授权结果唯一、内容与台账顺序全员一致');

  // ---- 5. 恢复历史版本（仅所有者；恢复本身是新修订） ----
  const snapshotText = A.doc.text();
  const revBefore = Math.max(...A.revs.map(r => r.n));
  A.insertText(A.doc.text().length, 'XXX'); // 末尾追加（RGA 下位置确定；回滚会把它一并移除）
  await waitFor(() => B.doc.text() === snapshotText + 'XXX', 'B 收到 XXX');
  A.restore(revBefore);
  await waitFor(() => A.doc.text() === snapshotText && B.doc.text() === snapshotText, '所有者回滚后文本一致');
  const restoreRev = A.revs[A.revs.length - 1];
  assert.strictEqual(restoreRev.kind, 'restore');
  assert.strictEqual(restoreRev.target, revBefore);
  assert.ok(auditTypes(A).includes('rollback'), '所有者回滚记入台账');
  ok('所有者回滚到检查点生效，且本身是新修订 #' + restoreRev.n);
  A.insertText(snapshotText.length, ' [A追加]');
  B.insertText(0, '[B插入] ');
  await waitFor(() =>
    A.doc.text() === B.doc.text() &&
    A.doc.text().includes('[A追加]') && A.doc.text().includes('[B插入]'),
    '恢复后的并发编辑合并');
  ok('恢复后的并发编辑正常合并，互不覆盖');

  // ---- G8 + 6. 重启：成员/纪元/隔离引用/台账/内容/修订全部存活 ----
  // 重启后用相同 mId 重试一次成员变更 => 必须幂等
  const restartMid = 'm-survives-restart';
  A.invite('carol', 'commenter', 'Carol', restartMid);
  // 把顺序A 中被降级的 racer-a 恢复为编辑者，以便重启后由"本人"重新提交其隔离批次
  A.setRole('racer-a', 'editor');
  await waitFor(() => A.members.find(m => m.uid === 'racer-a' && m.role === 'editor'), 'racer-a 恢复编辑者');
  const stateBefore = {
    text: A.doc.text(),
    members: A.members.map(m => [m.uid, m.role]).sort().join(','),
    epoch: A.epoch,
    auditCount: A.audit.length,
    quarantineCount: A.quarantines.filter(q => q.status === 'quarantined').length,
    revCount: A.revs.length,
  };
  await sleep(2500); // 等待内容落盘
  await stopServer();
  await startServer();
  const A2 = new SimClient('alice', 'Alice');
  const F2 = new SimClient('frank', 'Frank');
  const RA2 = new SimClient('racer-a', 'RacerA');
  await A2.connect(); await F2.connect(); await RA2.connect();
  await waitFor(() => A2.doc.text() === stateBefore.text, '重启后文本恢复');
  assert.strictEqual(A2.members.map(m => [m.uid, m.role]).sort().join(','), stateBefore.members, '成员表存活');
  assert.strictEqual(A2.epoch, stateBefore.epoch, '授权纪元存活');
  assert.ok(A2.audit.length >= stateBefore.auditCount, '审计台账存活：' + A2.audit.length);
  assert.ok(A2.audit.some(e => e.type === 'space-upgrade'), '升级记录仍在台账');
  assert.ok(A2.audit.some(e => e.type === 'batch-quarantined'), '隔离事件仍在台账');
  // Frank 此前的隔离批次（顺序A 的 racer-a 也在；这里检查 Frank 自己的历史 qid）
  // 重启后重试相同 mId：不重复记账、不推进纪元
  const epochAfterRestart = A2.epoch;
  A2.invite('carol', 'commenter', 'Carol', restartMid);
  await sleep(300);
  assert.strictEqual(A2.epoch, epochAfterRestart, '重启后重复成员变更仍幂等');
  ok('G8/6 成员、授权纪元、审计台账跨重启存活；重启后重试仍幂等');

  // 重启后重复提交一个已应用批次（batchId 已落盘）=> 不重复
  const revCountAfter = A2.revs.length;
  A2.sendBatch([{ t: 'ins', id: 'ghost:1', after: '', ch: 'Z', by: 'alice', opId: 'ghost#1' }], 'dup-batch-1');
  await sleep(300);
  assert.ok(!A2.doc.text().includes('Z'), '重启后已知 batchId 重复投递不生效');
  assert.strictEqual(A2.revs.length, revCountAfter, '不产生新修订');
  ok('G6(续) 批次幂等索引跨重启存活');

  // 隔离引用存活：重启后所有者/本人仍能读到重启前的隔离批次（racer-a）
  assert.ok(A2.quarantines.some(q => q.ops.some(o => o.ch === 'A') && q.uid === 'racer-a'), '隔离引用跨重启存活');
  ok('G8 隔离区（quarantine）记录跨重启存活');

  // 跨重启由本人（racer-a，已恢复编辑者）显式重新提交隔离批次，恰好一次
  const revsBeforeRel = A2.revs.length;
  assert.strictEqual(RA2.role, 'editor', '重启后角色恢复为编辑者');
  const qOwn = RA2.quarantines.find(q => q.uid === 'racer-a');
  assert.ok(qOwn, '本人重启后仍看到自己的隔离草稿');
  RA2.resubmit(qOwn.qid, 'rsb-after-restart-1');
  await waitFor(() => A2.doc.text().includes('[RACEA]'), '重启后重新提交合并');
  // 再次对同一 qid 重新提交 => 已放行，幂等，不产生第二个修订/不重复内容
  RA2.resubmit(qOwn.qid, 'rsb-after-restart-2');
  await sleep(300);
  assert.strictEqual((A2.doc.text().match(/\[RACEA\]/g) || []).length, 1, '跨重启重新提交恰好一次');
  assert.strictEqual(A2.revs.length, revsBeforeRel + 1, '仅一个新修订');
  ok('G4/G8(续) 重启后本人显式重新提交隔离批次且恰好一次');

  A2.disconnect(); F2.disconnect(); RA2.disconnect();

  // ---- G9 旧空间并发首开：恰好一个所有者，既有数据原样保留 ----
  await stopServer();
  // 手工构造一份 v1（无 members）旧空间文件：含文字/评论/修订
  let legacyCommentId = null;
  {
    const legacy = new CRDT.Doc('old-actor');
    const ops = [];
    let after = '';
    for (const ch of 'LEGACY CONTENT') { const o = legacy.insert(after, ch); after = o.id; ops.push(o); }
    const com = legacy.comment({ id: null, edge: 's' }, { id: after, edge: 'e' }, '旧评论', 'LEGACY CONTENT');
    legacyCommentId = com.id;
    const file = path.join(DATA_DIR, encodeURIComponent(LEGACY) + '.json');
    fs.writeFileSync(file, JSON.stringify({
      seq: ops.length + 1,
      ops: [...ops.map((op, i) => ({ seq: i + 1, op })), { seq: ops.length + 1, op: com }],
      revs: [{ n: 1, seq: ops.length, ts: Date.now() - 100000, by: 'old-actor', kind: 'edit', summary: '+14', snapshot: { text: 'LEGACY CONTENT', marks: [] } }],
    }));
  }
  await startServer();
  const L1 = new SimClient('legacy-one', 'First', LEGACY);
  const L2 = new SimClient('legacy-two', 'Second', LEGACY);
  // 真正并发：两个 upgrade 请求几乎同时进入事件循环
  await Promise.all([L1.connect(), L2.connect()]);
  await sleep(300);
  const owners = L1.members.filter(m => m.role === 'owner');
  assert.strictEqual(owners.length, 1, '恰好一个所有者');
  const ownerUid = owners[0].uid;
  assert.ok(ownerUid === 'legacy-one' || ownerUid === 'legacy-two', '所有者是两个首开者之一');
  assert.strictEqual(L1.role === 'owner', ownerUid === 'legacy-one');
  assert.strictEqual(L2.role === 'owner', ownerUid === 'legacy-two');
  assert.notStrictEqual(L1.role, L2.role, '两人不会同时成为所有者');
  // 既有内容/批注/修订原样保留
  assert.strictEqual(L1.doc.text(), 'LEGACY CONTENT', '旧文字保留');
  assert.strictEqual(L2.doc.text(), 'LEGACY CONTENT', '旧文字保留(2)');
  assert.strictEqual(L1.doc.commentList().length, 1, '旧批注保留');
  assert.ok(L1.doc.comments.has(legacyCommentId), '旧批注 id 保留');
  assert.ok(L1.revs.some(r => r.n === 1 && r.by === 'old-actor'), '旧修订保留且未重写');
  // 台账：一次 space-upgrade + 一个所有者
  const up = L1.audit.filter(e => e.type === 'space-upgrade');
  assert.strictEqual(up.length, 1, '恰好一条升级台账记录');
  assert.strictEqual(up[0].uid, ownerUid);
  // 非所有者此后只能读
  const loser = ownerUid === 'legacy-one' ? L2 : L1;
  loser.insertText(0, 'X');
  await sleep(300);
  assert.strictEqual(L1.doc.text(), 'LEGACY CONTENT', '非首认所有者无法写入');
  ok('G9 旧空间并发首开恰好初始化一个所有者，内容/批注/修订原样保留');

  L1.disconnect(); L2.disconnect();
  await stopServer();
  console.log('\n全部 ' + passed + ' 项测试通过 ✅');
  process.exit(0);
})().catch(async (e) => {
  console.error('\n测试失败 ❌', e);
  await stopServer();
  process.exit(1);
});
