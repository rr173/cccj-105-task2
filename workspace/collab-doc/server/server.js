'use strict';
/*
 * 协作服务器：零依赖（仅 Node 内置模块）。
 * - HTTP：托管前端静态文件与共享 CRDT/治理模块
 * - WebSocket（手写 RFC6455）：接收/广播操作
 * - 每个操作分配全局递增 seq => 所有客户端看到一致的修订时间线
 * - 每个操作批次形成一个修订（含快照）；恢复 = 生成一批普通的删除/插入/格式操作，
 *   作为新修订进入操作流，与他人的并发编辑按 CRDT 规则自然合并
 * - 操作日志落盘（data/<doc>.json），重启后重放恢复
 *
 * 成员治理：
 * - 四角色 owner/editor/commenter/viewer；每个空间一份成员表 + authzEpoch（授权纪元）
 * - 内容操作批次按"连接时成员的当前角色 / 当前纪元"原子授权：整批放行或整批拒绝；
 *   被拒批次不发布、不产生修订，整体进入持久化"隔离区（quarantine）"，等待显式重新提交
 * - 成员变更（邀请/改角色/移除）本身也是一条共享审计台账记录，与内容修订共用同一
 *   单调 seq 序列 => 台账与内容修订之间存在无歧义的全局顺序
 * - 所有写操作（mId / batchId / opId）均做持久化幂等去重：重试、重复投递、进程重启
 *   都不会重复执行成员变更、不会重复产生台账记录
 * - 旧空间（无成员元数据）升级时保留全部 ops/revs；首个 hello 原子认领为所有者，
 *   并发首开也只会产生一个所有者，且不重写既有数据
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const CRDT = require('../shared/crdt.js');
const GOV = require('../shared/gov.js');

const PORT = Number(process.env.PORT || 8080);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const MAX_REVS = 1000;
const MAX_AUDIT = 5000;
const MAX_QUARANTINE = 500;
const MAX_BATCH_INDEX = 10000;
fs.mkdirSync(DATA_DIR, { recursive: true });

function newId(prefix) { return prefix + '_' + crypto.randomBytes(9).toString('hex'); }
function clip(s, n) { return typeof s === 'string' ? s.slice(0, n) : s; }
function nowTs() { return Date.now(); }

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

// ---------------- 操作清洗 ----------------
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
    return { t: 'mark', id: clip(o.id, 160), start: o.start, end: o.end, attrs, ts: +o.ts || nowTs(), by, deleted: !!o.deleted, opId: clip(o.opId, 200) };
  }
  if (o.t === 'com' && typeof o.id === 'string' && o.start && o.end) {
    return { t: 'com', id: clip(o.id, 160), start: o.start, end: o.end, text: clip(String(o.text || ''), 4000), quote: clip(String(o.quote || ''), 4000), ts: +o.ts || nowTs(), by, resolved: !!o.resolved, opId: clip(o.opId, 200) };
  }
  return null;
}

// ---------------- 空间存储 ----------------
class Store {
  constructor(name) {
    this.name = name;
    this.doc = new CRDT.Doc('server');
    this.seq = 0;
    this.ops = [];              // [{ seq, op }] 内容操作流
    this.revs = [];             // [{ n, seq, ts, by, kind, summary, target?, snapshot }]
    this.seenOpIds = new Set();
    // 治理状态
    this.members = null;        // uid -> { uid, name, role, since }；null 表示旧空间待初始化
    this.authzEpoch = 0;
    this.audit = [];            // 共享审计台账（与 ops 共用 seq）
    this.quarantine = new Map();// qid -> 隔离批次
    this.seenMIds = new Set();
    this.batchIndex = new Map();// batchId -> { kind: 'applied'|'quarantined', qid?, seq? }
    this.mutResults = new Map();// mId -> 幂等响应快照
    this.rollbackSeen = new Set();// "uid:target:headN" 回滚重试去重（含重启持久化）
    this.dirty = false;
    this.file = path.join(DATA_DIR, encodeURIComponent(name) + '.json');
    this._load();
  }

  _load() {
    let j;
    try { j = JSON.parse(fs.readFileSync(this.file, 'utf8')); } catch (e) { return; /* 新空间 */ }
    this.seq = j.seq || 0;
    this.ops = j.ops || [];
    this.revs = j.revs || [];
    for (const { op } of this.ops) { this.doc.apply(op); if (op.opId) this.seenOpIds.add(op.opId); }
    // 旧空间升级：没有 members 字段 => 保留全部内容/批注/修订，等待首个 hello 原子认领所有者
    if (j.members && typeof j.members === 'object') {
      this.members = {};
      for (const [uid, m] of Object.entries(j.members)) {
        if (m && GOV.isValidRole(m.role)) {
          this.members[uid] = { uid, name: clip(String(m.name || uid), 64), role: m.role, since: m.since || nowTs() };
        }
      }
      this.authzEpoch = j.authzEpoch || 0;
      this.audit = Array.isArray(j.audit) ? j.audit : [];
      this.seenMIds = new Set(j.seenMIds || []);
      for (const q of (j.quarantine || [])) this.quarantine.set(q.qid, q);
      for (const [bid, info] of Object.entries(j.batchIndex || {})) this.batchIndex.set(bid, info);
      for (const [mid, r] of Object.entries(j.mutResults || {})) this.mutResults.set(mid, r);
      this.rollbackSeen = new Set(j.rollbackSeen || []);
      console.log(`[store] "${this.name}" 载入 ${this.ops.length} 操作 / ${this.revs.length} 修订 / ${Object.keys(this.members).length} 成员 / ${this.audit.length} 台账`);
    } else {
      console.log(`[store] "${this.name}" 载入旧空间：${this.ops.length} 操作 / ${this.revs.length} 修订，等待首个成员认领`);
    }
  }

  save() {
    if (!this.dirty) return;
    this.dirty = false;
    const quarantine = [...this.quarantine.values()];
    const batchIndex = {};
    for (const [k, v] of this.batchIndex) batchIndex[k] = v;
    const mutResults = {};
    for (const [k, v] of this.mutResults) mutResults[k] = v;
    const payload = {
      v: 2,
      seq: this.seq,
      ops: this.ops,
      revs: this.revs,
      members: this.members,
      authzEpoch: this.authzEpoch,
      audit: this.audit,
      seenMIds: [...this.seenMIds],
      quarantine,
      batchIndex,
      mutResults,
      rollbackSeen: [...this.rollbackSeen].slice(-2000),
    };
    const tmp = this.file + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(payload));
    fs.renameSync(tmp, this.file); // 原子替换，崩溃/重启不会出现半截文件
  }
  // 治理相关状态立即落盘（成员/纪元/隔离区必须跨重启存活）
  fsyncGovernance() { this.dirty = true; this.save(); }

  _nextSeq() { return ++this.seq; }

  // ---------------- 成员视图 ----------------
  memberOf(uid) { return this.members ? this.members[uid] || null : null; }
  roleOf(uid) { const m = this.memberOf(uid); return m ? m.role : null; }
  membersList() { return this.members ? Object.values(this.members) : []; }
  ownerCount() { return this.membersList().filter(m => m.role === 'owner').length; }

  // 旧空间首个连接者原子认领为所有者（Node 单线程：整个 hello 处理不可分割）
  // 返回 true 表示本次认领产生了 owner；并发/重复首开一律返回 false。
  claimFirstOwner(uid, name) {
    if (this.members) return false;
    const seq = this._nextSeq();
    const member = { uid, name: clip(String(name || uid), 64), role: 'owner', since: nowTs() };
    this.members = { [uid]: member };
    this.authzEpoch = 1;
    const entry = {
      seq, ts: nowTs(), type: 'space-upgrade',
      uid, by: uid, name: member.name, role: 'owner', epoch: 1,
      detail: '旧空间升级：' + member.name + ' 成为首位所有者（既有内容与修订原样保留）',
    };
    this._pushAudit(entry);
    this.fsyncGovernance(); // 立即持久化：第二个并发首开读到的就是已认领状态
    return entry;
  }

  _pushAudit(entry) {
    this.audit.push(entry);
    if (this.audit.length > MAX_AUDIT) this.audit.splice(0, this.audit.length - MAX_AUDIT);
  }

  // ---------------- 成员变更（owner 专用，全部幂等） ----------------
  // action: invite / role / remove
  mutateMember(byUid, action, uid, role, mId, name) {
    if (!this.members) return { error: '空间尚未初始化' };
    if (mId && this.mutResults.has(mId)) {
      // 重试/重复消息：返回首次结果，绝不重复执行、绝不重复记账
      return this.mutResults.get(mId);
    }
    const actor = this.memberOf(byUid);
    if (!actor || !GOV.canManage(actor.role)) {
      // 非所有者的管理尝试也留痕（但不推进授权纪元）；同一 mId 重试只留痕一次
      const seq = this._nextSeq();
      const entry = {
        seq, ts: nowTs(), type: 'member-denied',
        uid: byUid, by: byUid, name: actor ? actor.name : byUid,
        action: action + (uid ? ':' + uid : ''), detail: '非所有者尝试管理成员，已拒绝',
      };
      this._pushAudit(entry);
      this.fsyncGovernance();
      const result = { error: '只有所有者可以邀请成员或调整角色', audit: entry };
      if (mId) this.mutResults.set(mId, result);
      return result;
    }
    uid = clip(String(uid || ''), 64);
    if (!uid || uid === '?') return { error: '缺少目标成员' };
    if (action === 'remove') role = null;
    else if (!GOV.isValidRole(role)) return { error: '未知角色' };

    const target = this.memberOf(uid);
    if (action === 'invite') {
      if (!target) {
        const member = { uid, name: clip(String(name || uid), 64), role, since: nowTs() };
        this.members[uid] = member;
        const entry = this._commitMemberChange('member-invite', member, actor, null, role, mId);
        return this._memo(mId, { ok: true, entry, member });
      }
      // 已存在 => 语义化为角色变更（重复邀请同一角色则纯幂等，不记账）
      if (target.role === role) return { ok: true, duplicated: true, member: target };
      action = 'role';
    }
    if (!target) return { error: '该成员不在空间中' };

    if (action === 'role') {
      if (target.role === role) return { ok: true, duplicated: true, member: target };
      // 保护最后一个所有者：不能被降级（移除在下面处理）
      if (target.role === 'owner' && role !== 'owner' && this.ownerCount() <= 1) {
        return { error: '空间必须保留至少一名所有者' };
      }
      const oldRole = target.role;
      target.role = role;
      const entry = this._commitMemberChange('member-role', target, actor, oldRole, role, mId);
      return this._memo(mId, { ok: true, entry, member: target });
    }

    if (action === 'remove') {
      if (target.role === 'owner' && this.ownerCount() <= 1) {
        return { error: '不能移除最后一名所有者' };
      }
      const oldRole = target.role;
      delete this.members[uid];
      const entry = this._commitMemberChange('member-remove', target, actor, oldRole, null, mId);
      return this._memo(mId, { ok: true, entry, removed: uid });
    }
    return { error: '未知的成员操作' };
  }
  _memo(mId, result) {
    if (mId) this.mutResults.set(clip(mId, 200), result);
    return result;
  }
  // 提交一次成员变更：推进授权纪元 + 写审计台账（二者同一 seq、同一原子步骤）
  _commitMemberChange(type, target, actor, oldRole, newRole, mId) {
    const seq = this._nextSeq();
    this.authzEpoch++;
    const entry = {
      seq, ts: nowTs(), type,
      uid: target.uid, by: actor.uid, actorName: actor.name,
      name: target.name, role: newRole, fromRole: oldRole,
      epoch: this.authzEpoch, mId: mId || null,
      detail: this._changeText(type, actor.name, target.name, oldRole, newRole),
    };
    this._pushAudit(entry);
    this.fsyncGovernance();
    return entry;
  }
  _changeText(type, actorName, targetName, oldRole, newRole) {
    const L = GOV.ROLE_LABEL;
    if (type === 'member-invite') return actorName + ' 邀请 ' + targetName + ' 加入空间，角色：' + L[newRole];
    if (type === 'member-role') return actorName + ' 将 ' + targetName + ' 的角色从「' + L[oldRole] + '」调整为「' + L[newRole] + '」';
    if (type === 'member-remove') return actorName + ' 移除了成员 ' + targetName + '（原角色：' + L[oldRole] + '）';
    return '';
  }

  // ---------------- 内容批次授权与提交 ----------------
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

  // 原子处理一个内容批次（重连补发 / 重试 / 显式重新提交都走这里）。
  // 返回 { kind: 'applied'|'quarantined'|'duplicate'|'empty'|'error', ... }
  // Node 单线程 + 无 await：鉴权判定与状态变更之间不会插入任何成员变更。
  submitBatch(rawOps, uid, batchId, opts) {
    opts = opts || {};
    if (batchId && this.batchIndex.has(batchId)) {
      // 重复投递：返回首次结论，不重复应用、不重复隔离、不重复记账
      return { kind: 'duplicate', index: this.batchIndex.get(batchId) };
    }
    const member = this.memberOf(uid);

    const sanitized = [];
    for (const raw of (Array.isArray(rawOps) ? rawOps.slice(0, 5000) : [])) {
      const op = sanitizeOp(raw);
      if (op) sanitized.push(op);
    }
    if (!sanitized.length) return { kind: 'empty' };

    // 断连期间被移出空间：同样是"授权被撤销"，整批隔离并解释；草稿保留，重新受邀后可再提交
    if (!member) {
      return this._quarantineBatch({
        uid, name: uid, role: null, epoch: this.authzEpoch, ops: sanitized, batchId,
        violations: sanitized.map((op, i) => ({ i, kind: GOV.opKind(op), need: 'member', have: null })),
        reason: uid + ' 在断连期间已被移出空间，整批未发布，已保留为隔离草稿',
        fromRole: opts.lastKnownRole || null, removed: true,
      });
    }

    const role = member.role;
    const epochAtAuthz = this.authzEpoch;

    const check = GOV.checkBatch(role, sanitized);
    if (!check.allowed) {
      // 整批拒绝：不 apply、不分配内容 seq、不产生修订、不广播
      return this._quarantineBatch({
        uid, name: member.name, role, epoch: epochAtAuthz, ops: sanitized, batchId,
        violations: check.violations, reason: this._denyText(member, check.violations),
        fromRole: opts.lastKnownRole && opts.lastKnownRole !== role ? opts.lastKnownRole : null, removed: false,
      });
    }

    // 放行：应用（opId 去重保证单操作级幂等）
    const applied = [];
    for (const op of sanitized) {
      if (op.opId && this.seenOpIds.has(op.opId)) continue; // 重连补发/重启后的重复操作
      if (op.opId) this.seenOpIds.add(op.opId);
      this.seq++;
      this.ops.push({ seq: this.seq, op });
      this.doc.apply(op);
      applied.push({ seq: this.seq, op });
    }
    if (!applied.length) return { kind: 'empty' };
    const rev = Object.assign({
      n: this.revs.length ? this.revs[this.revs.length - 1].n + 1 : 1,
      seq: this.seq, ts: nowTs(), by: clip(String(member.name || uid), 64),
      kind: opts.kind === 'restore' ? 'restore' : 'edit',
      summary: this._summarize(applied.map(a => a.op)), snapshot: this._snapshot(),
      epoch: epochAtAuthz, uid,
    }, opts.kind === 'restore' ? { target: opts.target } : {});
    this.revs.push(rev);
    if (this.revs.length > MAX_REVS) this.revs.splice(0, this.revs.length - MAX_REVS);
    if (batchId) this._indexBatch(batchId, { kind: 'applied', seq: this.seq, rev: rev.n });
    this.dirty = true;
    return { kind: 'applied', applied, rev: this.publicRev(rev), epoch: epochAtAuthz };
  }
  // 把一个越权/身份失效批次整体隔离，并写一条审计台账（不 apply、不产生修订、不广播内容）
  _quarantineBatch(o) {
    const qid = newId('q');
    const rec = {
      qid, batchId: o.batchId || qid, uid: o.uid, name: o.name, role: o.role,
      epoch: o.epoch, ts: nowTs(), ops: o.ops, violations: o.violations,
      reason: o.reason, fromRole: o.fromRole || null, removed: !!o.removed,
      resubmits: [], status: 'quarantined',
    };
    this.quarantine.set(qid, rec);
    if (this.quarantine.size > MAX_QUARANTINE) {
      const oldest = this.quarantine.keys().next().value;
      this.quarantine.delete(oldest);
    }
    if (o.batchId) this._indexBatch(o.batchId, { kind: 'quarantined', qid });
    const seq = this._nextSeq();
    const entry = {
      seq, ts: nowTs(), type: 'batch-quarantined',
      uid: o.uid, by: o.uid, name: o.name, role: o.role, epoch: o.epoch,
      qid, batchId: rec.batchId, ops: o.ops.length, removed: !!o.removed,
      kinds: this._kindSummary(o.ops), detail: o.reason,
    };
    this._pushAudit(entry);
    rec.auditSeq = seq;
    this.fsyncGovernance();
    return { kind: 'quarantined', quarantine: this.publicQuarantine(rec), audit: entry };
  }
  _indexBatch(batchId, info) {
    this.batchIndex.set(clip(batchId, 200), info);
    if (this.batchIndex.size > MAX_BATCH_INDEX) {
      const first = this.batchIndex.keys().next().value;
      this.batchIndex.delete(first);
    }
  }
  _kindSummary(ops) {
    const c = {};
    for (const op of ops) { const k = GOV.opKind(op); if (k) c[k] = (c[k] || 0) + 1; }
    return c;
  }
  _denyText(member, violations) {
    const kinds = {};
    for (const v of violations) kinds[v.kind] = (kinds[v.kind] || 0) + 1;
    const parts = Object.keys(kinds).map(k => (GOV.KIND_LABEL[k] || k) + '×' + kinds[k]);
    let t = member.name + '（' + GOV.ROLE_LABEL[member.role] + '）的批次含越权操作（' + parts.join('、') + '），整批未发布';
    return t;
  }

  // 隔离区显式重新提交：按当前纪元/角色重新授权；成功则应用且恰好一次
  resubmit(qid, uid, batchId) {
    const rec = this.quarantine.get(qid);
    if (!rec) return { kind: 'error', error: '隔离批次不存在（可能已随重启前的清理过期）' };
    // 同一重新提交的重试（相同 qid）：首次已放行则直接返回，绝不重复应用
    if (rec.status === 'released') return { kind: 'released', qid, batchId: rec.batchId };
    const member = this.memberOf(uid);
    if (!member) return { kind: 'error', error: '你已不在此空间，无法重新提交' };
    const check = GOV.checkBatch(member.role, rec.ops);
    if (!check.allowed) {
      rec.resubmits.push({ ts: nowTs(), role: member.role, epoch: this.authzEpoch, result: 'denied' });
      this.fsyncGovernance();
      return {
        kind: 'quarantined',
        quarantine: this.publicQuarantine(rec),
        error: '当前角色「' + GOV.ROLE_LABEL[member.role] + '」仍不足以提交该批次，继续保留在隔离区',
      };
    }
    // 原 batchId 已记为"已隔离"，放行必须使用新的应用批次 id（重试携带同一新 id 仍幂等）
    const applyBatchId = (batchId && batchId !== rec.batchId) ? clip(batchId, 200) : newId('rsb');
    const r = this.submitBatch(rec.ops, uid, applyBatchId, { resubmit: true });
    if (r.kind === 'applied') {
      rec.status = 'released';
      rec.releasedTs = nowTs();
      rec.releaseSeq = r.applied[r.applied.length - 1].seq;
      rec.resubmits.push({ ts: nowTs(), role: member.role, epoch: this.authzEpoch, result: 'applied', batchId: applyBatchId });
      const seq = this._nextSeq();
      const entry = {
        seq, ts: nowTs(), type: 'quarantine-released',
        uid, by: uid, name: member.name, role: member.role, epoch: this.authzEpoch,
        qid, batchId: applyBatchId, rev: r.rev.n,
        detail: member.name + ' 显式重新提交了隔离批次（' + rec.ops.length + ' 个操作），已作为修订 #' + r.rev.n + ' 合并',
      };
      this._pushAudit(entry);
      this.fsyncGovernance();
      r.audit = entry;
    } else if (r.kind === 'duplicate') {
      // 同一重新提交的并发重复（相同 applyBatchId）：首个已生效，不重复应用
      rec.status = 'released';
      r.released = true;
    }
    return r;
  }

  // 仅所有者可回滚到检查点（修订）。非所有者 => 拒绝、不产生修订、台账留痕。
  restore(targetN, uid) {
    const member = this.memberOf(uid);
    // 幂等键：同一用户在同一当前修订头部重复回滚同一检查点 => 重试，不重复产生台账/修订
    const headN = this.revs.length ? this.revs[this.revs.length - 1].n : 0;
    const dedupKey = uid + ':' + targetN + ':' + headN;
    if (!member || !GOV.canRollback(member.role)) {
      if (this.rollbackSeen.has('denied:' + dedupKey)) {
        return { error: '只有所有者可以回滚到历史检查点', duplicate: true };
      }
      this.rollbackSeen.add('denied:' + dedupKey);
      const seq = this._nextSeq();
      const entry = {
        seq, ts: nowTs(), type: 'rollback-denied',
        uid, by: uid, name: member ? member.name : uid,
        role: member ? member.role : null, target: targetN,
        detail: (member ? member.name : uid) + ' 尝试回滚到检查点 #' + targetN + '，但仅所有者可回滚，已拒绝（未产生修订）',
      };
      this._pushAudit(entry);
      this.fsyncGovernance();
      return { error: '只有所有者可以回滚到历史检查点', audit: entry };
    }
    const rev0 = this.revs.find(r => r.n === targetN);
    if (!rev0 || !rev0.snapshot) return { error: '找不到该修订版本' };
    if (this.rollbackSeen.has(dedupKey)) {
      // 相同回滚的重复投递（重启后同样成立）：首次结果已在操作流与台账中，直接幂等返回
      return { kind: 'duplicate', duplicate: true };
    }
    this.rollbackSeen.add(dedupKey);
    const snap = rev0.snapshot;
    const ops = [];
    for (const id of this.doc.visibleIds()) ops.push({ t: 'del', id, by: uid });
    const actor = 'srv-' + clip(String(uid), 32) + '-r' + targetN + '-' + nowTs();
    let c = 0, after = '';
    const newIds = [];
    for (const ch of snap.text) {
      const id = actor + ':' + (++c);
      ops.push({ t: 'ins', id, after, ch, by: uid });
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
      ops.push({ t: 'mark', id: actor + ':m:' + (++c), start: anchor(m.s, 's'), end: anchor(m.e, 'e'), attrs: m.attrs, ts: nowTs(), by: uid, deleted: false });
    }
    const batchId = 'rb-' + uid + '-' + targetN + '-' + rev0.seq;
    const r = this.submitBatch(ops, uid, batchId, { kind: 'restore', target: targetN });
    if (r.kind !== 'applied') return { error: '回滚批次未能提交' };
    const seq = this._nextSeq();
    const entry = {
      seq, ts: nowTs(), type: 'rollback',
      uid, by: uid, name: member.name, role: member.role, epoch: this.authzEpoch,
      target: targetN, rev: r.rev.n,
      detail: member.name + ' 将空间回滚到检查点 #' + targetN + '（作为新修订 #' + r.rev.n + ' 合并）',
    };
    this._pushAudit(entry);
    this.fsyncGovernance();
    r.audit = entry;
    return r;
  }

  // ---------------- 对外视图（不含快照等内部大字段） ----------------
  publicRev(r) { return { n: r.n, seq: r.seq, ts: r.ts, by: r.by, kind: r.kind, summary: r.summary, target: r.target, epoch: r.epoch || 0, uid: r.uid || null }; }
  publicRevs() { return this.revs.map(r => this.publicRev(r)); }
  publicAudit() { return this.audit.slice(); }
  publicQuarantine(q) {
    return {
      qid: q.qid, batchId: q.batchId, uid: q.uid, name: q.name, role: q.role,
      epoch: q.epoch, ts: q.ts, ops: q.ops, violations: q.violations, reason: q.reason,
      fromRole: q.fromRole || null, removed: !!q.removed, status: q.status,
      releasedTs: q.releasedTs || null, resubmits: q.resubmits || [],
    };
  }
  publicQuarantines() {
    return [...this.quarantine.values()].filter(q => q.status === 'quarantined').map(q => this.publicQuarantine(q));
  }
  // 隔离批次内容仅对本人（及所有者）可见，避免把他人被拒草稿广播给全体成员
  publicQuarantinesFor(uid) {
    const role = this.roleOf(uid);
    return this.publicQuarantines().filter(q => q.uid === uid || role === 'owner');
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
  if (p.startsWith('/shared/')) {
    const file = path.normalize(path.join(__dirname, '..', p));
    const root = path.normalize(path.join(__dirname, '..', 'shared'));
    if (!file.startsWith(root)) { res.writeHead(403); return res.end(); }
    return serveFile(res, file);
  }
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
// 治理状态广播：角色与隔离草稿都是"每个接收者各自可见"，必须按连接分别下发
function broadcastGov(room, store) {
  const set = rooms.get(room);
  if (!set) return;
  for (const c of set) {
    c.send({ t: 'gov', role: c.uid ? store.roleOf(c.uid) : null, epoch: store.authzEpoch, members: store.membersList(), quarantine: store.publicQuarantinesFor(c.uid) });
  }
}

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
  let room = null, uid = '?', displayName = '?';

  function govState(store) {
    return {
      role: store.roleOf(uid),
      epoch: store.authzEpoch,
      members: store.membersList(),
      audit: store.publicAudit(),
      quarantine: store.publicQuarantinesFor(uid),
    };
  }

  conn.onmessage = (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.t === 'hello') {
      uid = clip(String(msg.uid || msg.clientId || '?'), 64);
      displayName = clip(String(msg.name || uid), 64);
      conn.uid = uid;
      const docName = clip(String(msg.doc || 'default'), 64) || 'default';
      const store = getStore(docName);
      room = docName;
      conn.room = room;

      // 旧空间升级：首个 hello 在单个同步临界区内原子认领所有者。
      // 并发首开时只有一个 hello 先被事件循环派发 => 恰好一个所有者；既有数据不重写。
      let upgradeEntry = null;
      if (!store.members) upgradeEntry = store.claimFirstOwner(uid, displayName) || null;

      if (!rooms.has(room)) rooms.set(room, new Set());
      rooms.get(room).add(conn);

      const lastSeq = +msg.lastSeq || 0;
      const welcome = Object.assign({
        t: 'welcome', seq: store.seq,
        ops: store.ops.filter(o => o.seq > lastSeq),
        revs: store.publicRevs(),
        you: uid, yourName: displayName,
      }, govState(store));
      conn.send(welcome);
      if (upgradeEntry) {
        broadcast(room, { t: 'audit', entries: [upgradeEntry] });
        broadcastGov(room, store); // 各连接分别收到自己的角色与各自可见的隔离草稿
      }
      broadcast(room, { t: 'presence', count: rooms.get(room).size });
      return;
    }

    if (!room) return;
    const store = getStore(room);

    if (msg.t === 'ops') {
      const batchId = clip(String(msg.batchId || ''), 200);
      const r = store.submitBatch(Array.isArray(msg.ops) ? msg.ops : [], uid, batchId, { lastKnownRole: msg.lastKnownRole || null });
      if (r.kind === 'applied') {
        // 顺序保证：先广播内容，再广播可能的台账更新（回滚等），所有参与者一致
        broadcast(room, { t: 'ops', applied: r.applied, rev: r.rev, epoch: r.epoch, batchId, by: uid });
      } else if (r.kind === 'quarantined') {
        // 不发布任何被禁操作；仅通知本人保留隔离草稿，全员收到治理状态与台账
        conn.send({ t: 'rejected', batchId, quarantine: r.quarantine, reason: r.quarantine.reason });
        broadcastGov(room, store);
        broadcast(room, { t: 'audit', entries: [r.audit] });
      } else if (r.kind === 'duplicate') {
        // 重复投递：回传首次结论，绝不重复执行
        const info = r.index;
        if (info.kind === 'applied') conn.send({ t: 'batch-ack', batchId, kind: 'applied', seq: info.seq, rev: info.rev, duplicate: true });
        else {
          const q = store.quarantine.get(info.qid);
          conn.send({ t: 'batch-ack', batchId, kind: 'quarantined', qid: info.qid, duplicate: true });
          if (q) conn.send({ t: 'rejected', batchId: q.batchId, quarantine: store.publicQuarantine(q), reason: q.reason });
        }
      } else if (r.kind === 'error') {
        conn.send({ t: 'error', message: r.error });
      }
      return;
    }

    if (msg.t === 'resubmit') {
      const qid = clip(String(msg.qid || ''), 120);
      const batchId = clip(String(msg.batchId || ''), 200);
      const r = store.resubmit(qid, uid, batchId);
      if (r.kind === 'applied') {
        broadcast(room, { t: 'ops', applied: r.applied, rev: r.rev, epoch: store.authzEpoch, batchId: batchId || undefined, by: uid, resubmitted: qid });
        broadcastGov(room, store);
        broadcast(room, { t: 'audit', entries: [r.audit] });
      } else if (r.kind === 'released') {
        conn.send({ t: 'batch-ack', qid, kind: 'released', duplicate: true });
        broadcastGov(room, store);
      } else if (r.kind === 'quarantined') {
        conn.send({ t: 'rejected', batchId: r.quarantine.batchId, quarantine: r.quarantine, reason: r.error || r.quarantine.reason, resubmitDenied: true });
      } else {
        conn.send({ t: 'error', message: r.error || '重新提交失败' });
      }
      return;
    }

    if (msg.t === 'member') {
      const r = store.mutateMember(
        uid,
        clip(String(msg.action || ''), 16),
        clip(String(msg.uid || ''), 64),
        clip(String(msg.role || ''), 16),
        clip(String(msg.mId || ''), 200),
        clip(String(msg.name || ''), 64)
      );
      if (r.error) {
        conn.send({ t: 'member-result', mId: msg.mId || null, ok: false, error: r.error });
        if (r.audit) broadcast(room, { t: 'audit', entries: [r.audit] });
      } else if (r.duplicated) {
        conn.send({ t: 'member-result', mId: msg.mId || null, ok: true, duplicated: true, member: r.member });
      } else if (r.ok) {
        conn.send({ t: 'member-result', mId: msg.mId || null, ok: true });
        // 成员变更 + 纪元 + 台账以同一消息序广播：所有参与者看到相同的授权结果与顺序
        broadcastGov(room, store);
        broadcast(room, { t: 'audit', entries: [r.entry] });
      }
      return;
    }

    if (msg.t === 'rename') {
      const nm = clip(String(msg.name || uid), 64);
      const m = store.memberOf(uid);
      if (m) {
        m.name = nm;
        store.fsyncGovernance();
        broadcastGov(room, store);
      }
      return;
    }

    if (msg.t === 'restore') {
      const r = store.restore(+msg.rev, uid);
      if (r.kind === 'duplicate') {
        conn.send({ t: 'batch-ack', kind: 'released', duplicate: true });
      } else if (r.error) {
        conn.send({ t: 'error', message: r.error });
        if (r.audit) broadcast(room, { t: 'audit', entries: [r.audit] });
      } else {
        broadcast(room, { t: 'ops', applied: r.applied, rev: r.rev, epoch: store.authzEpoch, by: uid });
        broadcast(room, { t: 'audit', entries: [r.audit] });
      }
      return;
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
