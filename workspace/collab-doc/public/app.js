/* global CRDT, GOV */
'use strict';
/*
 * 前端逻辑（含成员治理）：
 * - 本地即真：编辑先应用到本地 CRDT，再以"批次"为最小单位异步同步；断网时批次留在 localStorage
 * - 授权：服务器按连接身份的当前角色/授权纪元对整批原子授权——整批发布或整批拒绝
 * - 被拒批次绝不进入共享状态：从本地 CRDT 中摘除（rebuild），完整保留在"隔离草稿"面板，
 *   可读、可复制；仅当当前角色重新具备足够权限时，才允许"显式重新提交"
 * - 角色决定可用命令：所有者/编辑者可写文字样式，评论者只能批注，只读者只读；
 *   仅所有者可邀请/改角色/回滚
 */
(() => {
  const $ = (s) => document.querySelector(s);
  const $$ = (s) => [...document.querySelectorAll(s)];
  const docName = new URLSearchParams(location.search).get('doc') || 'default';
  const uid = localStorage.getItem('uid') ||
    ('u-' + (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2)));
  localStorage.setItem('uid', uid);
  let displayName = localStorage.getItem('uname') || uid.slice(0, 8);
  const LS_KEY = 'collab2:' + docName;
  const LOCAL_OPS_CAP = 20000;

  const doc = new CRDT.Doc(uid);
  let lastSeq = 0, opSeq = 0, batchSeq = 0;
  let confirmedOps = [];          // 已被服务器确认的操作
  let pendingBatches = [];        // [{batchId, ops}] 本地已应用、待服务器裁决的批次
  let revs = [];
  let ws = null, online = false, reconnectDelay = 500;
  let curText = '';
  let reattachFor = null;
  let pendingCommentRange = null;
  let composing = false;

  // 治理状态（welcome/gov 广播下发）
  let myRole = null;              // null = 尚未加入空间（只读）
  let epoch = 0;
  let members = [];
  let audit = [];
  let quarantines = [];           // 服务器权威的隔离批次列表

  const ROLE_LABEL = GOV.ROLE_LABEL;

  // ---------- 本地持久化 ----------
  function saveLocal() {
    try {
      if (confirmedOps.length > LOCAL_OPS_CAP) { localStorage.removeItem(LS_KEY); return; }
      localStorage.setItem(LS_KEY, JSON.stringify({ lastSeq, opSeq, batchSeq, confirmedOps, pendingBatches }));
    } catch (e) { /* 存储满则忽略 */ }
  }
  let saveTimer = null;
  function saveLocalSoon() { clearTimeout(saveTimer); saveTimer = setTimeout(saveLocal, 300); }
  (function loadLocal() {
    try {
      const j = JSON.parse(localStorage.getItem(LS_KEY) || 'null');
      if (j) {
        lastSeq = j.lastSeq || 0; opSeq = j.opSeq || 0; batchSeq = j.batchSeq || 0;
        confirmedOps = j.confirmedOps || [];
        if (Array.isArray(j.pendingBatches)) pendingBatches = j.pendingBatches;
        else if (Array.isArray(j.pending) && j.pending.length) {
          // 旧版本：扁平 pending 迁移为一个批次
          pendingBatches = [{ batchId: 'legacy-' + uid + '-1', ops: j.pending }];
        }
        for (const op of confirmedOps) doc.apply(op);
        for (const b of pendingBatches) for (const op of b.ops) doc.apply(op);
      }
    } catch (e) { /* 忽略损坏缓存 */ }
  })();

  // 被拒批次摘除后，从"已确认操作 + 剩余待裁批次"重建本地 CRDT，杜绝越权内容残留
  function rebuildLocal() {
    const fresh = new CRDT.Doc(uid);
    fresh.counter = opSeq;
    for (const op of confirmedOps) fresh.apply(op);
    for (const b of pendingBatches) for (const op of b.ops) fresh.apply(op);
    // 用新实例替换内部结构
    doc.chars = fresh.chars; doc.kids = fresh.kids;
    doc.marks = fresh.marks; doc.comments = fresh.comments;
    doc.waiting = fresh.waiting; doc.counter = fresh.counter;
  }

  // ---------- 网络 ----------
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(proto + '://' + location.host + '/ws');
    ws.onopen = () => {
      online = true; reconnectDelay = 500; setStatus();
      ws.send(JSON.stringify({ t: 'hello', uid, name: displayName, doc: docName, lastSeq }));
    };
    ws.onmessage = (ev) => { try { handle(JSON.parse(ev.data)); } catch (e) { console.error(e); } };
    ws.onclose = () => {
      online = false; setStatus();
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 8000);
    };
    ws.onerror = () => { try { ws.close(); } catch (e) { /* ignore */ } };
  }
  function send(obj) { if (online && ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }

  function handle(msg) {
    if (msg.t === 'welcome') {
      const seenOpIds = new Set();
      for (const { seq, op } of msg.ops) {
        doc.apply(op); confirmedOps.push(op);
        lastSeq = Math.max(lastSeq, seq);
        if (op.opId) seenOpIds.add(op.opId);
      }
      // 权威隔离区：建 opId -> q 索引，用于认领本地待裁批次
      const quarOps = new Map();
      quarantines = msg.quarantine || [];
      for (const q of quarantines) for (const op of q.ops) if (op.opId) quarOps.set(op.opId, q.qid);

      let removedPending = false;
      const kept = [];
      for (const b of pendingBatches) {
        const confirmed = b.ops.some(op => op.opId && seenOpIds.has(op.opId));
        const quarantined = b.ops.some(op => op.opId && quarOps.has(op.opId));
        if (confirmed || quarantined) { removedPending = true; continue; }
        kept.push(b);
      }
      pendingBatches = kept;
      revs = msg.revs || [];
      myRole = msg.role; epoch = msg.epoch || 0; members = msg.members || [];
      audit = msg.audit || [];
      if (removedPending) rebuildLocal();
      for (const b of pendingBatches) send({ t: 'ops', batchId: b.batchId, ops: b.ops, lastKnownRole: b.fromRole || null });
      renderAll(); saveLocalSoon();
      return;
    }
    if (msg.t === 'ops') {
      for (const { seq, op } of msg.applied) {
        doc.apply(op); confirmedOps.push(op);
        lastSeq = Math.max(lastSeq, seq);
      }
      if (msg.batchId) {
        const i = pendingBatches.findIndex(b => b.batchId === msg.batchId);
        if (i >= 0) pendingBatches.splice(i, 1);
      } else {
        const ids = new Set(msg.applied.map(a => a.op.opId).filter(Boolean));
        pendingBatches = pendingBatches.filter(b => !b.ops.some(op => ids.has(op.opId)));
      }
      if (msg.rev) upsertRev(msg.rev);
      if (msg.epoch) epoch = msg.epoch;
      renderAll(); saveLocalSoon();
      return;
    }
    if (msg.t === 'rejected') {
      // 整批被授权拒绝：从待裁队列摘除并重建本地状态——任何部分都不得泄漏
      const bid = msg.batchId || (msg.quarantine && msg.quarantine.batchId);
      const before = pendingBatches.length;
      pendingBatches = pendingBatches.filter(b => b.batchId !== bid);
      if (pendingBatches.length !== before) rebuildLocal();
      if (msg.quarantine) upsertQuarantine(msg.quarantine);
      const why = msg.reason || '批次含越权操作，已整批拒绝';
      setHint((msg.resubmitDenied ? '仍被拒绝：' : '⚠ 批次已隔离：') + why, 12000);
      renderAll(); saveLocalSoon();
      return;
    }
    if (msg.t === 'batch-ack') {
      if (msg.kind === 'applied' || msg.kind === 'released') {
        if (msg.batchId) pendingBatches = pendingBatches.filter(b => b.batchId !== msg.batchId);
        renderAll(); saveLocalSoon();
      } else if (msg.kind === 'quarantined') {
        pendingBatches = pendingBatches.filter(b => b.ops.every(op =>
          !quarantines.some(q => q.qid === msg.qid && q.ops.some(qo => qo.opId === op.opId))));
        rebuildLocal(); renderAll(); saveLocalSoon();
      }
      return;
    }
    if (msg.t === 'gov') {
      myRole = msg.role; epoch = msg.epoch || epoch;
      members = msg.members || []; quarantines = msg.quarantine || quarantines;
      renderAll();
      return;
    }
    if (msg.t === 'audit') {
      for (const e of (msg.entries || [])) {
        if (!audit.some(a => a.seq === e.seq)) audit.push(e);
      }
      audit.sort((a, b) => a.seq - b.seq);
      renderAudit();
      return;
    }
    if (msg.t === 'member-result') {
      if (!msg.ok) setHint('成员操作被拒绝：' + (msg.error || '未知原因'), 8000);
      else if (msg.duplicated) setHint('该成员变更此前已生效（幂等）');
      return;
    }
    if (msg.t === 'presence') { $('#presence').textContent = msg.count + ' 人在线'; return; }
    if (msg.t === 'error') { setHint(msg.message || '请求被服务器拒绝', 8000); return; }
  }

  function upsertRev(rev) {
    const i = revs.findIndex(r => r.n === rev.n);
    if (i >= 0) revs[i] = rev; else revs.push(rev);
  }
  function upsertQuarantine(q) {
    const i = quarantines.findIndex(x => x.qid === q.qid);
    if (i >= 0) quarantines[i] = q; else quarantines.push(q);
  }

  // 本地产生一批操作：立即应用、入待裁队列、整批发送（批次=授权与隔离的最小单位）
  function pushOps(ops) {
    const batchOps = [];
    for (const op of ops) {
      if (!op) continue;
      op.opId = uid + '#' + (++opSeq);
      doc.apply(op);
      batchOps.push(op);
    }
    if (!batchOps.length) return;
    const batch = { batchId: 'b-' + uid + '-' + (++batchSeq), ops: batchOps, fromRole: myRole };
    pendingBatches.push(batch);
    send({ t: 'ops', batchId: batch.batchId, ops: batch.ops, lastKnownRole: batch.fromRole });
    saveLocalSoon();
  }

  // ---------- 角色能力 ----------
  const canProse = () => GOV.canProse(myRole);
  const canStyle = () => GOV.canStyle(myRole);
  const canAnnotate = () => GOV.canAnnotate(myRole);
  const canManage = () => GOV.canManage(myRole);
  const canRollback = () => GOV.canRollback(myRole);
  // 隔离批次是否可由当前的我整批重新提交
  function canResubmit(q) {
    if (q.uid !== uid || q.status === 'released') return false;
    return GOV.checkBatch(myRole, q.ops).allowed;
  }

  // ---------- 编辑器 <-> CRDT ----------
  const ed = $('#editor');

  function localEdit(nt) {
    const ot = curText;
    let p = 0;
    while (p < ot.length && p < nt.length && ot[p] === nt[p]) p++;
    let s = 0;
    while (s < ot.length - p && s < nt.length - p && ot[ot.length - 1 - s] === nt[nt.length - 1 - s]) s++;
    const ids = doc.visibleIds();
    const ops = [];
    for (let i = p; i < ot.length - s; i++) ops.push(doc.remove(ids[i]));
    let after = p > 0 ? ids[p - 1] : null;
    for (const ch of nt.slice(p, nt.length - s)) { const op = doc.insert(after, ch); after = op.id; ops.push(op); }
    pushOps(ops);
    curText = nt;
  }

  ed.addEventListener('compositionstart', () => { composing = true; });
  ed.addEventListener('compositionend', () => { composing = false; localEdit(ed.textContent); render(); });
  ed.addEventListener('input', () => {
    if (composing) return;
    if (ed.textContent === curText) return;
    localEdit(ed.textContent);
    render();
  });
  ed.addEventListener('paste', (e) => {
    e.preventDefault();
    const text = (e.clipboardData || window.clipboardData).getData('text/plain');
    document.execCommand('insertText', false, text);
  });
  ed.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); document.execCommand('insertText', false, '\n'); }
  });

  // ---------- 选区 <-> 偏移 ----------
  function offsetOf(node, off) {
    if (node === ed) {
      let acc = 0;
      for (let i = 0; i < off && i < ed.childNodes.length; i++) acc += ed.childNodes[i].textContent.length;
      return acc;
    }
    const w = document.createTreeWalker(ed, NodeFilter.SHOW_TEXT);
    let n, acc = 0;
    while ((n = w.nextNode())) {
      if (n === node) return acc + off;
      acc += n.textContent.length;
    }
    return acc;
  }
  function setCaret(off) {
    if (off == null) return;
    off = Math.min(off, ed.textContent.length);
    const w = document.createTreeWalker(ed, NodeFilter.SHOW_TEXT);
    let n, acc = 0;
    while ((n = w.nextNode())) {
      const l = n.textContent.length;
      if (acc + l >= off) {
        const sel = window.getSelection();
        const r = document.createRange();
        r.setStart(n, off - acc); r.collapse(true);
        sel.removeAllRanges(); sel.addRange(r);
        return;
      }
      acc += l;
    }
  }
  function getSelRange() {
    const sel = window.getSelection();
    if (!sel.rangeCount) return null;
    const r = sel.getRangeAt(0);
    if (!ed.contains(r.startContainer) || !ed.contains(r.endContainer)) return null;
    const s = offsetOf(r.startContainer, r.startOffset);
    const e = offsetOf(r.endContainer, r.endOffset);
    if (s === e) return null;
    return [Math.min(s, e), Math.max(s, e)];
  }
  function anchorsFor(s, e) {
    const ids = doc.visibleIds();
    const st = s <= 0 ? { id: null, edge: 's' } : { id: ids[s], edge: 's' };
    const en = e >= ids.length ? { id: null, edge: 'e' } : { id: ids[e - 1], edge: 'e' };
    return [st, en];
  }

  // ---------- 渲染 ----------
  function esc(s) { return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

  function segments() {
    const text = doc.text();
    const bounds = new Set([0, text.length]);
    const marks = [], coms = [];
    for (const m of doc.activeMarks()) { bounds.add(m.s); bounds.add(m.e); marks.push(m); }
    for (const c of doc.commentList()) {
      if (c.resolved) continue;
      const r = doc.resolveComment(c);
      if (r.e > r.s) { bounds.add(r.s); bounds.add(r.e); coms.push({ id: c.id, s: r.s, e: r.e, status: r.status }); }
    }
    const pts = [...bounds].sort((a, b) => a - b);
    const segs = [];
    for (let i = 0; i < pts.length - 1; i++) {
      const s = pts[i], e = pts[i + 1];
      const byKey = {};
      for (const m of marks) {
        if (m.s <= s && m.e >= e) {
          for (const k in m.attrs) {
            const prev = byKey[k];
            if (!prev || m.ts > prev.ts || (m.ts === prev.ts && m.by > prev.by)) byKey[k] = { v: m.attrs[k], ts: m.ts, by: m.by };
          }
        }
      }
      const attrs = {};
      for (const k in byKey) attrs[k] = byKey[k].v;
      const covering = coms.filter(c => c.s <= s && c.e >= e);
      segs.push({ s, e, text: text.slice(s, e), attrs, cids: covering.map(c => c.id), cstatus: covering.length ? covering[0].status : null });
    }
    return { text, segs };
  }

  function render() {
    const { text, segs } = segments();
    let html = '';
    for (const g of segs) {
      const cls = [];
      if (g.attrs.bold) cls.push('b');
      if (g.attrs.italic) cls.push('i');
      if (g.attrs.underline) cls.push('u');
      if (g.cids.length) cls.push('chl', 'cs-' + g.cstatus);
      const data = g.cids.length ? ' data-cids="' + g.cids.join(',') + '"' : '';
      html += cls.length ? '<span class="' + cls.join(' ') + '"' + data + '>' + esc(g.text) + '</span>' : esc(g.text);
    }
    if (ed.innerHTML !== html) {
      const caret = canProse() ? getSelectionOffset() : null;
      ed.innerHTML = html;
      if (caret != null) setCaret(caret);
    }
    curText = text;
  }
  function getSelectionOffset() {
    const sel = window.getSelection();
    if (!sel.rangeCount || !ed.contains(sel.anchorNode)) return null;
    return offsetOf(sel.anchorNode, sel.anchorOffset);
  }

  const STATUS_INFO = {
    'anchored': ['已锚定', 'ok'],
    'split-insert': ['已拆分 · 锚定文字中插入了新内容', 'warn'],
    'split-delete': ['部分悬空 · 部分锚定文字被删除', 'warn'],
    'orphan-deleted': ['悬空 · 锚定文字已被删除', 'bad'],
    'changed': ['已变更 · 锚定文字被修改', 'warn'],
    'resolved': ['已解决', 'muted'],
  };

  function renderComments() {
    const box = $('#comments');
    const list = doc.commentList().sort((a, b) => b.ts - a.ts);
    box.innerHTML = '';
    if (!list.length) { box.innerHTML = '<div class="empty">暂无评论。选中文字后点击"评论选中文字"。</div>'; return; }
    for (const c of list) {
      const r = doc.resolveComment(c);
      const [label, cls] = STATUS_INFO[r.status] || ['未知', 'muted'];
      const card = document.createElement('div');
      card.className = 'card';
      card.dataset.cid = c.id;
      let inner = '<div class="head"><span><span class="who">' + esc(c.by) + '</span>' +
        '<span class="badge ' + cls + '">' + label + '</span></span>' +
        '<span class="time">' + new Date(c.ts).toLocaleString() + '</span></div>';
      inner += '<div class="quote"><span class="lbl">原始锚定文字</span>' + (c.quote ? esc(c.quote) : '（空）') + '</div>';
      if (r.status !== 'anchored' && r.status !== 'resolved') {
        inner += '<div class="cur"><span class="lbl">当前覆盖文字</span>' + (r.cur ? esc(r.cur) : '（已被删除）') + '</div>';
      }
      inner += '<div class="ctext">' + esc(c.text) + '</div>';
      inner += '<div class="ops"></div>';
      card.innerHTML = inner;
      const ops = card.querySelector('.ops');
      // 批注命令仅 owner/editor/commenter 可见可调用
      if (canAnnotate()) {
        if (!c.resolved) {
          const re = document.createElement('button');
          re.textContent = '重新挂接';
          re.title = '选择新的文字范围来锚定这条评论';
          re.onclick = () => { reattachFor = c.id; setHint('请在文档中选中新的锚定文字…'); ed.focus(); };
          ops.appendChild(re);
          const done = document.createElement('button');
          done.textContent = '标记解决';
          done.onclick = () => { pushOps([doc.updateComment(c.id, { resolved: true })]); renderAll(); };
          ops.appendChild(done);
        } else {
          const reopen = document.createElement('button');
          reopen.textContent = '重新打开';
          reopen.onclick = () => { pushOps([doc.updateComment(c.id, { resolved: false })]); renderAll(); };
          ops.appendChild(reopen);
        }
      }
      box.appendChild(card);
    }
  }

  function renderRevs() {
    const box = $('#revs');
    box.innerHTML = '';
    if (!revs.length) { box.innerHTML = '<div class="empty">暂无修订记录。</div>'; return; }
    const list = [...revs].sort((a, b) => b.n - a.n);
    for (const r of list) {
      const div = document.createElement('div');
      div.className = 'rev';
      const kind = r.kind === 'restore' ? '<span class="kind-restore">恢复</span> 至 #' + r.target : '编辑';
      div.innerHTML = '<span class="n">#' + r.n + '</span><div class="body">' +
        '<div class="sum">' + esc(r.summary) + '</div>' +
        '<div class="sub">' + kind + ' · ' + esc(r.by) + ' · ' + new Date(r.ts).toLocaleString() + '</div></div>';
      const btn = document.createElement('button');
      if (canRollback()) {
        btn.textContent = '恢复此版本';
        btn.onclick = () => {
          if (!online) { setHint('离线状态下无法恢复版本'); return; }
          if (confirm('确定要恢复到修订 #' + r.n + ' 吗？\n恢复会作为新修订与他人修改合并，不会丢失他人内容。')) {
            send({ t: 'restore', rev: r.n });
          }
        };
      } else {
        btn.textContent = '仅所有者可回滚';
        btn.disabled = true;
        btn.title = '回滚到检查点是所有者专属命令';
      }
      div.appendChild(btn);
      box.appendChild(div);
    }
  }

  // ---------- 成员面板 ----------
  function renderMembers() {
    const box = $('#memberList');
    box.innerHTML = '';
    const list = [...members].sort((a, b) => {
      const order = { owner: 0, editor: 1, commenter: 2, viewer: 3 };
      return (order[a.role] - order[b.role]) || a.name.localeCompare(b.name);
    });
    for (const m of list) {
      const row = document.createElement('div');
      row.className = 'member' + (m.uid === uid ? ' me' : '');
      let html = '<span class="mname">' + esc(m.name) + (m.uid === uid ? '（我）' : '') + '</span>' +
        '<span class="mrole role-' + m.role + '">' + ROLE_LABEL[m.role] + '</span>';
      row.innerHTML = html;
      if (canManage()) {
        const sel = document.createElement('select');
        for (const role of GOV.ROLES) {
          const o = document.createElement('option');
          o.value = role; o.textContent = ROLE_LABEL[role];
          if (role === m.role) o.selected = true;
          sel.appendChild(o);
        }
        const isLastOwner = m.role === 'owner' && members.filter(x => x.role === 'owner').length <= 1;
        if (isLastOwner) sel.disabled = true;
        sel.onchange = () => {
          const newRole = sel.value;
          sendMember('role', m.uid, newRole, null);
          setTimeout(renderMembers, 300);
        };
        row.appendChild(sel);
        const rm = document.createElement('button');
        rm.textContent = '移除';
        rm.className = 'danger';
        if (isLastOwner) rm.disabled = true;
        rm.onclick = () => {
          if (confirm('确定将 ' + m.name + ' 移出空间？其待提交批次将被拒绝。')) sendMember('remove', m.uid, null, null);
        };
        row.appendChild(rm);
      }
      box.appendChild(row);
    }
    $('#inviteBox').classList.toggle('hidden', !canManage());
  }
  let mSeq = 0;
  function sendMember(action, targetUid, role, name) {
    const mId = 'm-' + uid + '-' + Date.now().toString(36) + '-' + (++mSeq);
    send({ t: 'member', mId, action, uid: targetUid, role, name });
  }
  $('#inviteBtn').addEventListener('click', () => {
    const tuid = $('#inviteUid').value.trim();
    const tname = $('#inviteName').value.trim();
    const role = $('#inviteRole').value;
    if (!tuid) { setHint('请填写被邀请者的用户 ID'); return; }
    sendMember('invite', tuid, role, tname || tuid);
    $('#inviteUid').value = ''; $('#inviteName').value = '';
    setHint('邀请已发送；对方需以用户 ID「' + tuid + '」连接');
  });
  $('#myName').addEventListener('change', () => {
    const v = $('#myName').value.trim();
    if (!v) { $('#myName').value = displayName; return; }
    displayName = v; localStorage.setItem('uname', displayName);
    send({ t: 'rename', name: displayName });
  });

  // ---------- 隔离草稿面板 ----------
  function renderQuarantine() {
    const box = $('#quarantineList');
    box.innerHTML = '';
    const list = quarantines.filter(q => q.status !== 'released');
    $('#quarantineSec').classList.toggle('hidden', !list.length);
    if (!list.length) return;
    for (const q of list) {
      const card = document.createElement('div');
      card.className = 'qcard' + (q.uid === uid ? ' mine' : '');
      const kinds = Object.entries(q.kinds || {}).map(([k, n]) => (GOV.KIND_LABEL[k] || k) + '×' + n).join('、');
      let transition = '';
      if (q.removed) {
        transition = '<div class="qtrans">你在断连期间已被<b>移出空间</b>（授权纪元 ' + q.epoch +
          '）。草稿完整保留；重新被邀请后可显式重新提交。</div>';
      } else if (q.fromRole && q.fromRole !== q.role) {
        transition = '<div class="qtrans">角色变化：<b>' + ROLE_LABEL[q.fromRole] + '</b> → <b>' +
          ROLE_LABEL[q.role] + '</b>（授权纪元 ' + q.epoch + '）</div>';
      }
      card.innerHTML =
        '<div class="qhead"><span class="qwho">' + esc(q.name) + (q.uid === uid ? '（我）' : '') + '</span>' +
        '<span class="role-' + q.role + ' qrole">' + ROLE_LABEL[q.role] + '</span>' +
        '<span class="qtime">' + new Date(q.ts).toLocaleString() + '</span></div>' +
        '<div class="qreason">' + esc(q.reason) + '</div>' + transition +
        '<div class="qmeta">共 ' + q.ops.length + ' 个操作' + (kinds ? '：' + esc(kinds) : '') +
        '；批次 ' + esc(q.batchId) + '</div>' +
        '<textarea class="qjson" readonly rows="6" spellcheck="false"></textarea>' +
        '<div class="qops"></div>';
      card.querySelector('.qjson').value = JSON.stringify(q.ops, null, 2);
      const qops = card.querySelector('.qops');

      const copy = document.createElement('button');
      copy.textContent = '复制 JSON';
      copy.onclick = async () => {
        const ta = card.querySelector('.qjson');
        try { await navigator.clipboard.writeText(ta.value); setHint('隔离批次 JSON 已复制'); }
        catch (e) { ta.focus(); ta.select(); document.execCommand('copy'); setHint('已全选，可手动复制'); }
      };
      qops.appendChild(copy);

      if (q.uid === uid) {
        const sub = document.createElement('button');
        if (canResubmit(q)) {
          sub.textContent = '权限已恢复 · 显式重新提交整批';
          sub.className = 'primary';
          sub.onclick = () => {
            if (!online) { setHint('离线时不能重新提交，请先恢复连接'); return; }
            sub.disabled = true;
            send({ t: 'resubmit', qid: q.qid, batchId: 'rsb-' + uid + '-' + Date.now().toString(36) });
            setHint('正在按当前授权纪元重新提交隔离批次…');
          };
        } else {
          sub.textContent = '权限不足，无法重新提交';
          sub.disabled = true;
          const need = q.violations && q.violations[0] ? q.violations[0].kind : null;
          sub.title = need === 'annotation' ? '需要评论者或更高角色' : '需要编辑者或更高角色';
        }
        qops.appendChild(sub);
      }
      box.appendChild(card);
    }
  }

  // ---------- 审计台账面板 ----------
  const AUDIT_INFO = {
    'space-upgrade': ['空间升级', 'ok'],
    'member-invite': ['邀请成员', 'ok'],
    'member-role': ['角色变更', 'warn'],
    'member-remove': ['移除成员', 'bad'],
    'member-denied': ['越权管理被拒', 'bad'],
    'batch-quarantined': ['批次隔离', 'bad'],
    'quarantine-released': ['隔离批次重新提交', 'ok'],
    'rollback': ['回滚检查点', 'warn'],
    'rollback-denied': ['越权回滚被拒', 'bad'],
  };
  function renderAudit() {
    const box = $('#auditList');
    box.innerHTML = '';
    const list = [...audit].sort((a, b) => b.seq - a.seq).slice(0, 200);
    if (!list.length) { box.innerHTML = '<div class="empty">暂无治理记录。</div>'; return; }
    for (const e of list) {
      const [label, cls] = AUDIT_INFO[e.type] || [e.type, 'muted'];
      const row = document.createElement('div');
      row.className = 'audit-row';
      row.innerHTML = '<span class="aseq">#' + e.seq + '</span>' +
        '<span class="badge ' + cls + '">' + label + '</span>' +
        '<span class="adetail">' + esc(e.detail || '') + '</span>' +
        '<span class="atime">' + new Date(e.ts).toLocaleTimeString() + '</span>';
      box.appendChild(row);
    }
  }

  function setStatus() {
    const el = $('#status');
    el.className = online ? 'online' : 'offline';
    const roleTxt = myRole ? ROLE_LABEL[myRole] : '未加入';
    $('#roleBadge').textContent = roleTxt;
    $('#roleBadge').className = 'role-badge role-' + (myRole || 'none');
    $('#statusText').textContent = online
      ? '已连接 · 纪元 ' + epoch + (pendingBatches.length ? ' · 同步中(' + pendingBatches.length + '批)' : '')
      : '离线 · 批次保存在本地' + (pendingBatches.length ? '（待同步 ' + pendingBatches.length + ' 批）' : '');
  }
  let hintTimer = null;
  function setHint(t, ms) {
    $('#hint').textContent = t || '';
    clearTimeout(hintTimer);
    if (t) hintTimer = setTimeout(() => { $('#hint').textContent = ''; }, ms || 6000);
  }
  function renderAll() { render(); renderComments(); renderRevs(); renderMembers(); renderQuarantine(); renderAudit(); applyGates(); setStatus(); }

  // ---------- 命令级角色门控：未授权命令不可见/不可用 ----------
  function applyGates() {
    ed.contentEditable = canProse() ? 'true' : 'false';
    ed.classList.toggle('readonly', !canProse());
    $$('#toolbar [data-fmt]').forEach(b => { b.disabled = !canStyle(); });
    $('#commentBtn').disabled = !canAnnotate();
    $('#commentBtn').title = canAnnotate() ? '' : '仅评论者或更高角色可批注';
    // 正在编辑显示名时不要用外部值覆盖输入框
    if (document.activeElement !== $('#myName')) $('#myName').value = displayName;
  }

  // ---------- 工具栏 ----------
  document.querySelectorAll('#toolbar [data-fmt]').forEach(btn => {
    btn.addEventListener('click', () => {
      if (!canStyle()) { setHint('当前角色不能修改样式'); return; }
      const range = getSelRange();
      if (!range) { setHint('请先选中一段文字'); return; }
      const key = btn.dataset.fmt;
      const attrs = {}; attrs[key] = !selAttrs(range[0], range[1])[key];
      const [st, en] = anchorsFor(range[0], range[1]);
      pushOps([doc.mark(st, en, attrs)]);
      renderAll();
    });
  });
  function selAttrs(s, e) {
    const { segs } = segments();
    const res = { bold: true, italic: true, underline: true };
    let any = false;
    for (const g of segs) {
      if (g.e <= s || g.s >= e) continue;
      any = true;
      for (const k in res) res[k] = res[k] && !!g.attrs[k];
    }
    return any ? res : { bold: false, italic: false, underline: false };
  }

  $('#commentBtn').addEventListener('click', () => {
    if (!canAnnotate()) { setHint('当前角色不能添加批注'); return; }
    const range = getSelRange();
    if (!range) { setHint('请先选中要评论的文字'); return; }
    pendingCommentRange = range;
    $('#ncQuote').textContent = doc.text().slice(range[0], range[1]);
    $('#newComment').classList.remove('hidden');
    $('#ncText').value = '';
    $('#ncText').focus();
  });
  $('#ncCancel').addEventListener('click', () => { pendingCommentRange = null; $('#newComment').classList.add('hidden'); });
  $('#ncSubmit').addEventListener('click', () => {
    const text = $('#ncText').value.trim();
    if (!text || !pendingCommentRange) return;
    const [s, e] = pendingCommentRange;
    const [st, en] = anchorsFor(s, e);
    pushOps([doc.comment(st, en, text, doc.text().slice(s, e))]);
    pendingCommentRange = null;
    $('#newComment').classList.add('hidden');
    renderAll();
  });

  // 重新挂接：下一次在编辑器中的选区成为新锚点（仅批注权限者可触发）
  ed.addEventListener('mouseup', () => {
    if (!reattachFor) return;
    if (!canAnnotate()) { reattachFor = null; return; }
    const range = getSelRange();
    if (!range) return;
    const [s, e] = range;
    const [st, en] = anchorsFor(s, e);
    const quote = doc.text().slice(s, e);
    pushOps([doc.updateComment(reattachFor, { start: st, end: en, quote })]);
    setHint('评论已重新挂接到：「' + (quote.length > 20 ? quote.slice(0, 20) + '…' : quote) + '」');
    reattachFor = null;
    renderAll();
  });

  // 点击高亮文字 -> 定位评论卡片
  ed.addEventListener('click', (e) => {
    const span = e.target.closest && e.target.closest('.chl');
    if (!span) return;
    const cid = (span.dataset.cids || '').split(',')[0];
    const card = document.querySelector('.card[data-cid="' + cid + '"]');
    if (card) {
      card.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
      card.classList.add('flash');
      setTimeout(() => card.classList.remove('flash'), 1200);
    }
  });

  window.addEventListener('beforeunload', saveLocal);
  setInterval(saveLocal, 5000);

  renderAll();
  connect();
})();
