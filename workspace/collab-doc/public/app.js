/* global CRDT */
'use strict';
/*
 * 前端逻辑：
 * - 本地即真：所有编辑先应用到本地 CRDT，再异步同步；断网时操作进入 localStorage 队列
 * - 重连后：用 lastSeq 拉取缺失操作 + 补发离线队列；CRDT 保证合并不覆盖他人内容
 * - 编辑器为 contenteditable，用"公共前后缀 diff"把 DOM 变化转成插入/删除操作
 */
(() => {
  const $ = (s) => document.querySelector(s);
  const docName = new URLSearchParams(location.search).get('doc') || 'default';
  const clientId = localStorage.getItem('cid') ||
    (crypto.randomUUID ? crypto.randomUUID() : 'c-' + Math.random().toString(36).slice(2));
  localStorage.setItem('cid', clientId);
  const LS_KEY = 'collab:' + docName;
  const LOCAL_OPS_CAP = 20000; // 超出则放弃本地缓存，下次从服务器全量同步

  const doc = new CRDT.Doc(clientId);
  let lastSeq = 0, opSeq = 0, allOps = [], pending = [], revs = [];
  let ws = null, online = false, reconnectDelay = 500;
  let curText = '';
  let reattachFor = null;        // 等待重新挂接选区的评论 id
  let pendingCommentRange = null; // 等待填写内容的评论选区
  let composing = false;

  // ---------- 本地持久化（离线可编辑） ----------
  function saveLocal() {
    try {
      if (allOps.length > LOCAL_OPS_CAP) { localStorage.removeItem(LS_KEY); return; }
      localStorage.setItem(LS_KEY, JSON.stringify({ lastSeq, opSeq, allOps, pending }));
    } catch (e) { /* 存储满则忽略 */ }
  }
  let saveTimer = null;
  function saveLocalSoon() { clearTimeout(saveTimer); saveTimer = setTimeout(saveLocal, 300); }
  (function loadLocal() {
    try {
      const j = JSON.parse(localStorage.getItem(LS_KEY) || 'null');
      if (!j) return;
      lastSeq = j.lastSeq || 0; opSeq = j.opSeq || 0;
      allOps = j.allOps || []; pending = j.pending || [];
      for (const op of allOps) doc.apply(op);
      for (const op of pending) doc.apply(op);
    } catch (e) { /* 忽略损坏缓存 */ }
  })();

  // ---------- 网络 ----------
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(proto + '://' + location.host + '/ws');
    ws.onopen = () => {
      online = true; reconnectDelay = 500; setStatus();
      ws.send(JSON.stringify({ t: 'hello', clientId, doc: docName, lastSeq }));
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
      const seen = new Set();
      for (const { seq, op } of msg.ops) {
        doc.apply(op); allOps.push(op);
        lastSeq = Math.max(lastSeq, seq);
        if (op.opId) seen.add(op.opId);
      }
      // 服务器已确认过的离线操作无需补发
      pending = pending.filter(p => !seen.has(p.opId));
      revs = msg.revs || [];
      if (pending.length) send({ t: 'ops', ops: pending });
      renderAll(); saveLocalSoon();
    } else if (msg.t === 'ops') {
      for (const { seq, op } of msg.applied) {
        doc.apply(op); allOps.push(op);
        lastSeq = Math.max(lastSeq, seq);
        if (op.opId) {
          const i = pending.findIndex(p => p.opId === op.opId);
          if (i >= 0) pending.splice(i, 1); // 自己的操作被服务器确认
        }
      }
      if (msg.rev) upsertRev(msg.rev);
      renderAll(); saveLocalSoon();
    } else if (msg.t === 'presence') {
      $('#presence').textContent = msg.count + ' 人在线';
    } else if (msg.t === 'error') {
      setHint(msg.message || '出错了');
    }
  }

  function upsertRev(rev) {
    const i = revs.findIndex(r => r.n === rev.n);
    if (i >= 0) revs[i] = rev; else revs.push(rev);
  }

  // 本地产生一个操作：立即应用、入队、尝试发送
  function pushOps(ops) {
    const batch = [];
    for (const op of ops) {
      if (!op) continue;
      op.opId = clientId + '#' + (++opSeq);
      doc.apply(op);
      pending.push(op);
      batch.push(op);
    }
    if (batch.length) { send({ t: 'ops', ops: batch }); saveLocalSoon(); }
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
  // 粘贴纯文本化，换行行为统一
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
  function getCaret() {
    const sel = window.getSelection();
    if (!sel.rangeCount || !ed.contains(sel.anchorNode)) return null;
    return offsetOf(sel.anchorNode, sel.anchorOffset);
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
  function esc(s) { return s.replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

  // 把文本按格式/评论边界切成段，每段解析最终格式（逐属性 LWW）
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
      const caret = getCaret();
      ed.innerHTML = html;
      setCaret(caret);
    }
    curText = text;
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
      if (!c.resolved) {
        const re = document.createElement('button');
        re.textContent = '重新挂接';
        re.title = '选择新的文字范围来锚定这条评论';
        re.onclick = () => {
          reattachFor = c.id;
          setHint('请在文档中选中新的锚定文字…');
          ed.focus();
        };
        ops.appendChild(re);
        const done = document.createElement('button');
        done.textContent = '标记解决';
        done.onclick = () => pushOps([doc.updateComment(c.id, { resolved: true })]) || renderAll();
        ops.appendChild(done);
      } else {
        const reopen = document.createElement('button');
        reopen.textContent = '重新打开';
        reopen.onclick = () => pushOps([doc.updateComment(c.id, { resolved: false })]) || renderAll();
        ops.appendChild(reopen);
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
      const kind = r.kind === 'restore'
        ? '<span class="kind-restore">恢复</span> 至 #' + r.target
        : '编辑';
      div.innerHTML = '<span class="n">#' + r.n + '</span><div class="body">' +
        '<div class="sum">' + esc(r.summary) + '</div>' +
        '<div class="sub">' + kind + ' · ' + esc(r.by) + ' · ' + new Date(r.ts).toLocaleString() + '</div></div>';
      const btn = document.createElement('button');
      btn.textContent = '恢复此版本';
      btn.onclick = () => {
        if (!online) { setHint('离线状态下无法恢复版本'); return; }
        if (confirm('确定要恢复到修订 #' + r.n + ' 吗？\n恢复会作为新修订与他人修改合并，不会丢失他人内容。')) {
          send({ t: 'restore', rev: r.n });
        }
      };
      div.appendChild(btn);
      box.appendChild(div);
    }
  }

  function setStatus() {
    const el = $('#status');
    el.className = online ? 'online' : 'offline';
    $('#statusText').textContent = online
      ? '已连接' + (pending.length ? ' · 同步中(' + pending.length + ')' : '')
      : '离线 · 编辑已保存在本地' + (pending.length ? '（待同步 ' + pending.length + ' 条）' : '');
  }
  let hintTimer = null;
  function setHint(t) {
    $('#hint').textContent = t || '';
    clearTimeout(hintTimer);
    if (t) hintTimer = setTimeout(() => { $('#hint').textContent = ''; }, 6000);
  }
  function renderAll() { render(); renderComments(); renderRevs(); setStatus(); }

  // ---------- 工具栏 ----------
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
  document.querySelectorAll('#toolbar [data-fmt]').forEach(btn => {
    btn.addEventListener('click', () => {
      const range = getSelRange();
      if (!range) { setHint('请先选中一段文字'); return; }
      const key = btn.dataset.fmt;
      const cur = selAttrs(range[0], range[1]);
      const [st, en] = anchorsFor(range[0], range[1]);
      const attrs = {}; attrs[key] = !cur[key];
      pushOps([doc.mark(st, en, attrs)]);
      renderAll();
    });
  });

  $('#commentBtn').addEventListener('click', () => {
    const range = getSelRange();
    if (!range) { setHint('请先选中要评论的文字'); return; }
    pendingCommentRange = range;
    $('#ncQuote').textContent = doc.text().slice(range[0], range[1]);
    $('#newComment').classList.remove('hidden');
    $('#ncText').value = '';
    $('#ncText').focus();
  });
  $('#ncCancel').addEventListener('click', () => {
    pendingCommentRange = null;
    $('#newComment').classList.add('hidden');
  });
  $('#ncSubmit').addEventListener('click', () => {
    const text = $('#ncText').value.trim();
    if (!text || !pendingCommentRange) return;
    const [s, e] = pendingCommentRange;
    const [st, en] = anchorsFor(s, e);
    const quote = doc.text().slice(s, e);
    pushOps([doc.comment(st, en, text, quote)]);
    pendingCommentRange = null;
    $('#newComment').classList.add('hidden');
    renderAll();
  });

  // 重新挂接：下一次在编辑器中的选区成为新锚点
  ed.addEventListener('mouseup', () => {
    if (!reattachFor) return;
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
