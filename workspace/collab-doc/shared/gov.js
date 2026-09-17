'use strict';
/*
 * 空间成员治理 —— 角色 / 权限判定内核（浏览器与 Node 通用，零依赖）。
 *
 * 四种角色：
 *   owner     所有者：文字、格式、评论、回滚、邀请成员、调整角色
 *   editor    编辑者：文字（prose）与样式（style），也可评论
 *   commenter 评论者：仅批注（annotation：创建 / 解决 / 重新挂接）
 *   viewer    只读者：只读（未加入空间的访客按只读处理）
 *
 * 批次（batch）是授权与隔离的最小单位：一批操作要么全部放行，要么整批拒绝，
 * 绝不允许"部分泄漏"。
 */
(function (root, factory) {
  const mod = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = mod;
  else root.GOV = mod;
})(typeof self !== 'undefined' ? self : this, function () {

  const ROLES = ['owner', 'editor', 'commenter', 'viewer'];
  const ROLE_LABEL = {
    owner: '所有者', editor: '编辑者', commenter: '评论者', viewer: '只读者',
  };
  // 操作类别：prose 文字 / style 样式 / annotation 批注
  function opKind(op) {
    if (!op || typeof op !== 'object') return null;
    if (op.t === 'ins' || op.t === 'del') return 'prose';
    if (op.t === 'mark') return 'style';
    if (op.t === 'com') return 'annotation';
    return null;
  }
  const KIND_LABEL = { prose: '文字编辑', style: '样式修改', annotation: '批注操作' };

  function canProse(role) { return role === 'owner' || role === 'editor'; }
  function canStyle(role) { return role === 'owner' || role === 'editor'; }
  function canAnnotate(role) { return role === 'owner' || role === 'editor' || role === 'commenter'; }
  function canManage(role) { return role === 'owner'; }   // 邀请 / 改角色 / 移除
  function canRollback(role) { return role === 'owner'; } // 仅所有者可回滚检查点

  function canApplyOp(role, op) {
    const k = opKind(op);
    if (k === 'prose') return canProse(role);
    if (k === 'style') return canStyle(role);
    if (k === 'annotation') return canAnnotate(role);
    return false; // 无法识别的操作一律拒绝（整批原子拒绝）
  }

  // 整批授权：任一操作越权 => 整批拒绝，返回每个越权操作的原因
  function checkBatch(role, ops) {
    const violations = [];
    ops.forEach((op, i) => {
      if (!canApplyOp(role, op)) {
        violations.push({ i, kind: opKind(op), need: 'role', have: role || null });
      }
    });
    return { allowed: violations.length === 0, violations };
  }

  function isValidRole(r) { return ROLES.indexOf(r) >= 0; }

  return {
    ROLES, ROLE_LABEL, KIND_LABEL, opKind,
    canProse, canStyle, canAnnotate, canManage, canRollback,
    canApplyOp, checkBatch, isValidRole,
  };
});
