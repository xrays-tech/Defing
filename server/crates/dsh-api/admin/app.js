'use strict';
/* ============================================================
   Defing 配置中心 Admin UI —— 外置脚本（D-CSP：无 inline script / onclick）
   - 事件经 data-act 委托（click / change），动作注册于 actions 表
   - 所有服务端/用户数据插入 DOM 前经 esc() 转义或走 textContent
   - API 端点 / 请求响应形状 / Bearer 鉴权与旧版完全一致
   ============================================================ */

/* ---------------- 基础工具 ---------------- */
const $ = (id) => document.getElementById(id);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function esc(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/"/g, '&quot;')
    .replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/'/g, '&#39;');
}
const rid = () => 'ui-' + Date.now() + '-' + Math.floor(Math.random() * 1e6);
const fmtTime = (ms) => (ms ? new Date(ms).toLocaleString('zh-CN', { hour12: false }) : '—');
const skeleton = (n) => Array.from({ length: n }, () => '<div class="skel"></div>').join('');

/* ---------------- 剪贴板复制 ----------------
   Clipboard API 仅在安全上下文（HTTPS / localhost）可用；HTTP 部署下 navigator.clipboard
   为 undefined，直接调用会同步抛错。统一封装：Clipboard API 优先，失败回退
   textarea + execCommand('copy')（同为用户手势内同步执行）。 */
async function copyText(txt) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try { await navigator.clipboard.writeText(txt); return true; } catch (_) { /* 权限拒绝等 → 走回退 */ }
  }
  try {
    const ta = document.createElement('textarea');
    ta.value = txt;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.top = '-9999px';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    ta.setSelectionRange(0, ta.value.length); // iOS Safari 兼容
    const ok = document.execCommand('copy');
    ta.remove();
    return !!ok;
  } catch (_) { return false; }
}

/* ---------------- 状态 ---------------- */
const LS_TOKEN = 'dsh_admin_token', LS_ROLE = 'dsh_admin_role', LS_PROJ = 'dsh_admin_project', LS_THEME = 'dsh_theme';
const S = {
  token: '', role: '', roleProject: '',
  view: 'config', pane: 'draft',
  projects: [], project: '', branches: [], branch: '',
  version: 0, structV: 0, draftRev: 0, gray: null,
  // 未保存编辑保护：结构 textarea / 灰度规则有用户输入时，后台刷新不覆盖
  structDirty: false, structProj: '',
  grayDirty: false, grayBranch: '',
  // 保存状态指示（草稿/结构/共享库）：未保存 dirty 标记 + 已保存未发布计数
  draftDirty: false,        // 草稿页有未保存输入
  sharedDirty: false,       // 共享库表单有未保存输入
  sharedDraftCount: 0,      // 已保存未发布的共享草稿数
  hasStructDraft: false,    // 服务端存在已保存未发布的结构草稿
  // 结构：编辑器工作副本 / 已发布结构（GET /structure 实时拉取，权威）
  structDraft: null,          // {base_version, groups:[{name, items:[...]}]}
  pubStruct: null,            // {version, groups} 最近拉取的已发布结构（值新增级联首选源 + 无草稿时的基线）
  defs: { src: 'none', groups: [] }, // 值新增选择器数据源：①已发布 > ②结构草稿 > ③自由输入回退
};

/* ---------------- 请求层 ---------------- */
function authHeaders() {
  const h = { 'Content-Type': 'application/json' };
  if (S.token) h.Authorization = 'Bearer ' + S.token;
  return h;
}

async function j(method, url, body) {
  const r = await fetch(url, { method, headers: authHeaders(), body: body === undefined ? undefined : JSON.stringify(body) });
  if (r.status === 401 && !url.endsWith('/login')) sessionExpired();
  if (!r.ok) {
    let detail = '';
    try { detail = (await r.json()).message || ''; } catch (_) { /* 非 JSON 错误体 */ }
    const e = new Error('HTTP ' + r.status + (detail ? ' ' + detail : ''));
    e.status = r.status;
    e.expired = r.status === 401 && !url.endsWith('/login');
    throw e;
  }
  if (r.status === 204) return null;
  return r.json();
}

async function jtext(url) {
  const r = await fetch(url, { headers: authHeaders() });
  if (r.status === 401) sessionExpired();
  if (!r.ok) {
    let detail = '';
    try { detail = (await r.json()).message || ''; } catch (_) { /* 非 JSON 错误体 */ }
    const e = new Error('HTTP ' + r.status + (detail ? ' ' + detail : ''));
    e.expired = r.status === 401;
    throw e;
  }
  return r.text();
}

function sessionExpired() {
  if (!S.token) return;
  S.token = '';
  localStorage.removeItem(LS_TOKEN); localStorage.removeItem(LS_ROLE); localStorage.removeItem(LS_PROJ);
  $('app').classList.add('hidden');
  $('login-view').classList.remove('hidden');
  toast('会话已过期，请重新登录', 'err');
}

/* 请求进行中禁用按钮并显示加载态 */
async function withBusy(el, fn) {
  if (el && el.setAttribute) { el.setAttribute('data-busy', ''); el.disabled = true; }
  try { return await fn(); }
  finally { if (el && el.removeAttribute) { el.removeAttribute('data-busy'); el.disabled = false; } }
}

/* ---------------- 统一错误弹窗（报错一律在屏幕中心以模态弹窗展示） ---------------- */
function showErrorModal(message, title) {
  $('err-title').textContent = title || '操作失败';
  $('err-msg').textContent = message || '';
  $('err-overlay').classList.remove('hidden');
  const ok = $('err-ok');
  if (ok) ok.focus();
}
function closeErrorModal() {
  $('err-overlay').classList.add('hidden');
}

/* ---------------- 通知 ---------------- */
function toast(text, kind = 'ok', ms) {
  // 报错统一走屏幕中心错误弹窗；toast 仅保留成功 / 警告
  if (kind === 'err') { showErrorModal(text); return; }
  const box = document.createElement('div');
  box.className = 'toast ' + kind;
  const icon = kind === 'warn' ? 'i-info' : 'i-check';
  box.innerHTML =
    '<svg class="ic t-ic"><use href="#' + icon + '"/></svg>' +
    '<div class="toast-text"></div>' +
    '<button type="button" class="toast-x" title="关闭" aria-label="关闭"><svg class="ic ic-xs"><use href="#i-x"/></svg></button>';
  box.querySelector('.toast-text').textContent = text;
  $('toasts').appendChild(box);
  let gone = false;
  const kill = () => { if (gone) return; gone = true; box.classList.add('out'); setTimeout(() => box.remove(), 160); };
  box.querySelector('.toast-x').addEventListener('click', kill);
  setTimeout(kill, ms || 4000);
}

/* ---------------- 表单校验错误（已统一为屏幕中心错误弹窗） ---------------- */
function showErr(id, text) {
  showErrorModal(text);
}
function hideErr(id) { const el = $(id); if (el) el.classList.add('hidden'); }

/* ---------------- 主题 ---------------- */
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  $('btn-theme-moon').classList.toggle('hidden', t !== 'light');
  $('btn-theme-sun').classList.toggle('hidden', t !== 'dark');
}
function initTheme() {
  const mql = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
  applyTheme(localStorage.getItem(LS_THEME) || (mql && mql.matches ? 'dark' : 'light'));
}

/* ---------------- 弹窗（确认 / 输入） ---------------- */
let modalCb = null, modalCancelCb = null;
function openModal(o) {
  $('modal-title').textContent = o.title || '确认';
  const msg = $('modal-msg');
  if (o.message) { msg.textContent = o.message; msg.classList.remove('hidden'); }
  else msg.classList.add('hidden');
  const field = $('modal-field'), input = $('modal-input'), label = $('modal-label');
  if (o.input) {
    field.classList.remove('hidden');
    label.textContent = o.label || '';
    input.placeholder = o.placeholder || '';
    input.value = o.value || '';
    input.type = o.inputType || 'text';
  } else field.classList.add('hidden');
  const ok = $('modal-ok');
  ok.textContent = o.okText || '确定';
  ok.className = 'btn ' + (o.danger ? 'danger' : 'primary');
  modalCb = o.onOk || null;
  modalCancelCb = o.onCancel || null;
  $('modal-overlay').classList.remove('hidden');
  if (o.input) input.focus(); else ok.focus();
}
function closeModal(committed) {
  $('modal-overlay').classList.add('hidden');
  const cc = modalCancelCb;
  modalCb = null; modalCancelCb = null;
  if (!committed && cc) cc(); // 取消 / Esc / 点遮罩 → 回滚类回调（如恢复组名）
}

/* ============================================================
   动作表（data-act 委托）
   ============================================================ */
const actions = {};

/* ---------- 视图 / 会话 ---------- */
actions.switchView = function (el) {
  const v = el.dataset.nav;
  S.view = v;
  $$('.nav-item').forEach((b) => b.classList.toggle('active', b.dataset.nav === v));
  $$('.view').forEach((s) => s.classList.toggle('hidden', s.id !== 'view-' + v));
  if (v === 'shared') loadShared();
  if (v === 'audit') loadAudit();
  if (v === 'cluster') loadCluster();
  if (v === 'admins') loadAdmins();
};
function setPane(pane) {
  S.pane = pane;
  $$('#pane-seg button').forEach((b) => b.classList.toggle('active', b.dataset.pane === S.pane));
  $$('.pane').forEach((p) => p.classList.toggle('hidden', p.id !== 'pane-' + S.pane));
  if (pane === 'tokens') loadTokens();          // 进入访问令牌页拉取列表
}
actions.switchPane = function (el) { setPane(el.dataset.pane); };

/* ---------- 项目访问令牌 ---------- */
async function loadTokens() {
  if (!S.project) return;
  const tbody = $('tokens-body');
  if (!tbody) return;
  renderCurlCmd(); // 刷新构建脚本 curl 命令（项目/分支/格式）
  try {
    const list = (await j('GET', '/api/v1/projects/' + encodeURIComponent(S.project) + '/tokens')) || [];
    renderTokens(list);
  } catch (e) {
    if (e.status === 403) tbody.innerHTML = '<tr><td colspan="6" class="muted">仅全局管理员可查看访问令牌</td></tr>';
    else toast('加载令牌失败：' + e.message, 'err');
  }
}

function renderTokens(list) {
  const tbody = $('tokens-body');
  if (!tbody) return;
  if (!list.length) { tbody.innerHTML = '<tr><td colspan="6" class="muted">该项目暂无访问令牌</td></tr>'; return; }
  tbody.innerHTML = list.map((t) => `
    <tr>
      <td>${esc(t.name)}</td>
      <td class="mono small">${esc(t.id)}</td>
      <td>${esc(t.created_by || '')}</td>
      <td>${fmtTime(t.created_at)}</td>
      <td>${t.revoked ? '<span class="badge warn">已吊销</span>' : '<span class="badge">有效</span>'}</td>
      <td class="nowrap">${t.revoked ? '' : `<button type="button" class="btn sm danger" data-act="revokeToken" data-id="${esc(t.id)}" data-name="${esc(t.name)}">吊销</button>`}</td>
    </tr>`).join('');
}

actions.createToken = async function () {
  if (!S.project) return;
  openModal({
    title: '创建访问令牌',
    message: '为项目 ' + S.project + ' 创建数据面访问令牌（项目级只读）。',
    input: true,
    label: '令牌名称（如 订单服务-2025）',
    okText: '创建',
    onOk: async (name) => {
      name = (name || '').trim();
      if (!name) { toast('令牌名称不能为空', 'err'); return; }
      try {
        const r = await j('POST', '/api/v1/projects/' + encodeURIComponent(S.project) + '/tokens', { name });
        $('token-plaintext').textContent = r.token || '';
        $('token-overlay').classList.remove('hidden');
        toast('令牌已创建（明文仅展示一次）');
        loadTokens();
      } catch (e) { toast('创建失败：' + e.message, 'err'); }
    },
  });
};

actions.revokeToken = function (el) {
  const id = el.dataset.id, name = el.dataset.name;
  openModal({
    title: '吊销访问令牌',
    message: '确定吊销令牌「' + name + '」？使用该令牌的 SDK 将立即 401，需重建并重新分发。',
    danger: true,
    okText: '吊销',
    onOk: async () => {
      try {
        await j('DELETE', '/api/v1/projects/' + encodeURIComponent(S.project) + '/tokens/' + encodeURIComponent(id));
        toast('已吊销');
        loadTokens();
      } catch (e) { toast('吊销失败：' + e.message, 'err'); }
    },
  });
};

actions.copyToken = async function () {
  const txt = $('token-plaintext').textContent || '';
  if (!txt) return;
  if (await copyText(txt)) toast('已复制');
  else toast('复制失败，请手动选择复制', 'err');
};

/* ---------- 构建脚本取值：curl 命令展示 ---------- */
const CURL_FORMATS = ['yaml', 'json', 'toml', 'env'];
function renderCurlCmd() {
  const el = $('tok-curl-cmd');
  if (!el) return;
  const fmt = CURL_FORMATS.includes($('tok-fmt')?.value) ? $('tok-fmt').value : 'yaml';
  const branch = S.branch || 'dev';
  const url = location.origin + '/v1/projects/' + encodeURIComponent(S.project || '<项目>') + '/branches/' + encodeURIComponent(branch) + '/config?format=' + fmt;
  el.textContent = 'curl -s "' + url + '" -H "Authorization: Bearer <项目访问令牌>"';
}
actions.tokFmt = function () { renderCurlCmd(); };
actions.copyCurlUrl = async function () {
  const txt = $('tok-curl-cmd').textContent || '';
  if (!txt) return;
  if (await copyText(txt)) toast('curl 命令已复制');
  else toast('复制失败，请手动选择复制', 'err');
};

actions.closeTokenModal = function () { $('token-overlay').classList.add('hidden'); };
actions.closeErrModal = function () { closeErrorModal(); };
actions.toggleTheme = function () {
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  localStorage.setItem(LS_THEME, next);
  applyTheme(next);
};
actions.doLogout = function () {
  j('POST', '/api/v1/logout', {}).catch(() => { /* 登出失败也继续本地清理 */ });
  S.token = '';
  localStorage.removeItem(LS_TOKEN); localStorage.removeItem(LS_ROLE); localStorage.removeItem(LS_PROJ);
  location.reload();
};

/* ---------- 登录 ---------- */
async function doLogin() {
  const pw = $('login-pw').value;
  const user = $('login-user').value.trim();
  if (!pw) { showErr('login-err', '请输入密码'); return; }
  hideErr('login-err');
  const body = { password: pw };
  if (user) body.username = user; // 项目管理员登录（全局管理员留空，请求体与旧版一致）
  await withBusy($('login-submit'), async () => {
    try {
      const r = await j('POST', '/api/v1/login', body);
      S.token = r.token;
      localStorage.setItem(LS_TOKEN, r.token);
      localStorage.setItem(LS_ROLE, r.role || '');
      localStorage.setItem(LS_PROJ, r.project || '');
      $('login-pw').value = '';
      enterApp();
      toast('登录成功');
    } catch (e) {
      showErr('login-err', e.message);
    }
  });
}

function renderSession() {
  let name = '已登录', sub = '', ch = '·';
  if (S.role === 'admin') { name = '管理员'; sub = '全局'; ch = 'A'; }
  else if (S.role === 'project_admin') { name = '项目管理员'; sub = S.roleProject ? '项目 ' + S.roleProject : '项目'; ch = 'P'; }
  $('who-name').textContent = name;
  $('who-sub').textContent = sub;
  $('who-avatar').textContent = ch;
}

function enterApp() {
  S.role = localStorage.getItem(LS_ROLE) || '';
  S.roleProject = localStorage.getItem(LS_PROJ) || '';
  renderSession();
  // 导航按角色过滤：项目管理员仅保留「配置管理」「审计日志」（服务端矩阵：共享/集群/管理员对 PA 一律 403）
  const isPa = S.role === 'project_admin';
  for (const id of ['nav-shared', 'nav-admins', 'tab-tokens']) {
    const el = $(id);
    if (el) el.classList.toggle('hidden', isPa);
  }
  $('login-view').classList.add('hidden');
  $('app').classList.remove('hidden');
  loadProjects();
}

/* ---------- 项目 ---------- */
async function loadProjects() {
  try {
    S.projects = (await j('GET', '/api/v1/projects')) || [];
    if (!S.projects.some((p) => p.id === S.project)) S.project = S.projects.length ? S.projects[0].id : '';
    renderProjects();
    if (S.project) loadProject();
  } catch (e) {
    if (!e.expired) toast(e.message, 'err');
  }
}

function renderProjects() {
  $('proj-chips').innerHTML = S.projects
    .map((p) => `<button type="button" class="chip${p.id === S.project ? ' active' : ''}" data-act="selectProject" data-id="${esc(p.id)}" title="${esc(p.id)}">${esc(p.name)}</button>`)
    .join('');
  $('btn-del-proj').classList.toggle('hidden', !S.project);
  $('cfg-empty').classList.toggle('hidden', !!S.projects.length);
  $('cfg-work').classList.toggle('hidden', !S.projects.length);
}

actions.selectProject = function (el) {
  const id = el.dataset.id;
  if (!id || id === S.project) return;
  S.project = id;
  S.branch = '';
  S.gray = null;
  S.structV = 0;
  S.pubStruct = null; // 切项目：立即丢弃旧项目数据，待 loadProject 拉取新项目的已发布结构
  renderProjects();
  loadProject();
};

actions.newProjectModal = function () {
  openModal({
    title: '新建项目',
    input: true, label: '项目名', placeholder: '小写字母 / 数字 / 连字符，如 mall-order',
    okText: '创建',
    onOk: async (v) => {
      const name = (v || '').trim();
      if (!name) { toast('请输入项目名', 'err'); return; }
      try {
        const r = await j('POST', '/api/v1/projects', { name });
        toast('项目已创建');
        S.project = (r && r.id) || name;
        await loadProjects();
      } catch (e) { toast(e.message, 'err'); }
    },
  });
};

actions.deleteProject = function () {
  if (!S.project) return;
  const pid = S.project;
  openModal({
    title: '删除项目',
    message: `确认删除项目 ${pid}？该项目的全部分支、草稿与版本历史将被移除，操作不可恢复。`,
    okText: '删除', danger: true,
    onOk: async () => {
      try {
        await j('DELETE', '/api/v1/projects/' + pid + '?force=true');
        S.project = '';
        toast('项目已删除');
        loadProjects();
      } catch (e) { toast(e.message, 'err'); }
    },
  });
};

/* ---------- 分支 ---------- */
function fillBranchOptions(sel, bs, keep) {
  const prev = keep ? sel.value : '';
  if (!bs || !bs.length) {
    sel.innerHTML = '<option value="">（无分支）</option>';
    sel.disabled = true;
    return;
  }
  sel.disabled = false;
  sel.innerHTML = bs.map((b) => `<option value="${esc(b.name)}">${esc(b.name)} · v${b.active_version}</option>`).join('');
  if (prev && bs.some((b) => b.name === prev)) sel.value = prev;
}

function renderBranchSelects() {
  fillBranchOptions($('sel-branch'), S.branches, true);
  fillBranchOptions($('diff-a'), S.branches, true);
  fillBranchOptions($('diff-b'), S.branches, true);
  fillBranchOptions($('promote-from'), S.branches, true);
  fillBranchOptions($('promote-to'), S.branches, true);
}

// 已发布共享项缓存（草稿页「引用共享」绑定下拉的数据源）
async function loadSharedItems() {
  try {
    S.sharedItems = (await j('GET', '/api/v1/shared')) || [];
  } catch (_) {
    S.sharedItems = []; // 端点暂态失败 → 下拉为空（保存时服务端双校验兜底）
  }
  refreshDraftBindDropdowns(); // 就地刷新草稿页绑定下拉（结构页勾选框不依赖共享项列表）
}

// 就地刷新草稿页「引用共享」下拉的选项（保留当前选择）；下拉不存在时无操作。
// 注意：不可在此调用 renderStructEditor/loadSharedItems（会与结构页渲染互相递归）。
// 不按 pane 守卫：共享库页保存/发布共享项时草稿页下拉仍在 DOM（隐藏面板），需同步刷新。
function refreshDraftBindDropdowns() {
  if (!S.sharedItems) return;
  for (const sel of $$('#pane-draft .draft-shared-bind')) {
    const prev = sel.value;
    sel.innerHTML = sharedBindOptions(sel.dataset.g, sel.dataset.k, prev);
  }
}

// 「引用共享」绑定下拉的选项 HTML（按结构声明 type 过滤；secret 项带 🔒 徽标）
function sharedBindOptions(g, k, cur) {
  const def = ((S.pubStruct && S.pubStruct.groups) || [])
    .flatMap((gg) => gg.items)
    .find((it) => it.key === k);
  const ty = (def && def.type) || '';
  return '<option value="">— 请选择 —</option>' + (S.sharedItems || [])
    .filter((s) => !ty || s.type === ty)
    .map((s) => `<option value="${esc(s.key)}"${s.key === cur ? ' selected' : ''} title="${esc(s.description || '')}">${esc(s.key)}${s.secret ? ' 🔒' : ''}${s.description ? ' · ' + esc(s.description) : ''}</option>`).join('');
}

async function loadProject() {
  if (!S.project) return;
  try {
    const [bs, struct] = await Promise.all([
      j('GET', `/api/v1/projects/${S.project}/branches`),
      j('GET', `/api/v1/projects/${S.project}/structure-draft`),
      loadPublishedStruct(),          // 已发布结构（级联首选源 + 无草稿时的权威基线）
      loadSharedItems(),             // 共享引用下拉数据源
    ]);
    S.branches = bs || [];
    renderBranchSelects();
    applyServerStructDraft(struct);      // 规范化服务端草稿（无草稿 → 以已发布结构为基线与编辑起点）
    if (S.branches.length) {
      const target = S.branches.some((b) => b.name === S.branch) ? S.branch : S.branches[0].name;
      $('sel-branch').value = target;
      await loadBranch();                // 先取分支详情（刷新 S.structV 徽章），与已发布版本交叉校验
    } else {
      S.branch = '';
      renderNoBranch();
    }
    // 结构编辑器自动填充（切换项目或未编辑时；有未保存编辑则不覆盖）
    if (!S.structDirty || S.structProj !== S.project) {
      applyServerStructDraft(struct);
      renderStructEditor();
      hideErr('struct-err');
      S.structDirty = false;
    }
    S.structProj = S.project;
  } catch (e) {
    if (!e.expired) toast(e.message, 'err');
  }
}

function renderNoBranch() {
  S.version = 0; S.structV = 0; S.draftRev = 0; S.gray = null;
  S.draftValKeys = {};
  renderCtxBadges();
  $('draft-rev').textContent = 'r0';
  $('draft-groups').innerHTML =
    '<div class="empty mini"><svg class="ic"><use href="#i-branch"/></svg><h4>暂无分支</h4><p>新建分支后即可编辑草稿。</p></div>';
  $('versions-body').innerHTML = '';
  $('gray-summary').innerHTML = '<span class="muted small">选择分支后加载灰度状态</span>';
}

actions.selectBranch = function () { loadBranch(); }; // 仅响应 change（CHANGE_ONLY 过滤了 click）

actions.newBranchModal = function () {
  if (!S.project) return toast('请先选择项目', 'err');
  openModal({
    title: '新建分支',
    input: true, label: '分支名', placeholder: '如 feature-ttl',
    okText: '创建',
    onOk: async (v) => {
      const name = (v || '').trim();
      if (!name) { toast('请输入分支名', 'err'); return; }
      try {
        await j('POST', `/api/v1/projects/${S.project}/branches`, { name });
        toast('分支已创建');
        S.branch = name;
        loadProject();
      } catch (e) { toast(e.message, 'err'); }
    },
  });
};

actions.deleteBranch = function () {
  if (!S.project || !S.branch) return;
  const b = S.branch;
  openModal({
    title: '删除分支',
    message: `确认删除分支 ${S.project}/${b}？该分支的草稿与版本历史将被移除。`,
    okText: '删除', danger: true,
    onOk: async () => {
      try {
        await j('DELETE', `/api/v1/projects/${S.project}/branches/${b}`);
        toast('分支已删除');
        loadProject();
      } catch (e) { toast(e.message, 'err'); }
    },
  });
};

/* ---------- 上下文栏 / 徽章 ---------- */
function renderCtxBadges() {
  let html =
    `<span class="badge ok">稳定版 <span class="mono">v${S.version}</span></span>` +
    `<span class="badge">结构 <span class="mono">sv${S.structV}</span></span>`;
  if (S.gray && S.gray.gray_active) {
    html += `<span class="badge warn"><span class="dot"></span>灰度 <span class="mono">#${S.gray.gray_seq}</span></span>`;
  }
  $('ctx-badges').innerHTML = html;
}

/* ---------- 分支详情 / 草稿编辑 ---------- */
async function loadBranch() {
  const nb = $('sel-branch').value;
  if (!S.project || !nb) return;
  const branchChanged = nb !== S.grayBranch;
  S.branch = nb;
  if (branchChanged) S.grayDirty = false; // 切换分支时才重置灰度规则表单
  S.grayBranch = nb;
  try {
    const b = await j('GET', `/api/v1/projects/${S.project}/branches/${S.branch}`);
    loadVersions();
    S.version = b.active_version || 0;
    S.structV = b.structure_version || 0;
    renderCtxBadges();
    renderDraftEditor(b);
    loadGrayStatus();
  } catch (e) {
    if (!e.expired) toast(e.message, 'err');
  }
}

/* ---------- 保存状态指示（草稿 / 结构 / 共享库） ---------- */
// 草稿页：未保存（draftDirty）+ 已保存未发布（draftValKeys 数）
function updateDraftStatus() {
  const u = $('draft-unsaved'), p = $('draft-unpublished');
  if (u) u.classList.toggle('hidden', !S.draftDirty);
  if (p) {
    const n = Object.keys(S.draftValKeys || {}).length;
    p.classList.toggle('hidden', n === 0);
    if (n) p.textContent = n + ' 项草稿未发布';
  }
}
function markDraftDirty() { S.draftDirty = true; updateDraftStatus(); }

// 结构页：未保存（structDirty）+ 已保存未发布（hasStructDraft）
function updateStructStatus() {
  const u = $('struct-unsaved'), p = $('struct-unpublished');
  if (u) u.classList.toggle('hidden', !S.structDirty);
  if (p) p.classList.toggle('hidden', !S.hasStructDraft);
}

// 共享库：未保存（sharedDirty）+ 已保存未发布（sharedDraftCount）
function updateSharedStatus() {
  const u = $('sh-unsaved'), p = $('sh-unpublished');
  if (u) u.classList.toggle('hidden', !S.sharedDirty);
  if (p) {
    p.classList.toggle('hidden', S.sharedDraftCount === 0);
    if (S.sharedDraftCount) p.textContent = S.sharedDraftCount + ' 个共享草稿未发布';
  }
}
function markSharedDirty() { S.sharedDirty = true; updateSharedStatus(); }

function renderDraftEditor(b) {
  // 乐观锁：记录草稿修订号，保存时回传 expected_draft_rev
  S.draftRev = b.draft_rev || 0;
  $('draft-rev').textContent = 'r' + S.draftRev;
  // 现有草稿值索引（保存时空值 = 删除该草稿值）
  S.draftValKeys = {};
  for (const g of Object.keys(b.draft || {})) for (const k of Object.keys(b.draft[g] || {})) S.draftValKeys[g + '/' + k] = true;
  // 重渲染后视为已同步：清未保存标记，刷新「N 项草稿未发布」计数
  S.draftDirty = false;
  updateDraftStatus();
  // 引用项索引（绑定解析：值来自共享库；含未绑定项 shared_key=""）；每次重渲染重置，避免分支切换残留
  S.sharedRefs = {};
  for (const r of (b.shared_refs || [])) S.sharedRefs[r.group + '/' + r.key] = r;
  // 结构驱动：一次性展示已发布结构的全部组/配置项，直接改值保存（草稿不再是「添加配置项」模式）
  const groups = (S.pubStruct && S.pubStruct.groups) || [];
  if (!groups.length) {
    $('draft-groups').innerHTML =
      '<div class="empty mini"><svg class="ic"><use href="#i-inbox"/></svg><h4>暂无结构定义</h4><p>草稿按已发布结构全量展示：请先在「结构」页定义组与配置项并发布。</p></div>';
    return;
  }
  $('draft-groups').innerHTML = groups.map((g) => {
    const refCount = g.items.filter((it) => !!it.shared).length;
    const refBadge = refCount ? `<span class="badge acc" title="值由共享库物化：本分支在下拉选择引用的共享项">${refCount} 引用共享</span>` : '';
    const rows = g.items.map((it) => {
      if (it.shared) return sharedBindRowHtml(g, it);
      // 值基线：草稿值优先；无草稿时回退活动版本值（发布后草稿清空，显示已发布的值而非空框）
      const dv = (b.draft && b.draft[g.name] && b.draft[g.name][it.key]) ? b.draft[g.name][it.key].value : null;
      const av = (b.active && b.active[g.name] && b.active[g.name][it.key]) ? b.active[g.name][it.key].value : null;
      const hasActive = !!(b.active && b.active[g.name] && b.active[g.name][it.key]);
      return draftStructRowHtml(g, it, dv, av, hasActive);
    }).join('');
    return `<div class="card gcard">
      <div class="gcard-head"><code class="gname">${esc(g.name)}</code><span class="muted small">${g.items.length} 项</span>${refBadge}
        <span class="spacer"></span>
        <button type="button" class="btn sm ghost" data-act="manageGroups" data-g="${esc(g.name)}" title="分组管理（结构页）"><svg class="ic ic-xs"><use href="#i-config"/></svg>管理分组</button>
      </div>
      <div class="grows">${rows}</div>
    </div>`;
  }).join('');
}

// 结构驱动行：it = 结构定义（key/type/required/secret/description），
// v = 草稿值优先（dv），无草稿时回退活动版本值（av）；hasActive = 活动版本存在该值。
// masked（secret 掩码）不填入输入框 —— secret 恒显示「已加密」占位，留空不修改。
function draftStructRowHtml(g, it, v, av, hasActive) {
  const type = it.type || (v && v.type) || 'string';
  const val = (v && !v.masked) ? v : (av && !av.masked ? av : null);
  const common = `data-g="${esc(g.name)}" data-k="${esc(it.key)}"`;
  let ctl;
  if (type === 'bool') {
    ctl = `<label class="check"><input type="checkbox" class="draft-in" ${common} data-ty="bool" ${val && val.bool_value === true ? 'checked' : ''}></label>`;
  } else if (type === 'int' || type === 'float') {
    ctl = `<input type="number" step="${type === 'float' ? 'any' : '1'}" class="in mono draft-in" ${common} data-ty="${esc(type)}" value="${esc(val ? (type === 'int' ? val.int_value ?? '' : val.float_value ?? '') : '')}">`;
  } else if (type === 'json') {
    ctl = `<textarea class="in mono draft-in" rows="3" ${common} data-ty="json" spellcheck="false">${esc(val ? val.json_value ?? '' : '')}</textarea>`;
  } else if (type === 'array') {
    ctl = `<input class="in mono draft-in" ${common} data-ty="array" value="${esc(val ? (val.list_value || []).join(', ') : '')}">`;
  } else if (type === 'secret') {
    const ph = (S.draftValKeys[g.name + '/' + it.key] || hasActive)
      ? '已加密 · 留空不修改，输入以更新' : '输入明文，由服务端加密存储';
    ctl = `<input type="password" class="in draft-in" ${common} data-ty="secret" placeholder="${ph}" autocomplete="new-password">`;
  } else {
    ctl = `<input class="in draft-in" ${common} data-ty="string" value="${esc(val ? val.str_value ?? '' : '')}">`;
  }
  const icon = type === 'secret' ? '<svg class="ic ic-xs"><use href="#i-lock"/></svg>' : '';
  const badges = [];
  if (it.required) badges.push('<span class="badge warn" title="发布前必须有值">required</span>');
  if (it.secret || type === 'secret') badges.push('<span class="badge err" title="敏感值">secret</span>');
  const desc = it.description ? `<div class="hint small" style="margin:2px 0 0">${esc(it.description)}</div>` : '';
  // 状态标记：草稿有值 > 活动版本有值（发布后草稿清空 → 显示活动版本值）
  const hasVal = S.draftValKeys[g.name + '/' + it.key]
    ? '<span class="hint" style="margin:0">草稿已设值</span>'
    : (hasActive ? '<span class="hint" style="margin:0">活动版本</span>' : '');
  return `<div class="grow">
    <div class="gkey"><span class="mono">${esc(it.key)}</span> ${badges.join(' ')}${desc}</div>
    <div class="gtype"><span class="ty">${icon}${esc(type)}</span></div>
    <div class="gctl">${ctl}</div>
    <div class="gdel">${hasVal}</div>
  </div>`;
}

// 引用共享绑定行（草稿页）：下拉选择本分支引用的共享项（按结构声明 type 过滤）+ 物化值展示。
// 徽标放 gkey 下方（可换行）；gtype 列显示结构声明类型；值列展示物化值或「未选择」。
function sharedBindRowHtml(g, it) {
  const ref = S.sharedRefs[g.name + '/' + it.key];
  const cur = (ref && ref.shared_key) || '';
  const opts = sharedBindOptions(g.name, it.key, cur);
  const tip = (ref && ref.shared_key) ? '引用共享项 ' + ref.shared_key + (ref.version ? ' · v' + ref.version : '') : '未选择共享项';
  const sh = cur ? (S.sharedItems || []).find((s) => s.key === cur) : null;
  const valHtml = sharedBindValueHtml(cur, ref, sh);
  return `<div class="grow ref-grow">
    <div class="gkey"><span class="mono">${esc(it.key)}</span><span class="badge acc ref-badge" title="${esc(tip)}">引用共享</span>${it.description ? `<div class="hint small" style="margin:2px 0 0">${esc(it.description)}</div>` : ''}</div>
    <div class="gtype"><span class="ty">${esc(it.type || '')}</span></div>
    <div class="gctl">
      <select class="sel draft-shared-bind" data-g="${esc(g.name)}" data-k="${esc(it.key)}" title="本分支引用的共享项（值由共享库物化，只读）">${opts}</select>
      <div class="bind-val" style="margin-top:2px">${valHtml}</div>
    </div>
    <div class="gdel"><span class="hint" style="margin:0">${ref && ref.version ? 'v' + ref.version : ''}</span></div>
  </div>`;
}

// 绑定值展示：服务端解析（S.sharedRefs，含版本）优先；否则客户端按当前选择查 S.sharedItems 即时预览
function sharedBindValueHtml(sharedKey, ref, sh) {
  if (ref && ref.value) {
    const v = ref.value;
    const masked = v.masked ? ' <span class="hint">（已加密）</span>' : '';
    return `<span class="mono ${v.masked ? 'muted' : ''}">${esc(fmtVal(v))}</span>${masked}`;
  }
  if (sharedKey && sh && sh.value) {
    const v = sh.value;
    const masked = v && v.masked ? ' <span class="hint">（已加密）</span>' : '';
    const val = (v && fmtVal(v)) || '…';
    return `<span class="mono muted">${esc(val)}</span>${masked}<span class="hint"> · 待保存生效</span>`;
  }
  return '<span class="hint" style="margin:0">未选择共享项</span>';
}

function fmtVal(v) {
  if (!v || typeof v !== 'object') return '';
  if (v.masked) return '***（已加密）';
  if (v.str_value !== undefined) return String(v.str_value);
  if (v.int_value !== undefined) return String(v.int_value);
  if (v.float_value !== undefined) return String(v.float_value);
  if (v.bool_value !== undefined) return String(v.bool_value);
  if (v.json_value !== undefined) return v.json_value;
  if (Array.isArray(v.list_value)) return v.list_value.join(', ');
  return JSON.stringify(v);
}

actions.manageGroups = function () { setPane('structure'); };

function buildValue(ty, raw) {
  // 非法数值显式报错，不静默置 0
  switch (ty) {
    case 'int': { const n = parseInt(raw, 10); if (Number.isNaN(n)) throw new Error('int 值非法: ' + raw); return { type: 'int', int_value: n }; }
    case 'float': { const n = parseFloat(raw); if (Number.isNaN(n)) throw new Error('float 值非法: ' + raw); return { type: 'float', float_value: n }; }
    case 'bool': return { type: 'bool', bool_value: raw === 'true' || raw === 'on' };
    case 'json': return { type: 'json', json_value: raw };
    case 'array': return { type: 'array', list_value: raw.split(',').map((x) => x.trim()).filter(Boolean) };
    case 'secret': return { type: 'string', str_value: raw };
    default: return { type: 'string', str_value: raw };
  }
}

/* ---------- 添加配置项（级联选择：组 → key → 按类型渲染值输入） ---------- */
const TYPES = ['string', 'int', 'float', 'bool', 'json', 'array', 'secret'];

// 已发布结构（GET /api/v1/projects/{p}/structure，权威）：值新增级联首选源 + 无草稿时的编辑基线。
// 拉取失败 / 空分组 → 级联回退结构草稿 / 自由输入；基线回退分支 sv 徽章。
async function loadPublishedStruct() {
  if (!S.project) return;
  try {
    const p = await j('GET', `/api/v1/projects/${S.project}/structure`);
    S.pubStruct = {
      version: Number(p && p.version) || 0,
      groups: normalizeGroups((p && p.groups) || []),
    };
  } catch (_) {
    S.pubStruct = null; // 端点不可用/暂态失败 → 走 ②③ 回退（401 已由 j() 统一处理会话过期）
  }
}

// 当前结构版本基线：已发布结构端点为权威，分支 sv 徽章交叉校验 / 回退（PUT base_version 须等于它）
function structBaseline() {
  const pv = S.pubStruct && typeof S.pubStruct.version === 'number' ? S.pubStruct.version : 0;
  return pv || S.structV || 0;
}

function cloneGroups(gs) { return normalizeGroups(gs); }

// 服务端 ItemDef 未知字段（validate 等）原样保留，JSON 往返不丢数据；description/shared 显式建模
function extraItemFields(it) {
  if (!it || typeof it !== 'object') return {};
  const out = {};
  for (const [k, v] of Object.entries(it)) {
    if (k !== 'key' && k !== 'type' && k !== 'required' && k !== 'secret' && k !== 'description' && k !== 'shared' && v !== undefined && v !== null) out[k] = v;
  }
  return out;
}

function normalizeGroups(gs) {
  return (Array.isArray(gs) ? gs : []).map((g) => ({
    name: String(g && g.name != null ? g.name : ''),
    items: (g && Array.isArray(g.items) ? g.items : []).map((it) => ({
      key: String(it && it.key != null ? it.key : ''),
      type: TYPES.includes(it && it.type) ? it.type : 'string',
      required: !!(it && it.required),
      secret: !!(it && it.secret),
      description: (it && it.description) || '',
      shared: !!(it && it.shared),
      __extra: extraItemFields(it),
    })),
  }));
}

function serializeGroups(gs) {
  return gs.map((g) => ({
    name: g.name,
    items: g.items.map((it) => {
      const o = { ...it.__extra, key: it.key, type: it.type, required: it.required, secret: it.secret };
      if (it.description) o.description = it.description;
      if (it.shared) o.shared = true;
      return o;
    }),
  }));
}

// 规范化服务端结构草稿；base_version=null（无草稿）时以已发布结构为基线与编辑起点
// （GET /structure 的 version 为权威，与分支 sv 徽章交叉校验）—— PUT 要求 base_version 为 u64 且等于当前结构版本
function applyServerStructDraft(d) {
  const noDraft = d === null || d === undefined || d.base_version === null || d.base_version === undefined;
  // 服务端结构草稿是否存在 → 「结构草稿未发布」指示
  S.hasStructDraft = !noDraft;
  updateStructStatus();
  S.structDraft = {
    base_version: noDraft ? structBaseline() : Number(d.base_version),
    groups: noDraft
      ? cloneGroups((S.pubStruct && S.pubStruct.groups) || [])
      : normalizeGroups(d.groups),
  };
}

const VAL_HINTS = {
  string: '字符串值',
  int: '整数',
  float: '数值（可带小数）',
  bool: '勾选 = true，取消 = false',
  json: '填入合法 JSON 文本',
  array: '多个值用逗号分隔',
  secret: '输入明文，由服务端加密存储；留空不修改',
};

function valueControlHtml(ty, id) {
  const base = `id="${id}" class="in mono"`;
  if (ty === 'bool') return `<label class="check"><input type="checkbox" id="${id}"> 启用（true）</label>`;
  if (ty === 'int') return `<input type="number" step="1" ${base} placeholder="如 100">`;
  if (ty === 'float') return `<input type="number" step="any" ${base} placeholder="如 0.5">`;
  if (ty === 'json') return `<textarea ${base} rows="3" spellcheck="false" placeholder='{"retries": 3}'></textarea>`;
  if (ty === 'array') return `<input ${base} placeholder="a, b, c（逗号分隔）">`;
  if (ty === 'secret') return `<input type="password" ${base} placeholder="输入明文，由服务端加密存储" autocomplete="new-password">`;
  return `<input ${base} placeholder="字符串值">`;
}

actions.saveDraft = async function (el) {
  const updates = [];
  const deletes = [];
  let bad = null;
  // 结构全量展示：逐行收集——有值 → upsert；原草稿有值但被清空 → 删除；secret 留空 = 不修改
  for (const inp of $$('#pane-draft .draft-in')) {
    if (bad) break;
    const g = inp.dataset.g, k = inp.dataset.k, ty = inp.dataset.ty || 'string';
    const key = g + '/' + k;
    const hadDraft = !!S.draftValKeys[key];
    if (inp.type === 'password') {
      if (!inp.value) continue; // 留空 = 不修改，服务端保留原密文
      updates.push({ group: g, key: k, value: { type: 'string', str_value: inp.value } });
    } else if (inp.type === 'checkbox') {
      updates.push({ group: g, key: k, value: { type: 'bool', bool_value: inp.checked } });
    } else {
      const raw = inp.value;
      if (!String(raw).trim()) {
        if (hadDraft) deletes.push(key); // 清空 = 移除草稿值
        continue;
      }
      try { updates.push({ group: g, key: k, value: buildValue(ty, raw) }); }
      catch (e) { bad = `${key}：${e.message}`; }
    }
  }
  if (bad) return toast(bad, 'err');
  // 共享引用绑定：收集全部 shared 行（含空选择 = 解除绑定）
  const shared_bindings = [];
  for (const sel of $$('#pane-draft .draft-shared-bind')) {
    shared_bindings.push({ group: sel.dataset.g, key: sel.dataset.k, shared_key: sel.value || '' });
  }
  await withBusy(el, async () => {
    try {
      // 乐观锁：携带 expected_draft_rev；409 = 草稿已被他人修改
      await j('PUT', `/api/v1/projects/${S.project}/branches/${S.branch}/draft`, { updates, deletes, shared_bindings, expected_draft_rev: S.draftRev });
      toast('草稿已保存');
      loadBranch();
    } catch (e) {
      if (e.status === 409) {
        toast('草稿已被他人修改，已加载最新版本，请确认后继续', 'warn');
        loadBranch();
      } else if (!e.expired) toast(e.message, 'err');
    }
  });
};

actions.doPublish = function () {
  if (!S.project || !S.branch) return toast('请先选择项目与分支', 'err');
  if (S.draftDirty) toast('有未保存的修改：本次发布只包含已保存的草稿值，请先「保存草稿」', 'warn', 6000);
  openModal({
    title: '发布版本',
    message: `将 ${S.project}/${S.branch} 当前草稿发布为新版本。`,
    input: true, label: '备注', placeholder: '备注（可选）',
    okText: '发布',
    onOk: async (comment) => {
      try {
        const r = await j('POST', `/api/v1/projects/${S.project}/branches/${S.branch}/publish`, { comment, request_id: rid() });
        toast('已发布 v' + r.version);
        if (Array.isArray(r.warnings) && r.warnings.length) {
          toast('发布校验警告：' + r.warnings.join('；'), 'warn', 8000);
        }
        loadProject();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

/* ---------- 灰度发布 ---------- */
async function loadGrayStatus() {
  if (!S.project || !S.branch) return;
  try {
    S.gray = await j('GET', `/api/v1/projects/${S.project}/branches/${S.branch}/gray-status`);
  } catch (e) {
    S.gray = { gray_active: false, error: e.message };
  }
  renderGraySummary();
  renderCtxBadges();
  if (!S.grayDirty) populateGrayForm(S.gray && S.gray.gray_rule); // 有未发布编辑时不覆盖表单
}

function grayRuleChips(rule) {
  const chips = [];
  (rule.match_labels || []).forEach((l) => chips.push(`${l.key}=${l.value}`));
  if ((rule.ip_cidrs || []).length) chips.push(`CIDR ×${rule.ip_cidrs.length}`);
  if (rule.percentage !== null && rule.percentage !== undefined) chips.push(`比例 ${rule.percentage}%`);
  return chips.map((c) => `<span class="chip mini">${esc(c)}</span>`).join('');
}

function renderGraySummary() {
  const g = S.gray;
  if (!g) { $('gray-summary').innerHTML = '<span class="muted small">选择分支后加载灰度状态</span>'; return; }
  if (g.error) {
    $('gray-summary').innerHTML = `<span class="badge err">灰度状态不可用</span><span class="muted small">${esc(g.error)}</span>`;
    return;
  }
  const meta = `稳定版 <span class="mono">v${g.active_version}</span> · 结构 <span class="mono">sv${g.structure_version}</span>`;
  if (g.gray_active) {
    $('gray-summary').innerHTML =
      `<div class="gs-left">
        <span class="badge warn"><span class="dot"></span>灰度进行中</span>
        <span class="badge acc">序号 <span class="mono">#${g.gray_seq}</span></span>
        <span class="muted small">${meta}</span>
        <div class="gs-chips">${grayRuleChips(g.gray_rule || {})}</div>
      </div>
      <div class="gs-actions">
        <button type="button" class="btn primary" data-act="doGrayPromote"><svg class="ic"><use href="#i-up"/></svg>一键转正</button>
        <button type="button" class="btn danger" data-act="doGrayAbort"><svg class="ic"><use href="#i-rollback"/></svg>一键下量</button>
      </div>`;
  } else {
    $('gray-summary').innerHTML =
      `<div class="gs-left"><span class="badge">灰度未启用</span><span class="muted small">${meta}</span></div>
       <div class="gs-actions"><span class="muted small">编辑规则并发布灰度后，可在此转正或下量</span></div>`;
  }
}

function labelRowHtml(l) {
  return `<div class="label-row">
    <input class="in mono" data-lf="key" placeholder="key（如 zone）" value="${esc(l.key || '')}">
    <span class="muted small">=</span>
    <input class="in mono" data-lf="value" placeholder="value（如 cn-north-1）" value="${esc(l.value || '')}">
    <button type="button" class="icon-btn danger" data-act="removeLabelRow" title="移除此标签" aria-label="移除此标签"><svg class="ic"><use href="#i-x"/></svg></button>
  </div>`;
}

function populateGrayForm(rule) {
  rule = rule || {};
  const labels = (rule.match_labels || []).filter(Boolean);
  $('label-rows').innerHTML = (labels.length ? labels : [{}]).map(labelRowHtml).join('');
  $('gray-cidrs').value = (rule.ip_cidrs || []).join('\n');
  const pct = rule.percentage;
  $('gray-pct').value = (pct === null || pct === undefined) ? '' : String(pct);
  if (!$('gray-rule').classList.contains('hidden')) {
    $('gray-rule').value = JSON.stringify(rule, null, 2); // JSON 模式下同步
  }
}

function grayRuleFromForm() {
  const labels = $$('#label-rows .label-row').map((r) => ({
    key: r.querySelector('[data-lf="key"]').value.trim(),
    value: r.querySelector('[data-lf="value"]').value.trim(),
  })).filter((l) => l.key || l.value);
  const cidrs = $('gray-cidrs').value.split(/[\n,]/).map((x) => x.trim()).filter(Boolean);
  const raw = $('gray-pct').value.trim();
  const pct = raw === '' ? null : Number(raw);
  return { match_labels: labels, ip_cidrs: cidrs, percentage: pct };
}

function grayJsonMode() { return !$('gray-rule').classList.contains('hidden'); }

actions.toggleGrayJson = function () {
  if (grayJsonMode()) {
    let rule;
    try { rule = JSON.parse($('gray-rule').value || '{}'); }
    catch (e) { showErr('gray-err', 'JSON 非法，无法转回表单：' + e.message); return; }
    hideErr('gray-err');
    populateGrayForm(rule);
    $('gray-rule').classList.add('hidden');
    $('gray-form').classList.remove('hidden');
    $('btn-gray-mode-label').textContent = 'JSON 模式';
  } else {
    $('gray-rule').value = JSON.stringify(grayRuleFromForm(), null, 2);
    hideErr('gray-err');
    $('gray-form').classList.add('hidden');
    $('gray-rule').classList.remove('hidden');
    $('gray-rule').focus();
    $('btn-gray-mode-label').textContent = '表单模式';
  }
};

actions.addLabelRow = function () {
  $('label-rows').insertAdjacentHTML('beforeend', labelRowHtml({}));
  const rows = $$('#label-rows .label-row');
  rows[rows.length - 1].querySelector('[data-lf="key"]').focus();
};
actions.removeLabelRow = function (el) {
  const row = el.closest('.label-row');
  if (row) row.remove();
};

actions.loadGrayRule = async function (el) {
  if (!S.project || !S.branch) return toast('请先选择项目与分支', 'err');
  await withBusy(el, async () => {
    try {
      const g = await j('GET', `/api/v1/projects/${S.project}/branches/${S.branch}/gray-status`);
      S.gray = g;
      renderGraySummary(); renderCtxBadges();
      populateGrayForm(g.gray_rule || { match_labels: [], ip_cidrs: [], percentage: null });
      S.grayDirty = false; // 显式载入 = 以服务端规则为准
      toast('已载入当前规则');
    } catch (e) { if (!e.expired) toast(e.message, 'err'); }
  });
};

actions.doGrayPublish = function () {
  if (!S.project || !S.branch) return toast('请先选择项目与分支', 'err');
  let rule;
  if (grayJsonMode()) {
    try { rule = JSON.parse($('gray-rule').value); }
    catch (e) { showErr('gray-err', '规则 JSON 非法：' + e.message); return; }
  } else {
    rule = grayRuleFromForm();
    if (rule.match_labels.some((l) => !l.key || !l.value)) { showErr('gray-err', '标签的 key 与 value 需同时填写'); return; }
    if (rule.ip_cidrs.some((c) => !c.includes('/'))) { showErr('gray-err', 'CIDR 需为「地址/前缀」形式，如 10.0.0.0/8'); return; }
    if (rule.percentage !== null && (Number.isNaN(rule.percentage) || rule.percentage < 0 || rule.percentage > 100)) { showErr('gray-err', '百分比范围为 0–100'); return; }
    if (!rule.match_labels.length && !rule.ip_cidrs.length && rule.percentage === null) {
      showErr('gray-err', '规则至少需要一个判据：标签 / IP CIDR / 百分比'); return;
    }
  }
  hideErr('gray-err');
  openModal({
    title: '发布灰度',
    message: `将 ${S.project}/${S.branch} 当前草稿固化为灰度快照；稳定版不变，命中规则的客户端读灰度快照。`,
    input: true, label: '备注', placeholder: '备注（可选）',
    okText: '发布灰度',
    onOk: async (comment) => {
      try {
        const r = await j('POST', `/api/v1/projects/${S.project}/branches/${S.branch}/gray-publish`, { rule, comment, request_id: rid() });
        toast('灰度已发布 #seq=' + r.gray_seq);
        S.grayDirty = false;
        loadGrayStatus();
        loadProject();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

actions.doGrayPromote = function () {
  if (!S.project || !S.branch) return toast('请先选择项目与分支', 'err');
  openModal({
    title: '灰度转正',
    message: `将 ${S.project}/${S.branch} 的灰度内容发布为新稳定版，全量客户端切换到灰度内容。`,
    input: true, label: '备注', placeholder: '备注（可选）',
    okText: '转正',
    onOk: async (comment) => {
      try {
        const r = await j('POST', `/api/v1/projects/${S.project}/branches/${S.branch}/gray-promote`, { comment, request_id: rid() });
        toast('已转正，新稳定版 v' + r.active_version);
        S.grayDirty = false;
        loadGrayStatus();
        loadProject();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

actions.doGrayAbort = function () {
  if (!S.project || !S.branch) return toast('请先选择项目与分支', 'err');
  openModal({
    title: '灰度下量',
    message: `摘除 ${S.project}/${S.branch} 的灰度指针，灰度客户端回落到稳定版。`,
    input: true, label: '备注', placeholder: '备注（可选）',
    okText: '下量', danger: true,
    onOk: async (comment) => {
      try {
        const r = await j('POST', `/api/v1/projects/${S.project}/branches/${S.branch}/gray-abort`, { comment, request_id: rid() });
        toast('已下量，客户端回落稳定版 v' + r.fallback_version);
        S.grayDirty = false;
        loadGrayStatus();
        loadProject();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

/* ---------- 版本历史 / 回滚 ---------- */
async function loadVersions() {
  if (!S.project || !S.branch) return;
  try {
    const vs = await j('GET', `/api/v1/projects/${S.project}/branches/${S.branch}/versions`);
    renderVersions(vs);
  } catch (e) { if (!e.expired) toast(e.message, 'err'); }
}
actions.refreshVersions = function () { loadVersions(); };

function versionBadges(v) {
  let html = '';
  if (v.rollback_of) html += `<span class="badge warn">回滚 ← v${v.rollback_of}</span> `;
  if (v.gray) html += '<span class="badge acc">灰度</span> ';
  html += v.kind === 'diff' ? '<span class="badge">增量</span>' : '<span class="badge">完整</span>';
  return html;
}

function renderVersions(vs) {
  const tb = $('versions-body');
  if (!vs || !vs.length) {
    tb.innerHTML = '<tr><td colspan="7"><div class="empty mini"><svg class="ic"><use href="#i-history"/></svg><h4>暂无版本记录</h4><p>发布版本后在此生成历史，可随时回滚。</p></div></td></tr>';
    return;
  }
  tb.innerHTML = vs.slice().sort((a, b) => b.no - a.no).map((v) => `<tr>
    <td class="mono tnum">v${v.no}</td>
    <td>${versionBadges(v)}</td>
    <td class="cmt">${esc(v.comment || '—')}</td>
    <td class="mono muted tnum">sv${v.structure_version}</td>
    <td>${esc(v.operator || '—')}</td>
    <td class="muted small nowrap">${fmtTime(v.created_at)}</td>
    <td class="nowrap"><button type="button" class="btn sm ghost" data-act="doRollback" data-ver="${v.no}" title="回滚到此版本"><svg class="ic ic-xs"><use href="#i-rollback"/></svg>回滚</button></td>
  </tr>`).join('');
}

actions.doRollback = function (el) {
  const toVersion = Number(el.dataset.ver);
  openModal({
    title: '回滚到 v' + toVersion,
    message: `以 v${toVersion} 的内容创建一个新版本（历史不可变），当前草稿保持不变。`,
    input: true, label: '备注', placeholder: '备注（可选）',
    okText: '回滚', danger: true,
    onOk: async (comment) => {
      try {
        const r = await j('POST', `/api/v1/projects/${S.project}/branches/${S.branch}/rollback`, { to_version: toVersion, comment, request_id: rid() });
        toast('已回滚，新版本 v' + r.new_version);
        loadProject();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

/* ---------- 结构（组 → 配置项 结构化编辑器 + JSON 模式） ---------- */
const NAME_RE = /^[A-Za-z0-9._-]+$/;   // 镜像服务端 valid_key_name 字符集
const NAME_MAX = 128;                  // MAX_KEY_BYTES / MAX_GROUP_NAME_BYTES
const GROUP_LIMIT = 500;               // MAX_GROUPS_PER_PROJECT
const ITEM_LIMIT = 10000;              // MAX_ITEMS_PER_PROJECT

function structJsonActive() { return !$('struct-draft').classList.contains('hidden'); }
function markStructDirty() { S.structDirty = true; updateStructStatus(); }

function syncStructJsonTextarea() {
  $('struct-draft').value = JSON.stringify(
    { base_version: S.structDraft.base_version, groups: serializeGroups(S.structDraft.groups) }, null, 2);
}

function renderStructEditor() {
  const d = S.structDraft;
  if (!d) return;
  $('struct-base').textContent = 'sv' + d.base_version;
  // 滞后判定：与 structBaseline()（已发布结构版本为权威，分支 sv 交叉校验）比较
  const stale = d.base_version !== structBaseline();
  $('struct-stale').classList.toggle('hidden', !stale);
  $('struct-base-badge').classList.toggle('warn', stale);
  if (structJsonActive()) { syncStructJsonTextarea(); return; }
  if (!d.groups.length) {
    $('struct-groups').innerHTML = '';
    $('struct-empty').classList.remove('hidden');
    return;
  }
  $('struct-empty').classList.add('hidden');
  $('struct-groups').innerHTML = d.groups.map((g, gi) => structGroupHtml(g, gi)).join('');
}

function structGroupHtml(g, gi) {
  const head = '<div class="srow srow-head"><span>配置项 key</span><span>类型</span><span class="req-lab">required</span><span class="sec-lab">secret</span><span></span></div>';
  const rows = g.items.length
    ? head + g.items.map((it, ii) => structItemRowHtml(it, gi, ii)).join('')
    : '<div class="struct-item-empty">暂无配置项，点击右上「配置项」添加</div>';
  return `<div class="card gcard struct-group" data-gi="${gi}">
    <div class="struct-ghead">
      <input class="in mono gname-in" data-sf="gname" data-act="renameStructGroup" data-orig="${esc(g.name)}" value="${esc(g.name)}" placeholder="分组名（字母 / 数字 / . _ -）" spellcheck="false">
      <span class="badge">${g.items.length} 项</span>
      <span class="spacer"></span>
      <button type="button" class="btn sm ghost" data-act="addStructItem" data-gi="${gi}"><svg class="ic ic-xs"><use href="#i-plus"/></svg>配置项</button>
      <button type="button" class="icon-btn danger" data-act="delStructGroup" data-gi="${gi}" title="删除组" aria-label="删除组"><svg class="ic"><use href="#i-trash"/></svg></button>
    </div>
    <div>${rows}</div>
  </div>`;
}

function structItemRowHtml(it, gi, ii) {
  const tyOpts = TYPES.map((t) => `<option value="${t}"${t === it.type ? ' selected' : ''}>${t}</option>`).join('');
  const isShared = !!it.shared;
  // 引用共享勾选：声明本项为共享来源（值由共享库物化），各分支在草稿页选择引用的共享项
  const sharedChk = `<label class="check" title="勾选后本项值为共享来源：各分支在草稿页按下拉选择引用的共享项；type 声明为分支下拉的类型约束"><input type="checkbox" data-sf="ishared" data-act="structShared" ${isShared ? 'checked' : ''}>引用共享</label>`;
  const hint = isShared ? 'type 为分支下拉的类型约束；required/secret 由所选的共享项决定' : '';
  return `<div class="struct-item"${isShared ? ' data-ref="1"' : ''}>
    <div class="srow">
      <input class="in mono" data-sf="ikey" value="${esc(it.key)}" placeholder="key（字母 / 数字 / . _ -）" spellcheck="false">
      <select class="sel" data-act="structType" title="值类型（引用共享时为分支下拉的类型约束）">${tyOpts}</select>
      <label class="check" title="发布前必须有值（引用共享项无意义）"><input type="checkbox" data-sf="ireq" ${it.required ? 'checked' : ''} ${isShared ? 'disabled' : ''}></label>
      <label class="check" title="敏感值（类型须为 secret；引用共享项由所选的共享项决定）"><input type="checkbox" data-sf="isec" data-act="structSecret" ${it.secret ? 'checked' : ''} ${isShared ? 'disabled' : ''}></label>
      <button type="button" class="icon-btn danger" data-act="delStructItem" data-gi="${gi}" data-ii="${ii}" title="删除配置项" aria-label="删除配置项"><svg class="ic"><use href="#i-trash"/></svg></button>
    </div>
    <div class="srow-sub">
      <span class="muted small" style="width:34px">描述</span>
      <input class="in mono" data-sf="idesc" value="${esc(it.description || '')}" placeholder="助记（≤200 字节，不渲染进配置文件）" spellcheck="false">
      <span class="muted small" style="width:56px">共享来源</span>
      ${sharedChk}
      <span class="hint" data-role="shref-hint" style="margin:0">${hint}</span>
    </div>
  </div>`;
}

// 引用共享勾选联动：勾选 → required/secret 置灰（由所选的共享项决定）；type 保持可编辑（分支下拉的类型约束）
actions.structShared = function (el) {
  const item = el.closest('.struct-item');
  const isShared = !!el.checked;
  if (item) {
    item.setAttribute('data-ref', isShared ? '1' : '');
    const req = item.querySelector('[data-sf="ireq"]');
    const sec = item.querySelector('[data-sf="isec"]');
    const hint = item.querySelector('[data-role="shref-hint"]');
    if (req) req.disabled = isShared;
    if (sec) sec.disabled = isShared;
    if (hint) hint.textContent = isShared ? 'type 为分支下拉的类型约束；required/secret 由所选的共享项决定' : '';
  }
  markStructDirty();
}; // 仅响应 change

// 纯校验（镜像服务端 validate_structure：字符集 / 长度 / 重名 / secret 依赖类型 / 限额）
function validateGroups(groups) {
  const errs = [];
  const nameErr = (label, v) => {
    if (!v) return label + '不能为空';
    if (v.length > NAME_MAX) return label + '超过 128 字节上限';
    if (!NAME_RE.test(v)) return label + '「' + v + '」仅允许字母、数字与 . _ -';
    return '';
  };
  if (groups.length > GROUP_LIMIT) errs.push('分组数量超过上限（' + GROUP_LIMIT + '）');
  const seenG = new Set();
  let total = 0;
  for (const g of groups) {
    const ge = nameErr('组名', g.name);
    if (ge) errs.push(ge);
    if (g.name && seenG.has(g.name)) errs.push('分组名重复：' + g.name);
    seenG.add(g.name);
    const seen = new Set();
    for (const it of g.items) {
      const where = (g.name || '未命名组') + '/' + (it.key || '（未填 key）');
      const ke = nameErr('key ', it.key);
      if (ke) errs.push(where + '：' + ke);
      else if (seen.has(it.key)) errs.push((g.name || '未命名组') + '/' + it.key + '：key 重复');
      seen.add(it.key);
      if (it.secret && it.type !== 'secret') errs.push(where + '：勾选 secret 需将类型设为 secret');
      if (it.description && it.description.length > 200) errs.push(where + '：描述超过 200 字节上限');
    }
    total += g.items.length;
  }
  if (total > ITEM_LIMIT) errs.push('配置项总数超过上限（' + ITEM_LIMIT + '）');
  return errs;
}

// DOM → 模型（不校验，供渲染前同步，防止重建卡片丢失未保存输入）
function collectStructDraft() {
  const groups = [];
  for (const card of $$('#struct-groups .struct-group')) {
    const nameIn = card.querySelector('[data-sf="gname"]');
    const items = [];
    for (const row of card.querySelectorAll('.srow:not(.srow-head)')) {
      const itemEl = row.closest('.struct-item') || row;
      const keyIn = row.querySelector('[data-sf="ikey"]');
      const sel = row.querySelector('select[data-act="structType"]');
      const req = row.querySelector('[data-sf="ireq"]');
      const sec = row.querySelector('[data-sf="isec"]');
      const descIn = itemEl.querySelector('[data-sf="idesc"]');
      const shChk = itemEl.querySelector('[data-sf="ishared"]');
      items.push({
        key: keyIn ? keyIn.value.trim() : '',
        type: sel && TYPES.includes(sel.value) ? sel.value : 'string',
        required: !!(req && req.checked),
        secret: !!(sec && sec.checked),
        description: descIn ? descIn.value.trim() : '',
        shared: !!(shChk && shChk.checked),
        __extra: {},
      });
    }
    groups.push({ name: nameIn ? nameIn.value.trim() : '', items });
  }
  if (!groups.length && S.structDraft && S.structDraft.groups.length) {
    // 表单尚未渲染（如 JSON 模式切换中）→ 保留模型
    return S.structDraft.groups;
  }
  return groups;
}

actions.addStructGroup = function () {
  S.structDraft.groups = collectStructDraft();
  S.structDraft.groups.push({ name: '', items: [] });
  markStructDirty();
  renderStructEditor();
  const cards = $$('#struct-groups .struct-group');
  const last = cards[cards.length - 1];
  if (last) last.querySelector('[data-sf="gname"]').focus();
};

actions.delStructGroup = function (el) {
  const gi = Number(el.dataset.gi);
  S.structDraft.groups = collectStructDraft();
  const g = S.structDraft.groups[gi];
  const name = g ? g.name : '';
  openModal({
    title: '删除组' + (name ? '「' + name + '」' : ''),
    message: '该组下未发布的草稿值将在结构发布后自动清理，已发布版本不受影响。确认删除该组？',
    okText: '删除组', danger: true,
    onOk: () => {
      S.structDraft.groups = collectStructDraft();
      S.structDraft.groups.splice(gi, 1);
      markStructDirty();
      renderStructEditor();
      toast('组已删除（保存并发布结构后生效）');
    },
  });
};

actions.renameStructGroup = function (el) { // 仅响应 change（失焦确认）
  const orig = el.dataset.orig || '';
  const v = el.value.trim();
  if (v === orig) return;
  if (!orig) { el.dataset.orig = v; markStructDirty(); return; } // 新组首次命名，无需确认
  if (!v) { el.value = orig; showErr('struct-err', '组名不能为空，已恢复为「' + orig + '」'); return; }
  openModal({
    title: '重命名组',
    message: `将组「${orig}」重命名为「${v}」？重命名视为删除旧组并新建组，旧组名下的草稿值将在结构发布后清理；已发布版本不受影响。`,
    okText: '重命名', danger: true,
    onOk: () => { el.dataset.orig = v; markStructDirty(); },
    onCancel: () => { el.value = orig; },
  });
};

actions.addStructItem = function (el) {
  const gi = Number(el.dataset.gi);
  S.structDraft.groups = collectStructDraft();
  const g = S.structDraft.groups[gi];
  if (!g) return;
  g.items.push({ key: '', type: 'string', required: false, secret: false, __extra: {} });
  markStructDirty();
  renderStructEditor();
  const cards = $$('#struct-groups .struct-group');
  const card = cards[gi];
  if (card) {
    const rows = card.querySelectorAll('.srow:not(.srow-head) [data-sf="ikey"]');
    const inp = rows[rows.length - 1];
    if (inp) inp.focus();
  }
};

actions.delStructItem = function (el) {
  const gi = Number(el.dataset.gi), ii = Number(el.dataset.ii);
  S.structDraft.groups = collectStructDraft();
  const g = S.structDraft.groups[gi];
  const it = g && g.items[ii];
  const label = (g ? g.name : '') + '/' + (it ? it.key || '（未填 key）' : '');
  openModal({
    title: '删除配置项 ' + label,
    message: '该配置项未发布的草稿值将在结构发布后自动清理，已发布版本不受影响。确认删除？',
    okText: '删除', danger: true,
    onOk: () => {
      S.structDraft.groups = collectStructDraft();
      const gg = S.structDraft.groups[gi];
      if (gg) gg.items.splice(ii, 1);
      markStructDirty();
      renderStructEditor();
      toast('配置项已删除（保存并发布结构后生效）');
    },
  });
};

// 类型 / secret 联动：secret 勾选 → 类型自动切 secret；类型改离 secret → 取消勾选（服务端规则）
actions.structType = function (el) {
  if (el.value !== 'secret') {
    const sec = el.closest('.srow').querySelector('[data-sf="isec"]');
    if (sec && sec.checked) sec.checked = false;
  }
  markStructDirty();
}; // 仅响应 change
actions.structSecret = function (el) {
  if (el.checked) {
    const sel = el.closest('.srow').querySelector('select[data-act="structType"]');
    if (sel) sel.value = 'secret';
  }
  markStructDirty();
}; // 仅响应 change

actions.toggleStructJson = function () {
  if (structJsonActive()) {
    // JSON → 表单：解析 + 语义校验，未通过则留在 JSON 模式
    let d;
    try { d = JSON.parse($('struct-draft').value || '{}'); }
    catch (e) { showErr('struct-err', 'JSON 非法，无法转回表单：' + e.message); return; }
    const noBase = d.base_version === null || d.base_version === undefined;
    S.structDraft = {
      base_version: noBase ? structBaseline() : Number(d.base_version),
      groups: normalizeGroups(d.groups),
    };
    const errs = validateGroups(S.structDraft.groups);
    if (errs.length) { showErr('struct-err', '结构定义有误：' + errs.join('；')); return; }
    hideErr('struct-err');
    $('struct-draft').classList.add('hidden');   // 先切模式，renderStructEditor 才会渲染表单 DOM
    $('struct-form').classList.remove('hidden');
    renderStructEditor();
    $('btn-struct-mode-label').textContent = 'JSON 模式';
  } else {
    S.structDraft.groups = collectStructDraft();
    syncStructJsonTextarea();
    hideErr('struct-err');
    $('struct-form').classList.add('hidden');
    $('struct-draft').classList.remove('hidden');
    $('struct-draft').focus();
    $('btn-struct-mode-label').textContent = '表单模式';
  }
};

async function loadStructNow() {
  try {
    const [d] = await Promise.all([
      j('GET', `/api/v1/projects/${S.project}/structure-draft`),
      loadPublishedStruct(), // 同步刷新已发布结构（基线交叉校验 + 级联源）
    ]);
    applyServerStructDraft(d);
    renderStructEditor();
    hideErr('struct-err');
    S.structDirty = false;
    updateStructStatus();
    return true;
  } catch (e) {
    if (!e.expired) toast(e.message, 'err');
    return false;
  }
}

actions.loadStructDraft = function (el) {
  withBusy(el, async () => { if (await loadStructNow()) toast('已载入当前结构'); });
};

actions.saveStructDraft = async function (el) {
  if (!S.project) return;
  if (structJsonActive()) {
    // JSON 模式：以 textarea 为准（非法 JSON / 语义错误 → 内联报错，不发请求）
    let d;
    try { d = JSON.parse($('struct-draft').value || '{}'); }
    catch (e) { showErr('struct-err', '结构 JSON 非法：' + e.message); return; }
    const noBase = d.base_version === null || d.base_version === undefined;
    S.structDraft = {
      base_version: noBase ? structBaseline() : Number(d.base_version),
      groups: normalizeGroups(d.groups),
    };
    const errs = validateGroups(S.structDraft.groups);
    if (errs.length) { showErr('struct-err', errs.join('；')); return; }
  } else {
    const groups = collectStructDraft();
    S.structDraft.groups = groups;
    const errs = validateGroups(groups);
    if (errs.length) { showErr('struct-err', errs.join('；')); return; }
  }
  hideErr('struct-err');
  await withBusy(el, async () => {
    try {
      await j('PUT', `/api/v1/projects/${S.project}/structure-draft`, {
        base_version: S.structDraft.base_version,
        groups: serializeGroups(S.structDraft.groups),
      });
      S.structDirty = false; // 已保存，与服务端一致
      S.hasStructDraft = true; // 服务端现在有结构草稿（未发布）
      updateStructStatus();
      toast('结构草稿已保存');
    } catch (e) {
      if (e.status === 409) {
        toast('结构已被他人更新（base_version 不匹配），已载入当前结构，请检查后重试', 'warn');
        loadStructNow();
      } else if (!e.expired) toast(e.message, 'err');
    }
  });
};

actions.publishStruct = function () {
  if (!S.project) return;
  if (S.structDirty) { showErr('struct-err', '结构有未保存的修改，请先「保存草稿」再发布'); return; }
  openModal({
    title: '发布结构',
    message: '发布结构将推进全部分支的版本；结构中已不存在的组 / 配置项，其未发布的草稿值将被自动清理，已发布版本不受影响。',
    input: true, label: '备注', placeholder: '备注（可选）',
    okText: '发布结构',
    onOk: async (comment) => {
      try {
        const r = await j('POST', `/api/v1/projects/${S.project}/structure-draft/publish`, { comment, request_id: rid() });
        // 服务端发布后草稿即删除、结构版本推进；loadProject 内 GET /structure 重拉权威数据
        // （既作值新增级联源，也作结构编辑器的下一轮基线）
        S.structDirty = false;
        toast('结构已发布：' + ((r.affected_branches || []).length) + ' 个分支版本推进');
        loadProject();
      } catch (e) {
        if (e.status === 409) {
          toast('结构已更新（base_version 不匹配），已载入当前结构，请检查后重试', 'warn');
          loadStructNow();
        } else if (!e.expired) toast(e.message, 'err');
      }
    },
  });
};

/* ---------- 配置预览 ---------- */
actions.openCfgModal = function () {
  if (!S.project || !S.branch) return toast('请先选择项目与分支', 'err');
  $('cfgm-ctx').textContent = `${S.project} / ${S.branch}`;
  $('cfg-overlay').classList.remove('hidden');
  fetchCfg();
};
actions.closeCfg = function () { $('cfg-overlay').classList.add('hidden'); };
actions.cfgReveal = function () { fetchCfg(); };          // 仅响应 change
actions.cfgFormat = function () { fetchCfg(); };          // 仅响应 change
actions.refreshCfg = function () { fetchCfg(); };

async function fetchCfg() {
  const out = $('cfg-out');
  const reveal = $('cfg-reveal').checked;
  const fmt = $('cfg-format').value;
  // 明文（reveal）同样按所选格式渲染：?format=..&reveal=true 走渲染端点会话鉴权+审计，
  // 按所选格式（YAML/JSON/TOML/ENV）输出且 secret 解密为明文（不再退回管理面 JSON 结构）。
  $('cfg-format').disabled = false;
  out.textContent = '加载中…';
  try {
    const q = 'format=' + encodeURIComponent(fmt) + (reveal ? '&reveal=true' : '');
    out.textContent = await jtext(`/v1/projects/${S.project}/branches/${S.branch}/config?${q}`);
  } catch (e) {
    out.textContent = '';
    if (!e.expired) toast(e.message, 'err');
  }
}

actions.copyCfg = async function () {
  const t = $('cfg-out').textContent || '';
  if (!t || t === '加载中…') return;
  if (await copyText(t)) toast('已复制到剪贴板');
  else toast('复制失败，请手动选择复制', 'err');
};

/* ---------- 分支对比 / 提升 ---------- */
actions.showDiff = async function (el) {
  const a = $('diff-a').value, b = $('diff-b').value;
  if (!a || !b) return toast('请选择对比分支', 'err');
  await withBusy(el, async () => {
    try {
      const d = await j('GET', `/api/v1/projects/${S.project}/diff?branch_a=${encodeURIComponent(a)}&branch_b=${encodeURIComponent(b)}`);
      renderDiff(d, a, b);
    } catch (e) { if (!e.expired) toast(e.message, 'err'); }
  });
};

function renderDiff(d, a, b) {
  const diffs = d.diffs || [], missing = d.missing || [];
  if (!diffs.length && !missing.length) {
    $('diff-out').innerHTML = '<div class="empty mini"><svg class="ic"><use href="#i-check"/></svg><h4>两个分支完全一致</h4><p>没有发现值差异或缺失项。</p></div>';
    return;
  }
  let html = `<div class="table-wrap"><table class="table">
    <thead><tr><th>key</th><th>${esc(a)}</th><th>${esc(b)}</th></tr></thead><tbody>`;
  for (const x of diffs) {
    html += `<tr><td class="mono">${esc(x.group)}/${esc(x.key)}</td><td class="mono brk">${esc(fmtVal(x.branch_a))}</td><td class="mono brk">${esc(fmtVal(x.branch_b))}</td></tr>`;
  }
  for (const m of missing) {
    html += `<tr><td class="mono">${esc(m)}</td><td colspan="2" class="muted">仅一侧有值</td></tr>`;
  }
  html += '</tbody></table></div>';
  $('diff-out').innerHTML = html;
}

actions.doPromote = async function (el) {
  const from = $('promote-from').value, to = $('promote-to').value;
  if (!from || !to) return toast('请选择提升源 / 目标分支', 'err');
  if (from === to) return toast('源与目标分支不能相同', 'err');
  await withBusy(el, async () => {
    try {
      const r = await j('POST', `/api/v1/projects/${S.project}/promote`, { from, to, force: $('promote-force').checked });
      toast(`提升完成：写入 ${r.applied.length} 项，跳过 ${r.skipped.length} 项，源缺失 ${r.missing_from.length} 项`);
      if (r.skipped.length) toast('已跳过（目标草稿已修改，可勾选 force 覆盖）：' + r.skipped.join('、'), 'warn', 8000);
      loadBranch();
    } catch (e) { if (!e.expired) toast(e.message, 'err'); }
  });
};

/* ---------- 共享库 ---------- */
async function loadShared() {
  if ($('sh-value-wrap') && !$('sh-value-wrap').innerHTML) renderSharedValueControl(); // 首次进入/类型变更后初始化
  $('shared-body').innerHTML = '<tr><td colspan="8">' + skeleton(4) + '</td></tr>';
  try {
    const [pub, draft] = await Promise.all([
      j('GET', '/api/v1/shared').catch(() => []),
      j('GET', '/api/v1/shared-draft').catch(() => []),
    ]);
    // 已保存未发布的共享草稿计数 + 表单未保存标记重置（刷新后视为已同步）
    S.sharedDraftCount = (draft || []).length;
    S.sharedDirty = false;
    updateSharedStatus();
    const rows = (draft || []).map((x) => ({ ...x, __draft: true }))
      .concat((pub || []).map((x) => ({ ...x, __draft: false })));
    if (!rows.length) {
      $('shared-body').innerHTML = '<tr><td colspan="8"><div class="empty mini"><svg class="ic"><use href="#i-shared"/></svg><h4>暂无共享项</h4><p>在上方表单创建共享草稿，发布后各项目可在结构页引用。</p></div></td></tr>';
      return;
    }
    $('shared-body').innerHTML = rows.map((x) => {
      const refs = x.refs || [];
      const refTxt = refs.length ? refs.map((r) => r.project + '/' + r.branch + '/' + r.group + '/' + r.item_key).join('<br>') : '—';
      const delBtn = x.__draft
        ? `<button type="button" class="icon-btn danger" data-act="deleteSharedDraftItem" data-key="${esc(x.key)}" title="删除草稿" aria-label="删除草稿"><svg class="ic"><use href="#i-trash"/></svg></button>`
        : `<button type="button" class="icon-btn danger" data-act="deleteSharedItem" data-key="${esc(x.key)}" data-refs="${esc(refs.map((r) => r.project + '/' + r.branch + '/' + r.group + '/' + r.item_key).join(', '))}" title="删除共享项" aria-label="删除共享项"><svg class="ic"><use href="#i-trash"/></svg></button>`;
      return `<tr>
      <td class="mono">${esc(x.key)}</td>
      <td class="mono muted">${esc(x.ty || x.type || '')}</td>
      <td>${x.__draft ? '<span class="badge warn">草稿</span>' : `<span class="badge ok">v${x.version}</span>`}</td>
      <td class="mono brk">${esc(fmtVal(x.value))}</td>
      <td class="mono muted">${esc(x.description || '')}</td>
      <td class="small" title="被项目结构引用">${refs.length ? `<span class="badge acc" title="${esc(refTxt)}">${refs.length} 处</span>` : '<span class="muted">—</span>'}</td>
      <td>${x.secret ? '<span class="badge err"><svg class="ic ic-xs"><use href="#i-lock"/></svg>secret</span>' : ''}</td>
      <td>${delBtn}</td>
    </tr>`;
    }).join('');
  } catch (e) {
    if (!e.expired) { $('shared-body').innerHTML = ''; toast(e.message, 'err'); }
  }
}
actions.refreshShared = function () { loadShared(); };

// 共享项值输入：按类型渲染控件（与配置管理页一致；不再要求手写 Value JSON）
function renderSharedValueControl() {
  const ty = $('sh-type') ? $('sh-type').value : 'string';
  $('sh-value-wrap').innerHTML = valueControlHtml(ty, 'sh-value');
  const hint = $('sh-value-hint');
  if (hint) hint.textContent = '类型 ' + ty + ' · ' + (VAL_HINTS[ty] || '');
}
actions.shType = function () { renderSharedValueControl(); }; // 仅响应 change

actions.saveShared = async function (el) {
  const key = $('sh-key').value.trim();
  if (!key) { showErr('sh-err', 'key 必填'); return; }
  const ty = $('sh-type').value;
  const valEl = $('sh-value');
  if (!valEl) { showErr('sh-err', '请先填写值'); return; }
  const raw = (valEl.type === 'checkbox') ? ((valEl.checked) ? 'true' : 'false') : valEl.value;
  if (ty !== 'bool' && !raw.trim()) { showErr('sh-err', '请填写值'); return; }
  let value;
  try { value = buildValue(ty, raw); }
  catch (e) { showErr('sh-err', e.message); return; }
  const desc = $('sh-desc') ? $('sh-desc').value.trim() : '';
  if (desc && desc.length > 200) { showErr('sh-err', '描述超过 200 字节上限'); return; }
  hideErr('sh-err');
  const body = { key, type: ty, secret: $('sh-secret').checked, required: $('sh-required').checked, description: desc || undefined, value };
  await withBusy(el, async () => {
    try {
      await j('POST', '/api/v1/shared', body);
      toast('共享草稿已保存');
      loadShared();
    } catch (e) { if (!e.expired) toast(e.message, 'err'); }
  });
};

actions.publishShared = function () {
  if (S.sharedDirty) toast('有未保存的表单：本次发布只包含已保存的共享草稿，请先「保存共享草稿」', 'warn', 6000);
  openModal({
    title: '发布共享',
    message: '发布全部共享草稿；引用这些共享项的项目分支将自动级联生成新版本。',
    input: true, label: '备注', placeholder: '备注（可选）',
    okText: '发布共享',
    onOk: async (comment) => {
      try {
        const r = await j('POST', '/api/v1/shared/publish', { comment, request_id: rid() });
        toast(`共享已发布 v${r.version}，级联 ${ (r.affected || []).length } 个分支`);
        loadShared(); loadSharedItems();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

// 删除共享项（草稿 / 已发布）；被引用时服务端 409，toast 展示引用方
actions.deleteSharedItem = async function (el) {
  const key = el.dataset.key;
  const refs = el.dataset.refs || '';
  openModal({
    title: '删除共享项 ' + key,
    message: (refs ? '该项目当前被引用：' + refs + '。删除将被拒绝，请先在项目结构中移除引用。' : '删除已发布共享项（连同草稿）。已发布版本快照不受影响；被项目结构引用时将拒绝删除。') + ' 确认删除？',
    okText: '删除', danger: true,
    onOk: async () => {
      try {
        await j('DELETE', '/api/v1/shared/' + encodeURIComponent(key));
        toast('共享项已删除');
        loadShared(); loadSharedItems();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

actions.deleteSharedDraftItem = async function (el) {
  const key = el.dataset.key;
  try {
    await j('DELETE', '/api/v1/shared-draft/' + encodeURIComponent(key));
    toast('共享草稿已删除');
    loadShared();
  } catch (e) { if (!e.expired) toast(e.message, 'err'); }
};

/* ---------- 审计 ---------- */
async function loadAudit() {
  $('audit-body').innerHTML = '<tr><td colspan="8">' + skeleton(6) + '</td></tr>';
  try {
    const f = $('audit-filter').value.trim();
    const es = await j('GET', '/api/v1/audit?limit=200' + (f ? '&action=' + encodeURIComponent(f) : ''));
    if (!es.length) {
      $('audit-body').innerHTML = '<tr><td colspan="8"><div class="empty mini"><svg class="ic"><use href="#i-audit"/></svg><h4>暂无审计记录</h4><p>调整过滤条件或刷新查看最新操作。</p></div></td></tr>';
      return;
    }
    $('audit-body').innerHTML = es.map((x) => `<tr>
      <td class="mono muted tnum">${x.seq}</td>
      <td class="muted small nowrap">${fmtTime(x.ts)}</td>
      <td class="mono">${esc(x.action)}</td>
      <td class="mono">${esc(x.project || '')}${x.branch ? '/' + esc(x.branch) : ''}</td>
      <td class="mono tnum">${esc(x.version ?? '')}</td>
      <td>${esc(x.operator || '')}</td>
      <td class="mono muted small">${esc(x.request_id || '')}</td>
      <td class="mono brk small">${esc(JSON.stringify(x.detail || {}))}</td>
    </tr>`).join('');
  } catch (e) {
    if (!e.expired) { $('audit-body').innerHTML = ''; toast(e.message, 'err'); }
  }
}
actions.refreshAudit = function () { loadAudit(); };

/* ---------- 集群 ---------- */
async function loadCluster() {
  const box = $('cluster-out');
  box.innerHTML = '<div class="card">' + skeleton(3) + '</div>';
  try {
    const m = await j('GET', '/api/v1/cluster/members');
    const members = m.members || [];
    let html = `<div class="card cluster-meta">本节点 <code>${esc(m.node_id ?? 'dev-single')}</code> · 状态 <code>${esc(m.state)}</code> · 当前 Leader <code>${esc(m.current_leader ?? '—')}</code></div>`;
    if (members.length) {
      html += `<div class="card"><div class="table-wrap"><table class="table">
        <thead><tr><th>node_id</th><th>HTTP</th><th>gRPC</th><th>角色</th><th></th></tr></thead><tbody>
        ${members.map((x) => `<tr>
          <td class="mono tnum">${esc(x.node_id)}</td>
          <td class="mono">${esc(x.http_addr || '—')}</td>
          <td class="mono">${esc(x.grpc_addr || '—')}</td>
          <td>${x.is_voter ? '<span class="badge acc">voter</span>' : '<span class="badge">learner</span>'}${x.is_leader ? ' <span class="badge ok">leader</span>' : ''}</td>
          <td class="nowrap">${S.role === 'project_admin'
            ? '<span class="muted small">只读</span>'
            : (x.is_voter
              ? `<button type="button" class="btn sm ghost danger" data-act="removeNode" data-node="${esc(x.node_id)}" data-http="${esc(x.http_addr || '')}" data-raft="${esc(x.raft_addr || '')}">移除</button>`
              : `<button type="button" class="btn sm" data-act="promoteNode" data-node="${esc(x.node_id)}" data-http="${esc(x.http_addr || '')}" data-raft="${esc(x.raft_addr || '')}"><svg class="ic ic-xs"><use href="#i-up"/></svg>提升为 voter</button>`)}</td>
        </tr>`).join('')}
      </tbody></table></div></div>`;
    } else {
      html += '<div class="card empty"><svg class="ic"><use href="#i-cluster"/></svg><h4>无集群成员</h4><p>当前为单节点模式，没有 Raft 成员。集群模式下可在此查看与管理节点。</p></div>';
    }
    box.innerHTML = html;
  } catch (e) {
    if (e.expired) return;
    box.innerHTML = e.message.includes('404')
      ? '<div class="card empty"><svg class="ic"><use href="#i-cluster"/></svg><h4>单节点模式</h4><p>dev-single 模式没有集群管理；以集群模式启动后可在此查看成员、提升与移除节点。</p></div>'
      : `<div class="card empty"><svg class="ic"><use href="#i-alert"/></svg><h4>无法加载集群信息</h4><p>${esc(e.message)}</p></div>`;
  }
}
actions.refreshCluster = function () { loadCluster(); };

actions.promoteNode = function (el) {
  const { node, http, raft } = el.dataset;
  openModal({
    title: '提升节点',
    message: `将节点 ${node} 提升为 voter，参与 Raft 共识投票。`,
    okText: '提升',
    onOk: async () => {
      try {
        await j('POST', '/api/v1/cluster/promote', { node_id: Number(node), http_addr: http || '', raft_addr: raft || '' });
        toast('节点已提升');
        loadCluster();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

actions.removeNode = function (el) {
  const { node } = el.dataset;
  openModal({
    title: '移除节点',
    message: `将节点 ${node} 从成员表移除；移除前请确认该节点已安全停机。`,
    okText: '移除', danger: true,
    onOk: async () => {
      try {
        await j('POST', '/api/v1/cluster/remove', { node_id: Number(node) });
        toast('节点已移除');
        loadCluster();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

/* ---------- 管理员（全局管理员：全局改密 + 项目管理员账号管理） ---------- */
async function loadAdmins() {
  const sel = $('adm-project');
  if (!sel) return;
  if (!S.projects.length) {
    try { S.projects = (await j('GET', '/api/v1/projects')) || []; } catch (_) { /* 401 由 j() 统一处理 */ }
  }
  sel.innerHTML = S.projects.map((p) => `<option value="${esc(p.id)}">${esc(p.id)}</option>`).join('');
  if (S.project && S.projects.some((p) => p.id === S.project)) sel.value = S.project;
  refreshAdmins();
}

async function refreshAdmins() {
  const pid = $('adm-project').value;
  if (!pid) { $('adm-body').innerHTML = '<tr><td colspan="4" class="muted">请选择项目</td></tr>'; return; }
  const rows = await j('GET', `/api/v1/projects/${encodeURIComponent(pid)}/admins`).catch(() => []);
  if (!rows.length) {
    $('adm-body').innerHTML = '<tr><td colspan="4"><div class="empty mini"><svg class="ic"><use href="#i-admin"/></svg><h4>暂无项目管理员</h4><p>在上方表单创建；项目管理员只能管理本项目的配置。</p></div></td></tr>';
    return;
  }
  $('adm-body').innerHTML = rows.map((a) => `<tr>
    <td class="mono">${esc(a.username)}</td>
    <td class="muted small">${fmtTime(a.created_at)}</td>
    <td><button type="button" class="btn sm ghost" data-act="setAdminPw" data-u="${esc(a.username)}">改密</button></td>
    <td><button type="button" class="btn sm ghost danger" data-act="deleteAdmin" data-u="${esc(a.username)}">删除</button></td>
  </tr>`).join('');
}
actions.admSelectProject = function () { refreshAdmins(); }; // 仅响应 change

actions.createAdmin = async function (el) {
  const pid = $('adm-project').value;
  const u = $('adm-username').value.trim();
  const p = $('adm-password').value;
  if (!pid || !u || !p) { showErr('adm-err', '项目 / 用户名 / 密码必填'); return; }
  hideErr('adm-err');
  await withBusy(el, async () => {
    try {
      await j('POST', `/api/v1/projects/${encodeURIComponent(pid)}/admins`, { username: u, password: p });
      toast('项目管理员已创建');
      $('adm-username').value = ''; $('adm-password').value = '';
      refreshAdmins();
    } catch (e) { if (!e.expired) toast(e.message, 'err'); }
  });
};

actions.deleteAdmin = function (el) {
  const pid = $('adm-project').value;
  const u = el.dataset.u;
  openModal({
    title: '删除管理员 ' + u,
    message: '确认删除项目管理员「' + u + '」？其全部会话将一并失效。',
    okText: '删除', danger: true,
    onOk: async () => {
      try {
        await j('DELETE', `/api/v1/projects/${encodeURIComponent(pid)}/admins/${encodeURIComponent(u)}`);
        toast('已删除');
        refreshAdmins();
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

actions.setAdminPw = function (el) {
  const pid = $('adm-project').value;
  const u = el.dataset.u;
  openModal({
    title: '修改密码 · ' + u,
    message: '为项目管理员「' + u + '」设置新密码（至少 6 位），其现有会话将失效。',
    input: true, label: '新密码', placeholder: '至少 6 位',
    okText: '修改',
    onOk: async (pw) => {
      if (!pw || pw.length < 6) { toast('密码至少 6 位', 'err'); return; }
      try {
        await j('PUT', `/api/v1/projects/${encodeURIComponent(pid)}/admins/${encodeURIComponent(u)}`, { password: pw });
        toast('密码已修改');
      } catch (e) { if (!e.expired) toast(e.message, 'err'); }
    },
  });
};

/* ---------- 自助修改密码（全局管理员 / 项目管理员通用；校验当前密码） ---------- */
actions.openChangePw = function () {
  hideErr('pw-err');
  $('pw-current').value = ''; $('pw-new').value = ''; $('pw-new2').value = '';
  $('pw-overlay').classList.remove('hidden');
  $('pw-current').focus();
};
actions.closePwModal = function () {
  $('pw-overlay').classList.add('hidden');
};
actions.submitChangePw = async function (el) {
  const cur = $('pw-current').value;
  const n1 = $('pw-new').value;
  const n2 = $('pw-new2').value;
  if (!cur) { showErr('pw-err', '请输入当前密码'); return; }
  if (!n1 || n1.length < 6) { showErr('pw-err', '新密码至少 6 位'); return; }
  if (n1 !== n2) { showErr('pw-err', '两次输入不一致'); return; }
  hideErr('pw-err');
  await withBusy(el, async () => {
    try {
      await j('POST', '/api/v1/me/password', { current_password: cur, new_password: n1 });
      $('pw-overlay').classList.add('hidden');
      // 服务端已收回全部旧会话（含当前）：清除本地会话并回登录页（不触发「会话已过期」提示）
      S.token = '';
      localStorage.removeItem(LS_TOKEN); localStorage.removeItem(LS_ROLE); localStorage.removeItem(LS_PROJ);
      $('app').classList.add('hidden');
      $('login-view').classList.remove('hidden');
      toast('密码已修改，请重新登录');
    } catch (e) { if (!e.expired) showErr('pw-err', e.message); }
  });
};
/* ============================================================
   事件绑定与启动
   ============================================================ */
const CHANGE_ONLY = new Set([
  'selectBranch', 'cfgFormat', 'cfgReveal', 'admSelectProject', // 下拉/复选：仅响应 change，避免 click 误触发
  'structType', 'structSecret', 'renameStructGroup', // 结构编辑器行内控件
]);

function bindEvents() {
  // data-act 委托（D-CSP：无 onclick 属性）
  document.addEventListener('click', (e) => {
    const el = e.target.closest('[data-act]');
    if (!el || el.disabled) return;
    const name = el.dataset.act;
    if (CHANGE_ONLY.has(name)) return;
    const fn = actions[name];
    if (typeof fn === 'function') fn.call(el, el, e);
  });
  document.addEventListener('change', (e) => {
    const el = e.target.closest('[data-act]');
    if (!el || el.disabled) return;
    const fn = actions[el.dataset.act];
    if (typeof fn === 'function') fn.call(el, el, e);
  });

  // 保存状态指示：草稿页值输入/绑定选择 → 未保存标记；共享库表单 → 未保存标记
  document.addEventListener('change', (e) => {
    const t = e.target;
    if (!t || !t.classList) return;
    if (t.classList.contains('draft-in') || t.classList.contains('draft-shared-bind')) {
      markDraftDirty();
      // 绑定选择即时预览物化值（客户端查 S.sharedItems；保存后由服务端解析值替换）
      if (t.classList.contains('draft-shared-bind')) {
        const row = t.closest('.grow');
        const valEl = row && row.querySelector('.bind-val');
        if (valEl) {
          const sh = (S.sharedItems || []).find((s) => s.key === t.value);
          valEl.innerHTML = sharedBindValueHtml(t.value, null, sh);
        }
      }
    }
    else if (t.id === 'sh-key' || t.id === 'sh-desc' || t.id === 'sh-value'
      || t.id === 'sh-type' || t.id === 'sh-secret' || t.id === 'sh-required') markSharedDirty();
  });
  document.addEventListener('input', (e) => {
    const t = e.target;
    if (!t || !t.classList) return;
    if (t.classList.contains('draft-in')) markDraftDirty();
    else if (t.id === 'sh-key' || t.id === 'sh-desc' || t.id === 'sh-value') markSharedDirty();
  });

  // 登录（Enter 提交）
  $('login-form').addEventListener('submit', (e) => { e.preventDefault(); doLogin(); });

  // 弹窗
  $('modal-ok').addEventListener('click', () => {
    const v = $('modal-input').value;
    const cb = modalCb;
    closeModal(true);
    if (cb) cb(v);
  });
  $('modal-cancel').addEventListener('click', () => closeModal(false));
  $('modal-overlay').addEventListener('mousedown', (e) => { if (e.target === $('modal-overlay')) closeModal(false); });
  $('modal-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); $('modal-ok').click(); }
  });
  $('cfg-overlay').addEventListener('mousedown', (e) => { if (e.target === $('cfg-overlay')) actions.closeCfg(); });
  $('token-overlay').addEventListener('mousedown', (e) => { if (e.target === $('token-overlay')) actions.closeTokenModal(); });
  $('pw-overlay').addEventListener('mousedown', (e) => { if (e.target === $('pw-overlay')) actions.closePwModal(); });
  $('err-overlay').addEventListener('mousedown', (e) => { if (e.target === $('err-overlay')) closeErrorModal(); });

  // 修改密码弹窗：Enter 提交（任一输入框）
  for (const id of ['pw-current', 'pw-new', 'pw-new2']) {
    $(id).addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); $('pw-overlay').querySelector('[data-act="submitChangePw"]').click(); }
    });
  }

  // Esc 关闭弹窗
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (!$('err-overlay').classList.contains('hidden')) closeErrorModal();
    else if (!$('modal-overlay').classList.contains('hidden')) closeModal(false);
    else if (!$('cfg-overlay').classList.contains('hidden')) actions.closeCfg();
    else if (!$('token-overlay').classList.contains('hidden')) actions.closeTokenModal();
    else if (!$('pw-overlay').classList.contains('hidden')) actions.closePwModal();
  });

  // 审计过滤（Enter）
  $('audit-filter').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); loadAudit(); }
  });

  // 未保存编辑标记：结构（表单输入 + JSON textarea）与灰度规则
  $('pane-structure').addEventListener('input', () => { S.structDirty = true; });
  $('pane-gray').addEventListener('input', (e) => {
    if (e.target.closest('#gray-form') || e.target.id === 'gray-rule') S.grayDirty = true;
  });

  // 会话心跳（5 分钟）
  setInterval(() => {
    if (S.token) j('POST', '/api/v1/heartbeat', {}).catch(() => { /* 心跳失败不打扰 */ });
  }, 300000);
}

function boot() {
  initTheme();
  bindEvents();
  loadBuildInfo(); // 页脚部署版本标记（/healthz build 信息，登录页同样可见）
  S.token = localStorage.getItem(LS_TOKEN) || '';
  S.role = localStorage.getItem(LS_ROLE) || '';
  S.roleProject = localStorage.getItem(LS_PROJ) || '';
  if (S.token) enterApp();
  else $('login-view').classList.remove('hidden');
}

// 页脚显示构建版本：Defing · build <git 短哈希> · <构建时间>（便于确认部署产物）
function loadBuildInfo() {
  fetch('/healthz')
    .then((r) => r.json())
    .then((j) => {
      const el = $('build-info');
      if (!el || !j || !j.build) return;
      const t = new Date((j.build.time || 0) * 1000);
      const ts = Number.isNaN(t.getTime()) ? '' : ' · ' + t.toISOString().replace('T', ' ').slice(0, 16);
      el.textContent = 'Defing · build ' + (j.build.commit || 'unknown') + ts;
    })
    .catch(() => { /* 版本标记获取失败不打扰 */ });
}

boot();
