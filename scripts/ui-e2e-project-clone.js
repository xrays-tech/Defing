#!/usr/bin/env node
// 新建项目「从现有项目克隆结构」（project-clone）UI e2e
// 无头 Chrome CDP 驱动（node >= 18 内置 WebSocket / fetch，无外部依赖）。
// 用法: node scripts/ui-e2e-project-clone.js
// 前置: 已构建 server/target/debug/defing（cargo build -p dsh-cli）
'use strict';
const { spawn, execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');

const ROOT = path.resolve(__dirname, '..');
const BIN = process.env.BIN || path.join(ROOT, 'server/target/debug/defing');
const PORT = 8398;              // 独立端口（避开 8383/8384/8396/8397）
const CDP_PORT = 9334;          // 独立 CDP 端口（避开 9333）
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN_PW = 'admin123';
const KEY = path.join(os.tmpdir(), 'dsh-ui-e2e-clone.key');

let failures = 0;
function assert(cond, msg) {
  if (cond) console.log('  PASS ' + msg);
  else { failures++; console.error('  FAIL ' + msg); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function curl(args) {
  return execFileSync('curl', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
}

async function main() {
  let srv = null, chrome = null, ws = null;
  try {
    // ---- 0. 启动 dev-single ----
    fs.writeFileSync(KEY, crypto.randomBytes(32));
    srv = spawn(BIN, ['--dev-single', '--admin-password', ADMIN_PW, '--master-key-file', KEY,
      '--http-addr', `127.0.0.1:${PORT}`], { stdio: 'ignore' });
    let up = false;
    for (let i = 0; i < 40; i++) {
      try { if (curl(['-sf', `${BASE}/healthz`])) { up = true; break; } } catch (_) {}
      await sleep(500);
    }
    if (!up) { console.error('server start FAIL'); process.exit(1); }

    // ---- 1. 数据准备（管理面 API）----
    const TOKEN = JSON.parse(curl(['-sf', '-X', 'POST', `${BASE}/api/v1/login`, '-H', 'Content-Type: application/json',
      '-d', `{"password":"${ADMIN_PW}"}`])).token;
    const AUTH = ['-H', `Authorization: Bearer ${TOKEN}`, '-H', 'Content-Type: application/json'];
    const J = (method, p, body) => curl(['-sf', '-X', method, `${BASE}${p}`, ...AUTH, '-d', JSON.stringify(body)]);
    const structGroups = [{ name: 'app', items: [
      { key: 'host', type: 'string', required: true },
      { key: 'port', type: 'int' },
      { key: 'token', type: 'secret', secret: true },
      { key: 'shared_val', type: 'string', shared: true }, // 共享引用项
    ] }];
    // 源项目 demo：发布结构
    J('POST', '/api/v1/projects', { name: 'demo' });
    J('PUT', '/api/v1/projects/demo/structure-draft', { base_version: 1, groups: structGroups });
    J('POST', '/api/v1/projects/demo/structure-draft/publish', { comment: 's', request_id: 's1' });
    // 无关项目 other：空结构（用于下拉选项存在性断言）
    J('POST', '/api/v1/projects', { name: 'other' });

    // ---- 2. 无头 Chrome CDP ----
    const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-ui-e2e-clone-'));
    chrome = spawn('google-chrome', ['--headless=new', '--no-sandbox', '--disable-gpu',
      `--remote-debugging-port=${CDP_PORT}`, `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore' });
    await sleep(2500);
    const targets = await (await fetch(`http://127.0.0.1:${CDP_PORT}/json`)).json();
    const page = targets.find((t) => t.type === 'page');
    ws = new WebSocket(page.webSocketDebuggerUrl);
    let id = 0; const pending = new Map();
    const send = (method, params = {}) => new Promise((res, rej) => {
      const mid = ++id; pending.set(mid, { res, rej });
      ws.send(JSON.stringify({ id: mid, method, params }));
    });
    ws.onmessage = (ev) => {
      const m = JSON.parse(ev.data);
      if (m.id && pending.has(m.id)) { pending.get(m.id).res(m.result); pending.delete(m.id); }
    };
    await new Promise((r) => (ws.onopen = r));
    await send('Page.enable'); await send('Runtime.enable');
    const evalJs = async (expression) => {
      const r = await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
      if (r.exceptionDetails) throw new Error('page eval: ' + JSON.stringify(r.exceptionDetails.exception?.description || r.exceptionDetails));
      return r.result.value;
    };

    await send('Page.navigate', { url: `${BASE}/admin` });
    await sleep(1500);
    await evalJs(`(async () => {
      document.getElementById('login-pw').value = '${ADMIN_PW}';
      document.querySelector('#login-form button[type="submit"]').click();
      await new Promise(r => setTimeout(r, 900));
      return true;
    })()`);
    await sleep(1500);

    // 轮询等待条件成立（替代固定 sleep，容忍 headless 时序抖动）
    const waitFor = async (expr, label, timeout = 8000) => {
      const t0 = Date.now();
      while (Date.now() - t0 < timeout) {
        const v = await evalJs(expr);
        if (v) return v;
        await sleep(250);
      }
      throw new Error('waitFor timeout: ' + label);
    };
    // 等待某个项目成为当前选中（chip active 高亮）
    const waitActiveProject = (id) => waitFor(
      `document.querySelector('#proj-chips .chip.active') && document.querySelector('#proj-chips .chip.active').dataset.id === '${id}'`,
      `project ${id} active`,
    );

    // ---- 3. 断言 ----
    console.log('== 3.1 新建项目弹窗出现克隆下拉（含 demo / other）==');
    await evalJs(`document.querySelector('.proj-bar-actions [data-act="newProjectModal"]').click()`);
    await sleep(500);
    const selVisible = await evalJs(`!document.getElementById('modal-select-field').classList.contains('hidden')`);
    assert(selVisible, '弹窗显示「从现有项目克隆结构」下拉');
    const options = await evalJs(`Array.from(document.getElementById('modal-select').options).map(o => o.value)`);
    console.log('  options:', JSON.stringify(options));
    assert(options.includes(''), '默认「不克隆（空结构）」');
    assert(options.includes('demo') && options.includes('other'), '下拉含 demo / other');
    const selLabel = await evalJs(`document.getElementById('modal-select-label').textContent`);
    assert(selLabel.includes('克隆'), `下拉标签含「克隆」（实际 ${selLabel}）`);

    console.log('== 3.2 克隆创建：toast 提示来源 + 结构页渲染克隆组 ==');
    await evalJs(`(async () => {
      document.getElementById('modal-input').value = 'clone1';
      const sel = document.getElementById('modal-select');
      sel.value = 'demo'; sel.dispatchEvent(new Event('change', { bubbles: true }));
      await new Promise(r => setTimeout(r, 100));
      document.getElementById('modal-ok').click();
      await new Promise(r => setTimeout(r, 800));
      return true;
    })()`);
    await waitActiveProject('clone1');
    await waitFor(
      `document.querySelectorAll('#struct-groups .struct-group').length > 0 || !document.getElementById('struct-empty').classList.contains('hidden')`,
      'structure editor rendered',
    );
    const toastText = await evalJs(`Array.from(document.querySelectorAll('.toast .toast-text')).map(t => t.textContent).join(' | ')`);
    console.log('  toast:', JSON.stringify(toastText));
    assert(toastText.includes('结构克隆自 demo'), `toast 提示克隆来源（实际 ${toastText}）`);
    // 进入结构页
    await evalJs(`document.querySelector('[data-act="switchPane"][data-pane="structure"]').click()`);
    await sleep(400);
    const groups = await evalJs(`Array.from(document.querySelectorAll('#struct-groups .struct-group')).map(g => ({
      name: g.querySelector('.gname-in').value,
      keys: Array.from(g.querySelectorAll('[data-sf="ikey"]')).map(i => i.value),
    }))`);
    console.log('  groups:', JSON.stringify(groups));
    assert(groups.length === 1 && groups[0].name === 'app', '结构页渲染克隆组 app');
    assert(JSON.stringify(groups[0].keys) === JSON.stringify(['host', 'port', 'token', 'shared_val']), '克隆项 keys 齐全');
    // secret / shared 标记
    const marks = await evalJs(`({
      secretType: document.querySelector('#struct-groups [data-sf="ikey"][value="token"]').closest('.struct-item').querySelector('select[data-act="structType"]').value,
      sharedChecked: document.querySelector('#struct-groups [data-sf="ikey"][value="shared_val"]').closest('.struct-item').querySelector('[data-sf="ishared"]').checked,
    })`);
    assert(marks.secretType === 'secret', `token 类型为 secret（实际 ${marks.secretType}）`);
    assert(marks.sharedChecked === true, 'shared_val 引用共享勾选保留');

    console.log('== 3.3 不克隆创建：空结构 ==');
    await evalJs(`document.querySelector('.proj-bar-actions [data-act="newProjectModal"]').click()`);
    await sleep(400);
    await evalJs(`(async () => {
      document.getElementById('modal-input').value = 'clone2';
      await new Promise(r => setTimeout(r, 100));
      document.getElementById('modal-ok').click();
      await new Promise(r => setTimeout(r, 800));
      return true;
    })()`);
    await waitActiveProject('clone2');
    await waitFor(
      `!document.getElementById('struct-empty').classList.contains('hidden')`,
      'clone2 empty structure',
    );
    const toastsAll = await evalJs(`Array.from(document.querySelectorAll('.toast .toast-text')).map(t => t.textContent)`);
    const toast2 = toastsAll[toastsAll.length - 1] || '';
    console.log('  last toast:', JSON.stringify(toast2));
    assert(toast2.includes('项目已创建') && !toast2.includes('克隆自'), `不克隆时 toast 无克隆提示（实际 ${toast2}）`);
    await evalJs(`document.querySelector('[data-act="switchPane"][data-pane="structure"]').click()`);
    await sleep(400);
    const emptyVisible = await evalJs(`!document.getElementById('struct-empty').classList.contains('hidden')`);
    assert(emptyVisible, 'clone2 结构页为空结构（「暂无配置项」空态）');

    console.log('== 3.4 克隆项目可正常继续编辑结构（保存草稿）==');
    // 在 clone1 追加一个组并保存结构草稿（验证克隆结构可编辑、base_version=1 无冲突）
    await evalJs(`(async () => {
      document.querySelector('[data-act="selectProject"][data-id="clone1"]').click();
      await new Promise(r => setTimeout(r, 400));
      return true;
    })()`);
    await waitActiveProject('clone1');
    await waitFor(
      `document.querySelectorAll('#struct-groups .struct-group').length > 0`,
      'clone1 structure rendered',
    );
    await evalJs(`(async () => {
      document.querySelector('[data-act="switchPane"][data-pane="structure"]').click();
      await new Promise(r => setTimeout(r, 400));
      document.querySelector('[data-act="addStructGroup"]').click();
      await new Promise(r => setTimeout(r, 200));
      const last = Array.from(document.querySelectorAll('#struct-groups .struct-group')).pop();
      last.querySelector('.gname-in').value = 'newg';
      last.querySelector('.gname-in').dispatchEvent(new Event('input', { bubbles: true }));
      document.querySelector('[data-act="saveStructDraft"]').click();
      await new Promise(r => setTimeout(r, 1200));
      return true;
    })()`);
    const savedOk = await evalJs(`!document.getElementById('struct-unpublished').classList.contains('hidden')`);
    assert(savedOk, '克隆项目结构保存草稿成功（出现「草稿未发布」徽标）');

    // ---- 4. 清理 ----
    console.log(failures === 0 ? 'ALL PASS' : `FAILURES: ${failures}`);
  } finally {
    if (ws) { try { ws.close(); } catch (_) {} }
    if (chrome) { try { chrome.kill(); } catch (_) {} }
    if (srv) { try { srv.kill(); } catch (_) {} }
    try { fs.rmSync(KEY, { force: true }); } catch (_) {}
  }
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
