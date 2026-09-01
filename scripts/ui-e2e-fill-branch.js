#!/usr/bin/env node
// 草稿页「从其他分支取值填充」（fill-from-branch）UI e2e
// 无头 Chrome CDP 驱动（node >= 18 内置 WebSocket / fetch，无外部依赖）。
// 用法: node scripts/ui-e2e-fill-branch.js
// 前置: 已构建 server/target/debug/defing（cargo build -p dsh-cli）
'use strict';
const { spawn, execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');

const ROOT = path.resolve(__dirname, '..');
const BIN = process.env.BIN || path.join(ROOT, 'server/target/debug/defing');
const PORT = 8396;              // 独立端口，避开现有脚本 8383/8384/8397
const CDP_PORT = 9333;
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN_PW = 'admin123';
const KEY = path.join(os.tmpdir(), 'dsh-ui-e2e-fill.key');

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
    // ---- 0. 启动 dev-single（secret 场景需主密钥）----
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
      { key: 'debug', type: 'bool' },
      { key: 'tags', type: 'array' },
      { key: 'cfg', type: 'json' },
      { key: 'token', type: 'secret', secret: true },
      { key: 'shared_val', type: 'string', shared: true }, // 共享引用项：无填充图标
    ] }];
    J('POST', '/api/v1/projects', { name: 'demo' }); // 自动生成默认分支 dev/test/prod
    J('PUT', '/api/v1/projects/demo/structure-draft', { base_version: 1, groups: structGroups });
    J('POST', '/api/v1/projects/demo/structure-draft/publish', { comment: 's', request_id: 's1' });
    // 共享引用项：建共享项并发布，供各分支绑定（发布校验要求 shared 项已选择引用）
    J('POST', '/api/v1/shared', { key: 'lib_timeout', type: 'string', description: '共享超时', value: { type: 'string', str_value: '30s' } });
    J('POST', '/api/v1/shared/publish', { comment: 'lib', request_id: 'sl1' });
    const bindShared = { shared_bindings: [{ group: 'app', key: 'shared_val', shared_key: 'lib_timeout' }] };
    // test：全量发布 + 未发布草稿覆盖 host / token（草稿优先 + 发布并列、secret 草稿置灰场景）
    J('PUT', '/api/v1/projects/demo/branches/test/draft', { updates: [
      { group: 'app', key: 'host', value: { type: 'string', str_value: 't.example.com' } },
      { group: 'app', key: 'port', value: { type: 'int', int_value: 8080 } },
      { group: 'app', key: 'debug', value: { type: 'bool', bool_value: true } },
      { group: 'app', key: 'tags', value: { type: 'array', list_value: ['x', 'y'] } },
      { group: 'app', key: 'cfg', value: { type: 'json', json_value: '{"a":1}' } },
      { group: 'app', key: 'token', value: { type: 'string', str_value: 'tok-secret-test' } },
    ], ...bindShared });
    J('POST', '/api/v1/projects/demo/branches/test/publish', { comment: 't1', request_id: 't1' });
    J('PUT', '/api/v1/projects/demo/branches/test/draft', { updates: [
      { group: 'app', key: 'host', value: { type: 'string', str_value: 't-draft.example.com' } },
      { group: 'app', key: 'token', value: { type: 'string', str_value: 'tok-secret-test-draft' } }, // 未发布草稿 secret：置灰行场景
    ] });
    // prod：部分发布（无 tags/cfg → 这些 key 的弹窗只列 test）
    J('PUT', '/api/v1/projects/demo/branches/prod/draft', { updates: [
      { group: 'app', key: 'host', value: { type: 'string', str_value: 'p.example.com' } },
      { group: 'app', key: 'port', value: { type: 'int', int_value: 443 } },
      { group: 'app', key: 'debug', value: { type: 'bool', bool_value: false } },
      { group: 'app', key: 'token', value: { type: 'string', str_value: 'tok-secret-prod' } },
    ], ...bindShared });
    J('POST', '/api/v1/projects/demo/branches/prod/publish', { comment: 'p1', request_id: 'p1' });
    // solo：单分支项目（空态「暂无其他分支」）
    J('POST', '/api/v1/projects', { name: 'solo' }); // 默认分支 dev/test/prod；删掉 test/prod → 单分支空态场景
    J('DELETE', '/api/v1/projects/solo/branches/test');
    J('DELETE', '/api/v1/projects/solo/branches/prod');
    J('PUT', '/api/v1/projects/solo/structure-draft', { base_version: 1, groups: [{ name: 'app', items: [{ key: 'host', type: 'string', required: true }] }] });
    J('POST', '/api/v1/projects/solo/structure-draft/publish', { comment: 's', request_id: 's2' });

    // ---- 2. 无头 Chrome CDP ----
    const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-ui-e2e-'));
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
    await evalJs(`(async () => {
      document.querySelector('[data-act="selectProject"][data-id="demo"]').click();
      await new Promise(r => setTimeout(r, 1200));
      const sel = document.getElementById('sel-branch');
      sel.value = 'dev'; sel.dispatchEvent(new Event('change', { bubbles: true }));
      await new Promise(r => setTimeout(r, 1200));
      return true;
    })()`);
    await sleep(800);

    // ---- 3. 断言 ----
    console.log('== 3.1 图标存在性（非共享行 6 个）==');
    const iconCount = await evalJs(`document.querySelectorAll('#pane-draft .draft-fill').length`);
    assert(iconCount === 6, `非共享行 6 个填充图标（实际 ${iconCount}）`);

    console.log('== 3.2 host：草稿优先 + 发布并列 + 排除当前分支/无值分支 ==');
    await evalJs(`document.querySelector('#pane-draft .draft-fill[data-k="host"]').click()`);
    await sleep(900);
    const hostRows = await evalJs(`Array.from(document.querySelectorAll('#fill-pop .fill-row')).map(r => ({
      text: r.textContent.replace(/\\s+/g, ' ').trim(), disabled: r.hasAttribute('disabled') }))`);
    console.log('  rows:', JSON.stringify(hostRows));
    assert(hostRows.length === 3, `host 弹层 3 行（test 草稿 + test 发布 + prod 发布；实际 ${hostRows.length}）`);
    assert(hostRows.some((r) => r.text.includes('test') && r.text.includes('草稿') && r.text.includes('t-draft.example.com')), 'test 草稿行（t-draft.example.com）');
    assert(hostRows.some((r) => r.text.includes('test') && r.text.includes('发布 v2') && r.text.includes('t.example.com')), 'test 发布 v2 行（t.example.com）');
    assert(hostRows.some((r) => r.text.includes('prod') && r.text.includes('发布 v2') && r.text.includes('p.example.com')), 'prod 发布 v2 行（p.example.com）');
    assert(!hostRows.some((r) => r.text.includes('dev')), '当前分支 dev 被排除');

    console.log('== 3.3 填充 + 未保存标记 ==');
    await evalJs(`(async () => {
      const row = Array.from(document.querySelectorAll('#fill-pop .fill-row')).find(r => r.textContent.includes('t-draft.example.com'));
      row.click(); await new Promise(r => setTimeout(r, 300));
      return true;
    })()`);
    const hostVal = await evalJs(`document.querySelector('#pane-draft .draft-in[data-k="host"]').value`);
    assert(hostVal === 't-draft.example.com', `host 输入框已填 t-draft.example.com（实际 ${hostVal}）`);
    const unsaved = await evalJs(`!document.getElementById('draft-unsaved').classList.contains('hidden')`);
    assert(unsaved, '出现「未保存」标记');
    const popHidden = await evalJs(`document.getElementById('fill-pop').classList.contains('hidden')`);
    assert(popHidden, '点击行后浮层关闭');

    console.log('== 3.4 类型化填充（port 数字 / debug 勾选 / tags 数组 / cfg JSON）==');
    await evalJs(`(async () => {
      document.querySelector('#pane-draft .draft-fill[data-k="port"]').click();
      await new Promise(r => setTimeout(r, 700));
      Array.from(document.querySelectorAll('#fill-pop .fill-row')).find(r => r.textContent.includes('prod')).click();
      await new Promise(r => setTimeout(r, 300));
      document.querySelector('#pane-draft .draft-fill[data-k="debug"]').click();
      await new Promise(r => setTimeout(r, 700));
      Array.from(document.querySelectorAll('#fill-pop .fill-row')).find(r => r.textContent.includes('test')).click();
      await new Promise(r => setTimeout(r, 300));
      document.querySelector('#pane-draft .draft-fill[data-k="tags"]').click();
      await new Promise(r => setTimeout(r, 700));
      Array.from(document.querySelectorAll('#fill-pop .fill-row'))[0].click();
      await new Promise(r => setTimeout(r, 300));
      document.querySelector('#pane-draft .draft-fill[data-k="cfg"]').click();
      await new Promise(r => setTimeout(r, 700));
      Array.from(document.querySelectorAll('#fill-pop .fill-row'))[0].click();
      await new Promise(r => setTimeout(r, 300));
      return true;
    })()`);
    const vals = await evalJs(`({
      port: document.querySelector('#pane-draft .draft-in[data-k="port"]').value,
      debug: document.querySelector('#pane-draft .draft-in[data-k="debug"]').checked,
      tags: document.querySelector('#pane-draft .draft-in[data-k="tags"]').value,
      cfg: document.querySelector('#pane-draft .draft-in[data-k="cfg"]').value,
    })`);
    assert(vals.port === '443', `port 填 443（实际 ${vals.port}）`);
    assert(vals.debug === true, `debug 勾选 true（test 行；实际 ${vals.debug}）`);
    assert(vals.tags === 'x, y', `tags 填 x, y（实际 ${vals.tags}）`);
    assert(vals.cfg === '{"a":1}', `cfg 填 {"a":1}（实际 ${vals.cfg}）`);
    // tags/cfg 仅 test 有值 → prod 未列
    await evalJs(`document.querySelector('#pane-draft .draft-fill[data-k="tags"]').click()`);
    await sleep(700);
    const tagsRows = await evalJs(`Array.from(document.querySelectorAll('#fill-pop .fill-row')).length`);
    assert(tagsRows === 1, `tags 弹层仅 1 行（prod 无该值；实际 ${tagsRows}）`);
    await evalJs(`document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))`);
    await sleep(200);

    console.log('== 3.5 secret：发布行可填充 + 草稿行置灰 + 明文不显示 + 审计 ==');
    await evalJs(`document.querySelector('#pane-draft .draft-fill[data-k="token"]').click()`);
    await sleep(900);
    const tokenRows = await evalJs(`Array.from(document.querySelectorAll('#fill-pop .fill-row')).map(r => ({
      text: r.textContent.replace(/\\s+/g, ' ').trim(), disabled: r.hasAttribute('disabled') }))`);
    console.log('  rows:', JSON.stringify(tokenRows));
    assert(tokenRows.length === 3, `token 弹层 3 行（test 草稿 + test 发布 + prod 发布；实际 ${tokenRows.length}）`);
    assert(tokenRows.every((r) => r.text.includes('已加密')), 'token 行均显示「已加密」（明文不显示）');
    assert(!JSON.stringify(tokenRows).includes('tok-secret'), '浮层 DOM 不含 secret 明文');
    const tdraft = tokenRows.find((r) => r.text.includes('草稿'));
    assert(!!tdraft && tdraft.disabled, 'test 草稿 secret 行置灰不可点');
    assert(tokenRows.filter((r) => !r.disabled).length === 2, '两条发布行可点');
    await evalJs(`Array.from(document.querySelectorAll('#fill-pop .fill-row')).find(r => r.hasAttribute('disabled')).click()`);
    await sleep(300);
    const tokPre = await evalJs(`document.querySelector('#pane-draft .draft-in[data-k="token"]').value`);
    assert(tokPre === '', '点击置灰行不填充（值仍为空）');
    await evalJs(`(async () => {
      const row = Array.from(document.querySelectorAll('#fill-pop .fill-row')).find(r => !r.hasAttribute('disabled') && r.textContent.includes('test'));
      row.click(); await new Promise(r => setTimeout(r, 1200));
      return true;
    })()`);
    const tokVal = await evalJs(`document.querySelector('#pane-draft .draft-in[data-k="token"]').value`);
    assert(tokVal === 'tok-secret-test', `token 输入框已写入明文但界面不可见（值正确；实际 ${JSON.stringify(tokVal)}）`);
    const tokType = await evalJs(`document.querySelector('#pane-draft .draft-in[data-k="token"]').type`);
    assert(tokType === 'password', 'token 输入框仍为 password 类型（不显示明文）');
    const tokDisp = await evalJs(`document.querySelector('#pane-draft .draft-in[data-k="token"]').value.length`);
    assert(tokDisp > 0, 'token 输入框值非空（已赋值）');
    const pageHtml = await evalJs(`document.body.innerHTML`);
    assert(!pageHtml.includes('tok-secret-test'), '页面 DOM 不含 secret 明文');
    const audit = JSON.parse(curl(['-sf', `${BASE}/api/v1/audit`, '-H', `Authorization: Bearer ${TOKEN}`]));
    const reveals = audit.filter((x) => x.action === 'config_reveal' && x.branch === 'test');
    assert(reveals.length >= 1, `审计出现 config_reveal（branch=test，${reveals.length} 条）`);

    console.log('== 3.8 共享引用行无填充图标 ==');
    const sharedRows = await evalJs(`document.querySelectorAll('#pane-draft .ref-grow').length`);
    const sharedIcons = await evalJs(`document.querySelectorAll('#pane-draft .ref-grow .draft-fill').length`);
    assert(sharedRows === 1, `共享引用行存在（实际 ${sharedRows}）`);
    assert(sharedIcons === 0, '共享引用行无填充图标');

    console.log('== 3.6 空态：单分支项目「暂无其他分支」 ==');
    await evalJs(`(async () => {
      document.querySelector('[data-act="selectProject"][data-id="solo"]').click();
      await new Promise(r => setTimeout(r, 1200));
      document.querySelector('#pane-draft .draft-fill[data-k="host"]').click();
      await new Promise(r => setTimeout(r, 800));
      return true;
    })()`);
    const emptyText = await evalJs(`document.getElementById('fill-pop').textContent.replace(/\\s+/g, ' ').trim()`);
    assert(emptyText.includes('暂无其他分支'), `空态文案（实际 ${emptyText.slice(0, 60)}）`);

    console.log('== 3.7 Esc 关闭 + 点击外部关闭 ==');
    await evalJs(`(async () => {
      document.querySelector('[data-act="selectProject"][data-id="demo"]').click();
      await new Promise(r => setTimeout(r, 1200));
      document.querySelector('#pane-draft .draft-fill[data-k="host"]').click();
      await new Promise(r => setTimeout(r, 800));
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));
      await new Promise(r => setTimeout(r, 200));
      return true;
    })()`);
    const escHidden = await evalJs(`document.getElementById('fill-pop').classList.contains('hidden')`);
    assert(escHidden, 'Esc 关闭浮层');
    await evalJs(`document.querySelector('#pane-draft .draft-fill[data-k="host"]').click()`);
    await sleep(800);
    await evalJs(`document.body.click()`);
    await sleep(200);
    const outHidden = await evalJs(`document.getElementById('fill-pop').classList.contains('hidden')`);
    assert(outHidden, '点击外部关闭浮层');

    console.log('== 3.10 刷新按钮 + 保存后缓存失效 ==');
    await evalJs(`document.querySelector('#pane-draft .draft-fill[data-k="host"]').click()`);
    await sleep(800);
    const before = await evalJs(`document.querySelectorAll('#fill-pop .fill-row').length`);
    assert(before === 3, `刷新前 host 弹层 3 行（实际 ${before}）`);
    J('PUT', '/api/v1/projects/demo/branches/prod/draft', { updates: [{ group: 'app', key: 'host', value: { type: 'string', str_value: 'p-draft.example.com' } }] });
    await evalJs(`document.querySelector('#fill-pop [data-act="fillRefresh"]').click()`);
    await sleep(900);
    const after = await evalJs(`Array.from(document.querySelectorAll('#fill-pop .fill-row')).map(r => r.textContent.replace(/\\s+/g, ' ').trim())`);
    assert(after.length === 4, `刷新后 host 弹层 4 行（实际 ${after.length}）`);
    assert(after.some((t) => t.includes('p-draft.example.com') && t.includes('草稿')), '刷新后出现 prod 草稿行（p-draft.example.com）');
    await evalJs(`(async () => {
      const row = Array.from(document.querySelectorAll('#fill-pop .fill-row')).find(r => r.textContent.includes('p-draft.example.com'));
      row.click(); await new Promise(r => setTimeout(r, 300));
      document.querySelector('[data-act="saveDraft"]').click();
      await new Promise(r => setTimeout(r, 1800));
      return true;
    })()`);
    J('PUT', '/api/v1/projects/demo/branches/test/draft', { updates: [{ group: 'app', key: 'host', value: { type: 'string', str_value: 't2.example.com' } }] });
    await evalJs(`document.querySelector('#pane-draft .draft-fill[data-k="host"]').click()`);
    await sleep(900);
    const afterSave = await evalJs(`Array.from(document.querySelectorAll('#fill-pop .fill-row')).map(r => r.textContent.replace(/\\s+/g, ' ').trim())`);
    assert(afterSave.some((t) => t.includes('t2.example.com')), '保存草稿后重开浮层显示最新值（缓存已失效）');
    await evalJs(`document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))`);
    await sleep(200);

    console.log(failures === 0 ? '\n======== UI e2e（fill-from-branch）全部通过 ========' : `\n======== UI e2e 失败 ${failures} 项 ========`);
    process.exitCode = failures === 0 ? 0 : 1;
  } finally {
    try { if (ws) ws.close(); } catch (_) {}
    try { if (chrome) chrome.kill(); } catch (_) {}
    try { if (srv) srv.kill(); } catch (_) {}
    try { fs.unlinkSync(KEY); } catch (_) {}
  }
}

main().catch((e) => { console.error('e2e error:', e); process.exit(1); });
