# 开发计划：草稿页「从其他分支取值填充」（fill-from-branch）

日期: 2026-09-01
范围确认: 与用户确认（2026-09-01 三项决策）——① secret 支持填充（写入不显示明文，走 reveal 审计通道）；② 值来源草稿优先 + 发布值并列；③ 纯前端复用分支详情接口 + 会话内缓存，零 API 变更
上游: dev_docs/design/fill-from-branch.md（设计文档，本期交叉复核后为 v1）
执行路线: 单工作区逐 slice 实现，每 slice 后跑对应验证；全部完成后整体验证 + reviewer 交叉对比
基线: main HEAD 935efb9（工作区干净）

## TDD Route

```text
TDD Route:
- Mode: off
- Decision: skipped
- Strict authority: not applicable（用户未要求 strict TDD；本期为纯前端 UI 功能）
- Test posture: post-change regression —— Rust/契约测试全量回归（无服务端改动，应全绿）；新 UI 行为用无头 Chrome（CDP）e2e 断言 + node --check 语法校验
- Reason: 零服务端/契约改动；UI 功能以真实浏览器交互验证为主；e2e 脚本与 api-surface-test.sh 保持同一主密钥策略（--master-key-file），口径一致
- Verification: node --check app.js + 重编 dsh-api + 无头 Chrome e2e 清单 + cargo test --workspace + bash scripts/check-contracts.sh + bash scripts/api-surface-test.sh
```

## 0. 目标与基线

- 目标：草稿页非共享配置项行内右侧显示填充图标；点击弹出「有值分支」列表（草稿优先 + 发布并列，排除当前分支），点击即类型化填入当前输入控件；secret 经 reveal 审计通道填充（仅发布值，明文不显示）；会话内缓存 + 失效。
- 明确不做（本期）：服务端/API/openapi/schema 改动；共享引用项图标；跨项目取值；批量填充；secret 明文展示。
- 兼容边界：零 API 契约变化；`draft-in`/`draft-shared-bind` 收集与 dirty 监听语义不变；共享引用行无图标（行为不变）。
- 基线命令：
  - `cd server && source ../scripts/build-env.sh && cargo test --workspace`（全绿基线，job bash-354 验证中）
  - UI 改动需重编 dsh-api（rust_embed 嵌入 admin/，build.rs rerun-if-changed=admin）：`cargo build -p dsh-api`
  - 契约：`bash scripts/check-contracts.sh`；e2e：`bash scripts/api-surface-test.sh`（自起 dev-single 8384）
  - UI e2e：无头 Chrome CDP 脚本（scripts/ui-e2e-fill-branch.js，node 内置 WebSocket 驱动 chrome --headless --remote-debugging-port）

## 1. 文件地图

| 文件 | 动作 | 内容 |
| --- | --- | --- |
| server/crates/dsh-api/admin/index.html | 改 | sprite 追加 `i-import` 符号（:14-45 区）；body 级追加 `<div id="fill-pop" class="fill-pop hidden"></div>`（模态区附近，如 :496 配置预览弹窗附近） |
| server/crates/dsh-api/admin/app.js | 改 | 状态 `S.branchValues`（§47-66 状态区）；`loadBranch` 失效（:593-611）；`draftStructRowHtml` 非共享行 `.gctl` 包 flex + 填充图标（:688-723）；新增「填充」模块（取值缓存 / 候选行 / 浮层渲染 / 填充引擎 / secret reveal / 关闭）；Esc 链（:2172-2180）与事件绑定区（:2093-2197）扩展 |
| server/crates/dsh-api/admin/styles.css | 改 | `.gctl` flex 布局兼容、`.draft-fill`、`.fill-pop` 浮层与行样式 |
| docs/04-draft.md | 改 | §4.1 补「从其他分支取值填充」小节 |
| scripts/ui-e2e-fill-branch.js | 新增 | 无头 Chrome CDP e2e 脚本（本次验证用；留在 scripts/ 供回归） |

零改动：api/openapi.v1.yaml、schema/、server/crates/*（Rust）、scripts/api-surface-test.sh（无 API 变更，预计不需要）。

## 2. Slice 划分

- S1 UI 实现（index.html + styles.css + app.js）→ node --check + 重编 dsh-api + 无头 Chrome 清单
- S2 文档（docs/04-draft.md）→ 审读
- S3 e2e 脚本固化（scripts/ui-e2e-fill-branch.js）→ 跑通
- 全量：cargo test --workspace + check-contracts + api-surface-test + UI e2e
- 完成后：reviewer（subagent）对照设计文档/计划复核实现；修复发现的问题；全部提交 → push → CI 全绿

## 3. S1：Admin UI 实现（T1-T4）

### 任务 3.1 index.html（T1）

1. sprite（:14-45 区）追加：
   ```html
   <symbol id="i-import" viewBox="0 0 24 24"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M4 21h16"/></symbol>
   ```
2. 模态区（:496「配置预览弹窗」附近）追加浮层容器：
   ```html
   <!-- 从其他分支取值填充浮层 -->
   <div id="fill-pop" class="fill-pop hidden" role="listbox"></div>
   ```

### 任务 3.2 app.js：状态 + 缓存失效（T2）

1. 状态区（:47-66）追加：
   ```js
   branchValues: null,   // { project, byName: {branch: {draft, active, active_version}} } —— 填充浮层数据缓存（会话内）
   ```
2. `loadBranch()`（:593-611）开头（`S.branch = nb;` 之后）追加：
   ```js
   S.branchValues = null; // 分支上下文变化（切换/保存/发布/提升/回滚/结构发布等全部经此）→ 填充缓存失效
   ```
3. `loadSharedItems()`（:468-475）开头追加 `S.branchValues = null;`（共享发布只级联共享引用项、不重载分支，经此兜底「发布 vN」徽标过期——共享引用项无填充图标，取值正确性不受影响）。

### 任务 3.3 app.js：行内填充图标（T3）

`draftStructRowHtml`（:688-723）：非共享行 `.gctl` 由裸控件改为 flex 包裹（bool 行 checkbox 同列对齐）：

```js
ctl = `<div class="fill-ctl"><label class="check">...checkbox...</label>${fillBtn(g, it)}</div>`;  // bool
// 其余类型：
ctl = `<div class="fill-ctl">${原控件}${fillBtn(g, it)}</div>`;
```

`fillBtn` 输出（新增辅助，紧邻 draftStructRowHtml）：

```js
function fillBtn(g, it) {
  return `<button type="button" class="icon-btn draft-fill" data-act="fillFromBranch" data-g="${esc(g.name)}" data-k="${esc(it.key)}" data-ty="${esc(it.type || 'string')}" title="从其他分支取值填充" aria-label="从其他分支取值填充"><svg class="ic"><use href="#i-import"/></svg></button>`;
}
```

注意：`saveDraft`（:892）收集 `#pane-draft .draft-in`，图标按钮无 `draft-in` class，天然不参与收集；dirty 监听（:2118-2141）只认 `draft-in`/`draft-shared-bind`，不受影响。

### 任务 3.4 app.js：填充模块（T4）

在「分支详情 / 草稿编辑」区块（:592 之后）新增「从其他分支取值填充」模块：

```js
/* ---------- 从其他分支取值填充（fill-from-branch） ---------- */

// 候选行：{ branch, version(active_version), source: 'draft'|'active', value(typed Value JSON), secret: bool, maskedText }
function fillCandidates(g, k, activeVersion) { ... } // 见下方逻辑

async function ensureBranchValues() { ... } // 缓存 → 并行 GET 各分支详情

function fillValueRaw(v) { // 源 Value JSON → 原始文本
  if (!v || typeof v !== 'object') return '';
  if (v.str_value !== undefined) return String(v.str_value);
  if (v.int_value !== undefined) return String(v.int_value);
  if (v.float_value !== undefined) return String(v.float_value);
  if (v.bool_value !== undefined) return v.bool_value ? 'true' : 'false';
  if (v.json_value !== undefined) return String(v.json_value);
  if (Array.isArray(v.list_value)) return v.list_value.join(', ');
  return '';
}

function applyFillToControl(g, k, ty, row) { ... } // 按控件类型填入 + markDraftDirty

async function fillSecretPlaintext(g, k, branch) { // 既有审计通道
  const txt = await jtext(`/v1/projects/${S.project}/branches/${encodeURIComponent(branch)}/config?format=json&reveal=true`);
  const tree = JSON.parse(txt || '{}');
  return (tree[g] && tree[g][k] !== undefined && tree[g][k] !== null) ? String(tree[g][k]) : null;
}

function renderFillPop(g, k, ty, anchor) { ... } // 浮层渲染：表头(key+刷新)、行列表、空态/错误态
function closeFillPop() { ... }
```

具体行为（与设计文档 §3/§4 逐条对应）：

1. **ensureBranchValues**：`S.branchValues && S.branchValues.project === S.project` → 复用；否则 `Promise.all` 并行 `GET /api/v1/projects/{p}/branches/{b}`（仅 `S.branches` 中 `name !== S.branch` 的分支），单分支失败降级为 `{error}` 行（不阻断整体）；成功缓存 `{project, byName}`。
2. **fillCandidates**（草稿优先 + 发布并列，排除无值分支）：
   - `dv = byName[b].draft?.[g]?.[k]?.value`；`av = byName[b].active?.[g]?.[k]?.value`；
   - `isSecret(v) = v && (v.masked === true || v.type === 'secret')`；
   - **空串规则（仅非 secret）**：`raw(v) === '' && !isSecret(v)`（空字符串源值，仅 API 可造）视为该分支「无值」不列出——避免填充后保存被当「清空=删除」处理（app.js:904-906）；**secret 值豁免**（secret 密文/掩码对象 `raw` 恒为 `''`，不能按空串排除，须按存在性列出置灰行）；
   - 有 `dv` → 推入 `{source:'draft', value: dv}`（secret → 行置灰不可点）；
   - `av` 存在且（无 `dv` 或 `raw(dv) !== raw(av)`）→ 推入 `{source:'active', version: active_version, value: av}`（secret → 行可点，走 reveal）。
3. **浮层**：body 级 `#fill-pop`；`renderFillPop` 填充 innerHTML（全部经 esc）；`fillPopAnchor(anchor)` 用 `getBoundingClientRect()` 定位（下方展开，视口越界上翻/夹紧，`max-width` 自适应）；行点击委托：非 secret 直接 `applyFillToControl`；secret 先 `withBusy(icon, async () => { 明文 = await fillSecretPlaintext(...); 若 null → showErrorModal('源分支该配置项明文不可获取'); 否则填入 + toast })`；
4. **applyFillToControl**：按 `data-ty` 找到 `#pane-draft .draft-in[data-g][data-k]`：
   - checkbox → `.checked = row.value.bool_value === true`；
   - textarea（json）→ `.value = fillValueRaw(row.value)`；
   - 其余 → `.value = fillValueRaw(row.value)`；secret 行 → `.value = 明文`；
   - 填后 `markDraftDirty()`；toast `已从分支 ${branch} 填充`（secret 版文案 `已从分支 ${branch} 填充 secret（明文不显示，保存后生效）`）。
5. **关闭**：`closeFillPop()` 隐藏；事件绑定区追加：`document click` 点击浮层外关闭（浮层自身与图标点击不关闭——判断 `!e.target.closest('#fill-pop') && !e.target.closest('.draft-fill')`）；Esc 链（:2172-2180）**在 `err-overlay` 判断之后、`modal-overlay` 判断之前**插入 `else if (!$('fill-pop').classList.contains('hidden')) closeFillPop();`（fill-pop 优先级：低于错误弹窗、高于普通弹窗）；**打开任何模态前先调用 `closeFillPop()`**（openModal / showErrorModal 的调用点或 openModal 函数内部）。
6. **刷新**：浮层表头刷新按钮 → `S.branchValues = null; renderFillPop(...)` 重拉。

### 任务 3.5 styles.css（T4）

追加（:569 `.grow .icon-btn` 附近）：

```css
/* fill-from-branch：行内填充图标 */
.fill-ctl { display: flex; align-items: center; gap: 6px; min-width: 0; }
.fill-ctl .in, .fill-ctl textarea { max-width: 520px; flex: 1 1 auto; }
.grow .icon-btn.draft-fill { margin-top: 0; flex-shrink: 0; }

/* 填充浮层：置于普通模态 overlay（z-index 90）之下、页面内容之上 */
.fill-pop {
  position: fixed; z-index: 70; min-width: 260px; max-width: 420px;
  background: var(--surface); border: 1px solid var(--border-strong);
  border-radius: 8px; box-shadow: var(--shadow-md); /* 或 0 6px 24px rgba(0,0,0,.18) */
  padding: 8px; max-height: 300px; overflow-y: auto;
}
.fill-head { display: flex; align-items: center; gap: 6px; padding: 2px 4px 8px; border-bottom: 1px solid var(--border); }
.fill-head .mono { flex: 1 1 auto; overflow-wrap: anywhere; }
.fill-row {
  display: flex; align-items: center; gap: 8px; width: 100%;
  padding: 6px 8px; border-radius: 6px; cursor: pointer; text-align: left;
  background: none; border: 0; font: inherit; color: inherit;
}
.fill-row:hover { background: var(--surface-2); }
.fill-row[disabled] { opacity: .55; cursor: not-allowed; }
.fill-row .fill-val { flex: 1 1 auto; min-width: 0; overflow-wrap: anywhere; font-family: var(--mono); font-size: 12px; }
.fill-empty, .fill-err { padding: 10px 8px; color: var(--fg-faint); font-size: 12px; }
.fill-err { color: var(--err); }
```

（CSS 变量以现有 styles.css 实际定义为准：`--surface` / `--surface-2` / `--border` / `--border-strong` / `--fg-faint` / `--err` / `--shadow-md` 均已存在。）

### 任务 3.6 验证（T1-T4 后）

```bash
node --check server/crates/dsh-api/admin/app.js
cd server && source ../scripts/build-env.sh && cargo build -p dsh-api
# 无头 Chrome e2e 见 §4（S3 固化前先手工跑同一脚本流程）
```

## 4. S3：无头 Chrome e2e 脚本（T5）

新增 `scripts/ui-e2e-fill-branch.js`（node ≥18，用内置 WebSocket 驱动 CDP，无外部依赖）：

流程（自起 dev-single 服务 → CDP 驱动浏览器 → 断言）：

1. 启动 `defing --dev-single --admin-password admin123 --master-key-file <临时密钥文件> --http-addr 127.0.0.1:8396`（独立端口 8396，避开现有脚本端口 8383/8384/8397；secret 场景需主密钥，临时密钥用 `dd if=/dev/urandom bs=32 count=1 of=/tmp/dsh-ui-e2e.key` 生成）；
2. 用 curl 准备数据（管理面 API）：
   - 项目 `demo`（**创建时自动生成默认分支 dev / test / prod**，无需显式建分支——state.rs apply_project_create:1222）；分支 `dev` / `test` / `prod`；
   - 发布结构（组 `app`：`host`(string) `port`(int) `debug`(bool) `tags`(array) `cfg`(json) `token`(secret)）；
   - `test` 分支草稿：host=`t.example.com`、port=8080、debug=true、tags=`x,y`、cfg=`{"a":1}`、token=`tok-secret-test` 并发布 v1，再追加**未发布草稿** host=`t-draft.example.com`（草稿优先 + 发布并列场景）；
   - `prod` 分支草稿：host=`p.example.com`、port=443、debug=false、token=`tok-secret-prod` 并发布 v1；
   - `dev` 分支留空（无草稿、无发布——验证「无值分支不列出」）；
   - 空态项目 `solo`：建项目后 **DELETE 掉 test/prod 分支**（项目创建恒生成 3 个默认分支，删到只剩 dev 才是单分支场景）→ 浮层显示「暂无其他分支」；
3. 启动 Chrome `--headless=new --remote-debugging-port=9333 --no-sandbox` 打开 `http://127.0.0.1:8396/admin`；
4. CDP 脚本：登录（填密码提交）→ 选项目/分支（dev 当前分支）→ 打开草稿页 → 断言：
   - 非共享行（host/port/debug/tags/cfg/token）存在 `.draft-fill` 图标；共享引用行（无）不测；
   - 点击 host 行图标 → 浮层出现且**仅含 test/prod 两行**（dev 排除、prod 无值项不列出）、值显示 `t.example.com` / `p.example.com`、来源徽章含 `发布 v1`（test/prod 均发布，无草稿 → 发布行）；
   - 点击 test 行 → host 输入框值 = `t.example.com`、出现「未保存」徽章；
   - port 行图标 → 点 prod 行 → port 输入框值 = `443`（数字控件）；
   - debug 行图标 → 点 test 行 → checkbox 勾选；
   - tags 行图标 → 点 test 行 → 输入框值 = `x, y`；
   - cfg 行图标 → 点 test 行 → textarea 值 = `{"a":1}`；
   - token 行图标 → 浮层中仅一条可点（发布 v1），点击 → toast「已填充 secret」、password 框被赋值（`value` 非空、`type=password` 不显示明文）；
   - 审计接口（管理面）出现 `config_reveal` 且 branch=test；
   - 空态：无其他分支场景（单独临时项目）→ 浮层显示「暂无其他分支」；
5. 输出 PASS/FAIL 汇总；非零退出码表示失败。

脚本留在 `scripts/` 供回归（`node scripts/ui-e2e-fill-branch.js`）。

## 5. S2：文档（T6）

### 任务 5.1 docs/04-draft.md

§4.1 表格后补「从其他分支取值填充」小节：

```markdown
**从其他分支取值填充**：非共享引用项输入框右侧的 ⤵ 图标，点击弹出「有值分支」列表
（当前分支排除；草稿值优先展示，已发布值并列显示），点击某行即把该分支该配置项的值
填入当前输入框（无需切换分支复制粘贴）：

- 支持 string / int / float / bool / json / array / secret 全部类型，按类型填入对应控件；
- **secret 项**：列表显示「已加密」，仅**已发布值**可填充——点击后经审计通道
  （审计日志记 `config_reveal`）取明文写入草稿，**明文不在界面展示**，保存后生效；
  草稿中的 secret 明文不可回读，故草稿行置灰不可填充；
- 数据为会话内缓存，分支重载自动失效；浮层内可点「刷新」强制重拉。
```

## 6. 全量验证（T7）

```bash
cd server && source ../scripts/build-env.sh && cargo test --workspace
bash scripts/check-contracts.sh
bash scripts/api-surface-test.sh
node scripts/ui-e2e-fill-branch.js
node --check server/crates/dsh-api/admin/app.js
```

## 7. 自检（self-review）

- 规格覆盖：图标/浮层/草稿优先+发布并列/类型化填充/secret reveal 填充/缓存失效/空态错误态/Esc 关闭/文档——设计文档 §6 验收标准 1-8 全部有对应任务（T1-T6）；
- 零契约变化：无 openapi/schema/Rust 改动；check-contracts 与 api-surface-test 应原样通过；
- 类型一致性：fillValueRaw 与既有 fmtVal（app.js:761-771）/ buildValue（:775-786）字段一致；
- 兼容：`draft-in` 收集与 dirty 监听不变；共享引用行无图标；saveDraft/publish 路径零改动；
- 安全：secret 明文仅 reveal 单次获取、不缓存不显示；审计复用既有 `config_reveal`；全部输出 esc() 转义；
- 验证闭环：语法 + 重编 + 无头 Chrome e2e（含 secret/审计/空态）+ Rust 全量 + 契约 + api-surface。

## 8. 风险与权衡

1. **浮层定位与遮挡**：`position: fixed` + 视口夹紧；z-index 70（低于模态 overlay 90、高于页面内容）；打开任何模态前先 `closeFillPop()`，避免同屏叠层。
2. **secret 草稿行不可填充**：系统安全模型所致（明文单向），非缺陷；文案明示（见设计 §4.3）。
3. **分支数规模**：缓存 + 刷新兜底；本期不做服务端聚合端点（Q3 已定）。
4. **无头 Chrome 环境差异**：CI 无 Chrome 时该脚本可跳过（UI e2e 为本机验证补充，不进 CI gating）；CI 的 Rust/契约/e2e 脚本保持不变。
