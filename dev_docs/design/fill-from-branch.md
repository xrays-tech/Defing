# 设计文档：草稿页「从其他分支取值填充」（fill-from-branch）

状态: v1 待交叉复核
日期: 2026-09-01
范围: admin UI（`server/crates/dsh-api/admin/` 三文件：index.html / app.js / styles.css）+ 教程文档 docs/04-draft.md
关联文档: docs/04-draft.md（草稿页行为基线）、docs/02-project.md §2.4（分支对比与值提升）、dev_docs/aegis/plans/2026-09-01-fill-from-branch.md（开发计划）

---

## 1. 背景与问题

### 1.1 现状

草稿页（配置管理 → 草稿）按已发布结构全量渲染配置项（`renderDraftEditor` / `draftStructRowHtml`，app.js:644-723）：

- **非共享项**（`it.shared = false`）：`.gctl` 列渲染单个输入控件（string/int/float/bool/json/array/secret），值基线 = 草稿值优先、无草稿回退活动版本值；
- **引用共享项**（`it.shared = true`）：`sharedBindRowHtml` 渲染「引用共享」下拉 + 物化值展示（只读），不参与本功能。

跨分支复用同一配置项的值（典型：dev/test/prod 同构项目），当前只能：

1. 手动切分支（`sel-branch`）→ 找到该项 → 复制 → 切回 → 粘贴；或
2. 到「对比与提升」页做**整分支**值提升（promote，粒度太大、会覆盖目标分支其它草稿值）。

### 1.2 目标

在**草稿模式**下，对**非共享引用**配置项，在填值输入控件的**右侧水平对齐**一个小图标；点击弹出「有值的分支」列表，**选项内显示该配置项在该分支的值**；点击选项即把该值填入当前输入控件。

设计目标（用户原话拆解）：

- **最大量减少打字填写**：一次点击完成「取值 → 填入」，且值可见可确认；
- **对无法确认的值（secret 等）不需要反复切换分支粘贴复制**：secret 明文不可显示，但点击即可填充，无需人工搬运。

### 1.3 用户已确认的三项决策（2026-09-01）

| # | 决策点 | 结论 | 理由 |
| --- | --- | --- | --- |
| Q1 | secret 项是否支持填充 | **支持：写入但不显示明文** | 最大程度减少打字（secret 恰恰是最难打的）；弹窗中 secret 行显示「已加密」，点击后经既有 reveal 审计通道取明文写入草稿，toast 确认，不显示明文 |
| Q2 | 每个分支显示哪个值 | **草稿优先 + 发布值并列**：优先显示该分支草稿值；无草稿时显示已发布值；两者都存在且不同时显示两条（草稿值 / 发布 vN），均可点击 | 覆盖「抄稳定版」与「抄未发布工作」两种场景 |
| Q3 | 跨分支值的数据来源 | **纯前端**：复用 `GET /api/v1/projects/{p}/branches/{b}` + 会话内缓存，零 API 变更 | 典型分支数 2-10，一次点击并行 N 个 GET，会话内缓存；不改 openapi/服务端/权限矩阵/契约校验，风险面最小 |

---

## 2. 范围

### 2.1 本期实现（in scope）

- 草稿页**非共享项**（`it.shared = false`）行内填充图标 + 弹出浮层；
- 跨分支取值：**草稿值优先 + 发布值并列**（两者存在且不同时各显示一条）；仅列出「该 key 有值」的分支；排除当前分支；
- 类型化填充：string / int / float / bool / json / array / secret 全部支持；
- secret 填充：仅**已发布值**可填充（经 `config_reveal` 审计通道取明文，写入不显示）；**草稿 secret 值置灰不可填充**（明文不可回读，见 §4.3）；
- 会话内缓存 + 失效（分支重载即失效）+ 浮层内「刷新」按钮；
- 空态 / 错误态 / 拉取失败重试；
- 文档同步：docs/04-draft.md 补小节。

### 2.2 明确不做（out of scope）

- **服务端 / API / openapi / 存储 schema 零改动**（纯前端）；
- 引用共享项（`it.shared = true`）不提供填充图标（值由共享库物化，语义不同，已有下拉绑定）；
- 「对比与提升」页既有功能（diff / promote）不动；
- 跨项目取值、多选批量填充、从当前分支自身取值、结构页填充；
- secret 明文在任何 UI 表面展示（保持既有安全模型）。

---

## 3. 数据流

### 3.1 取值（弹窗打开时）

```
点击填充图标（data-g/data-k/data-ty 定位当前项）
  → 缓存命中？用缓存 : Promise.all(并行 GET /api/v1/projects/{p}/branches/{b}，排除当前分支)
  → 缓存 { project, byName: { branchName: { draft, active, active_version } } }（S.branchValues）
  → 为当前 key 提取候选行：
      draft 值：b.draft[g][k].value（secret 为 {type:'secret', ciphertext} 或 {masked:true}）
      active 值：b.active[g][k].value（secret 恒 masked）
      行规则：
        - 草稿值存在 → 生成「草稿」行（secret 类 → 置灰不可填充）
        - 草稿值不存在 → 若 active 存在 → 生成「发布 vN」行
        - 草稿值存在 且 active 存在 且 两者不同 → 额外生成「发布 vN」行
        - 两者都无 → 该分支不列出
```

- 分支详情接口响应形状（lib.rs branch_detail :1336-1432）：`{ name, active_version, structure_version, draft_rev, draft: {g:{k:{value,updated_at}}}, shared_refs, active: {g:{k:{value,updated_at}}} }`；
- 有效性判定：`v` 为 `{type:'secret'}`（草稿密文）或 `{masked:true}`（活动掩码）→ 视为 secret 类；其余按类型字段取显示值。

### 3.2 填充（点击行时）

```
非 secret 行：
  raw = 源值原始文本（见 §4.2 表）
  按当前控件类型填入（input.value / textarea.value / checkbox.checked）
  markDraftDirty()；toast「已从分支 X 填充」
secret 行（仅发布行可点）：
  GET /v1/projects/{p}/branches/{b}/config?format=json&reveal=true   ← 既有审计通道（config_reveal）
  明文 = JSON.parse(body)[g][k]（secret 渲染为普通字符串）
  填入 password 输入框（不显示明文）；markDraftDirty()；toast「已从分支 X 填充 secret（明文不显示，保存后生效）」
```

- reveal 端点：`render_config`（lib.rs:2596-2690），管理面会话 → reveal=true 解密并记 `config_reveal` 审计（operator = admin / pa:username）；PA 仅限本项目（B2，既有矩阵）；
- 填充写入草稿保存时走既有 saveDraft 路径：password 输入框 → `{type:'string', str_value: 明文}` → 服务端加密。

### 3.3 缓存失效

- `S.branchValues` 在 `loadBranch()`（app.js:593-611）中置空 —— 该函数被分支切换、保存草稿、发布、提升、回滚、结构发布、灰度发布/转正/下量、建/删分支、项目切换等全部路径调用，覆盖所有会使**非共享项**分支值变化的操作；
- 共享发布（`publishShared`，app.js:1847）只级联**共享引用项**（无填充图标、不产生候选行），其 `active_version` 变化不影响本功能取值正确性；浮层另有「刷新」按钮强制重拉兜底。

---

## 4. UI 设计

### 4.1 图标

- 新 sprite 符号 `i-import`（箭头入托盘，24x24，追加进 index.html sprite）；
- 非共享项行 `.gctl` 改为 flex 容器：`输入控件 + <button class="icon-btn draft-fill">`，水平对齐输入框右侧；
- `title="从其他分支取值填充"`、`aria-label` 同文案；不携带 `draft-in` class（避免被保存草稿收集与 dirty 监听误伤）。

### 4.2 弹出浮层（#fill-pop）

- body 级 `position: fixed` 浮层（视口锚定），锚定图标下方（视口越界自动上翻/夹紧）；z-index 低于模态 overlay（90），高于页面内容；
- 结构：
  - 表头：`key 名（g/k）` + 「刷新」图标按钮；
  - 行列表：每行 = 分支名 + 来源徽章（`草稿` / `发布 vN`）+ 值（fmtVal 格式化，esc 转义）；
  - secret 值行：值区显示「已加密」；发布行可点击，草稿行置灰（`disabled` 样式 + 说明）；
  - 空态：「其他分支暂无该配置项的值」；无其他分支：「暂无其他分支」；
  - 错误态：拉取失败行显示错误 + 整层可重试；
- 关闭：点击浮层外 / Esc（并入既有 Esc 链，插入在错误弹窗判断之后、普通弹窗判断之前）；打开任何模态（openModal / 错误弹窗等）前先关闭浮层；打开新浮层前关闭旧的。

### 4.3 secret 语义（关键约束）

- 分支详情接口中，**草稿 secret 是密文 JSON**（`{type:'secret', ciphertext}`），**活动 secret 恒掩码**（`{masked:true, str_value:'***'}`）——两者都不含明文；
- 明文唯一来源是 reveal 端点，而 reveal 端点只渲染**已发布快照**（`get_config(version=0)`，不含草稿）；
- 结论：**secret 仅「发布值」可填充**；「草稿值」行置灰并提示「草稿明文不可回读，仅支持从已发布版本填充」；
- 弹窗列表本身**不**拉取明文（按需最小化）：仅点击 secret 行时才发起一次 reveal 请求，且每次记审计。

### 4.4 类型化填充（fill 引擎）

源值 → 原始文本：

| 源类型 | raw | 填入目标 |
| --- | --- | --- |
| string | `str_value` | input.value |
| int | `String(int_value)` | input.value |
| float | `String(float_value)` | input.value |
| bool | `'true'/'false'` | checkbox.checked = `bool_value ?? raw === 'true'` |
| json | `json_value` | textarea.value |
| array | `list_value.join(', ')` | input.value |
| secret | （仅 reveal 明文） | password.value（不显示） |

- 类型失配（源值与当前结构类型不同，如结构版本变迁）：按源值原始文本填入，保存时既有 `buildValue`（app.js:775-786）校验报错，不静默转换语义；
- **空字符串源值**：源分支某 key 的值原始文本为空（空串，仅 API 可造、UI 无法产生）视为该分支「无值」不列出——与系统「空输入 = 清除草稿值」语义一致（app.js:904-906），避免填充后被保存逻辑误删当前分支草稿；**该规则仅限非 secret 值**（secret 密文/掩码对象无原始文本字段，按存在性列出置灰行，见 §4.3）；
- 填充后调用 `markDraftDirty()`（程序赋值不触发原生 input/change 事件）。

---

## 5. 安全与兼容边界

- **明文最小化**：secret 明文仅在用户点击填充行时经 reveal 端点获取一次，不缓存、不显示、不入日志（审计只记 `config_reveal` 元信息，复用既有机制）；
- **授权**：分支详情与 reveal 均受服务端既有授权矩阵约束（PA 限本项目）；UI 无新增越权面；
- **XSS**：分支名 / 值 / 徽章文本全部经 `esc()` 转义（沿用既有模式）；浮层为 body 级元素，不参与 `#pane-draft` 的保存收集；
- **兼容**：零 API/契约/存储变化；旧 UI 行为不受影响；`draft-in`/`draft-shared-bind` 语义不变；共享引用行无图标（行为不变）；
- **缓存**：仅内存、会话内；不持久化。

---

## 6. 验收标准（observable acceptance criteria）

1. 草稿页非共享项（string/int/float/bool/json/array/secret）行内、输入控件右侧显示填充图标；共享引用项不显示；
2. 点击图标弹出列表：仅含「该 key 有值」的分支；当前分支被排除；草稿优先 + 发布并列（两者存在且不同时两条）；
3. 点击非 secret 行 → 值按类型填入对应控件，出现未保存标记，toast 确认；
4. secret 项：发布行显示「已加密」且可点击填充（password 框被赋值但界面不显示明文，toast 提示）；草稿行置灰不可点；
5. secret 填充后审计日志出现 `config_reveal`（operator=当前会话主体、分支=源分支）；
6. 空态/错误态正确；Esc / 点击外部关闭；刷新按钮强制重拉；
7. 保存草稿后再次打开浮层值已更新（缓存失效正确）；
8. 回归：`cargo test --workspace` 全绿、`bash scripts/check-contracts.sh` 通过、`bash scripts/api-surface-test.sh` 通过；既有 UI 流程（保存/发布/引用共享绑定）不受影响。

---

## 7. 影响面与文件

| 文件 | 动作 | 内容 |
| --- | --- | --- |
| server/crates/dsh-api/admin/index.html | 改 | sprite 追加 `i-import` 符号；body 级追加 `#fill-pop` 浮层容器 |
| server/crates/dsh-api/admin/app.js | 改 | 状态 `S.branchValues`；`loadBranch` 失效；`draftStructRowHtml` 行内 icon；取值/填充/浮层渲染/关闭逻辑；secret reveal 路径；Esc 链扩展 |
| server/crates/dsh-api/admin/styles.css | 改 | `.gctl` flex 布局、`.draft-fill`、`.fill-pop` 浮层样式 |
| docs/04-draft.md | 改 | §4.1 补「从其他分支取值填充」小节 |

零改动：api/openapi.v1.yaml、schema/、server/crates/*（Rust）、scripts/（若 api-surface 需补断言则另议，本期预计不需要——无 API 变更）。

---

## 8. 风险与权衡

1. **secret 仅发布值可填充**：草稿明文不可回读是系统安全模型（密文单向），非本功能限制；文案已明示。若未来需要「草稿 secret 可填充」，需新增服务端解密回读通道（本期不做，需单独评审）。
2. **并行 N 个 GET 的规模**：分支数很大（>50）时一次点击开销大；缓存 + 刷新按钮缓解；如实际出现性能问题，后续可加服务端聚合端点（本期不做，Q3 已定纯前端）。
3. **类型失配填充**：按原始文本填入 + 保存时校验报错，避免静默转换造成错误配置；用户可在浮层看到源值后自行判断。
4. **浮层定位**：`position: fixed` + 视口夹紧；窄屏下浮层宽度自适应（max-width）。
