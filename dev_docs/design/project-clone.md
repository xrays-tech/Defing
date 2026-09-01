# 设计文档：新建项目支持「从现有项目克隆结构」（project-clone）

状态: v1 待交叉复核
日期: 2026-09-01
范围: admin UI（`server/crates/dsh-api/admin/` 三文件 index.html / app.js / styles.css）+ 服务端（`dsh-core` 状态机 + `dsh-api` handler）+ openapi 契约 + 教程文档 docs/02-project.md
关联文档: docs/02-project.md（项目与分支）、docs/03-structure.md（结构与共享引用）、dev_docs/aegis/plans/2026-09-01-project-clone.md（开发计划）、dev_docs/design-modules/09-admin-ui.md

---

## 1. 背景与问题

### 1.1 现状

「配置管理」页的「新建项目」（`actions.newProjectModal`，app.js:413-429）只支持输入项目名，创建出**空结构**项目（`apply_project_create`，state.rs:1222-1253：`Structure { version: 1, groups: [] }` + dev/test/prod 分支）。

同一组织下多个服务/应用的配置结构高度同构（典型：`mall-order` / `mall-pay` / `mall-warehouse` 共享同一组配置项）。当前从零搭建一个新项目的结构，只能：

1. 新建项目后，在「结构」页逐个手工添加分组与配置项（key / type / required / secret / 引用共享）；或
2. 打开已有项目的结构，人工对照复制。

### 1.2 目标（用户原话拆解）

> 在 admin-UI 的配置管理页面，点击新建项目的时候，可以选择从现有的项目的结构为基础进行克隆，这样创建的新的项目就一开始就有基础的结构了，就不需要手动复制了。

- 新建项目时**可选**从某个已有项目克隆结构；
- 克隆完成后，新项目**一开始就具备**基础结构（结构页立即可见，草稿页立即按结构渲染可填项）；
- 全程无需手动复制。

### 1.3 明确不做（本期）

| # | 不做 | 理由 |
| --- | --- | --- |
| N1 | 不克隆**值**（各分支草稿值 / 已发布版本 / 分支 shared_bindings） | 用户明确只要「结构」；值属于分支级业务数据，跨项目复制值语义不明确且可能泄露 secret 明文（草稿值内部为密文，但复制语义仍不干净） |
| N2 | 不克隆**非结构**资源（分支列表、灰度、管理员、访问令牌、审计） | 结构克隆只解决「基础结构」诉求；其余资源与结构无必然关联 |
| N3 | 不克隆源项目的**未发布结构草稿** | 已发布结构是权威基线（代码注释「级联首选源 + 无草稿时的权威基线」）；未发布草稿是编辑中的不稳定状态，克隆它语义不确定（详见 D2） |
| N4 | 不改权限矩阵 | 创建项目仍仅全局管理员（PA 对 POST /api/v1/projects 403，现有行为不变）；克隆读取源项目已发布结构，管理员可读任意项目，无越权面 |
| N5 | 不做「克隆后自动发布 / 自动填值」 | 新项目结构直接以 version=1 落地为**已发布**形态（与普通新建一致），但**不触发**分支版本推进与结构发布事件（详见 D3） |

---

## 2. 设计决策

| # | 决策点 | 结论 | 理由 |
| --- | --- | --- | --- |
| D1 | 克隆什么 | 源项目的**已发布结构**（`Structure.groups`：GroupDef/ItemDef 全字段 key/type/required/secret/validate/description/shared） | 权威基线、确定性强；结构页与草稿页都以它为渲染基线 |
| D2 | 源结构取「已发布」还是「草稿」 | **已发布**；源存在未发布草稿时不克隆草稿 | 已发布是稳定基线；草稿克隆会把未定稿编辑扩散到新项目，且 base_version 语义复杂化 |
| D3 | 克隆结果形态 | 新项目 `Structure { version: 1, groups: 克隆组 }` 直接落地（已发布形态）；分支保持 `active_version=0`，**不**产生结构发布事件/版本记录 | 与普通新建完全同构（仅 groups 非空）；「一开始就有结构」；避免空值 v1 版本记录噪音 |
| D4 | 实现位置 | **服务端原子**：`Command::ProjectCreate` 增加 `#[serde(default)] clone_from: Option<String>`，`apply_project_create` 内读取源项目已发布结构 | 单一命令内完成 → Raft 复制确定性、无中间态、无命令载荷膨胀；旧日志重放兼容（serde default → None） |
| D5 | 校验规则 | ① `clone_from` 名非法 → validation；② `clone_from == 新项目名` → validation（更清晰报错）；③ 源项目不存在（结构记录缺失）→ validation「clone source not found」；④ 源结构为空 groups（从未发布）→ 等价普通创建（空结构，不报错）；⑤ handler 层 `clone_from==""` 归一为 None；⑥ 克隆组落地前补一次 `validate_structure`（防御 Warn 发布边缘的无效结构） | 与既有 apply 校验风格一致（validation/conflict 分类）；③④ 的区分点是「项目是否存在」而非「结构是否为空」（`apply_project_create` 恒写 struct_key，存在即 Some） |
| D6 | API 形态 | `POST /api/v1/projects` 请求体增加**可选** `clone_from`（源项目 id=name）；响应不变；审计 `project_create` 附加 clone_from 字段 | 向后兼容：旧客户端不传即普通创建；审计可追溯克隆来源 |
| D7 | UI 形态 | 新建项目弹窗在项目名输入框下增加可选下拉「从现有项目克隆结构」，选项 = 当前可见项目列表（管理员可见全部），默认「不克隆（空结构）」；`openModal` 扩展支持 select（向后兼容，不影响既有弹窗） | 一个弹窗内完成「命名 + 选源」，符合现有交互模式 |
| D8 | 克隆后提示 | toast 区分：普通创建「项目已创建」；克隆创建「项目已创建（结构克隆自 &lt;源&gt;）」 | 明确克隆来源，避免误以为结构凭空出现 |

---

## 3. 服务端设计

### 3.1 命令扩展（server/crates/dsh-core/src/command.rs）

```rust
ProjectCreate {
    name: String,
    #[serde(default)]
    operator: String,
    /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
    #[serde(default)]
    ts: i64,
    /// 可选：从该项目的已发布结构克隆初始化结构（groups 直接进入新项目已发布结构 v1）。
    #[serde(default)]
    clone_from: Option<String>,
},
```

`#[serde(default)]` 保证旧 Raft 日志（无此字段）重放为 `None`，与 operator/ts 既有兼容策略一致。

### 3.2 状态机（server/crates/dsh-core/src/state.rs）

`apply_inner` 匹配解构增加 `clone_from`；`apply_project_create` 签名增加 `clone_from: Option<&str>`，逻辑：

```text
1. 既有校验：valid_name(name) / MAX_PROJECTS / 重名冲突
2. 若 clone_from = Some(src)：
   a. src == name → validation「clone source must differ from new project name」
   b. !valid_name(src) → validation「invalid clone source」
   c. get_structure(&ProjectId(src)) → None → validation「clone source not found」
   d. groups = 源结构.groups（浅拷贝 Vec<GroupDef>）
3. Structure { version: 1, groups } 落库；dev/test/prod 分支照旧创建（BranchState::new(1)）
```

确定性：`apply` 不读请求/墙钟（D20 原则），源结构读取走 `self.get_structure`（Raft 复制状态内），所有副本一致。防御：克隆组落地前补一次 `validator::validate_structure`（API 路径下源结构恒有效——draft-set 无条件校验；此步防御 Warn 发布策略边缘，确定性不受影响）。

### 3.3 API handler（server/crates/dsh-api/src/lib.rs）

```rust
#[derive(Deserialize)]
struct CreateProjectReq {
    name: String,
    #[serde(default)]
    clone_from: Option<String>,
}
```

`create_project`：透传 `clone_from` 进 `Command::ProjectCreate`；审计 detail 在克隆时附加：

```rust
let mut detail = serde_json::json!({});
if let Some(src) = &req.clone_from {
    detail["clone_from"] = serde_json::Value::String(src.clone());
}
```

### 3.4 契约（api/openapi.v1.yaml）

`POST /api/v1/projects` 请求体 properties 增加：

```yaml
clone_from: { type: string, description: 可选：从该项目克隆已发布结构（新项目结构直接以 v1 落地） }
```

顺带修正该端点既有文档漂移（非本期引入，但本期改同一契约）：`201` → `200`，响应 schema `Project` → `{ id, branches: [dev, test, prod] }`（与 handler 实际返回一致，lib.rs:779-781）。

（`schema/storage.v1.schema.json` 不含 Command 定义，无需改动；`check-contracts.sh` 只校验 openapi 结构完整性与 storage $defs 存在性。）

---

## 4. Admin UI 设计

### 4.1 弹窗扩展（通用机制，向后兼容）

`index.html` 通用弹窗（:481-495）在 `modal-field` 之后追加：

```html
<div class="field hidden" id="modal-select-field">
  <label for="modal-select" id="modal-select-label"></label>
  <select class="sel" id="modal-select"></select>
</div>
```

`app.js openModal`（:171-192）增加 select 支持：`o.select` 提供 `{ label, options: [{value,label}] }` 时显示该字段并填充选项，否则隐藏；聚焦逻辑保持输入框优先。

`modal-ok` 点击回调（:2371-2375）按 select 是否可见区分回调参数：

```js
const sel = $('modal-select-field').classList.contains('hidden') ? null : $('modal-select').value;
if (cb) cb(sel === null ? v : { value: v, select: sel });
```

既有弹窗（无 select）回调参数不变（仍为字符串），零影响。

### 4.2 新建项目动作（app.js `actions.newProjectModal`）

```text
1. 源列表 = S.projects（管理员可见全部；PA 无此入口权限，服务端仍 403）
2. openModal：
   - input: 项目名（不变）
   - select: 「从现有项目克隆结构（可选）」；选项 = 「不克隆（空结构）」 + 各项目（value=id, label=name）
   - 无项目可克隆时（S.projects 为空）不显示 select（与空态按钮场景一致）
3. onOk(r)：
   - name = r.value.trim()（无 select 时 r 为字符串，兼容）
   - cloneFrom = r.select（空串 = 不克隆）
   - body = { name }；cloneFrom 非空时追加 clone_from
   - 成功后 toast 区分克隆/普通；S.project = resp.id；loadProjects()
```

### 4.3 样式（styles.css）

`modal-select-field` 复用既有 `.field` / `.sel` 样式，无新增样式需求（如间距需微调则加极小改动）。

---

## 5. 兼容性边界

| 面 | 保证 |
| --- | --- |
| 旧客户端 / 旧日志 | `clone_from` 可选 + serde default；不传即原行为 |
| openapi | 仅新增可选属性，无必填/删除/重命名 |
| 权限矩阵 | POST /api/v1/projects 仍仅全局管理员；克隆读源结构在管理员既有读权限内 |
| 既有 UI 弹窗 | openModal 扩展向后兼容，无 select 的弹窗行为不变 |
| 结构校验 | 克隆组源自已发布结构（API 路径恒有效）；落地前补一次 `validate_structure` 防御（D5⑥/§3.2，防 Warn 发布边缘）；共享项 shared 标记保留，分支 shared_bindings 不克隆（草稿页显示未绑定下拉，管理员按需绑定，语义同源项目新分支） |
| 数据面 | 无改动 |

---

## 6. 测试计划

| 层 | 用例 | 位置 |
| --- | --- | --- |
| state 单测 | 克隆成功（结构字段逐项相等：key/type/required/secret/shared/description）；源不存在 → validation；clone_from==name → validation；源结构为空 → 空结构等价普通创建；普通创建（无 clone_from）行为不变 | `server/crates/dsh-core/src/state.rs` 测试模块 |
| API 集成 | 管理员：建源项目+发布结构 → 带 clone_from 建新项目 → `GET /structure` 断言克隆组；`GET /structure-draft` 断言无草稿；分支 active_version=0；负例：源不存在 400、自克隆 400；PA 仍 403 | 新增 `server/crates/dsh-api/tests/http_project_clone.rs` |
| UI e2e | 无头 Chrome CDP：登录 → 建源项目（含 string/int/secret/shared 项并发布结构）→ 新建项目选克隆 → 断言结构页渲染克隆组、toast 提示克隆来源 | 新增 `scripts/ui-e2e-project-clone.js`（沿用 fill-from-branch 的 CDP 模式） |
| 契约 | `bash scripts/check-contracts.sh` | — |
| 全量回归 | `cargo test --workspace` + `bash scripts/api-surface-test.sh` | — |

---

## 7. 风险与回退

- **命令字段扩展的日志兼容**：`#[serde(default)]` 兜底；Raft 快照持久化不涉及命令体（命令仅日志载荷），无存储迁移。
- **27 处 `Command::ProjectCreate` 构造字面量**（13 个文件）需要补 `clone_from: None`（机械改动，编译期保证不漏；`state.rs` 1 处穷举 match 同步更新，其余 `..` 解构点无需改）。
- 回退：API 层不传 `clone_from` 即完全回到旧行为；UI 下拉不选即不克隆。
- 克隆结果与源结构**不联动**（后续源结构变更不影响已克隆项目）——模板语义，符合预期。
