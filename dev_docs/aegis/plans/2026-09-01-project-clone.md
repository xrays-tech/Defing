# 开发计划：新建项目支持「从现有项目克隆结构」（project-clone）

日期: 2026-09-01
范围确认: 设计文档 dev_docs/design/project-clone.md v1（交叉复核后定稿）
上游: dev_docs/design/project-clone.md（设计文档，本期交叉复核后为 v1）
执行路线: 单工作区逐 slice 实现，每 slice 后跑对应验证；全部完成后整体验证 + reviewer 交叉对比
基线: main HEAD 5b8b693（工作区干净）

## TDD Route

```text
TDD Route:
- Mode: off
- Decision: skipped
- Strict authority: not applicable（用户未要求 strict TDD；本期为功能开发 + 全量回归）
- Test posture: post-change regression —— 先写 state 单测 + API 集成测试覆盖新行为（实现后跑通），再 UI e2e 验证交互；全量回归 cargo test --workspace + check-contracts + api-surface-test + UI e2e
- Reason: 服务端改动集中在单个命令变体 + 单函数；测试按「实现 + 断言」顺序编写，无需 RED 先行
- Verification: cargo test --workspace（含新单测/集成测试）+ bash scripts/check-contracts.sh + bash scripts/api-surface-test.sh + node scripts/ui-e2e-project-clone.js + cargo fmt --check + cargo clippy --all-targets --all-features -- -D warnings
```

## 0. 目标与基线

- 目标：新建项目时可选从已有项目克隆**已发布结构**；克隆后新项目结构以 v1 直接落地（已发布形态），分支 dev/test/prod 照旧、active_version=0、无结构发布事件；UI 弹窗增加「从现有项目克隆结构」可选下拉；openapi 增加可选 `clone_from`；docs/02-project.md 更新。
- 明确不做（本期）：克隆值/分支 shared_bindings/分支/灰度/管理员/令牌/审计；源未发布结构草稿；权限矩阵改动；数据面改动。
- 兼容边界：`clone_from` 可选 + `#[serde(default)]`（旧日志重放 None）；openapi 仅新增可选属性；openModal 扩展向后兼容（无 select 弹窗回调参数仍为字符串）；普通创建行为不变。
- 基线命令：
  - `cd server && source ../scripts/build-env.sh && cargo test --workspace`（全绿基线）
  - UI 改动需重编 dsh-api（rust_embed 嵌入 admin/）：`cargo build -p dsh-api`
  - 契约：`bash scripts/check-contracts.sh`；e2e：`bash scripts/api-surface-test.sh`（自起 dev-single 8384）

## 1. 文件地图

| 文件 | 动作 | 内容 |
| --- | --- | --- |
| server/crates/dsh-core/src/command.rs | 改 | `Command::ProjectCreate` 增加 `#[serde(default)] clone_from: Option<String>` 字段 |
| server/crates/dsh-core/src/state.rs | 改 | `apply_inner` 解构增加 clone_from；`apply_project_create` 增加参数与克隆逻辑（源校验 + groups 落地）；测试模块新增克隆用例 |
| server/crates/dsh-api/src/lib.rs | 改 | `CreateProjectReq` 增加 `#[serde(default)] clone_from`；`create_project` 透传 + 审计 detail |
| server/crates/dsh-api/tests/http_project_clone.rs | 新增 | API 集成测试（克隆成功/负例/PA 403/无草稿/分支 active_version=0） |
| server/crates/dsh-publish/src/lib.rs | 改 | 测试构造点补 `clone_from: None` |
| server/crates/dsh-jobs/src/lib.rs | 改 | 测试构造点补 `clone_from: None` |
| server/crates/dsh-api/admin/index.html | 改 | 通用弹窗追加 `#modal-select-field`（select 支持） |
| server/crates/dsh-api/admin/app.js | 改 | `openModal` select 支持；`modal-ok` 回调参数区分；`actions.newProjectModal` 克隆下拉与 body 组装 |
| api/openapi.v1.yaml | 改 | `POST /api/v1/projects` 请求体增加可选 `clone_from` |
| docs/02-project.md | 改 | §2.1 新建项目补充「从现有项目克隆结构」 |
| scripts/ui-e2e-project-clone.js | 新增 | 无头 Chrome CDP UI e2e（沿用 fill-from-branch 模式） |

零改动：schema/storage.v1.schema.json（无 Command 定义）、proto/、docs/09-admin.md（权限矩阵不变）、scripts/api-surface-test.sh。

## 2. Slice 划分

- S1 服务端（command.rs + state.rs + lib.rs + 27 处构造点补字段 + openapi）→ `cargo test -p dsh-core` + `cargo build -p dsh-api` + 新集成测试
- S2 API 集成测试（http_project_clone.rs）→ `cargo test -p dsh-api --test http_project_clone`
- S3 Admin UI（index.html + app.js）→ `node --check app.js` + 重编 dsh-api
- S4 UI e2e（scripts/ui-e2e-project-clone.js）→ 跑通
- S5 文档（docs/02-project.md）→ 审读
- 全量：`cargo test --workspace` + check-contracts + api-surface-test + UI e2e
- 完成后：reviewer（subagent）对照设计文档/计划复核实现；修复发现的问题；全部提交 → push → CI 全绿

## 3. S1：服务端（T1-T5）

### 任务 3.1 command.rs（T1）

`Command::ProjectCreate`（command.rs:29-36）变体增加字段：

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

### 任务 3.2 state.rs apply_inner + apply_project_create（T2）

`apply_inner`（state.rs:949-951）解构增加 `clone_from` 并透传：

```rust
Command::ProjectCreate { name, operator, ts, clone_from } => {
    self.apply_project_create(
        name,
        Self::eff_ts(ts, now_ms),
        operator,
        clone_from.as_deref(),
    )
}
```

`apply_project_create`（state.rs:1222-1253）签名与逻辑：

```rust
fn apply_project_create(
    &mut self,
    name: &str,
    now_ms: i64,
    _operator: &str,
    clone_from: Option<&str>,
) -> ApplyOutcome {
    if !valid_name(name) {
        return Err(Error::validation(format!("invalid project name: {name:?}")));
    }
    // N2：限额表 MAX_PROJECTS 强制
    if self.list_projects()?.len() >= MAX_PROJECTS {
        return Err(Error::limit_exceeded("too many projects"));
    }
    let id = ProjectId(name.to_string());
    if self.get_project(&id)?.is_some() {
        return Err(Error::conflict(format!("project {name} already exists")));
    }
    // 克隆源：读取源项目已发布结构（确定性：仅读 Raft 复制状态；D20）
    let mut groups: Vec<GroupDef> = Vec::new();
    if let Some(src) = clone_from {
        if src == name {
            return Err(Error::validation(
                "clone source must differ from new project name",
            ));
        }
        if !valid_name(src) {
            return Err(Error::validation(format!("invalid clone source: {src:?}")));
        }
        let src_struct = self
            .get_structure(&ProjectId(src.to_string()))?
            .ok_or_else(|| Error::validation(format!("clone source {src:?} not found")))?;
        // 防御：克隆组落地前校验（API 路径源结构恒有效；防 Warn 发布策略边缘，确定性不受影响）
        let errs = validator::validate_structure(&Structure {
            version: src_struct.version,
            groups: src_struct.groups.clone(),
        });
        if !errs.is_empty() {
            return Err(Error::publish_blocked(serde_json::json!({ "errors": errs })));
        }
        groups = src_struct.groups;
    }
    let project = Project {
        id: id.clone(),
        name: name.to_string(),
        created_at: now_ms,
    };
    let structure = Structure {
        version: 1,
        groups,
    };
    self.save_pending(&project_key(&id), &project)?;
    self.save_pending(&idx_pname(name), &"1")?;
    self.save_pending(&struct_key(&id), &structure)?;
    for default_branch in [BranchName::DEV, BranchName::TEST, BranchName::PROD] {
        self.save_pending(
            &branch_state_key(&id, &BranchName(default_branch.to_string())),
            &BranchState::new(1),
        )?;
    }
    Ok(vec![])
}
```

（`GroupDef`/`validator`/`Structure` 均在 state.rs 作用域内：`use crate::model::*;` + `use crate::validator;`——若 `validator` 未导入则补 `use crate::validator;`。）

### 任务 3.2b 状态单测（T2b，state.rs mod tests）

1. `project_create_clone_from_structure`：建源项目 → draft-set（含 required/secret/shared/description 项）→ 发布结构 → 克隆创建 → 断言新项目 structure.version==1、groups 逐项相等（key/type/required/secret/shared/description）、dev/test/prod 分支 active_version==0 且 structure_version==1、structure-draft 为空。
2. `project_create_clone_errors`：源不存在 → Validation；自克隆 → Validation；非法源名 `a/b` → Validation。
3. `project_create_clone_empty_source_equivalent_plain`：源从未发布结构 → 克隆后空 groups、version==1。
4. 旧日志兼容（对齐 project_admin.rs:595 惯例）：`serde_json::from_str(r#"{"ProjectCreate":{"name":"a"}}"#)` → `Command::ProjectCreate { clone_from, .. }` 且 `clone_from.is_none()`。

### 任务 3.3 lib.rs（T3）

`CreateProjectReq`（lib.rs:252-255）与 `create_project`（lib.rs:753-782）：

```rust
#[derive(Deserialize)]
struct CreateProjectReq {
    name: String,
    #[serde(default)]
    clone_from: Option<String>,
}
```

```rust
async fn create_project(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    Json(req): Json<CreateProjectReq>,
) -> ApiResult<serde_json::Value> {
    // 空串归一 None（可选语义；reviewer 建议）
    let clone_from = req
        .clone_from
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(String::from);
    let pid = ProjectId(req.name.clone());
    app.write(
        &Command::ProjectCreate {
            name: req.name.clone(),
            operator: "admin".to_string(),
            ts: now_ms(),
            clone_from: clone_from.clone(),
        },
        now_ms(),
    )
    .await?;
    let mut detail = serde_json::json!({});
    if let Some(src) = &clone_from {
        detail["clone_from"] = serde_json::Value::String(src.clone());
    }
    app.audit
        .append(
            "project_create",
            Some(pid.as_str().into()),
            None,
            None,
            None,
            detail,
            &principal_op(&principal),
        )
        .await;
    Ok(Json(
        serde_json::json!({ "id": pid.as_str(), "branches": ["dev", "test", "prod"] }),
    ))
}
```

### 任务 3.4 全仓构造点补字段（T4）

全仓 `Command::ProjectCreate {` 构造字面量（grep 实测 13 个文件，见清单）统一补 `clone_from: None,`：

```text
server/crates/dsh-api/src/lib.rs
server/crates/dsh-api/tests/http_project_admin.rs
server/crates/dsh-api/tests/http_project_token.rs
server/crates/dsh-core/src/state.rs
server/crates/dsh-core/tests/data_token.rs
server/crates/dsh-core/tests/project_admin.rs
server/crates/dsh-core/tests/state_machine.rs
server/crates/dsh-jobs/src/lib.rs
server/crates/dsh-publish/src/lib.rs
server/crates/dsh-raft/tests/cluster.rs
server/crates/dsh-raft/tests/forward_hint.rs
server/crates/dsh-raft/tests/snapshot_persist.rs
server/crates/dsh-testkit/src/lib.rs
```

机械操作：对每处 `ts: <expr>,\n        },` 形式的结尾补 `clone_from: None,`；用 `cargo build --workspace` 编译期校验不漏。

### 任务 3.5 openapi.v1.yaml（T5）

`POST /api/v1/projects` 请求体（api/openapi.v1.yaml:89-93）properties 增加：

```yaml
properties:
  name: { type: string, maxLength: 128 }
  clone_from: { type: string, description: 可选：从该项目克隆已发布结构（新项目结构直接以 v1 落地） }
```

顺带修正该端点既有文档漂移（reviewer 建议）：`201` → `200`，响应 schema `Project` → `{ id, branches: [dev,test,prod] }`（与 handler 实际返回一致，lib.rs:779-781）。

验证：`bash scripts/check-contracts.sh`。

## 4. S2：API 集成测试（T6）

新增 `server/crates/dsh-api/tests/http_project_clone.rs`（仿 http_project_admin.rs 的 TestServer 模式）：

1. 起服务（admin 密码 "admin-pw"）；登录拿 token。
2. 建源项目 `src`：POST /api/v1/projects {name:"src"} → PUT structure-draft（base_version:1，groups 含 string/int/secret/shared 项）→ POST structure-draft/publish。
3. POST /api/v1/projects {name:"dst", clone_from:"src"} → 200，body.id=="dst"。
4. GET /api/v1/projects/dst/structure → version==1 且 groups 与 src 逐项相等（key/type/required/secret/shared/description）。
5. GET /api/v1/projects/dst/structure-draft → 200 且 `base_version==null`、`groups==[]`（无草稿恒 200，lib.rs:866-871）。
6. GET /api/v1/projects/dst/branches → dev/test/prod 存在且 active_version==0。
7. 负例：POST {name:"e1", clone_from:"nope"} → 422；POST {name:"e2", clone_from:"e2"} → 422（ErrorKind::Validation → 422 UNPROCESSABLE_ENTITY，仓库惯例）。
8. 权限：PA 登录 → POST /api/v1/projects（含 clone_from）→ 403（回归既有矩阵）。
9. 普通创建回归：POST /api/v1/projects {name:"plain"} → 200，structure 空 groups。

## 5. S3：Admin UI（T7-T8）

### 任务 5.1 index.html（T7）

通用弹窗（:486-489 的 `#modal-field` 之后）追加：

```html
<div class="field hidden" id="modal-select-field">
  <label for="modal-select" id="modal-select-label"></label>
  <select class="sel" id="modal-select"></select>
</div>
```

### 任务 5.2 app.js（T8）

1. `openModal`（:171-192）追加 select 分支：

```js
const sField = $('modal-select-field'), sSel = $('modal-select'), sLabel = $('modal-select-label');
if (o.select && o.select.options && o.select.options.length) {
  sField.classList.remove('hidden');
  sLabel.textContent = o.select.label || '';
  sSel.innerHTML = o.select.options
    .map((op) => `<option value="${esc(op.value)}">${esc(op.label)}</option>`)
    .join('');
} else sField.classList.add('hidden');
```

（`modal-field` 与 `modal-select-field` 同列展示；input 聚焦逻辑保持 `o.input` 优先。）

2. `modal-ok` 回调（:2371-2375）按 select 可见性区分参数：

```js
$('modal-ok').addEventListener('click', () => {
  const v = $('modal-input').value;
  const sv = $('modal-select-field').classList.contains('hidden') ? null : $('modal-select').value;
  const cb = modalCb;
  closeModal(true);
  if (cb) cb(sv === null ? v : { value: v, select: sv });
});
```

3. `actions.newProjectModal`（:413-429）重写：

```js
actions.newProjectModal = function () {
  const sources = (S.projects || []).map((p) => ({ value: p.id, label: p.name }));
  openModal({
    title: '新建项目',
    input: true, label: '项目名', placeholder: '小写字母 / 数字 / 连字符，如 mall-order',
    select: sources.length ? {
      label: '从现有项目克隆结构（可选）',
      options: [{ value: '', label: '不克隆（空结构）' }, ...sources],
    } : null,
    okText: '创建',
    onOk: async (r) => {
      const name = ((typeof r === 'string' ? r : (r && r.value) || '') || '').trim();
      const cloneFrom = (typeof r === 'string' ? '' : ((r && r.select) || ''));
      if (!name) { toast('请输入项目名', 'err'); return; }
      try {
        const body = { name };
        if (cloneFrom) body.clone_from = cloneFrom;
        const resp = await j('POST', '/api/v1/projects', body);
        toast(cloneFrom ? `项目已创建（结构克隆自 ${cloneFrom}）` : '项目已创建');
        S.project = (resp && resp.id) || name;
        await loadProjects();
      } catch (e) { toast(e.message, 'err'); }
    },
  });
};
```

验证：`node --check server/crates/dsh-api/admin/app.js` + `cd server && cargo build -p dsh-api`（rust_embed 重编）。

## 6. S4：UI e2e（T9）

新增 `scripts/ui-e2e-project-clone.js`（沿用 fill-from-branch 的 CDP 模式：spawn dev-single、登录、数据准备、headless Chrome CDP 断言；**端口须独立**，避开 8383/8384/8396/8397/9333）：

1. 建源项目 `demo`：结构含 string/int/secret/shared 项并发布结构；再建一个无关项目 `other`。
2. 登录 admin → 打开「新建项目」弹窗 → 断言出现「从现有项目克隆结构」下拉且含 demo / other。
3. 输入新项目名 `clone1`，选择 demo → 创建 → 断言 toast 含「结构克隆自 demo」。
4. 断言结构页渲染 demo 的组与项（key 可见、类型正确、secret/shared 徽标）。
5. 再建 `clone2`（不选克隆）→ 断言结构页为空结构（「暂无配置项」提示）。

## 7. S5：文档（T10）

`docs/02-project.md` §2.1 追加：

```markdown
「新建项目」弹窗支持**从现有项目克隆结构**（可选）：下拉选择任一已有项目后创建，新项目将直接继承该项目的已发布结构（组与配置项定义），无需手工逐个添加；值、分支、权限等资源不随之复制。创建后可照常在「结构」页调整并发布，在「草稿」页填值。
```

## 8. 全量验证

```bash
cd server && source ../scripts/build-env.sh
cargo test --workspace          # 全量单测 + 集成测试
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cd .. && bash scripts/check-contracts.sh
bash scripts/api-surface-test.sh
node scripts/ui-e2e-project-clone.js
```

## 9. 风险与回退

- 27 处构造字面量（13 文件）+ state.rs:949 穷举 match 补字段：编译期强制，机械改动（其余 `..` 解构点免改）。
- 日志兼容：`#[serde(default)]`；新增旧日志 JSON 兼容单测（`{"ProjectCreate":{"name":"a"}}` → clone_from None）。
- 克隆防御：apply 内克隆分支补一次 `validator::validate_structure`（源结构经 API 恒有效，防御 Warn 发布边缘）。
- API 语义：`clone_from==""` 在 handler 归一为 None（可选语义）。
- 回退：不传 clone_from 即旧行为；UI 下拉不选即不克隆。
- 克隆不联动源（模板语义）。
- 本地验证说明：api-surface-test.sh 与 UI e2e（ui-e2e-*.js）为本地验证、不进 CI（与 fill-from-branch 仓库实践一致）；CI 以 cargo test/fmt/clippy/check-contracts 为准。
