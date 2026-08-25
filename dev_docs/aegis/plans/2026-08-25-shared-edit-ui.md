# 开发计划：共享库编辑体验（shared-edit-ui）

日期: 2026-08-25
范围确认: 与用户确认——**保持「仅全局管理员可写」授权不变，仅补齐共享库 UI 的编辑体验**（现状：共享库页只有"新建表单 + 列表（每行仅删除按钮）"，无法把已有共享项载入表单修改）
上游: dev_docs/design/project-admin.md（授权矩阵，不改）；docs/06-shared.md（功能文档同步）
执行路线: 单工作区逐 slice 实现，每 slice 后跑对应验证；全部完成后整体验证
基线: main HEAD ee8f4c6（工作区干净）

## TDD Route

```text
TDD Route:
- Mode: off
- Decision: skipped
- Strict authority: not applicable（用户未要求 strict TDD；本次为 UI 编辑体验 + 服务端一处小语义）
- Test posture: post-change regression —— 既有测试不动（授权矩阵/契约保持）；新行为（secret 空值=保留密文）配单元测试；UI 走 dev-single 手动清单
- Reason: 仓库既有测试基线即回归网；改动面小、授权零变化
- Verification: cargo test --workspace（基线全绿）+ bash scripts/check-contracts.sh + bash scripts/api-surface-test.sh + dev-single 手动清单
```

## 0. 目标与基线

- 目标：共享库页支持**编辑已有共享项**（草稿行、已发布行均可点「编辑」载入顶部表单修改后保存草稿再发布）；secret 共享项编辑时支持"留空 = 保留当前密文"（仅改描述/required 不必重输密钥）。
- 明确不做（本期）：key 重命名（key 是绑定标识，重命名 = 删除+新建，风险大；已绑定项更不可行）；授权模型变更（PA 仍只读、导航仍隐藏）；多选批量编辑；版本历史对比。
- 兼容边界：API 请求体形状不变（value 仍必填，仅新增"secret 项空字符串 = 保留"语义）；旧客户端不受影响；存储 schema 零变化。
- 基线命令：
  - `cd server && source ../scripts/build-env.sh && cargo test --workspace`（全绿）
  - UI 改动需重编 dsh-api（rust_embed 嵌入 admin/，build.rs 已 rerun-if-changed=admin）：`cargo build -p dsh-api` 后以 dev-single 起服务验证
  - e2e：`bash scripts/api-surface-test.sh`（自起 dev-single 8399）；`bash scripts/dev-single-demo.sh`

## 1. 文件地图

| 文件 | 动作 | 内容 |
| --- | --- | --- |
| server/crates/dsh-core/src/state.rs | 改 | 新增 `get_shared_effective`（草稿优先回落已发布）——`get_shared` 只读已发布 key，不合并草稿（执行中修正） |
| server/crates/dsh-api/src/lib.rs | 改 | `write_shared_draft`（:1704-1783）：secret 项 value 空字符串 = 保留当前生效密文（草稿优先，经 `get_shared_effective`）；无既有密文/当前非 secret → 400 |
| server/crates/dsh-api/src/lib.rs | 改 | 新增 `#[cfg(test)] mod shared_secret_keep_tests`（复用 join_token_tests 的 ApiState 构造模式） |
| server/crates/dsh-api/admin/app.js | 改 | 行内「编辑」按钮（loadShared :1672-1688）；`actions.editSharedItem` 载入表单 + 编辑态；`actions.resetSharedForm` 新建/取消；`actions.saveShared`（:1704-1726）编辑态支持（key 取编辑态、secret 留空透传、type 变更确认）；`renderSharedValueControl`（:1696-1702）secret 占位/留空 |
| server/crates/dsh-api/admin/index.html | 改 | sprite 新增 `i-edit` 铅笔图标（:15-43 区）；表单区「保存共享草稿」旁加「新建」按钮 + 编辑态提示元素（:373 附近） |
| api/openapi.v1.yaml | 改 | `SharedItem` value 描述补：secret 项 value 传空字符串 = 保留当前密文（:461-500 附近注释） |
| scripts/api-surface-test.sh | 改 | §7 secret 段（:80-82）后追加 keep 语义断言 |
| docs/06-shared.md | 改 | §6.2 补「编辑已有共享项」小节 |

不动：schema/storage.v1.schema.json、dsh-core、授权矩阵（pa_allowed）、PA 相关测试（回归即验证）。

## 2. Slice 划分

- S1 服务端 keep 语义 + 单元测试 → `cargo test -p dsh-api`
- S2 Admin UI（app.js + index.html）→ 重编 dsh-api + dev-single 手动清单
- S3 契约与脚本（openapi + api-surface-test）→ `bash scripts/check-contracts.sh` + `bash scripts/api-surface-test.sh`
- S4 文档（docs/06-shared.md）→ 审读
- 全量：`cargo test --workspace` + api-surface + dev-single 手动清单

## 3. S1：dsh-api `write_shared_draft` secret 保留语义（T1-T2）

### 任务 3.1 lib.rs :1704-1783 改造（T1）

现状：secret 项无条件要求字符串明文并加密（`value` 空串会被当明文加密成"空密文"，覆盖原值——这是"编辑 secret 项必须重输密钥"的根因，也是本计划要修的洞）。

改造后的 secret 分支（其余代码不变）：

```rust
let mut value = req.value;
let mut keep_cipher = false;
if req.secret {
    // F9：secret 共享项只接受 secret 类型的字符串值
    if req.r#type != ValueType::Secret {
        return Err(ApiError(dsh_core::Error::validation(
            "secret 共享项 type 必须为 secret",
        ))
        .into());
    }
    match &value {
        // shared-edit-ui：value 空字符串 = 保留当前生效密文（get_shared_effective：
        // 草稿优先回落已发布——get_shared 仅读已发布 key，执行中发现并修正），
        // 使"仅改描述/required"的编辑无需重输密钥。
        Value::String(s) if s.is_empty() => {
            let cur = app
                .sm
                .read()
                .map_err(lock_err)?
                .get_shared_effective(&req.key)
                .map_err(ApiError::from)?;
            match cur {
                Some(c) if matches!(c.value, Value::Secret(_)) => {
                    value = c.value; // 保留既有密文（草稿优先）
                    keep_cipher = true;
                }
                Some(_) => {
                    return Err(ApiError(dsh_core::Error::validation(
                        "该共享项当前非 secret，请先输入明文值",
                    ))
                    .into())
                }
                None => {
                    return Err(ApiError(dsh_core::Error::validation(
                        "secret 共享项首次保存必须输入值",
                    ))
                    .into())
                }
            }
        }
        Value::String(_) => { /* 有明文，走下方加密 */ }
        _ => {
            return Err(
                ApiError(dsh_core::Error::validation("secret 共享项值必须为字符串")).into(),
            )
        }
    }
    if !keep_cipher {
        let cipher = app.cipher.as_ref().ok_or_else(|| {
            ApiError(dsh_core::Error::validation(
                "secret 共享项需要主密钥（--master-key-file 或 DSH_MASTER_KEY）",
            ))
        })?;
        let plain = match &value {
            Value::String(s) => s.clone(),
            _ => unreachable!("secret 空值路径已分流"),
        };
        let ct = cipher
            .encrypt_secret(plain.as_bytes())
            .map_err(|e| ApiError(dsh_core::Error::internal(format!("encrypt: {e}"))))?;
        value = Value::Secret(ct);
    }
} else if req.r#type == ValueType::Secret {
    // 现状不变：type=secret 的共享项必须标记 secret=true
    return Err(ApiError(dsh_core::Error::validation(
        "type=secret 的共享项必须标记 secret=true",
    ))
    .into());
}
```

要点：
- 保留路径**不触碰密文**（原样复用），不要求主密钥在场（纯元数据编辑可用）。
- "当前非 secret → 空值"返回 400（禁止隐式把明文项转 secret；转 secret 必须显式输入新明文）。
- 语义对齐草稿页既有模式：留空 = 保留**当前生效值**（有草稿保留草稿密文，无草稿保留发布密文）——与 `get_shared` 合并语义一致。

### 任务 3.2 单元测试（T2）

在 lib.rs 底部新增 `#[cfg(test)] mod shared_secret_keep_tests`（参照 join_token_tests 的 `ApiState::with_retention` + InMemoryStore 构造）：

- 正例：种子密文（直接 `sm.apply(&Command::SharedDraftUpdate { item: SharedItem { value: Value::Secret(vec![1,2,3]), .. }, operator: "admin".into() }, 0)` 写入，无需真主密钥）→ 调 `write_shared_draft`（secret=true、value=`Value::String("".into())`、description=Some("新描述")）→ 断言返回 Ok 且 `sm.get_shared("api-key")` 的 value 仍为 `Value::Secret(vec![1,2,3])`、description 已更新。
- 负例 1：key 不存在 + 空值 → Err（"首次保存必须输入值"）。
- 负例 2：既有明文值 + 空值 → Err（"当前非 secret"）。
- 负例 3：空值但 type != Secret → Err（既有 F9 校验，回归）。

验证：`cd server && source ../scripts/build-env.sh && cargo test -p dsh-api`。

## 4. S2：Admin UI 编辑体验（T3-T5）

### 任务 4.1 index.html（T3）

- sprite（:15-43）新增：`<symbol id="i-edit" viewBox="0 0 24 24"><path d="M17 3a2.83 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3Z"/></symbol>`（铅笔）。
- 表单区（「保存共享草稿」按钮 :373 附近）：
  - 「保存共享草稿」左侧加：`<button type="button" class="btn" data-act="resetSharedForm" id="sh-reset" class="hidden">新建</button>`
  - 加提示元素：`<p class="hint hidden" id="sh-edit-hint"></p>`（编辑态显示"正在编辑 {key}；key 不可修改；发布后自动级联 N 处"）。

### 任务 4.2 app.js：行内「编辑」按钮 + 载入表单（T4）

- `loadShared()`（:1672-1688）行模板：删除按钮前加
  `<button type="button" class="icon-btn" data-act="editSharedItem" data-key="${esc(x.key)}" title="编辑共享项" aria-label="编辑共享项"><svg class="ic"><use href="#i-edit"/></svg></button>`
- 新增 `actions.editSharedItem`：
  1. 取行数据：编辑前 `loadShared` 时把行对象存 `S.sharedRows`（key → row，草稿优先覆盖发布行），edit 时 `const x = S.sharedRows[el.dataset.key]`（行模板已有 key/ty/value/description/secret/required/refs，无需再请求）。
  2. 填表单：`sh-key` = key 且 `disabled = true`（key 不可改）；`sh-type`、`sh-secret`、`sh-required`、`sh-desc` 赋值；`renderSharedValueControl()` 后按类型填 `sh-value`：
     - string `str_value` / int `int_value` / float `float_value` / bool 勾选 / json `json_value` / array `list_value.join(', ')`
     - **secret：留空**；若该 key 有当前值（`x.value` 存在）→ `sh-value.placeholder = '已加密 · 留空不修改，输入以更新'`（复用草稿页 :702-703 文案）；`sh-value` 无值时占位保持默认。
  3. 编辑态：`S.sharedEditKey = key`；`sh-edit-hint` 显示（含 `x.refs.length ? '被 N 处引用，发布后自动级联' : ''`）；「新建」按钮显示；`markSharedDirty()`（表单载入了未保存内容）。
  4. 类型变更防呆：记录 `S.sharedEditOrigType = x.ty`；若保存时 type 变化且该 key 有 refs（被绑定）→ `openModal` 确认「类型变更发布后，绑定该共享项的分支绑定将失配丢弃（需重新选择），确认？」（服务端 state.rs:1512-1530 已有丢弃逻辑，此处仅防呆）。
- 新增 `actions.resetSharedForm`：清空 key/type/secret/required/desc/value；`sh-key.disabled=false`；`S.sharedEditKey=null`；`sh-edit-hint` 隐藏、「新建」按钮隐藏；`S.sharedDirty=false`；`updateSharedStatus()`。
- 保存成功后（见 4.3）调用 `resetSharedForm()` 复位（列表已 `loadShared()` 刷新）。

### 任务 4.3 app.js：`actions.saveShared` 编辑态（T5）

- key 来源：`S.sharedEditKey` 存在时取之（跳过"key 必填"校验，`sh-key.disabled` 下 `.value` 仍可读，双保险）。
- secret 留空放行：当 `ty === 'secret'` 且 `raw` 为空且编辑态（或该 key 已有值）→ 不报"请填写值"，body.value = `{ type: 'string', str_value: '' }`（服务端保留密文）。
- 其余校验不变（非 secret 空值仍报错；json 语法、desc ≤200 照旧）。
- 保存成功 toast：编辑态 → `共享草稿已保存（更新 ${key}）`；新建 → 现状文案。
- `type` 变更且被引用 → 先 `openModal` 确认（见 4.2 第 4 点）。
- 请求端点不变（POST /api/v1/shared 即 upsert 草稿）。

### 任务 4.4 dev-single 手动验证清单（T5 验证）

`cd server && cargo build -p dsh-api && ./target/debug/defing --dev-single --admin-password x --data-plane-token y --master-key-file <临时密钥文件>`（端口 8384，Admin UI http://localhost:8384/admin）：
1. 全局管理员登录 → 共享库：新建 string 项 → 列表出现「编辑」按钮 → 点编辑载入表单 → 改值/描述 → 保存 → 列表草稿行更新 → 发布 → 已发布行值更新。
2. 新建 secret 项（输入明文）→ 发布 → 点编辑 → 值框为空、占位"已加密 · 留空不修改" → 只改描述 → 保存 → 发布 → 绑定分支 reveal 后**原明文仍在**（密文未被空值覆盖）。
3. secret 项编辑时输入新值 → 保存发布 → reveal 为新值。
4. 编辑态 key 输入框禁用；「新建」按钮复位表单（key 恢复可输入、提示消失）。
5. 编辑被引用项并改 type → 出现确认弹窗。
6. 草稿行编辑载入的是草稿值（非发布值）。
7. 回归：PA 登录导航无「共享库」（授权未动）；草稿页「引用共享」下拉正常。

## 5. S3：契约与脚本（T6）

### 任务 5.1 api/openapi.v1.yaml

- `SharedItem` value 描述（或 `/api/v1/shared` POST / `/api/v1/shared-draft` PUT 的 summary/requestBody 注释）补：
  `value: secret 项传空字符串（{type: string, str_value: ""}）表示保留当前密文（编辑场景）；无既有值或当前非 secret 时返回 422`。

### 任务 5.2 scripts/api-surface-test.sh

- §7 secret 段（:80-82）后追加：
  ```bash
  # shared-edit-ui：secret 留空 = 保留密文（仅改描述）
  J -X PUT $BASE/api/v1/shared-draft -d '{"key":"api-key","type":"secret","secret":true,"description":"更新描述","value":{"type":"string","str_value":""}}' >/dev/null
  J $BASE/api/v1/shared-draft | python3 -c "import json,sys; l=json.load(sys.stdin); sk=[x for x in l if x['key']=='api-key'][0]; assert sk['value'].get('masked')==True and sk.get('description')=='更新描述', l" && echo "  shared secret keep-cipher OK"
  ```

验证：`bash scripts/check-contracts.sh` + `bash scripts/api-surface-test.sh`。

## 6. S4：文档（T7）

### 任务 6.1 docs/06-shared.md

- §6.2 补「编辑已有共享项」小节：
  - 列表行「编辑」→ 表单载入 → 修改 → 保存共享草稿 → 发布共享（草稿行载入草稿值；已发布行载入发布值）。
  - **secret 项**：值框留空 = 保留当前密文（只改描述/required 不必重输密钥）；输入新值即更新。
  - **key 不可修改**（key 是全局唯一标识与绑定锚点；需要新 key = 新建后另行迁移绑定）。
  - 类型/secret 变更需确认：发布后绑定该共享项的分支绑定将失配丢弃，需在分支重新选择。
- 授权说明不变（§6 现状：仅全局管理员可创建/更新/发布/删除）。

## 7. 自检（self-review）

- 规格覆盖：编辑（草稿/发布行）、secret 保留、类型变更防呆、key 锁定、复位、文档/契约/脚本同步——全覆盖。
- 授权零变化：`pa_allowed`（lib.rs:607-659）与 `pa_shared_read_allowed_writes_denied` 测试不动；UI 导航过滤（app.js:366-371）不动。
- 契约兼容：请求体形状不变（value 仍必填），仅新增空字符串语义；旧客户端不受影响；存储 schema 不变。
- 类型一致性：保留路径 value 仍为 `Value::Secret`，`apply_shared_draft_update`（state.rs:2098-2122）无感知。
- 安全：密文不落明文日志（审计只记 key）；掩码不变；保留路径不重新加密。
- 验证闭环：cargo test --workspace / check-contracts / api-surface / dev-single 手动清单（§4.4）。

## 8. 风险与权衡

1. **编辑态与 dirty 指示**：载入表单后 `markSharedDirty`，发布前提示"有未保存的表单"——语义正确（载入内容尚未保存），但首次使用可能困惑；文案已含说明。
2. **secret 保留依赖合并语义**：有草稿时保留草稿密文（而非发布密文）——与草稿页"留空不修改"一致，文档注明。
3. **UI 缓存**：admin 静态资源经 rust_embed 嵌入，改动需重编 dsh-api；浏览器需硬刷新（Cache-Control: no-store 已落地，见 commit b90c323）。
4. **发布即全量级联**：编辑后发布仍为显式操作（现状语义），不会静默级联。
