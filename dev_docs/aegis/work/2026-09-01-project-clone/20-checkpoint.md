# 检查点（TodoCheckpointDraft）— project-clone 2026-09-01

## 当前 todo
1. ✅ 探索项目与 admin-UI 新建项目现状，确认数据模型/命令/测试模式
2. ✅ 编写设计文档 dev_docs/design/project-clone.md
3. ✅ 编写开发计划 dev_docs/aegis/plans/2026-09-01-project-clone.md
4. ✅ 交叉审核（subagent 783678e7）：结论「具备直接开发条件」，高 1（负例 400→422）+ 低 4（数字/端口/断言钉死/openapi 漂移）全部修复
5. ✅ 服务端实现：command.rs / state.rs（apply_inner + apply_project_create + 3 单测）/ lib.rs（空串归一 + 审计 detail）/ 27 处构造点 / project_admin.rs 旧日志兼容扩展
6. ✅ openapi：clone_from 属性 + 响应 200 修正 + 422
7. ✅ API 集成测试 http_project_clone.rs（6 用例全过）
8. ✅ Admin UI：index.html modal-select-field + app.js（openModal select / modal-ok 回调区分 / newProjectModal 克隆下拉）
9. ✅ UI e2e scripts/ui-e2e-project-clone.js（12 断言全过；修掉陈旧 defing 二进制 + toast 累计断言）
10. ✅ 文档 docs/02-project.md §2.1
11. ✅ 全量验证：cargo test --workspace 全绿；fmt --check；clippy -D warnings；check-contracts；api-surface-test；UI e2e
12. ✅ 实现交叉审核（subagent 7d74cd69）：无高/中问题，低 3（§5 表述矛盾/计划数字 29→27/行号偏移）已修复前两者
13. ⏳ 提交 + push + CI 全绿

## 已完成 / 证据
- 基线：main HEAD 5b8b693 工作区干净；`cargo check --workspace` exit 0
- **设计 review**（subagent 783678e7）：2a 状态机语义自洽（v1 落地/active_version=0 无不变量破坏）、2b 日志重放兼容成立、2c 确定性成立（D20）；修正：负例断言 422（ErrorKind::Validation→422，lib.rs:224-226）、计划数字 29→27+1、e2e 独立端口 8398/9334、测试 step5 钉死 base_version==null、openapi 响应漂移修正（201→200 + {id,branches}）
- **实现 review**（subagent 7d74cd69）：D1-D8 全部落地、T1-T10 全部完成；27 处构造点编译期保证；唯一穷举 match state.rs:949 已同步；openapi 422 引用 ValidationFailed 存在；modal-ok 仅 select 可见时改传对象（其余 21 处弹窗零影响）；XSS 面安全；低 3 处文档问题已修复
- **验证全绿**：cargo test --workspace（35 套件 0 失败，含新 state 单测 3 + 集成 6）；cargo fmt --all -- --check；cargo clippy --all-targets --all-features -- -D warnings；bash scripts/check-contracts.sh（ALL OK）；bash scripts/api-surface-test.sh（exit 0）；node scripts/ui-e2e-project-clone.js（12 PASS / 0 FAIL）

## 阻塞项
- 无

## 下一步
- git add 全部改动 → 按仓库惯例分组提交（feat + docs）→ push origin/main → 观察 CI 直至全绿
