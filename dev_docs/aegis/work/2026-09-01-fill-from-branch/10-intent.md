# 任务意图（TaskIntentDraft）— fill-from-branch

日期: 2026-09-01
关联: dev_docs/design/fill-from-branch.md（设计 v1）、dev_docs/aegis/plans/2026-09-01-fill-from-branch.md（计划）

## 目标（requested outcome）

admin-UI 配置管理草稿页：非共享引用配置项行内右侧小图标 → 点击弹出「有值分支」列表（草稿优先 + 发布并列，排除当前分支，选项显示该配置项在该分支的值）→ 点击类型化填入当前输入框；secret 支持填充（明文不显示，经既有 reveal 审计通道）；会话内缓存 + 失效。零 API/服务端改动。

## 成功证据（success evidence）

1. 设计文档 + 开发计划 + 交叉复核记录存在，复核结论「具备进入开发条件」；
2. 实现后 reviewer 对照文档复核无阻塞问题；
3. `cargo test --workspace` / `check-contracts.sh` / `api-surface-test.sh` 全绿；无头 Chrome e2e 断言全部通过；
4. 提交并 push 到 main，CI 全部 job 绿。

## 停止条件（stop condition）

- done：全部证据满足且已 push、CI 全绿；
- blocked：交叉复核发现阻塞性缺陷且无法修复（需用户决策）时报告具体条件；
- needs-verification：实现完成但验证被环境阻断（如无 Chrome）时如实报告。

## 非目标（non-goals）

- 服务端/API/openapi/schema 改动；共享引用项图标；跨项目取值；批量填充；secret 明文展示。

## 范围（scope）

admin UI 三文件（index.html/app.js/styles.css）+ docs/04-draft.md + scripts/ui-e2e-fill-branch.js（e2e 脚本）。

## 风险（risks）

- secret 仅发布值可填充（安全模型所致，已明示）；
- 无头 Chrome 在本机可用（/usr/bin/google-chrome 151），CI 未含该脚本（不进 gating）；
- 浮层定位/层级与既有模态交互。
