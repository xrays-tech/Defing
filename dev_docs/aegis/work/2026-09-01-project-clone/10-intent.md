# 任务意图（TaskIntentDraft）— project-clone

日期: 2026-09-01
关联: dev_docs/design/project-clone.md（设计 v1 定稿）、dev_docs/aegis/plans/2026-09-01-project-clone.md（计划）

## 目标（requested outcome）

admin-UI 配置管理页「新建项目」支持**从现有项目克隆结构**：弹窗可选下拉选择源项目，创建的新项目直接继承源项目**已发布结构**（组与配置项定义，含 required/secret/共享引用标记），以 v1 落地，无需手动复制。

## 成功证据（success evidence）

1. 设计文档 + 开发计划 + 交叉复核记录存在，复核结论「具备直接开发条件」（首轮 review 修正：422 断言/数字/端口等）；
2. 实现后 reviewer（subagent）对照文档复核：实现与设计 D1-D8、计划 T1-T10 一致，无高/中问题；
3. `cargo test --workspace` / `cargo fmt --check` / `cargo clippy -D warnings` / `check-contracts.sh` / `api-surface-test.sh` 全绿；新单测 3 个 + 集成测试 6 个 + UI e2e 12 断言全部通过；
4. 提交并 push 到 main，CI 全部 job 绿。

## 停止条件（stop condition）

- done：全部证据满足且已 push、CI 全绿；
- blocked：交叉复核发现阻塞性缺陷且无法修复（需用户决策）时报告具体条件；
- needs-verification：实现完成但验证被环境阻断时如实报告。

## 非目标（non-goals）

- 克隆值（各分支草稿值/已发布版本/分支 shared_bindings）；源未发布结构草稿；分支/灰度/管理员/令牌/审计；权限矩阵改动；数据面改动。

## 范围（scope）

服务端：dsh-core（command.rs/state.rs）、dsh-api（lib.rs + 新集成测试 + 27 处构造点）、openapi；Admin UI：index.html/app.js；文档：docs/02-project.md；脚本：scripts/ui-e2e-project-clone.js。
