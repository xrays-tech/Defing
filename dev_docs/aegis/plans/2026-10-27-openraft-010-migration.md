# 开发计划：openraft 0.10 迁移、二进制流式快照与独立心跳

日期: 2026-10-27
设计: `dev_docs/design/openraft-010-migration.md`（v1，待交叉评审）
基线: `main d629ec8`（压力测试已提交）；另有未提交的 0.9 临时缓解补丁（guard/startup grace/调参）
执行模式: deepwork，阶段化；每阶段 oracle 门禁；设计门禁通过前不进入开发
TDD Route:
- Mode: off（迁移以编译驱动 + 兼容性 spike + 真实 e2e 回归为主）
- Test posture: 每阶段先建立/保留可失败的兼容性证据，再实现，再跑既有与新增回归
- Verification: 见每阶段命令；最终必须 storm 全绿

## 0. 基线整理（开发前）

- 将当前未提交的 0.9 临时补丁（ElectionGuard/startup grace/参数调优）保存为本地 patch：
  `git diff > /tmp/openraft09-mitigations.patch`，然后恢复工作区到基线。
- 压力测试已提交，保留；不修改 harness 的验收标准。
- 记录 0.9 基线数据：`git rev-parse HEAD`、`cargo test --workspace` 结果、
  storm 失败摘要（153MB snapshot / lag 12000 / AppendEntries 100ms timeout）。
- 基线验收：工作区干净；`cargo test --workspace` 通过；storm 失败复现一次并记录。

## 1. Phase 0：兼容性/API Spike（阻断门禁）

目标：在写生产代码前，用最小实验回答“能不能升、怎么升、数据怎么办”。

任务：
1. 新建 worktree/branch（如 `migrate/openraft-010`），pin
   `openraft = "=0.10.0-alpha.37"`，保留 `rmp-serde` 实验依赖。
2. 编译最小 `dsh-raft`：映射 `RaftNetworkV2`、`RaftNetworkFactory`
   (`new_client`/`new_heartbeat_client`/`new_snapshot_client`)、`RaftStateMachine`、
   `RaftLogStorage`、`Config`、`RaftMetrics` 的 0.10 API 差异。
3. 兼容性实验：
   - 用 0.9 生成的数据目录（含 log/snapshot/vote/meta）尝试 0.10 启动；
   - 验证旧 snapshot meta、committed/last_applied、log 能否读取；
   - 结论二选一：可直接升级 / 必须导出重灌或新集群。
4. SnapshotData 选型实验：`Arc<Vec<u8>>` vs `PathBuf`/临时文件；
   记录 153MB 快照的内存峰值、构建/安装耗时。
5. 输出 `dev_docs/research/openraft-010-compat.md`（或更新设计 §4.5/§5），
   包含 API 映射表、升级/回滚 runbook 草案、未解决问题。

门禁：
- oracle review：兼容性结论是否可信、迁移/回滚是否可执行、是否允许进入 Phase 1。
- 若 0.10 不能读 0.9 数据且没有可接受的数据迁移方案 → 停止，要求产品决策
  （维护窗口重建/导出导入/暂缓升级），不得硬推。

## 2. Phase 1：Storage / StateMachine / Log / Metrics 迁移

目标：0.10 类型系统在非网络层先跑通，保留确定性/持久化语义。

文件：
- `server/crates/dsh-raft/src/store.rs`
- `server/crates/dsh-raft/src/types.rs`
- `server/crates/dsh-raft/src/raft.rs`
- `server/crates/dsh-observability/src/lib.rs`
- `server/crates/dsh-raft/tests/*`（storage/cluster 适配）

任务：
1. `TypeConfig`/`SnapshotData`/`Entry`/`SnapshotMeta` 按 0.10 调整。
2. `LogStore` 迁移到 0.10 `RaftLogStorage`/`RaftLogReader`；保留
   `limited_get_log_entries`、purge/last_applied 语义。
3. `StateMachineStore` 迁移：`SnapshotBuilder`、`apply` stream/responder、
   `install_snapshot`、`get_current_snapshot`；保持 redb 持久化与启动恢复。
4. 观测 metrics 适配 0.10 字段；`/cluster/members` lag 语义不变。
5. 更新 storage 单测/官方 conformance suite（可用则接入）。

验证：
- `cargo check --workspace --all-targets`
- `cargo test -p dsh-raft`
- 0.9→0.10 数据兼容实验按 P0 结论执行（读取或导入）
- oracle gate：状态机确定性、快照持久化、恢复路径、metrics 语义

## 3. Phase 2：Network V2 + 二进制 RPC + 独立 clients

目标：普通复制路径先稳定，heartbeat 与 snapshot 连接解耦。

文件：
- `server/crates/dsh-raft/src/http_network.rs`
- `server/crates/dsh-raft/src/raft_http_server.rs`
- `server/crates/dsh-raft/src/raft.rs`
- `server/crates/dsh-raft/tests/*`

任务：
1. `RaftNetworkV2`：append_entries、vote、pre_vote、transfer_leader、backoff；
   P0 决定是否必须 `stream_append`（若可选，先普通 append）。
2. 所有 RPC 改 MessagePack 二进制帧；`Content-Type: application/msgpack`；
   request/response 错误分类（Unreachable vs Network）。
3. 独立 clients：`new_client` / `new_heartbeat_client` / `new_snapshot_client`
   使用独立 `reqwest::Client`（独立连接池）；使用 `RPCOption::soft_ttl()` 设置请求超时。
4. HTTP 路由：/raft/v2/{append-entries,vote,pre-vote,snapshot,transfer-leader}；
   删除/停用旧 JSON 端点；snapshot 路由独立 body limit。
5. 配置：`enable_pre_vote`、heartbeat/election、max_payload_entries/字节上限；
   移除 0.9 的 guard/startup grace。

验证：
- 3 节点真实 HTTP 集群 bootstrap/写/读/leader kill；
- 验证 heartbeat 在重复大 AppendEntries 时仍能到达（独立 client/连接）；
- `cargo test -p dsh-raft -p dsh-api`；
- oracle gate：连接独立性、超时语义、错误分类、PA/GA 授权回归（api 层）

## 4. Phase 3：流式二进制 Snapshot

目标：大快照不再受 body limit/JSON/整包缓冲影响，follower 不因 snapshot 竞选。

文件：
- `http_network.rs`（`full_snapshot`）
- `raft_http_server.rs`（snapshot stream handler）
- `store.rs`（SnapshotData 文件/内存实现）
- `tests/http_snapshot.rs`（改为 voter + 153MB 场景）

任务：
1. `full_snapshot`：Header frame + Chunk frames + End frame；单帧 ≤ 1MiB；
   读取 `SnapshotData` 流式发送，不整包缓冲。
2. Follower handler：axum body stream → 写入临时文件/内存 → 校验 → `install_full_snapshot`。
3. `max_in_snapshot_log_to_keep` 提升并可配置（P5 按写入速率定值）。
4. 校验和/长度校验；损坏帧/中断重试语义；snapshot 路由 body limit。
5. 测试：voter（不是 learner）大快照追赶；snapshot 安装后 12k 日志追赶；
   leader churn 期间 0 次 follower 竞选。

验证：
- `cargo test -p dsh-raft --test http_snapshot`
- 真实 5 节点 storm（缩短版）snapshot 阶段；
- 日志/指标证明：snapshot 期间 heartbeat 持续、victim 0 election、lag→0；
- oracle gate：协议正确性、流式内存/磁盘、恢复/回滚、测试盲区

## 5. Phase 4：配置 / CLI / 观测 / 文档 / 升级 runbook

任务：
1. `raft_config()` 集中配置；CLI 覆盖关键参数；生产默认值与 P5 结果一致。
2. 观测：snapshot 进度、heartbeat 延迟、lag；OpenAPI/metrics 文档更新。
3. 删除临时补丁与旧 JSON 端点；更新 `dev_docs/deployment-guide.md`：
   停机窗口、备份、升级、回滚、不可混合版本。
4. 升级演练脚本/手工步骤（备份→升级→验证→回滚）。

验证：
- `scripts/check-contracts.sh`、fmt/clippy/test；
- runbook 在本地/测试集群演练；
- oracle gate：运维可执行性、回滚、配置一致性

## 6. Phase 5：全量压力/混沌/发布门禁

验证：
- `scripts/stress-leader-storm.sh --profile storm` 全绿；
- `--profile extreme`（资源允许）或至少延长 storm；
- 153MB snapshot + 12k 日志 + leader churn；victim 0 election；lag 0；
- `cargo test --workspace`、clippy、fmt、contracts；
- 数据兼容/升级 runbook 演练；
- oracle 最终 gate：是否可发布；BLOCKER/MAJOR 清零。

## 7. 交叉评审门禁（设计阶段）

必须由两个独立 oracle 完成：
1. **架构/标准符合性**：0.10 方案是否真正消除根因；是否有更小/更标准的替代；
   临时补丁是否应回退；API/存储迁移风险。
2. **迁移/兼容/回滚/测试**：P0 兼容性门禁是否足够；
   停机/备份/回滚 runbook 是否可执行；测试是否能证明 storm 全绿。
两份 review 都通过（BLOCKER/MAJOR 有结论）后才进入 Phase 0 实施。
每阶段实现后再按 deepwork 规则做 oracle gate（每 gate 最多两次 re-review）。

## 8. 回滚与发布

- 升级前：备份 0.9 二进制 + 数据目录；确认恢复演练。
- 0.10 写入后不可直接回 0.9（除非恢复备份）；runbook 必须写明。
- 发布前在测试集群执行完整 runbook；生产维护窗口执行。
- 若 P0 证明无法原地升级：采用导出/新集群方案，单独评审后再执行。

## 9. Gate 0 review revisions (v1.1, 2026-10-27)

本节覆盖前文，作为执行时必须遵守的补充。

### 9.1 Phase 0 增补（M1/M2/M8 + ora-2 验证链）
- **先修验证链**：stress harness 必须检测二进制是否早于源码（或每次强制构建），启动后断言
  日志中的实际 heartbeat/election/max_payload 与预期一致；否则旧二进制会污染所有结论。
- 记录一次干净构建的 storm 失败/通过，并保存 lag 时间轨迹（区分“恒定不动”的活锁 vs 缓慢下降）。
- 0.9→0.10 兼容性验收三级：serde 字段矩阵、3 节点功能验证、旧 snapshot 恢复；不得只验启动。
- fallback：原型验证 genesis-restore（dump_all JSON → 新集群初始快照）；核实当前无 import CLI，
  不得假设现成命令。
- SnapshotData 选型规则：153MB 下 RSS 峰值 ≤2× 且 install ≤30s → Vec；否则 PathBuf。
  计入 `get_current_snapshot` 的整包 clone。
- P0 末尾增加 **0.10 alpha go/no-go**：若组织不接受 alpha，停止并切换到 0.9 二进制传输方案。

### 9.2 Phase 1 重划（M3）
P1 定义为“全 workspace 机械编译迁移”，包含：
- dsh-raft：types/store/log/metrics + `http_network.rs`/`raft_http_server.rs`/`network.rs`
  的 0.10 trait **签名 stub**（行为不变，协议在 P2 实现）；
- dsh-api/dsh-cli 使用 `dsh_raft::openraft` 类型的机械替换。
验证：`cargo check --workspace --all-targets` + `cargo test -p dsh-raft --lib` + storage/conformance；
网络 e2e 测试移到 P2 gate。不要同时在 P1 承诺 `cargo test -p dsh-raft`（含网络测试）。

### 9.3 Phase 3 测试增补（M6/m8/m4/m5）
- `http_snapshot.rs`：保留 learner 用例，新增 voter 大快照用例（不替换）。
- 新增“snapshot 追赶期间注入 1–2 次 leader kill/freeze”的并发场景；断言 victim 0 election、
  lag→0、所有节点日志无 forbidden token。
- FORBIDDEN 增加 0.10 失败签名：msgpack 解码错误、full_snapshot/soft_ttl 超时。
- storm catchup timeout 显式定值（0.10 大快照 180s）；debug/release 分别记录。
- 证据工件：leader 心跳时间序列/包间隔、victim election 计数、lag 轨迹。

### 9.4 Phase 4 运维增补（M4/M5/M7/m6/m7）
- Snapshot 路由：单帧 1MiB + total body limit ≥ 最大快照（或禁用并流式落盘）；文档写清 token/DoS。
- 代理：raft 端口优先直连；若代理，`client_max_body_size`、`proxy_request_buffering off`、
  读超时按 install 预算配置；更新 deployment-guide §6.5。
- metrics 最小集：snapshot in-flight、最近 install 耗时、election 计数、lag；告警 lag>0 5 分钟。
- `max_in_snapshot_log_to_keep=50_000` 需给出磁盘容量指引（60KB/entry ≈3GB/节点）。
- runbook：全集群停止后备份；升级前后 `admin snapshot` dump 比对；回滚触发条件；
  0.10 启动后回滚=恢复备份；维护窗口预估。

### 9.5 补丁/提交卫生（m1/m2/m3）
- 当前 0.9 缓解补丁：保存 repo 内 patch + 分支 `mitigations/openraft-09`（不合入 main），
  然后回退；注释/代码不一致（4s vs 30s）在 patch 说明中标注。
- `.gitignore`、设计/计划文档、deepwork 忽略项先做卫生提交，与源码补丁分开。

## 10. Gate 0 v1.2 revisions (architecture/standard, ora-3)

- P0 增补 AppendEntries 预算测量：60KB×N msgpack 体积、往返、redb fsync、release/debug；
  结论决定 P2 是否必须 `stream_append`。
- P0/Phase 0 兼容性回退三选一：原地兼容 / 新建导入工具（成本估期）/ 数据重建（产品决策）；
  不得引用不存在的 import 命令。
- P1 明确包含最小 V2 编译桥（或 legacy 适配）、`Raft<C,SM>` 泛型波及 dsh-api/dsh-cli；
  验证 `cargo check --workspace --all-targets`，网络 e2e 移到 P2。
- P2 增补：`soft_ttl(heartbeat_interval)` 预算；`limited_get_log_entries` 字节截断；
  stream_append 触发判据；heartbeat reqwest timeout ≤ soft_ttl。
- P3 增补：snapshot 路由 total body limit 语义；`full_snapshot` cancel/option.snapshot_chunk_size；
  SnapshotData 同型约束；Header/End/checksum；整包重传。
- P1 增补 apply responder 回路测试；P4 删除 snapshot_id 构造逻辑、使用 borrow_watched。
