# 设计文档：openraft 0.10 迁移、二进制流式快照与独立心跳

状态: v1（待交叉评审）
日期: 2026-10-27
范围: server/crates/dsh-raft、dsh-cli、dsh-observability、dsh-api（仅编译/类型适配）、
tests、dev_docs。**不改** Defing 对外 HTTP API、命令语义、数据模型。
上游研究:
- `lib-1` openraft 0.9/0.10 官方实现调研（2026-10-27）
- `exp-2` Defing vs openraft 标准偏差清单（2026-10-27）
- `dev_docs/design/…` 既有 Raft 设计；`scripts/stress/leader_storm.py` 实测证据

## 1. 背景与实测证据

压力测试 `scripts/stress-leader-storm.sh --profile storm` 暴露了两类问题：

1. **大快照传输期间 follower 竞选**：openraft 0.9 的 `Inflight::Snapshot` 会抑制该目标的
   周期 heartbeat，InstallSnapshot 中间分块也不续期 follower lease。快照总时长超过
   election timeout 时 follower 必然竞选，抬高 term 后拒绝 leader 的旧 vote，形成永久落后。
2. **snapshot 安装后日志追赶卡住**：leader 按 `max_payload_entries` 发送 AppendEntries，
   0.9 的硬超时等于 `heartbeat_interval`（当前 100ms）；超时后 error path 只清 inflight，
   用**同一批 payload** 重试（`exp-2` 核对 `replication/mod.rs:455-457`、
   `replication_handler/mod.rs:286-307`、`progress/entry/mod.rs:182-254`），不会 shrink。
   大 payload 批次在 100ms 内无法完成时，lag 永远不下降。

已做的临时缓解（body limit / snapshot guard / startup grace / 调参）只能降低症状，
不是标准架构；本设计以 openraft 0.10 的 dedicated heartbeat worker +
independent heartbeat/snapshot client + 二进制流式传输为根因修复。

## 2. 目标 / 非目标

### 目标

- 升级到 openraft 0.10（pin `=0.10.0-alpha.37`），实现 `RaftNetworkV2`。
- Snapshot 使用**二进制流式 body**（length-prefixed MessagePack frames），不再用 JSON
  `Vec<u8>` 整包/数字数组。
- 覆盖 `new_heartbeat_client` / `new_snapshot_client`，heartbeat 与 snapshot/大 batch
  使用独立连接池，互不阻塞。
- 迁移 state machine / log store / metrics 到 0.10 trait。
- 调整 `max_in_snapshot_log_to_keep`、`enable_pre_vote`、batch/timeout，覆盖大快照 + 高写入。
- 保留 Defing 对外 API、审计、渲染、鉴权语义。

### 非目标

- 不改 Defing 配置数据模型/命令 wire（`Command` 仍由 serde 兼容）。
- 不引入新的集群成员管理语义。
- 不在本期实现多集群/跨地域复制。
- 不支持 0.9/0.10 混合滚动升级（见 §6）。

## 3. 标准实现对照

| 维度 | openraft 0.10 标准/官方示例 | Defing 现状 | 目标 |
|---|---|---|---|
| Snapshot 传输 | 0.10 rocksdb 示例：流式 length-prefixed MessagePack；`full_snapshot` 自定义 | HTTP+JSON，1MiB chunk 数字数组 | 二进制帧流式 |
| Heartbeat | 独立 `HeartbeatWorker` + `new_heartbeat_client` | 0.9 无；snapshot 在途被抑制 | 独立 client + worker |
| Snapshot client | `new_snapshot_client` 独立连接 | 与普通 RPC 共用 reqwest client | 独立 client |
| AppendEntries 超时 | 0.10 `soft_ttl`/`hard_ttl`；`stream_append` 支持长连接 | 100ms hard timeout，同 payload 重试 | 二进制 + 软超时 + 必要时 stream_append |
| 大 payload | 按字节设计 batch；官方示例调 heartbeat/install timeout | `max_payload_entries=64` 条数，未按字节 | 按字节/连接能力调参 |
| 大快照日志保留 | `max_in_snapshot_log_to_keep` 按写入速率增大（issue #1706） | 默认 1000 | 显著增大并可配置 |
| Pre-vote | 0.10 `enable_pre_vote` | 0.9 无 | 开启 |
| Snapshot 数据 | 文件/句柄或应用自定义 `SnapshotData` | redb 内存 `Vec<u8>` | `SnapshotData` 抽象；P0 定内存/文件 |

## 4. 目标架构

### 4.1 依赖与配置

- `server/Cargo.toml` / `dsh-raft/Cargo.toml`: `openraft = "=0.10.0-alpha.37"`，
  features 按 P0 编译结果确定（serde/storage-v2）。
- 新增 `rmp-serde`（MessagePack）；可选 `serde_bytes`（若类型需要）。
- `dev_config` 改名为 `raft_config()`，集中所有可调参数；生产配置不再散落硬编码。
- 新增 CLI 覆盖（至少 heartbeat/election/install timeout/snapshot chunk/log retain），
  默认值按标准示例与 stress 证据设定；P0/P5 调优后冻结。

### 4.2 Network V2

实现 `RaftNetworkV2`：

- `append_entries` / `stream_append`（P0 确认是否必须实现；若可选，先实现普通 append，
  stress 证明不足时实现 stream_append）。
- `vote` / `pre_vote`。
- `full_snapshot`：读取 `SnapshotData`，按帧流式发送。
- `transfer_leader` / `backoff`：按 0.10 默认/官方示例实现。

`HttpNetworkFactory`：

- `new_client`：数据/普通 RPC client。
- `new_heartbeat_client`：独立 heartbeat client（独立连接池）。
- `new_snapshot_client`：独立 snapshot client（独立连接池）。
- 每个 client 用 `RPCOption::soft_ttl()` 设置请求级 timeout；不忽略 `RPCOption`。
- 二分错误映射：连接失败 → `Unreachable`（让 openraft backoff）；其他 → `Network`。

传输：

- 所有 Raft RPC 使用 MessagePack 二进制帧：`Content-Type: application/msgpack`。
- Snapshot：`POST /raft/v2/snapshot`，body = `Header frame` + 多个 `Chunk frame` + `End frame`；
  length-prefixed（4-byte big-endian 或 MessagePack frame），单 chunk ≤ 1MiB。
- Follower 侧 axum handler 以 body stream 接收，不 buffer 整个 snapshot；写入
  `SnapshotData`（P0 决定 `Vec<u8>` 临时文件或 `PathBuf`），完成后
  `install_full_snapshot(vote, snapshot)`。
- body limit：snapshot 路由单独设置（例如 8MiB/帧），不要依赖全局 32MiB 兜底。
- 旧 `/raft/*` JSON 端点在新版本中移除或保留为 501；不支持混合版本。

### 4.3 SnapshotData 与存储

- P0 决策：`SnapshotData = Arc<Vec<u8>>`（低改动，153MB 量级可接受）或 `PathBuf`
  （官方大快照推荐，避免内存峰值）。默认倾向 `PathBuf` + 临时文件；
  若 P0 证明改动过大，先用 `Arc<Vec<u8>>` 并在计划中保留文件化后续项。
- `StateMachineStore` 迁移到 0.10 `RaftStateMachine`：
  - `SnapshotData` 关联类型；`SnapshotBuilder`；`apply` stream/responder；
  - `get_current_snapshot` / `install_snapshot` / `build_snapshot`；
  - 保留当前 redb 持久化与 `last_applied`/`purged` 语义；如改文件，需清理旧文件。
- `LogStore` 适配 0.10 `RaftLogStorage`/`RaftLogReader`；保留 `limited_get_log_entries`。
- `max_in_snapshot_log_to_keep` 默认从 1000 提升（例如 50_000，P5 按写入速率压测确定），
  避免 snapshot 安装后 leader 已 purge 所需日志。

### 4.4 观测与 CLI

- 迁移 `RaftMetrics` 类型；保留 `/cluster/members` 的 `last_applied`/`lag`，
  新增 snapshot 传输进度/心跳指标（可选）。
- `dsh-cli` 生产路径显式构建 `raft_config()`；新增参数覆盖关键时序。
- 删除临时 `ElectionGuard` / startup grace / 0.9 调参补丁；它们不属于目标架构。

### 4.5 升级与兼容

- 0.10 网络/快照 wire 与 0.9 不兼容；**不支持 0.9/0.10 混合集群**。
- P0 必须验证：0.10 能否读取 0.9 redb 数据/log/snapshot meta。
  - 能：全集群停机升级（维护窗口），保留数据。
  - 不能：导出/重灌（优先使用现有 `defing admin snapshot`/redb 备份能力），
    或新集群 + 数据恢复；必须在 P0 给出可执行 runbook，不能在实现中临时决定。
- 升级前备份二进制与数据目录；0.10 写入后不可用 0.9 回滚（除非恢复备份）。
- 发布文档必须写明停机窗口、备份、回滚步骤和不可混合版本。

## 5. 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| 0.10 alpha API 不稳定 | 编译/行为变更 | pin 精确版本；P0 锁 API；不跟 latest |
| Storage trait 迁移大 | 数据损坏/回归风险 | P0 兼容性 spike；官方 storage conformance suite；备份 |
| Snapshot 二进制协议错误 | 快照不可恢复 | 端到端大快照 + 校验和；fault injection |
| 独立 client 未真正独立 | heartbeat 仍被阻塞 | 独立 reqwest client/连接池；抓包/测试验证 |
| AppendEntries 大 payload | 追赶仍超时 | 二进制 + soft TTL + stream_append/按字节 batch；stress 验证 |
| 升级停机/回滚 | 生产可用性 | 维护窗口 + 备份 + runbook；灰度预演 |
| 测试覆盖不足 | 假绿 | 真实 HTTP、voter（非 learner）、153MB snapshot、12k 日志、leader churn |

## 6. 验收与门禁

### 设计门禁（进入开发前）

- 两份独立 oracle review 通过：架构/标准符合性；迁移/兼容/回滚/测试。
- 所有 BLOCKER/MAJOR 有结论；P0 兼容性方案明确。
- 明确 P0 是否允许继续、是否需要产品决策（停机窗口/数据重建）。

### 实现门禁（每阶段）

- `cargo fmt --check`、`cargo clippy --workspace --all-targets --all-features -D warnings`、
  `cargo test --workspace`、`scripts/check-contracts.sh`。
- P3/P5 必须通过：
  - `cargo test -p dsh-raft --test http_snapshot`（voter，不只是 learner）；
  - `scripts/stress-leader-storm.sh --profile storm` 全绿（含 153MB 快照 + 12k 日志追赶）；
  - leader churn + snapshot 期间 0 次 follower 竞选（日志/指标证据）。
- 升级 runbook 演练通过（备份→升级→验证→回滚）。

## 7. 明确不做

- 不保留 0.9 的 guard/startup grace 作为长期机制。
- 不依赖“调大 body limit / 调小 batch”作为唯一修复。
- 不支持 0.9/0.10 混合滚动升级。
- 不在本期做跨地域/多集群复制。

## 8. Gate 0 review revisions (v1.1, 2026-10-27)

本节覆盖前文的不足，作为 P0 及后续阶段的补充约束。

### 8.1 P0 兼容性验收（M1）
不能只验证“0.10 能启动/能读”。P0 必须完成三级证据：
1. **serde 兼容矩阵**：对 `Entry`/`Vote`/`LeaderId`/`LogId`/`SnapshotMeta`/`StoredMembership`/
   storage meta 逐类型、逐字段比对 0.9 写入 vs 0.10 读取；字段删除/重命名必须显式检测，
   不能依赖 serde 静默忽略未知字段。
2. **功能验证**：用 0.9 生成的数据目录启动 3 节点 0.10 集群，完成选主、接受写、follower 追平。
3. **旧快照恢复**：0.9 落盘的 snapshot 在 0.10 节点安装成功（或明确判定不能读）。
任一失败 = “不能直接升级”。

### 8.2 无法直接升级时的 fallback（M2）
- 核实：`defing admin snapshot` 只有导出（`dump_all`），**没有 import/restore CLI**；
  redb 文件备份在类型不兼容时同样不可用。
- P0 必须原型验证 **genesis-restore**：dump_all JSON → 新集群初始快照（合成
  last_applied/membership/snapshot index），确认可执行；否则上报产品决策。
- 设计论点：dump/restore 使用应用自有 JSON，绕开 openraft 类型 churn，是版本迁移的
  最稳路径；但必须补齐 genesis-restore 设计，不能假设现成命令。

### 8.3 Snapshot HTTP body 语义（M4）
- **单帧解码上限** = 1MiB（应用层帧长度校验），与 HTTP body limit 不是同一概念。
- Snapshot 路由的 **total request body limit** 必须 ≥ 允许的最大快照尺寸，或禁用
  `DefaultBodyLimit` 并以流式写入/内存上限控制；不能写“8MiB/帧”当作 body limit。
- 端点保留 token 鉴权；DoS 风险通过内网/大小上限/并发限制控制。

### 8.4 代理/网关要求（M5）
- 优先让 Raft 端口（raft_addr）**不经反向代理**，仅内网直连。
- 若必须代理：`client_max_body_size ≥ 最大快照尺寸`、`proxy_request_buffering off`、
  读超时 ≥ install 预算；P4 更新 `dev_docs/deployment-guide.md` §6.5。

### 8.5 SnapshotData 选型规则（M8）
- 基线测量必须计入 `get_current_snapshot` 整包 `data.clone()` 的隐性成本。
- 决策规则（P0 冻结）：153MB 快照下，进程 RSS 峰值 ≤ 2× 快照大小且 install 耗时
  ≤ 30s → `Vec<u8>` 可接受；否则 `PathBuf`/临时文件。不能以“改动过大”作为理由。

### 8.6 测试与观测（M6/M7/m5/m6/m7/m8）
- P3 增加 **snapshot 追赶期间注入 leader kill/freeze** 的并发场景；断言 victim 0 election、
  lag→0；日志扫描扩展到所有节点，并加入 0.10 失败签名（msgpack 解码、full_snapshot/soft_ttl 超时）。
- 保留 learner snapshot 用例，新增 voter 大快照用例，不替换删除。
- metrics 最小集：snapshot in-flight、最近 install 耗时、election 计数、lag；建议告警
  lag>0 持续 5 分钟。删除“可选”措辞。
- `max_in_snapshot_log_to_keep` 需配套磁盘容量指引（60KB/entry 时 50k 约 3GB/节点）。
- storm 的 catchup timeout 显式定值（0.10 大快照用 180s；debug 性能另列）。

### 8.7 升级/回滚 runbook（M7）
- 备份必须在**全集群停止后**拷贝数据目录；运行中拷贝不保证 redb 一致性。
- 升级前/后执行 `admin snapshot` dump 比对（数据完整性证据）+ 选主/全员 lag=0/试写试读。
- 回滚触发条件：N 分钟无 leader、写持续失败、lag 不收敛；回滚 = 恢复备份 + 0.9 二进制，
  不是直接降级（0.10 启动后数据目录视为已污染）。
- 给出维护窗口时长预估与演练 checklist。

### 8.8 版本策略 / 临时补丁
- 0.10 为 alpha：P0 设 go/no-go；若组织不接受 alpha，停止 0.10 路线，切换到
  “0.9 二进制传输 + 存储批量化 + ElectionGuard”方案（ora-2 推荐）。
- 现有 0.9 guard/startup grace 补丁：保存到 repo 内 patch + 分支 `mitigations/openraft-09`
  （非经决策不合入 main），然后回退；`.gitignore`/文档卫生单独提交。

## 9. Gate 0 v1.2 revisions (architecture/standard, ora-3)

本节覆盖前文冲突处，作为 v1.2 的最终约束。

### 9.1 AppendEntries 时序预算（M2）
0.10 replication 会话给网络的 `RPCOption` 仍以 `heartbeat_interval` 为 hard TTL（官方示例用
`option.soft_ttl()` ≈ 3/4 hard TTL）。必须显式推导，不能只抄示例：
- 目标：单批 AppendEntries 的 p99 端到端延迟（leader 序列化→HTTP→follower 解码→redb 写入→响应）
  ≤ `soft_ttl(heartbeat_interval)`；否则会把活锁从 0.9 带到 0.10。
- P0/P2 测量：60KB × N（N=16/32/64）MessagePack 体积、往返、follower fsync 耗时；记录 release/debug。
- 0.10 没有“按字节 batch”配置；字节上限通过覆写 `RaftLogReader::limited_get_log_entries`
  按累计字节截断实现（返回前缀是 0.10 允许的行为）。
- `stream_append` 启用判据：若普通 append 在目标负载下 p99 超 soft_ttl，或 leader 观测到
  PartialSuccess/backoff 频繁，则 P2 必须实现/启用；否则记录不启用理由。
- heartbeat 请求 timeout 必须 ≤ `soft_ttl(heartbeat_interval)`（HeartbeatWorker 自身用
  `C::timeout(heartbeat_interval)`）。

### 9.2 否决的替代方案（M5）
- **0.9 + 自定义 `install_full_snapshot`**：只能解决快照传输/JSON/body limit；不能解决
  `Inflight::Snapshot` 抑制心跳与 AppendEntries 硬超时（core 行为），无法消除两个根因。
- **0.10 + `openraft_legacy` v1 桥**：可降低迁移坡度，但保留最差传输（JSON/分块/共享 client），
  且 storage 迁移成本不变；只应作为 P1 机械编译桥，不作为目标架构。
- **仅调参**：0.9 的批量/心跳/超时此消彼长，且会永久依赖 ElectionGuard；不可作为终态。
- 结论：直接 `RaftNetworkV2` + 二进制流式快照 + 独立 clients 是合理主路径；legacy 仅临时桥。

### 9.3 无法原地升级的三选一（M3）
P0 门禁必须输出可执行选择及成本，不能引用不存在的命令：
- A 原地兼容：serde 矩阵 + 3 节点功能 + 旧 snapshot 恢复（最优）。
- B 新建导入工具：新增 `admin restore`/genesis-restore 设计与估期；Dump/restore 使用应用自有
  JSON，绕开 openraft 类型 churn；必须补工具与测试。
- C 数据重建/有损升级：需产品决策与业务确认。
runbook 不得写“用现有 import 命令”；当前不存在。

### 9.4 P1 编译桥（M4）
0.10 `Raft<C, SM>` / Network associated types 会波及 dsh-api/dsh-cli。P1 必须包含：
- `http_network.rs`/`raft_http_server.rs`/`network.rs` 的最小 V2 机械实现（可委派现有 JSON post）
  或 `openraft_legacy::network_v1` 适配；标记临时，P2 替换；
- `dsh_raft::openraft` 重导出类型的机械替换；
- 验证 `cargo check --workspace --all-targets`；网络 e2e 移到 P2。

### 9.5 协议与类型细节（M1/m2/m3/m4/m7/m8/m10）
- Snapshot 路由：帧级 1MiB 解码上限 + **total body limit ≥ 最大快照尺寸或禁用**；不得写 8MiB/帧
  当作 body limit。
- `full_snapshot` 实现必须处理 `cancel` future（`tokio::select!`），chunk size 取
  `option.snapshot_chunk_size()`，不硬编码。
- `N::Network: NetSnapshot<C, SnapshotData = SM::SnapshotData>`：网络与状态机 SnapshotData 同型；
  0.10 已从 TypeConfig 移除该类型。
- `apply` stream：每个 entry 必须调用 `ApplyResponder::send`，否则 client_write 不返回；P1 加回路测试。
- Header 帧含 vote + SnapshotMeta；End 帧含总长 + checksum（算法/覆盖范围写明）；失败整包重传，
  不做断点续传。
- `SnapshotMeta.snapshot_id` 在 0.10 移除；删除 store.rs 的 snapshot_id 构造逻辑。

### 9.6 其他修订
- 0.10 已无 `storage-v2` feature；依赖 feature 用 `serde`（必要时 `tokio-rt`），legacy 是单独 crate。
- 官方 rocksdb 示例 heartbeat 仍为默认 50ms；只显式调 `install_snapshot_timeout=10s` 和
  `enable_pre_vote`。不要写成“官方示例调 heartbeat”。
- pre-vote 的作用是减少无谓 term 抬升；snapshot 期间不竞选主要靠独立 heartbeat worker，
  pre-vote 是第二道防线。
- metrics 0.10 用 `borrow_watched()`；lag 语义保持；ReplicationMetrics 类型仍可映射。
