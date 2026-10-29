# P4 设计：快照 payload 的二进制化与流式恢复（msgpack）

状态：APPROVE WITH CHANGES（gate attempt 2）。基线以 `P4base` 为准：358.2MB / episode 3.611s（末次 2.474s）/ net 1.00x、raw 1.31x。P3 实测 release+btrfs 历史：269MB 快照 install 端到端 1.78s、
峰值 RSS 1.37x；payload 仍是 `serde_json` 的 `Vec<(Vec<u8>, Vec<u8>)>`。

## 1. 现状与代价

- 写：`serde_json::to_vec(&dump_all())` → 269–400MB 文本；写入走 `fs::copy`（无 Vec 中转）。
- 读：`read_payload_pairs` = `serde_json::from_reader` → 一次性 `Vec<(Vec<u8>, Vec<u8>)>`（~1x payload 常驻）。
- 观测：安装窗口 = transfer + install + apply；400MB 时 apply 段 1.0–1.5s，transfer 0.7–1.0s。
  当前已满足 30s 预算，但 **payload 体积与解析是 apply 段的主要 CPU**，且整份 `Vec` 常驻让 RSS 上限
  随快照线性增长（1.37x 主要是它 + redb 脏页）。

## 2. 目标（按收益排序）

1. **有界内存恢复**：不再一次性 materialize 全部 `(k,v)`；按 64MiB 批次「解析一批 → 恢复一批」，
   峰值 RSS 目标：**net ≤ 1.2x payload**（P4base 基线 net 1.00x；旧「≤0.5x」不可达，作废）。
2. **更小的 payload**：msgpack（binary）替代 JSON 文本；同类数据的经验值：体积 −30~40%、解析快 2–3x。
3. **格式可演进**：带 magic + version，读端接受 v1(JSON) / v2(msgpack)，写端默认 v2；
   不做 0.9/0.10 混跑，但同一节点升级要能读旧文件。

## 3. 格式

```
payload := MAGIC(8B "DSHSNP\x01\x00") || VERSION(u8) || BODY
VERSION 1: BODY = JSON 文本（`Vec<(Vec<u8>,Vec<u8>)>`，即现状）
VERSION 2: BODY = msgpack：数组 header(n) + 逐项 [bin(k), bin(v)]
```

- 写 v2：手工写数组头 + 逐项 `rmp_serde::to_vec_named`（或 `rmp::encode`），**不构造整份 Vec**：
  `dump_all()` 已是 Vec，但可用迭代器分批写；若 dump 本身是整份 Vec，则先解决 dump 的分页
  （见 §5 未决）。
- 读 v2：`rmp` 流式解码器读数组长度，循环读 `(bin, bin)`，按字节预算组装 batch（64MiB）→
  `restore_all_batched(batch, chunk_bytes)`。
- 读 v1：保持现状 `serde_json::from_reader`（升级期一次性路径即可）。
- 兼容：`MAGIC` 不匹配 → 视为 v1（旧文件没有头），但若解析失败则报错并保留文件（不删）。

## 4. 施工顺序（每步可独立合并、可回滚）

1. **P4a 写入头部 + 读取兼容层**：新写文件带 MAGIC+VERSION=1(JSON)；读端识别头。
   测试：旧文件（无头）仍可读；新文件可读；损坏头报明确错误。
2. **P4b 流式恢复**：`read_payload_batches(path, budget) -> Iterator<Vec<(k,v)>>`，
   `restore_all_batched` 直接消费；JSON 与 msgpack 共用该接口。
   验收：400MB 快照峰值 RSS ≤ 0.7x payload（相对现状 1.37x），install 不退化。
3. **P4c 写端 msgpack + 流式写**：`dump_all` 分批 → 直接写文件（无整份 Vec）。
   验收：体积 ≤ 0.75x JSON；install 端到端 ≤ 现状；旧 v1 文件仍可读。
4. **P4d 清理**：默认只写 v2；v1 读路径保留一个发布周期，之后按 anti-entropy 流程下线。

## 5. 未决（gate 决策）

- **`dump_all()` 本身是否整份 Vec**：`StateMachine::dump_all` 当前返回 `KeyValuePairs`（整份）。
  要真正做到「写端无整份驻留」，需要给状态机加前缀迭代/分页 dump（`dump_batches(prefix, budget)`）。
  这是 P4c 的前置，可能牵动 `get_prefix_merged` 的调用点；建议 P4c 先做只读分页，不改 apply 路径。
- **snapshot 文件的双写**：当前 HTTP 传输 + install 各落一份（P3 已用 rename→copy 折中）。P4 可让
  server 直接把流写进 `snapshots/<canonical>`，install 只做「认领 + 校验」，彻底去掉一份写。
  这会影响 `restore-in-progress` 标记里的 payload 指向（见该设计 §6）。
- **压缩**：是否在 v2 外层加 zstd（体积再降，但 CPU 与 RSS 权衡）；建议 P5 用实测决定，格式预留 flag 位。

## 6. 验收（P4 gate 用）

| 指标 | 现状（P3） | P4 目标 |
|---|---|---|
| payload 体积（同数据集） | 384.2MB（JSON，P3m） | ≤ 280MB（msgpack，目标 −25%） |
| install episode（358MB 档） | 3.611s（P4base） | ≤ 3.7s（不退化） |
| 峰值 RSS | net 1.00x（P4base） | net ≤ 1.2x（有界化） |
| 旧 v1 文件可读 | — | 必须（升级路径） |
| 流式恢复多批边界 | 现有测试只注入 1/4 字节 chunk（无 64MiB 多批） | P4b 新增 ≥3 批流式测试 |

功能回归：voter/learner 快照 e2e、`snapshot_persist`、`snapshot_sweep`、storm 断言与 P3 相同。

## 7. 修订（P4 design gate attempt 1 之后，必须按此实现）

复核 REJECT，两条 blocker + 若干 major：

- **B1（blocker）清空语义**：`restore_all_batched()` 每次调用都会重算 `existing` 并在首批删光所有旧 key
  → 逐批调用会把前 N-1 批删掉。**改为**：新增
  `StateMachine::restore_stream(batches: impl Iterator<Item=Vec<(k,v)>>)`，由它**只清空一次**、
  后续批只做插入；`restore_all(pairs)` 变成「单批的 restore_stream」。验收：≥3 批（每批 < 64MiB，
  总 > 128MiB）全部 key 存活 + 旧 key 被清 + 空流清空。
- **B2（blocker）上线兼容**：payload 是**原样上线**的（`FrameEmitter` 直接流文件），MAGIC/v2 头会
  出现在 wire 上，而帧 Header 没有 format/version 协商 → 滚动升级单向打断 catch-up。**发布策略**：
  1. **P4a1（可先做）**：读端同时接受 v1(旧，无头) / v2(带 MAGIC)；写端**仍写 v1**；
  2. **P4a2**：等全集群都跑 P4a1 后，用 `DSH_RAFT_SNAPSHOT_PAYLOAD=msgpack` 打开写端 v2；
  3. 回滚：关掉 flag 即回到 v1（读端一直兼容 v1）。
  混跑矩阵与「协调重启」作为备选写进 runbook。
- **B2'（major）payload 级完整性**：头部补 `total_len` + body 的 `CRC32`（帧 CRC 只保护 wire，
  且由发送方对可能已损坏的文件计算）；install、boot replay、发送前都校验；加截断/损坏测试。
- **B3 基线更正（P4base 重跑，2026-10-29）**：修正 harness 基线采样后重跑 12k gate：
  snapshot 358.2MB、episode 3.611s（attempts=2，末次 2.474s）、窗口 RSS 468.0MB / 基线 118.9MB
  → **net ≈ 1.00x、raw ≈ 1.31x**。此前 0.62/0.64 是「用 transfer 后采样当基线」的虚低值，作废。
  P4b 目标据此改为 `net <= 1.2x`（有界内存的相对改进），不再沿用 0.7x。
- **B3（major）口径统一**：只认两个指标 —— `net = (window_max - baseline)/snapshot`（1024 换算）
  与 `raw = window_max/snapshot`。P3m 基线：net 0.64x / raw 1.24x；目标写成 `net <= 0.7x`。
- **B4（major）撤回错误声明**：现有测试只注入 1/4 字节 chunk 的**分块**测试，没有 64MiB 多批流式
  测试；本文 §6 那一行改为「P4b 新增」。
- **B5（major）顺序**：`dump_all()` 分页拆成 **P4c0**（只读分页 dump + 验收），P4c 依赖它。
- minors：tuple 用 positional 编码 + KAT（避免 §13.5 同类）；MAGIC 不匹配走**有界嗅探**（读头部
  再判），加 v2 截断测试；明确 v1 读路径下线条件；P4d 触发条件写清。
