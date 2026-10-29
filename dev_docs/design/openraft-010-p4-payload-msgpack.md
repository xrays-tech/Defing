# P4 设计：快照 payload 的二进制化与流式恢复（msgpack）

状态：草案（待 gate）。前置：P3 实测 release+btrfs、269MB 快照 install 端到端 1.78s、
峰值 RSS 1.37x；payload 仍是 `serde_json` 的 `Vec<(Vec<u8>, Vec<u8>)>`。

## 1. 现状与代价

- 写：`serde_json::to_vec(&dump_all())` → 269–400MB 文本；写入走 `fs::copy`（无 Vec 中转）。
- 读：`read_payload_pairs` = `serde_json::from_reader` → 一次性 `Vec<(Vec<u8>, Vec<u8>)>`（~1x payload 常驻）。
- 观测：安装窗口 = transfer + install + apply；400MB 时 apply 段 1.0–1.5s，transfer 0.7–1.0s。
  当前已满足 30s 预算，但 **payload 体积与解析是 apply 段的主要 CPU**，且整份 `Vec` 常驻让 RSS 上限
  随快照线性增长（1.37x 主要是它 + redb 脏页）。

## 2. 目标（按收益排序）

1. **有界内存恢复**：不再一次性 materialize 全部 `(k,v)`；按 64MiB 批次「解析一批 → 恢复一批」，
   峰值 RSS 目标 ≤ 0.5x payload + redb 脏页。
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
| payload 体积（同数据集） | 269MB（JSON） | ≤ 200MB（msgpack） |
| install 端到端（269MB 档） | 1.78s | ≤ 1.78s（不退化） |
| 峰值 RSS | 1.37x payload | ≤ 0.7x payload |
| 旧 v1 文件可读 | — | 必须（升级路径） |
| 流式恢复多批边界 | 已测（64MiB 单测） | 保留并通过 |

功能回归：voter/learner 快照 e2e、`snapshot_persist`、`snapshot_sweep`、storm 断言与 P3 相同。
