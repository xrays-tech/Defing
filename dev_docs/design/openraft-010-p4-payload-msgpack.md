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
payload := MAGIC(6B "DSHSNP") || VERSION(u8) || FLAGS(u8) || TOTAL_LEN(u64 LE) || CRC32(u32 LE) || BODY
VERSION 1: BODY = JSON 文本（`Vec<(Vec<u8>,Vec<u8>)>`，P3 布局；头部可选，见下）
VERSION 2: BODY = msgpack：array(n) of [bin(k), bin(v)]（rmp positional；有 KAT）
```
- **头布局定稿（P4a2 实现）**：20B as above。P4a1 的读端兼容层先落成 8B
  （`MAGIC|VERSION|FLAGS`），P4a2 补齐 `TOTAL_LEN + CRC32`（B2'）——P4a1 写端从未写头，
  因此没有旧带头文件需要兼容，布局可直接定稿。
- **无头 payload**：P3 及更早写的 JSON 没有头（写端默认仍是它），读端按「MAGIC 不匹配 → v1」处理。
- `TOTAL_LEN`/`CRC32` 覆盖 **BODY**（不含头），由 payload 创建方在落盘时写入（头与 body 分两段
  写，不为拼「头+体」再复制一份 payload）；install、boot 重放、发送前（`verify_payload_file`）
  三处同口径校验。无头旧文件没有完整性头，`verify_payload_file` 直接放行。

- 写 v2（P4a2 已落地）：`rmp::encode::write_array_len` + `write_bin` 逐项编码，body 目前仍整份
  `Vec`（`dump_all()` 返回整份）。**流式写**（dump 分页 → 边 dump 边写）属 P4c，前置是 P4c0。
- 读 v2（P4b）：`rmp::decode::read_array_len` / `read_bin_len` 逐项解码，按字节预算组装 batch
  （64MiB）→ `restore_stream`（B1：只清空一次 + ≥3 批）。P4a2 只提供整份解码的单测路径。
- 读 v1：保持 `serde_json::from_reader`（升级期一次性路径，P4d 后按发布周期下线）。
- 兼容：`MAGIC` 不匹配 → 视为 v1（旧文件没有头），解析失败报错并保留文件（不删）。

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

- ~~**`dump_all()` 本身是否整份 Vec**~~ → **P4c0 已落地**：`Store::get_prefix_page(prefix, after, budget)`
  （redb 用 `range(after..)` 覆盖，不再物化整份前缀）+ `StateMachine::dump_batches(budget)`；
  与 `dump_all` 拼接等价、空状态零页、每页前进且只允许最后一条越过预算。
  P4c 的写端流式化依赖它（见 §8）。
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

## 8. 实现落地（P4a2，2026-10-29）

- **写端 flag**：`DSH_RAFT_SNAPSHOT_PAYLOAD=msgpack`（`v2`/`2` 同义）→ `build_snapshot` 写
  v2（20B 头 + msgpack body）；未设置/其它 → **无头 JSON**（与 P3 逐字节一致，回滚即关变量）。
  运行期可覆盖：`StateMachineStore::set_msgpack_payload(bool)`（doc-hidden，测试/运维用）。
- **编码**：`rmp::encode::write_array_len` + `write_bin` 逐项写 —— 直接 `rmp_serde` 会把
  `Vec<u8>` 编成 int 序列（体积翻倍），所以不用它。KAT 锁定 body 字节形状：
  `[("k","v")]` → `91 92 c4 01 6b c4 01 76`。
- **解码**：`rmp::decode::read_array_len` / `read_bin_len` + `take(len).read_to_end`
  （不按声明长度预分配，防损坏头 OOM）；单次遍历同时算 CRC 与字节数（`CountingCrcReader`）。
- **校验点**：`read_payload_pairs`（install / boot 重放共用）与 `verify_payload_file`
  （发送前，`http_network::full_snapshot` 调用）→ total_len 不符 / CRC 不符 / 非零 flags /
  未知 version / 头截断 全部 Err 并保留文件。
- **e2e**：`learner_catches_up_via_http_snapshot` 在 leader 打开 msgpack → 断言 learner 落盘
  payload 带 `DSHSNP` 头、version=2、CRC 自洽（v2 真的过了 wire）；voter 用例保持 v1
  （无头 JSON）→ 两种格式都有端到端证据。
- **验收对照（§6）**：体积/install/RSS 三项要等 storm 重跑（P4c 用 `--snapshot-payload=msgpack`
  打开写端后才能测，P4base 是 v1 基线）；「旧 v1 文件可读」由 `default_writer_stays_headerless_json`
  + P4a1 用例覆盖；「多批流式」属于 P4b（`restore_stream`）。

## 9. 实现落地（P4b：流式恢复）

- **`StateMachine::restore_stream`（dsh-core，B1 blocker）**：按批消费，**只在第一批前清空一次**
  （`get_prefix_keys(b"")` + 首批 `write_batch(batch, deletes)`），后续批只插入；空流等价于清空。
  `restore_all` 保留为「切块 → `restore_stream`」，与流式路径共用同一份语义。
  测试：3 批全存活 + 旧 key 被清（逐批清空的旧写法会立刻失败）、空流清空、批错误传播。
- **`read_payload_batches(path, budget)`（dsh-raft）**：返回 `PayloadBatches` 迭代器。
  - v2：打开时先 `verify_payload_file`（total_len + crc32，B2'）并校验文件长度，再**惰性**逐项
    `rmp::decode`，按 64MiB 预算切批（批因最后一条略超预算；单条超预算单独成批）。
  - v1（无头/带头 JSON）：整份解析后切批 —— 升级期一次性路径，设计 §3 已声明。
  - 完整性校验**先于**状态机写入：坏 payload 在 copy/marker 之前失败（
    `install_rejects_corrupt_v2_before_state_change`）。
- **install / boot 重放都改走流式**：`install_snapshot` 打开批次 → copy canonical → marker →
  `restore_stream`；`recover_interrupted_restore` 同样按批重放（boot 内存不再随 payload 线性增长，
  回应 restore 设计 §8 残留 (b)）。
- **验收口径**：≥3 批多批测试（`multi_batch_restore_realistic_size`，`DSH_TEST_LARGE_SNAPSHOT=1`
  时为 3×45MiB >128MiB，本地实测通过并留证）；批错误 → restore Err → 调用方 poison + marker 兜底
  （P4-A 的 fail-stop 语义不变）。
- **尚未做（P4c）**：写端仍整份 `dump_all()` → `Vec`（`restore_stream` 只解决读端）；`dump_all`
  分页属 P4c0。

## 10. 实现落地（P4c0：只读分页 dump）

- `Store::get_prefix_page(prefix, after, budget)`：默认实现基于 `get_prefix`（内存后端），
  redb 覆盖为 `range(after..)` 分页扫描（只读、无整份物化）；`after` 独占、前缀边界即停、
  至少前进一条。
- `StateMachine::dump_batches(budget)`：与 `dump_all` 同序同内容的**分页**版本；空状态零页；
  游标 = 上一页最后一条 key（严格递增）。前置假设与 `dump_all` 相同（持有读锁 → 无 pending_ops）。
- 测试：拼接 == `dump_all`（4 档预算）、空状态零页、每页前进/预算、redb 分页 == 整份扫描
  （跨前缀边界 + 游标严格递增）。
- 尚未做（P4c）：`build_snapshot` 仍 `dump_all()` 整份 + 整份编码；P4c 改为
  「dump_batches → 逐批 encode → seek-back 写 header(含 total_len/crc)」。

## 11. 实现落地（P4c：写端流式 msgpack）

- **`build_snapshot`（v2 路径）不再整份驻留**：`count_prefix_keys`（msgpack 数组头要总数）→
  `dump_batches(64MiB)` 逐页 → 逐项 `rmp::encode::write_bin` 直接写 `.part` →
  `CountingCrcWriter` 边写边算 CRC/长度 → **seek 回 0 回填 20B 头** → fsync → rename。
  头里 `total_len`/`crc32` 仍由创建方写入（B2'），发送前 `verify_payload_file` 复核。
- **一致性/代价**：读锁贯穿「计数 + 流式写」，保证数组条数与内容来自同一状态视图；
  代价是快照构建期间写命令要等一次写盘时间（构建是低频事件）。锁内无 await。
- **v1 路径不变**：默认写端仍是无头 JSON（`serde_json::to_vec(&dump_all())`），回滚即关 flag。
- **测试**：预算 1B 强制多页时，流式写出的字节与「整份编码」**逐字节一致**（同编码/同顺序）、
  头 total_len/crc 正确；写→读→install 闭环；空状态产出合法 `array(0)` payload。
- **体积目标（§6 的 ≤0.75x）已远超**：实测 v1 JSON 386.0MB vs v2 msgpack 95.4MB = **0.25x**
  （原因：serde_json 把 `Vec<u8>` 编成十进制数字数组，每字节 ≈4 字符；msgpack 用 bin）。详见 §12。

## 12. 实测（P4c 12k storm A/B + 350MB 尺度探针，2026-10-29）

**同一命令、同一 release 二进制，只差写端 flag**（`--profile storm --duration 25 --leader-kills 5
--snapshot-writes 12000 --chaos-during-snapshot --catchup-timeout 120 --max-install-seconds 30`）：

| 指标 | v1（默认，无头 JSON） | v2（msgpack） | 说明 |
|---|---|---|---|
| victim 快照 | **386.0MB** | **95.4MB（0.25x）** | JSON 把 `Vec<u8>` 序列化成**十进制数字数组**（每字节 "120," ≈4 字符）→ 4x 膨胀 |
| install（release） | 3.67s（attempts=2，末次 2.70s） | **0.799s**（attempts=1） | 传输量 4x 之差 + 解析 |
| catch-up | 8.33s | 5.62s | |
| victim 峰值 RSS | 386.7MB | 306.2MB | |
| victim 基线 RSS | 131.4MB | 126.5MB | |
| 净增量 delta | 255.3MB | **179.8MB（−30%）** | |
| delta / payload | 0.66x | 1.88x | **比值不可跨格式比较**（见下） |
| 窗口内选举 / 无法解释选举 | 0 / 0 | 0 / 0 | |
| kill 验证 / 换人 / 项目比对 | 5/5 / 5/5 / 8 | 5/5 / 5/5 / 8 | |
| storm 判定 | `ok:true` | 仅 C4 `net 1.88x > 1.2x` 失败 | 见下 |

**关键口径修正（P4 复核请注意）**：`net = delta / snapshot_bytes` 在格式变化后不再可比——
v1 的 `snapshot_bytes` 被 JSON 数字数组放大了 ~4x，而 delta 主要由**解码后的状态**（≈ msgpack 体积）
+ 固定开销决定。绝对内存 v2 少 75MB；比值反而更高，纯属分母变化。格式无关的证据是：

- **350MB 尺度探针**（`large_install_rss_probe`，流式生成 payload、单独进程语义、10ms 采样 VmRSS）：
  payload **349MiB**、install 6.99s（debug；release 见上表）、baseline 11.4MiB、peak 169.6MiB、
  delta 154MiB → **net 0.44x / raw 0.47x**（目标 net ≤1.2x、raw ≤2.0x 均大幅达标）。
- 绝对 delta：v2 179.8MB < v1 255.3MB（−30%）。

**建议 P4 gate 采纳的口径**：RSS 以「净增量 / **逻辑状态字节**」为准（逻辑状态可用 victim 落盘
redb 尺寸或 msgpack payload 近似），不再用 v1-JSON payload 当分母；或直接改用绝对增量 + 探针曲线。
