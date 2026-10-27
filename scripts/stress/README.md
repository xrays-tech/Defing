# Leader Storm — 集群压力测试 (leader churn + 高并发读写 + snapshot 追赶)

高强度、非常规 harness 对 `defing` 集群。纯 Python 3.8+ 标准库（`urllib` + `threading`，无第三方
依赖），进程全部通过 `subprocess.Popen(start_new_session=True)`，端口/临时目录动态（socket bind 0），
因此与 `chaos-test.sh` / `bench.sh` / `cluster-demo.sh` 的固定端口/固定 tmp 目录不会冲突。

## 适用场景
- 验证**频繁 leader 变更**（leader roulette: 每 ~0.8–2.5s 随机 SIGKILL 当前 leader → 重新选举 →
  继续写入，重启复用同一 data-dir/端口，旧 session token 经 Raft 持久化继续复用）
  + follower freeze(SIGSTOP/CONT) + 低概率 follower kill。
- 验证 **snapshot 追赶**：把被重新加入 follower 永久落后（旧配置恒 `413` / `InstallSnapshot RPC timed out`，
  见 dev_docs/deployment-guide.md §6.5, dsh-raft raft.rs: /raft/* body limit 32MiB, chunk 1MiB,
  install_snapshot_timeout 30s）。bulk writes 超过 openraft `snapshot_policy since_last` 阈值 → 被-restart
  follower 必须追平走 InstallSnapshot，断言 `catch_up_seconds < --catchup-timeout`。

## 运行
```bash
# 1) 先构建（可跳过，wrapper auto-builds 时缺）
cd server && cargo build --bin defing && cd ..

# 2) 冒烟（CI-friendly, snapshot 阶段跳过）
scripts/stress-leader-storm.sh --profile quick --bin server/target/debug/defing

# 默认 storm / 极限 extreme（资源 heavy）
scripts/stress-leader-storm.sh                 # == --profile storm
scripts/stress-leader-storm.sh --profile extreme --keep
```

## Profiles（所有参数可独立 override 通过 CLI flag）
| profile | nodes | duration | workers | leader_kills | snapshot_writes |
|---|---|---|---|---|---|
| quick   | 5 | 8s   | 6  | 3  | 0 (skip snapshot) |
| storm   | 5 | 60s  | 12 | 12 | 6500 |
| extreme | 5 | 180s | 24 | 30 | 8000 |

Flags: `--profile quick|storm|extreme` `--bin PATH` `--keep` `--nodes` `--workers` `--duration`
`--leader-kills` `--snapshot-writes` `--value-bytes` (默认 60000，上限 MAX_VALUE_BYTES=64KiB) `--seed`
`--catchup-timeout` (默认 60s)。固定 `--seed` → 可复现。

## 退出码
- `0` 全部 invariant 通过。
- `1` 任一 invariant / 日志扫描 / snapshot 追赶 timeout 失败。
- `2` 启动失败（binary 非执行 / 集群 boot / healthz / login 失败）。

## 最终 invariant（任一失败 → exit 1）
1. 每个项目：leader config `version >= worker 记录的 acked_version`；leader 最终 value 的 `|W{wid}#S{seq}#`
   owner 校验：wid == 该 writer 且 `seq <= max_attempted_seq`（防串数据；允许未确认-but-applied write 时
   response lost 但实际 applied — 幂otent request_id 语义）。
2. 每个节点（incl. 重启节点）：同项目 config `version == leader version`, marker == leader marker（节点间最终一致）。
3. 集群：恰好 one leader（所有节点 `current_leader` 一致）；5 voter；无 node down。
4. 复制 lag 收敛：从 **leader** 的 replication view，每个 live voter 的 `lag` 必须 be a numeric `0`;
   `lag == null` 对 a non-leader voter 是 a FAILURE（replication not built / InstallSnapshot not
   applied yet，not caught up）. leader 自身 entry lag 恒 null by design → tolerated.
5. 日志扫描（**this run's** log files, case-insensitive）：no `panic` / `fatal` / `length limit exceeded`
   / `installsnapshot rpc timed out` (concrete 413 / snapshot-timeout bug signatures; bare "413" not
   scanned — collides with raft log indices like `T2-N4-413`).
6. reader per `(project,node)` 单调性：version seq 不能倒退。
7. snapshot 阶段 (storm/extreme only): victim catch-up `catch_up_seconds < --catchup-timeout`; not
   "length limit exceeded"/"installsnapshot rpc timed out"; interrupted-by-election 诊断 (see Known issue).

## 看 lag
读 leader 的 `/api/v1/cluster/members`（需 `Authorization: Bearer <token>`）→ `members[].lag`。
lag 语义（dsh-observability `cluster_members_json` / `replication_lag`）:

- `lag` 是 only meaningful when read from the CURRENT **leader**（openraft replication map exists only
  on the leader）; harness always reads members from the leader node.
- `lag == null` on a non-leader voter = matched log index not established on the leader (not in the
  replication view yet / just joined / InstallSnapshot not applied) → **NOT** settled. Final invariant
  requires `lag == 0`, so null is a failure, never silently counted as caught-up.
- `lag == null` on the **leader's own** entry is expected（replication map excludes the leader）→ tolerated.

harness 在 summary 打印 `max_lag`（numeric only; **`null` if no numeric lag was ever observed, never
defaulted to 0**), `snapshot_catchup_seconds`, plus snapshot-only `victim_lag_none_seen` (bool) and
`snapshot_interrupted_by_election` (bool). 失败时打印 victim/leader log tail + `lag_values` 观察序列.

## 已知限制
- 本机资源：extreme 24 workers × 60KiB value + 5 进程 heavy，需可执行 `defing`；**debug binary perf 较低 —
  建议 release** (`server/target/release/defing`, pass via `--bin`) 时资源-tight。
- 5 节点固定 quorum floor：chaos 保证 live ≥ 3（quorum 3/5）；不足时只 freeze/wait。
- 只管理自 spawn 的 Popen 进程（never `pkill defing`），不影响其它 defing 进程。
- quick 不 run snapshot 阶段；storm/extreme run full catch-up（后续 orchestrator 执行）。
- 写请求经 non-leader 返回 428 ERR_LEADER_REDIRECT 自动 forwarded transparently; retry 用 same `request_id`
  (幂otent I10) → 未确认但实际成功写 accepted。

## Known issue: snapshot transfer vs election timeout（NOT a harness false-positive）
storm/extreme snapshot phase → victim catches up over a large log gap, so the leader ships its state via
**InstallSnapshot**. If the total InstallSnapshot duration exceeds the node's **election timeout**, the
follower's election timer fires **mid-transfer** → it campaigns again, interrupting/forcing a retry of
the snapshot. With the dev election timeout (~300–600ms range) the storm large-snapshot phase reproduces
this easily.

The harness reports it as a **diagnosis**, not via `FORBIDDEN`（election-timeout 日志 is otherwise normal
chatter）: when the victim log contains both `election timeout passed` and `received snapshot`/`InstallSnapshot`,
it sets `snapshot_interrupted_by_election: true` and appends:

`snapshot transfer interrupted by election timeout (victim repeatedly campaigned during InstallSnapshot); see node N log`

This is a real cluster-robustness signal, not a harness misreport. Fix directions（product-side, not the
harness）: raise the election timeout, or keep the election timer renewed/heartbeat during snapshot chunk
delivery so InstallSnapshot can complete across chunks. Do NOT lower product config just to make storm green.

## 实现 files
- `scripts/stress/leader_storm.py` — main harness。
- `scripts/stress-leader-storm.sh` — thin wrapper（check/build binary then exec python）。
- `scripts/stress/README.md` — 本 file。
