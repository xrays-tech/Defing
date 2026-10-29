#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""leader_storm — 集群压力测试 harness (高强度、非常规).

Focus: 频繁 leader 变更 (leader roulette) + 高并发读写 + snapshot 追赶.
The snapshot stage reproduces the fixed "follower 永久落后" scenario
(dev_docs/deployment-guide.md §6.5, dsh-raft raft.rs: /raft/* body limit 32MiB,
chunk 1MiB, install_snapshot_timeout 30s). 超过 openraft snapshot_policy
since_last 阈值 → 被重新加入 follower 追平必须走 InstallSnapshot, 旧
配置恒 413 / InstallSnapshot RPC timed out.

Pure Python 3.8+ stdlib (urllib, threading). Exit codes:
  0  全绿
  1  任一 invariant / 日志扫描 / 追赶 timeout 失败
  2  启动失败 (二进制/集群 boot/login 失败)

Usage (thin wrapper scripts/stress-leader-storm.sh):
  leader_storm.py [--profile quick|storm|extreme] [--bin PATH] [--keep]
                  [--nodes N] [--workers W] [--duration S] [--leader-kills N]
                  [--snapshot-writes N] [--value-bytes N] [--seed N]
                  [--catchup-timeout S] [--chaos-during-snapshot]

--chaos-during-snapshot: churn 并发 — 1-2 leader SIGKILL/SIGSTOP-freeze 事件注入 WHILE the
catch-up victim is behind; invariant: victim campaigns 0x, lag still → numeric 0.

Default profile = storm.
"""

import argparse
import json
import os
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
import urllib.error
import urllib.request
from datetime import datetime, timedelta


# module-level shared state (set in main(); helpers reference one instance)
from typing import Any  # noqa: E402
CLUSTER: Any = None
STATS: Any = None
STATS_LOCK: Any = None
SESSION_TOKEN: Any = None
START_TS: float = 0.0
SNAPSHOT_PROJECTS: list = []
READERS: list = []


# ---------------------------------------------------------------- constants / profiles

ADMIN_PASSWORD = "stress-pass"
JOIN_TOKEN = "stress-token"
RAFT_TOKEN = "stress-token"
BRANCH = "dev"
MAX_VALUE_BYTES = 64 * 1024  # == dsh-core limits.MAX_VALUE_BYTES

PROFILES = {
    # nodes, duration, workers, leader_kills, snapshot_writes
    "quick":   dict(nodes=5, duration=8,   workers=6,  leader_kills=3,  snapshot_writes=0),
    "storm":   dict(nodes=5, duration=60,  workers=12, leader_kills=12, snapshot_writes=6500),
    "extreme": dict(nodes=5, duration=180, workers=24, leader_kills=30, snapshot_writes=8000),
}

# 日志扫描禁止 token (大小写不敏感, 本次 log files only)
# Concrete failure signatures for the snapshot-body-limit / RPC-timeout bug.
# A bare "413" substring collides with raft log indices (T2-N4-413 etc), so we match the
# strings the components actually emit:
#   - "length limit exceeded"   = axum DefaultBodyLimit 413 on /raft/* (raft_http_server.rs)
#   - "installsnapshot rpc timed out" = openraft 0.9 InstallSnapshot timeout (0.9-era note)
# plus hard-crash markers panic/fatal.
# 0.10 additions (streaming/msgpack stack — emitted ONLY on failure by dsh-raft):
#   - "msgpack decode"/"msgpack encode"  = raft rpc / snapshot frame codec failure
#     (raft_http_server.rs "raft rpc msgpack decode failed", snapshot_stream.rs proto_err)
#   - "crc mismatch"       = full-snapshot stream end-crc guard fired (wire corruption)
#   - "frame too large"    = snapshot_stream FrameDecoder MAX_FRAME_BYTES guard
#   - "snapshot too large" = snapshot_stream check_total MAX_SNAPSHOT_BYTES budget guard
#   - "/raft/full_snapshot" = 0.9 JSON InstallSnapshot *wire route* — must be gone on 0.10
#     (0.10 route is /raft/full-snapshot with hyphen). Scope the token to the route: openraft
#     0.10 INFO logs legitimately contain "Raft::install_full_snapshot()",
#     "handle_install_full_snapshot" and "finished sending full_snapshot", so a bare
#     "full_snapshot" token false-positives on a perfectly healthy install.
#   - "soft_ttl"           = 0.9-era Config field name — must be gone on 0.10 (0.10 wires
#     RPCOption budgets in-network, never logs this token)
FORBIDDEN = (
    "panic",
    "fatal",
    "length limit exceeded",
    "installsnapshot rpc timed out",
    "msgpack decode",
    "msgpack encode",
    "crc mismatch",
    "frame too large",
    "snapshot too large",
    "/raft/full_snapshot",
    "soft_ttl",
)

# 0.10 openraft election-start markers (INFO level, raft_core.rs: "trigger election"
# line 2238, "a quorum would grant the vote; starting a real election"; 0.9-era
# "election timeout passed" kept for back-compat diagnosis). Used for
# interrupted-by-election + victim-0-election assertions (NOT a FORBIDDEN token —
# elections are normal chatter elsewhere).
ELECTION_MARKERS = (
    # 0.10（openraft raft_core.rs:2208/2235/2238，均 INFO）
    "election timeout expired, triggering election",
    "trigger pre-vote",
    "trigger election",
    # 0.9 术语，保留兼容
    "starting a real election",
    "election timeout passed",
)

# login/healthz 免 Bearer; 其余 /api/v1 需 Bearer
# data-plane /v1/... 需 session 或 project token (admin session 覆盖全项目 → 可读 config)


# 启动参数断点: dsh-cli main.rs prints the EFFECTIVE Raft config once per node startup
# ("raft config: heartbeat_interval=... install_snapshot_timeout=... ..."), and the harness
# parses + asserts it equals the dsh-raft raft.rs dev_config() expectations below.
# Any "heartbeat 100ms / InstallSnapshot 30s" conclusion therefore maps to the config that
# ACTUALLY ran (stale binary / drifted config → exit 2, no silent green run).
# cluster_name is NOT overridden by dev_config() → openraft Config default "foo".
# 0.10 superset: dev_config also pins enable_pre_vote=Some(true) + enable_tick=true
# (P3a-C1 pre_vote before append); booleans compare as lowercase "true" strings.
EXPECTED_RAFT_CONFIG = {
    "heartbeat_interval": 100,
    "election_timeout_min": 300,
    "election_timeout_max": 600,
    "max_payload_entries": 64,
    "install_snapshot_timeout": 30_000,      # ms
    "snapshot_max_chunk_size": 1024 * 1024,  # 1 MiB raw
    "cluster_name": "foo",
    "enable_pre_vote": "true",
    "enable_tick": "true",
}


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------- free port (socket bind 0)

_alloc_lock = threading.Lock()
_taken = set()


def free_port():
    """Bind port 0 to obtain an OS-assigned free port, never reused during the run.

    独立端口 per node (http/raft/grpc), 避免 chaos-test/bench/cluster-demo 固定端口冲突.
    """
    while True:
        with _alloc_lock:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            s.close()
            if port not in _taken:
                _taken.add(port)
                return port


# ---------------------------------------------------------------- HTTP helper (urllib)

# Never auto-follow redirects; caller handles 428/leader redirect explicitly.


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(NoRedirect)


def http_json(base, token, method, path, body=None, timeout=10.0):
    """POST/PUT/GET JSON to node base. Returns dict {status, data, err}.

    Never raises; transient errors surface as status=None/err so writers retry.
    """
    url = base.rstrip("/") + path
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = _OPENER.open(req, timeout=timeout)
        raw = resp.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        return {"status": resp.status, "data": parsed}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        return {"status": e.code, "data": parsed}
    except Exception as e:  # URLError / timeout / connection reset (node killed mid-flight)
        return {"status": None, "err": repr(e)}


def http_get(base, token, path, timeout=10.0):
    return http_json(base, token, "GET", path, None, timeout)


def members(base, token=None):
    # /api/v1/cluster/members needs Bearer: default to the global session token
    if token is None:
        token = SESSION_TOKEN
    r = http_get(base, token, "/api/v1/cluster/members")
    if r["status"] == 200 and isinstance(r["data"], dict):
        return r["data"]
    return None


GLOBAL = {}


def repo_root(_bin_path=None):
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def git_status(_bin_path=None):
    try:
        out = subprocess.check_output(
            ["git", "-C", repo_root(), "status", "--porcelain"],
            stderr=subprocess.DEVNULL, timeout=5,
        )
        return out.decode().strip()
    except Exception:
        return None


def source_newest_mtime(root):
    newest = 0.0
    base = os.path.join(root, "server")
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if d not in ("target", ".git")]
        for fn in filenames:
            if fn.endswith(".rs") or fn == "Cargo.toml":
                try:
                    newest = max(newest, os.path.getmtime(os.path.join(dirpath, fn)))
                except OSError:
                    pass
    return newest


def git_rev(bin_path):
    """worktree 的 git HEAD（能定位到被测源码版本）。"""
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out = subprocess.check_output(
            ["git", "-C", root, "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, timeout=5
        )
        return out.decode().strip()
    except Exception:
        return None


def sha256_of(path, limit=None):
    """二进制 sha256（>512MB 时只算前 limit 字节并注明）。"""
    import hashlib

    try:
        size = os.path.getsize(path)
        h = hashlib.sha256()
        with open(path, "rb") as f:
            remain = size if limit is None else min(size, limit)
            while remain > 0:
                chunk = f.read(min(1 << 20, remain))
                if not chunk:
                    break
                h.update(chunk)
                remain -= len(chunk)
        return h.hexdigest() if limit is None else "%s(prefix-%d)" % (h.hexdigest(), limit)
    except OSError:
        return None


def healthz(base):
    r = http_get(base, None, "/healthz", timeout=4.0)
    return r["status"] == 200


def read_rss_kb(node):
    """当前 RSS（VmRSS）。install 窗口内采样用——VmHWM 是整段进程生命周期的峰值，
    不能证明「安装窗口内」的内存；复核 major 指出过这点。"""
    try:
        pid = node.proc.pid if node and node.proc else None
    except AttributeError:
        pid = None
    if not pid:
        return None
    try:
        with open("/proc/%d/status" % pid, "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


def read_peak_rss_kb(node):
    """Victim 进程的峰值 RSS（VmHWM，内核高水位）—— P3 C4 的 install RSS 证据。

    victim 在 snapshot 阶段是 restart 后的新进程，VmHWM 覆盖的正是 「重启 → install
    full-snapshot → 追平」窗口，可直接当作 install 峰值内存。读不到（进程已退出）→ None。
    """
    try:
        pid = node.proc.pid if node and node.proc else None
    except AttributeError:
        pid = None
    if not pid:
        return None
    try:
        with open("/proc/%d/status" % pid, "r") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


def latest_snapshot_bytes(node):
    """victim 落盘的最新快照字节数（{data_dir}/snapshots/*.snap 中最大者）——C4 规模证据。"""
    try:
        snap_dir = os.path.join(node.data_dir, "snapshots")
        sizes = [
            os.path.getsize(os.path.join(snap_dir, f))
            for f in os.listdir(snap_dir)
            if f.endswith(".snap")
        ]
    except OSError:
        return None
    return max(sizes) if sizes else None


_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)")


def _line_ts(line):
    """从 tracing 日志行首解析 RFC3339 时间戳。"""
    m = _TS_RE.match(line)
    if not m:
        return None
    try:
        return datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
    except ValueError:
        return None


def snapshot_bytes_mtime(node):
    """最新快照文件的 mtime（epoch 秒）——用于确认它属于本轮追赶，而不是历史残留。"""
    try:
        snap_dir = os.path.join(node.data_dir, "snapshots")
        snaps = [
            os.path.getmtime(os.path.join(snap_dir, f))
            for f in os.listdir(snap_dir)
            if f.endswith(".snap")
        ]
    except OSError:
        return None
    return max(snaps) if snaps else None


def snapshot_episode(node, offset=0):
    """解析 victim 日志，返回「快照追赶 episode」——**第一次 transfer 到最终 done，含 retry**。

    为什么必须含 retry：churn 打断传输后 harness 会重试，openraft 只在最后一次 install 完成时
    打印 `Done install complete snapshot`。只取「最后一个完整窗口」会把被打断的那次（以及发生在
    其中的选举）整段丢掉 —— 复核 blocker 正是靠这个把真实 campaign 藏起来。

    返回 dict：
      transfer_ts / done_ts   episode 起止（datetime），缺任一则 None
      attempts                transfer 出现次数（>=2 表示有 retry）
      install_ts              最后一次 install 标记时间
      episode_seconds         端到端（transfer→done，含 transfer+install+apply+retry 间隔）
      last_attempt_seconds    最后一次完整窗口（transfer→done）
    """
    try:
        with open(node.log_path, "r", errors="replace") as f:
            if offset:
                f.seek(offset)
            lines = f.readlines()
    except OSError:
        return {"transfer_ts": None, "done_ts": None, "attempts": 0, "install_ts": None,
                "episode_seconds": None, "last_attempt_seconds": None}
    first_transfer = None
    last_done = None
    last_install = None
    last_attempt_start = None
    attempts = 0
    for ln in lines:
        low = ln.lower()
        ts = _line_ts(ln)
        if ts is None:
            continue
        if "full-snapshot transfer start" in low:
            attempts += 1
            if first_transfer is None:
                first_transfer = ts
            last_attempt_start = ts
        elif "install full snapshot," in low:
            last_install = ts
        elif "done install complete snapshot" in low and last_attempt_start is not None:
            last_done = ts
    if first_transfer is None or last_done is None:
        return {"transfer_ts": first_transfer, "done_ts": last_done, "attempts": attempts,
                "install_ts": last_install, "episode_seconds": None,
                "last_attempt_seconds": None}
    return {
        "transfer_ts": first_transfer,
        "done_ts": last_done,
        "attempts": attempts,
        "install_ts": last_install,
        "episode_seconds": round((last_done - first_transfer).total_seconds(), 3),
        "last_attempt_seconds": (
            round((last_done - last_attempt_start).total_seconds(), 3)
            if last_attempt_start is not None else None
        ),
    }


# 同一次竞选会连续打印多行 marker（0.10：`election timeout expired, triggering election`
# + `trigger pre-vote` + `trigger election`）。按时间聚类成「事件」，避免按行计数夸大。
ELECTION_EVENT_GAP = timedelta(milliseconds=300)


def election_events(node, offset=0):
    """victim 重启后的竞选**事件**时间列表（marker 行按 300ms 聚类）。"""
    try:
        with open(node.log_path, "r", errors="replace") as f:
            if offset:
                f.seek(offset)
            lines = f.readlines()
    except OSError:
        return []
    ts_list = []
    for ln in lines:
        low = ln.lower()
        if not any(tok in low for tok in ELECTION_MARKERS):
            continue
        ts = _line_ts(ln)
        if ts is not None:
            ts_list.append(ts)
    ts_list.sort()
    events = []
    for ts in ts_list:
        if not events or (ts - events[-1]) > ELECTION_EVENT_GAP:
            events.append(ts)
    return events


def classify_elections(episode, events, chaos_windows, grace_ms=600):
    """把竞选事件分成「episode 内」/「churn 解释得了的」/「无法解释的」。

    - in_episode：落在 [transfer, done] 内（含被打断的 retry 段）
    - during_chaos：落在某次 kill/freeze 的 [at, resumed_at + grace] 内 —— leader 不可达期间
      follower 竞选是 Raft 的正常行为，不算失败；grace 覆盖「恢复后 follower 还没收到心跳」的窗口。
    - unexplained：in_episode 且不属于任何 churn 窗口 → 0.9 的失败模式（install 期间心跳被抑制），
      必须为 0。

    `chaos_windows[*].at / resumed_at` 允许是 epoch 秒（harness 用 time.time()）或 datetime。
    """

    def _epoch(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        return v.timestamp()

    t0, t1 = _epoch(episode.get("transfer_ts")), _epoch(episode.get("done_ts"))
    in_ep, during_chaos, unexplained = [], [], []
    for ts in events:
        e = _epoch(ts)
        if t0 is None or t1 is None or not (t0 <= e <= t1):
            continue
        in_ep.append(ts)
        explained = False
        for w in chaos_windows:
            ws, we = _epoch(w.get("at")), _epoch(w.get("resumed_at"))
            if ws is None:
                continue
            if we is None:
                we = ws
            if ws <= e <= we + grace_ms / 1000.0:
                explained = True
                break
        (during_chaos if explained else unexplained).append(ts)
    return {
        "in_episode": len(in_ep),
        "during_chaos": len(during_chaos),
        "unexplained": len(unexplained),
        "since_restart": len(events),
    }


# ---------------------------------------------------------------- Node model


class Node:
    """Owns one defing process. Restart reuses same data-dir + argv (auto-rejoin)."""

    def __init__(self, node_id, bin_path, data_dir, log_path):
        self.node_id = node_id
        self.bin = bin_path
        self.data_dir = data_dir
        self.log_path = log_path
        self.http_addr = "127.0.0.1:%d" % free_port()
        self.raft_addr = "127.0.0.1:%d" % free_port()
        self.grpc_addr = "127.0.0.1:%d" % free_port()
        self.proc = None
        self.down = True            # True while not running (killed / not yet started)
        self.frozen = False         # SIGSTOP'd follower (still "live" for quorum counting)
        self.log = open(log_path, "ab", buffering=0)

    def http_base(self):
        return "http://%s" % self.http_addr

    def argv(self, seed_map):
        return [
            self.bin,
            "--node-id", str(self.node_id),
            "--http-addr", self.http_addr,
            "--raft-addr", self.raft_addr,
            "--grpc-addr", self.grpc_addr,
            "--data-dir", self.data_dir,
            "--bootstrap-peers", seed_map,
            "--admin-password", ADMIN_PASSWORD,
            "--allow-no-master-key",
            "--join-token", JOIN_TOKEN,
            "--raft-token", RAFT_TOKEN,
        ]

    def spawn(self, seed_map):
        """Start / restart. Log appends (restart → same file). start_new_session so
        we own a fresh process group and never touch foreign defing processes."""
        self.log = open(self.log_path, "ab", buffering=0)
        self.proc = subprocess.Popen(
            self.argv(seed_map),
            stdout=self.log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        self.down = False
        self.frozen = False

    def kill(self):
        """SIGKILL own process; wait for exit (port release before restart)."""
        if self.proc:
            try:
                self.proc.send_signal(signal.SIGKILL)
                self.proc.wait(timeout=10)
            except Exception:
                pass
        self.down = True
        self.frozen = False

    def stop_freeze(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.send_signal(signal.SIGSTOP)
                self.frozen = True
            except Exception:
                pass

    def resume_freeze(self):
        if self.proc and self.frozen:
            try:
                self.proc.send_signal(signal.SIGCONT)
                self.frozen = False
            except Exception:
                pass

    def alive(self):
        return self.proc is not None and self.proc.poll() is None and not self.down

    def tail(self, n=40):
        try:
            with open(self.log_path, "rb") as f:
                return f.read().decode("utf-8", "replace").splitlines()[-n:]
        except Exception:
            return []

    def close_log(self):
        try:
            self.log.close()
        except Exception:
            pass


# ---------------------------------------------------------------- Cluster


class Cluster:
    def __init__(self, nodes, seed_map):
        self.nodes = nodes            # list[Node], index == node_id-1
        self.seed_map = seed_map
        self.lock = threading.Lock()  # guards process lifecycle vs chaos/cleanup races

    def by_id(self, node_id):
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        return None

    def live(self):
        return [n for n in self.nodes if n.alive()]

    def live_count(self):
        return len(self.live())


def cluster_leader_id(node):
    """Read /cluster/members (from a live node); return (leader_id, data).

    Any live node exposes current_leader (openraft Metrics.current_leader). Returns
    (None, None) when node unreachable.
    """
    data = members(node.http_base(), None)
    if data is None:
        return (None, None)
    return (data.get("current_leader"), data)


def wait_leader(timeout):
    """Poll until some live node reports a non-null current_leader. Returns node_id."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        for n in CLUSTER.live():
            lid, _ = cluster_leader_id(n)
            if lid is not None:
                return lid
        time.sleep(0.2)
    return None


def wait_healthz(node, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if healthz(node.http_base()):
            return True
        time.sleep(0.2)
    return False


# ---------------------------------------------------------------- raft config assertion (启动参数断点)


def raft_config_from_log(node):
    """Parse the LAST "raft config:" line in the node log (restart/rejoin re-emits it).

    dsh-cli main.rs logs the effective openraft Config after Raft::new wiring:
      raft config: heartbeat_interval=100 election_timeout_min=300 ... cluster_name=foo
    Returns dict[str,str] or None (missing line = pre-断点 binary → stale binary signal).
    """
    found = None
    try:
        with open(node.log_path, "rb") as f:
            for line in f.read().decode("utf-8", "replace").splitlines():
                if "raft config:" in line:
                    found = line
    except OSError:
        return None
    if found is None:
        return None
    kv = {}
    for part in found.split("raft config:", 1)[1].split():
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k] = v
    return kv if kv else None


def assert_raft_config(nodes):
    """Every node's log must carry a raft-config line matching dev_config() exactly.

    Mismatch or missing line → caller exits 2 (启动失败 class): never validate semantics
    under an unknown/unverifiable Raft configuration.
    """
    ok = True
    for n in nodes:
        got = raft_config_from_log(n)
        if got is None:
            log("FAIL: node %d log has no 'raft config:' line "
                "(binary predates the config 断点 or logged elsewhere — rebuild)" % n.node_id)
            ok = False
            continue
        bad = []
        for k, exp in EXPECTED_RAFT_CONFIG.items():
            v = got.get(k)
            if v is None:
                bad.append("%s missing" % k)
            elif str(exp) != v:
                bad.append("%s=%s != expected %s" % (k, v, exp))
        if bad:
            log("FAIL: node %d raft config mismatch: %s (expected %s)"
                % (n.node_id, ", ".join(bad), EXPECTED_RAFT_CONFIG))
            ok = False
        else:
            log("  raft config断点 ok node %d: hb=%s election=%s..%s payload=%s "
                "install_snapshot_timeout=%s chunk=%s cluster_name=%s"
                % (n.node_id, got["heartbeat_interval"],
                   got["election_timeout_min"], got["election_timeout_max"],
                   got["max_payload_entries"], got["install_snapshot_timeout"],
                   got["snapshot_max_chunk_size"], got["cluster_name"]))
    return ok


# ---------------------------------------------------------------- stats (thread-safe)


class Stats:
    def __init__(self, nodes):
        self.lock = threading.Lock()
        self.attempts = 0
        self.ok = 0
        self.retries = 0
        self.transient = 0
        self.publish_ok = 0
        self.leader_kills = 0
        self.follower_kills = 0
        self.freeze_count = 0
        self.observed_leader_changes = 0
        self.snapshot_chaos_injections = 0
        # C3 证据：kill 目标身份校验 + leader 变化观测
        self.leader_kill_targets_verified = 0
        self.leader_kill_target_not_leader = 0
        self.leader_change_missing = 0
        self.per_node = {n.node_id: 0 for n in nodes}

    def bump(self, attr, node_id=None, n=1):
        with self.lock:
            setattr(self, attr, getattr(self, attr) + n)
            if node_id is not None and node_id in self.per_node:
                self.per_node[node_id] += n


# ---------------------------------------------------------------- request primitives


def write_draft(node, token, project, value_bytes, marker):
    val = ("x" * value_bytes) + marker
    body = {
        "updates": [{"group": "g", "key": "k", "value": {"type": "string", "str_value": val}}]
    }
    return http_json(node.http_base(), token, "PUT",
                     "/api/v1/projects/%s/branches/%s/draft" % (project, BRANCH), body,
                     timeout=15.0)


def write_publish(node, token, project, comment, request_id):
    body = {"comment": comment, "request_id": request_id}
    return http_json(node.http_base(), token, "POST",
                     "/api/v1/projects/%s/branches/%s/publish" % (project, BRANCH), body,
                     timeout=15.0)


def read_config(node, token, project):
    """GET data-plane config json; parse (version, marker) from rendered groups.

    同一节点 (format=json) 的 config version 单调性 checked by caller (monotonic map).
    Returns dict or None on transient error.
    """
    r = http_get(node.http_base(), token,
                 "/v1/projects/%s/branches/%s/config?format=json" % (project, BRANCH),
                 timeout=10.0)
    if r["status"] != 200:
        return None
    data = r["data"]
    if not isinstance(data, dict):
        return None
    # 数据面 render (dsh-render Renderer→plain_tree) 顶层 = group→{key:value}，
    # 旧 harness 期望 {"groups": ...} 外壳（管理面 shape）→ 顶层 shape 时 val 恒 None
    # → catch-up gate 永不 settle (harness parse bug, not product). Accept both shapes.
    groups = data.get("groups") or data
    val = None
    gv = groups.get("g")
    if isinstance(gv, dict):
        val = gv.get("k")
    if val is None:
        return None
    return {"groups": groups, "value": val}


def mgmt_config(node, token, project):
    """Management-plane GET /api/v1/.../config → {version, value}. Used for final checks
    (data-plane /v1 render carries no version; string values are not masked)."""
    r = http_get(node.http_base(), token,
                 "/api/v1/projects/%s/branches/%s/config" % (project, BRANCH), timeout=10.0)
    if r["status"] != 200 or not isinstance(r.get("data"), dict):
        return None
    groups = r["data"].get("groups") or {}
    val = None
    gv = groups.get("g")
    if isinstance(gv, dict):
        val = gv.get("k")
    return {"version": r["data"].get("version"), "value": val}



# ---------------------------------------------------------------- project prebuild


def ensure_project(token, project, deadline=None):
    """Create project + structure (idempotent; publish via leader). Used at setup."""
    # create project (POST /projects idempotent-ish: existing returns error 4xx — ignore)
    for n in CLUSTER.live():
        r = http_json(n.http_base(), token, "POST", "/api/v1/projects", {"name": project})
        if r["status"] in (200, 201) or (r["status"] is not None and r["status"] < 500):
            break
    # structure draft + publish (retry until some node 200 within deadline)
    while deadline is None or time.time() < deadline:
        for n in CLUSTER.live():
            http_json(n.http_base(), token, "PUT",
                      "/api/v1/projects/%s/structure-draft" % project,
                      {"base_version": 1, "groups": [
                          {"name": "g", "items": [{"key": "k", "type": "string", "required": True}]}]},
                      timeout=15.0)
            r = http_json(n.http_base(), token, "POST",
                          "/api/v1/projects/%s/structure-draft/publish" % project,
                          {"comment": "init", "request_id": "sinit-" + project},
                          timeout=15.0)
            if r["status"] == 200:
                return True
        time.sleep(0.3)
    return False


# ---------------------------------------------------------------- writers


class Writer:
    def __init__(self, wid, project, args, stats, token):
        self.wid = wid
        self.project = project
        self.args = args
        self.stats = stats
        self.token = token
        self.stop = threading.Event()
        self.acked_version = 0
        self.all_markers = set()
        self.local_seq = 0
        self.max_attempted_seq = -1
        self.t: Any = None

    def run(self, deadline):
        value_bytes = min(self.args.value_bytes, MAX_VALUE_BYTES - 64)
        while not self.stop.is_set() and time.time() < deadline:
            live = CLUSTER.live()
            if not live:
                time.sleep(0.1)
                continue
            seq = self.local_seq
            self.local_seq += 1
            marker = "|W%d#S%d#%s#" % (self.wid, seq, uuid.uuid4().hex[:8])
            rid = "r-%d-%d-%s" % (self.wid, seq, uuid.uuid4().hex[:6])
            self.max_attempted_seq = max(self.max_attempted_seq, seq)
            val_bytes = value_bytes
            ok_pub = None
            attempts_local = 0
            back = 0.05
            # retry loop with the SAME request_id → idempotent (allow unconfirmed-but-applied)
            while True:
                live = CLUSTER.live()
                if not live:
                    time.sleep(0.1)
                    if time.time() >= deadline or self.stop.is_set():
                        break
                    continue
                node = random.choice(live)
                if not self.stop.is_set():
                    self.stats.bump("attempts", node_id=node.node_id)
                d = write_draft(node, self.token, self.project, val_bytes, marker)
                attempts_local += 1
                p = write_publish(node, self.token, self.project, "w%d.%d" % (self.wid, seq), rid)
                if attempts_local > 1:
                    self.stats.bump("retries")
                if p["status"] == 200:
                    ok_pub = p
                    break
                if p["status"] == 428 or (p["status"] is not None and p["status"] >= 500):
                    # leader redirect / 5xx → transient, retry
                    self.stats.bump("transient")
                elif p["status"] is None:
                    self.stats.bump("transient")
                if time.time() >= deadline or self.stop.is_set():
                    break
                time.sleep(back)
                back = min(back * 2, 1.0)
            if ok_pub is not None:
                ver = (ok_pub["data"] or {}).get("version") if isinstance(ok_pub.get("data"), dict) else None
                if ver is not None:
                    self.acked_version = max(self.acked_version, int(ver))
                self.all_markers.add(marker)
                self.stats.bump("publish_ok")

    def owner_ok(self, value):
        # leader value must be one THIS writer generated: its |W{wid}#S{seq}# must match
        # this writer's id and a seq it actually attempted (tolerates unconfirmed-but-applied).
        i = value.find("|W")
        if i < 0:
            return False
        try:
            j = value.find("#S", i)
            if j < 0:
                return False
            wid = int(value[i + 2:j])
            s = value[j + 2:]
            end = s.find("#")
            seq = int(s[:end]) if end >= 0 else int(s)
        except Exception:
            return False
        return wid == self.wid and 0 <= seq <= self.max_attempted_seq


class Reader:
    def __init__(self, rid, projects, args, stats, token):
        self.rid = rid
        self.projects = projects
        self.args = args
        self.stats = stats
        self.token = token
        self.stop = threading.Event()
        # monotonic per (project,node): seq must never regress. last_seq map.
        self.last_seq = {}
        self.violations = []  # (project, node_id, prev, new)
        self.t: Any = None

    def parse_seq(self, value):
        # value ends with |W{wid}#S{seq}#rand#  → extract seq
        if value is None:
            return None
        idx = value.rfind("#S")
        if idx < 0:
            return None
        try:
            s = value[idx + 2:]
            end = s.find("#")
            return int(s[:end]) if end >= 0 else int(s)
        except Exception:
            return None

    def run(self, deadline):
        while not self.stop.is_set() and time.time() < deadline:
            live = CLUSTER.live()
            if not live:
                time.sleep(0.05)
                continue
            node = random.choice(live)
            project = random.choice(self.projects)
            r = read_config(node, self.token, project)
            if r is None:
                self.stats.bump("transient")
                continue
            seq = self.parse_seq(r.get("value"))
            if seq is None:
                continue
            key = (project, node.node_id)
            prev = self.last_seq.get(key)
            if prev is not None and seq < prev:
                self.violations.append((project, node.node_id, prev, seq))
            self.last_seq[key] = max(prev or 0, seq)


# ---------------------------------------------------------------- chaos (leader roulette core)


class Chaos:
    """Inject leader kills + follower freeze + (rare) follower kill during load.

    Guarantees live_count >= 3 at all times (5-node quorum). Every injection recorded.
    """

    def __init__(self, args, stats, rng):
        self.args = args
        self.stats = stats
        self.rng = rng
        self.stop = threading.Event()
        self.timeline = []  # (t, kind, node_id)
        self.last_leader = None
        self.kills_done = 0
        self.round_idx = 0
        self.leader_kill_count = 0
        self.follower_kill_count = 0
        self.t: Any = None

    def record(self, kind, node_id):
        self.timeline.append((round(time.time() - START_TS, 2), kind, node_id))

    def track_leader_change(self):
        # authoritative leader = the current_leader value the most live nodes agree on
        # (the leader reports itself; followers may lag, so use a majority vote).
        live = CLUSTER.live()
        if not live:
            return
        vals = []
        for n in live:
            lid, _ = cluster_leader_id(n)
            if lid is not None:
                vals.append(lid)
        if not vals:
            return
        lid = max(set(vals), key=vals.count)
        if self.last_leader is not None and lid != self.last_leader:
            STATS.bump("observed_leader_changes")
        self.last_leader = lid

    def leader_roulette(self):
        # honor --leader-kills cap; current leader read from a live node's members view
        if self.leader_kill_count >= self.args.leader_kills:
            return
        live = CLUSTER.live()
        if not live:
            return
        # C3 证据：只杀**自认 leader** 的节点。随机节点报的 current_leader 可能是陈旧视图，
        # 杀掉 follower 不能算 leader-kill 证据；这里直接扫各节点自视图的 is_leader。
        leader = None
        for n in live:
            own = members(n.http_base(), None) or {}
            # is_leader 在 members[] 的元素里（node 自身视角），不在顶层 dict
            entry = next(
                (m for m in own.get("members", [])
                 if str(m.get("node_id")) == str(n.node_id)),
                None,
            )
            if entry and entry.get("is_leader"):
                leader = n
                break
        if leader is None or not leader.alive():
            self.stats.bump("leader_kill_target_not_leader")
            return
        lid = leader.node_id
        if CLUSTER.lock.locked():
            return
        with CLUSTER.lock:
            # need >=4 live before killing so live stays >=3 (5-node quorum floor)
            if CLUSTER.live_count() < 4:
                return
            leader.kill()
        self.leader_kill_count += 1
        self.stats.bump("leader_kills")
        self.stats.bump("leader_kill_targets_verified")
        self.record("leader_kill", leader.node_id)
        # C3：**击杀后、重启前**观测 leader 是否真的换人。被杀节点 0.2–1.2s 后就会以同
        # data-dir 重新上线并可能立刻重新当选，把「换人」掩盖掉（旧实现在 restart 之后才
        # wait_leader，于是 5 次 kill 只记到 1 次变化）。
        changed = False
        deadline = time.time() + 8.0
        while time.time() < deadline:
            nl = wait_leader(1)
            if nl is not None and nl != lid:
                changed = True
                STATS.bump("observed_leader_changes")
                self.last_leader = nl
                self.record("leader_change", nl)
                break
            time.sleep(0.2)
        if not changed:
            STATS.bump("leader_change_missing")
        # restart 0.2–1.2s later with same data-dir/argv
        time.sleep(self.rng.uniform(0.2, 1.2))
        with CLUSTER.lock:
            leader.spawn(CLUSTER.seed_map)
        self.record("leader_restart", leader.node_id)
        wait_healthz(leader, 25)

    def follower_freeze(self):
        live = [n for n in CLUSTER.live() if not n.frozen]
        if CLUSTER.live_count() < 4:  # keep >=3 responsive while one is frozen
            return
        lid = None
        if live:
            lid, _ = cluster_leader_id(random.choice(live))
        cands = [n for n in live if n.node_id != lid]
        if not cands:
            return
        target = random.choice(cands)
        target.stop_freeze()
        self.stats.bump("freeze_count")
        self.record("follower_freeze", target.node_id)
        time.sleep(self.rng.uniform(1.0, 3.0))
        target.resume_freeze()

    def follower_kill(self):
        live = CLUSTER.live()
        if CLUSTER.live_count() < 4:  # live stays >=3 after kill
            return
        lid = None
        if live:
            lid, _ = cluster_leader_id(random.choice(live))
        cands = [n for n in live if n.node_id != lid and not n.frozen]
        if not cands:
            return
        target = random.choice(cands)
        with CLUSTER.lock:
            target.kill()
        self.follower_kill_count += 1
        self.stats.bump("follower_kills")
        self.record("follower_kill", target.node_id)
        time.sleep(self.rng.uniform(1.0, 3.0))
        with CLUSTER.lock:
            target.spawn(CLUSTER.seed_map)
        self.record("follower_restart", target.node_id)
        wait_healthz(target, 25)

    def run(self, deadline):
        # seed the baseline leader so the first re-election is counted as a change
        self.last_leader = wait_leader(5)
        # rotation: leader kills prioritized until leader_kills reached, freeze interleaved,
        # follower_kill every 5th round (low probability)
        while not self.stop.is_set() and time.time() < deadline:
            self.round_idx += 1
            self.track_leader_change()
            if self.round_idx % 5 == 0:
                self.follower_kill()
            elif self.leader_kill_count < self.args.leader_kills:
                # leader roulette until cap hit; else freeze
                self.leader_roulette()
                self.track_leader_change()
                if time.time() < deadline and self.leader_kill_count >= self.args.leader_kills:
                    self.follower_freeze()
            else:
                self.follower_freeze()
            # random interval 0.8–2.5s between injections
            time.sleep(self.rng.uniform(0.8, 2.5))


# ---------------------------------------------------------------- snapshot catch-up phase


def _sample_lag_points(points, max_pts=100):
    """截断/采样 lag 轨迹 for summary+JSON: head 10 + tail 40 exact, middle stride-sampled."""
    if len(points) <= max_pts:
        return list(points)
    head, tail = points[:10], points[-40:]
    mid = points[10:-40]
    room = max(1, max_pts - len(head) - len(tail))
    step = max(1, len(mid) // room)
    return head + mid[::step][:room] + tail


def describe_lag_trend(points):
    """Classify the lag time series: constant-stuck (活锁) vs slow descent vs divergence."""
    if not points:
        return "no numeric lag observations"
    lags = [p[1] for p in points]
    first, last = lags[0], lags[-1]
    span = points[-1][0] - points[0][0]
    if len(set(lags)) == 1:
        return ("constant at %d across %.1fs (恒定不动 — 活锁: InstallSnapshot 未推进 matched index)"
                % (first, span))
    if last < first:
        rate = (first - last) / span if span > 0 else float("inf")
        return ("decreasing %d → %d over %.1fs (缓慢下降 ~%.1f entries/s — 追赶但未 settled)"
                % (first, last, span, rate))
    return ("increasing/diverging %d → %d over %.1fs (追-up 落后 grow)" % (first, last, span))


def snapshot_phase(args, stats, token, rng):
    """非常规重点: kill a follower (keep down) → bulk writes to exceed snapshot_policy →
    restart follower → it must catch up via InstallSnapshot (not 413 / timed out).
    """
    log("== snapshot 追赶阶段 (snapshot_writes=%d) ==" % args.snapshot_writes)
    # 1. pick a live non-leader follower, SIGKILL, keep down
    live = CLUSTER.live()
    if not live:
        return None, None
    lid, _ = cluster_leader_id(random.choice(live))
    cands = [n for n in live if n.node_id != lid] or live
    victim = random.choice(cands)
    with CLUSTER.lock:
        victim.kill()
    victim_id = victim.node_id
    log("  killed follower node %d (kept down during bulk writes)" % victim_id)

    # baseline leader last_log_index
    leader = CLUSTER.by_id(lid) if lid else CLUSTER.by_id(wait_leader(20))
    base = None
    if leader:
        md = members(leader.http_base(), None)
        if md:
            base = md.get("last_log_index")

    # 2. 8 writers over 8 dedicated snapshot projects → snapshot_writes total
    total = args.snapshot_writes
    projects = SNAPSHOT_PROJECTS
    nw = len(projects)
    share = [total // nw + (1 if i < total % nw else 0) for i in range(nw)]
    stop = threading.Event()
    threads = []
    done = [0]
    dlock = threading.Lock()
    vb = min(4096, MAX_VALUE_BYTES - 64)

    def bulk(wi, project, count):
        seq = 0
        while seq < count and not stop.is_set():
            live = CLUSTER.live()
            if not live:
                time.sleep(0.05)
                continue
            node = random.choice(live)
            seq += 1
            marker = "|SNP%d#S%d#%s#" % (wi, seq, uuid.uuid4().hex[:6])
            rid = "sn-%d-%d-%s" % (wi, seq, uuid.uuid4().hex[:6])
            stats.bump("attempts", node_id=node.node_id)
            write_draft(node, token, project, vb, marker)
            p = write_publish(node, token, project, "snp", rid)
            if p["status"] == 200:
                with dlock:
                    done[0] += 1
                stats.bump("publish_ok")
            else:
                stats.bump("transient")
                stats.bump("retries")

            with STATS_LOCK:
                stats.per_node[node.node_id] += 1

    t0 = time.time()
    for wi in range(nw):
        th = threading.Thread(target=bulk, args=(wi, projects[wi], share[wi]), name="snapw%d" % wi)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()
    bulk_seconds = time.time() - t0
    log("  bulk writes done: %d/%d publishes in %.1fs (victim still down)" % (done[0], total, bulk_seconds))

    # 3. wait leader last_log_index to reflect the bulk volume (gap accumulates on victim)
    if leader is None:
        leader = CLUSTER.by_id(wait_leader(30))
    if leader and base is not None:
        target_idx = base + total * 2  # draft+publish each append ~2 log entries
        wl = time.time() + args.catchup_timeout
        while time.time() < wl:
            md = members(leader.http_base(), None)
            if md and md.get("last_log_index") is not None and md.get("last_log_index") >= base + total:
                break
            time.sleep(0.3)

    # 4. restart the killed follower NOW → large log gap → must catch up via full-snapshot
    #    (0.10: InstallSnapshot == SendFullSnapshot RPC → /raft/full-snapshot streaming)
    victim_restart_offset = os.path.getsize(victim.log_path) if os.path.exists(victim.log_path) else 0
    with CLUSTER.lock:
        victim.spawn(CLUSTER.seed_map)
    log("  restarted victim node %d (catch-up must use full-snapshot)" % victim_id)
    if not wait_healthz(victim, 25):
        log("  WARN: victim healthz timeout after restart")
    # RSS 基线：重启后、传输开始前采样（复核：窗口内首个采样可能已经在 install 中，会把 net 压成 0）
    rss_baseline_kb = read_rss_kb(victim)

    # --chaos-during-snapshot (churn 并发): SIGKILL or SIGSTOP-freeze the CURRENT leader 1-2x
    # WHILE the victim is still behind. The victim must NEVER campaign (0 elections) and its
    # lag must still descend to numeric 0 under leadership churn.
    chaos_schedule = []
    if args.chaos_during_snapshot:
        n_ev = rng.choice([1, 2])
        t_ev = rng.uniform(2.0, 5.0)
        for _ in range(n_ev):
            chaos_schedule.append(t_ev)
            t_ev += rng.uniform(4.0, 9.0)
        log("  chaos-during-snapshot: 计划 %d 次 leader kill/freeze；优先在 victim 的 "
            "transfer 窗口内注入（fallback 墙钟 t+%s）"
            % (n_ev, [round(x, 1) for x in chaos_schedule]))

    def current_leader_obj():
        # majority-vote live leader (Chaos.track_leader_change pattern): chaos kills the pinned
        # leader mid-catch-up → lag polling must follow the NEW leader.
        vals = []
        for n in CLUSTER.live():
            lid, _ = cluster_leader_id(n)
            if lid is not None:
                vals.append(lid)
        if not vals:
            return None
        lid = max(set(vals), key=vals.count)
        n = CLUSTER.by_id(lid)
        return n if (n and n.alive()) else None

    def snapshot_chaos_once(kind_note):
        leader_n = current_leader_obj()
        if leader_n is None or CLUSTER.live_count() < 4:
            log("  snapshot chaos skipped (%s): no live leader / live<4" % kind_note)
            return None
        # C3：和 load 阶段的 roulette 一样，kill 之前必须确认目标**自认 leader**
        # （majority-vote 找到的 id 也可能是陈旧视图）。freeze 不做身份校验（不产生 C3 计数）。
        own = members(leader_n.http_base(), None) or {}
        entry = next(
            (m for m in own.get("members", [])
             if str(m.get("node_id")) == str(leader_n.node_id)),
            None,
        )
        is_leader = bool(entry and entry.get("is_leader"))
        kind = rng.choice(["kill", "freeze"])
        if kind == "kill":
            if not is_leader:
                stats.bump("leader_kill_target_not_leader")
                log("  snapshot chaos skipped (%s): 目标 %d 自认 follower"
                    % (kind_note, leader_n.node_id))
                return None
            at = time.time()
            with CLUSTER.lock:
                leader_n.kill()
            stats.bump("leader_kills")
            stats.bump("leader_kill_targets_verified")
            stats.bump("snapshot_chaos_injections")
            log("  snapshot chaos: leader %d SIGKILL @ catchup+t%.1fs"
                % (leader_n.node_id, time.time() - catchup_t0))
            # C3：与 load 阶段一致——kill 后、重启前观测是否真的换人（否则 C3 计数不平）
            changed = False
            deadline = time.time() + 3.0
            while time.time() < deadline:
                nl = wait_leader(1)
                if nl is not None and nl != leader_n.node_id:
                    changed = True
                    stats.bump("observed_leader_changes")
                    break
                time.sleep(0.2)
            if not changed:
                stats.bump("leader_change_missing")
            time.sleep(rng.uniform(0.3, 1.0))
            with CLUSTER.lock:
                leader_n.spawn(CLUSTER.seed_map)
            wait_healthz(leader_n, 25)
            return {"kind": "kill", "target": leader_n.node_id, "at": at,
                    "resumed_at": time.time()}
        else:
            with CLUSTER.lock:
                leader_n.stop_freeze()
            stats.bump("freeze_count")
            stats.bump("snapshot_chaos_injections")
            at = time.time()
            log("  snapshot chaos: leader %d SIGSTOP @ catchup+t%.1fs"
                % (leader_n.node_id, at - catchup_t0))
            time.sleep(rng.uniform(1.0, 3.0))
            with CLUSTER.lock:
                leader_n.resume_freeze()
            return {"kind": "freeze", "target": leader_n.node_id, "at": at,
                    "resumed_at": time.time()}

    # 5. poll leader members: victim lag → NUMERIC 0, AND snapshot project config == leader's.
    #    config-equal-but-lag-null is NOT catch-up: lag==null means the victim's matched index
    #    is not yet established on the leader (stale/absent replication), so require an explicit
    #    victim lag == 0 from the leader's replication view before accepting config equality.
    catchup_t0 = time.time()
    catchup_seconds = None
    max_lag = None          # only numeric lag observations update it; None is NOT "0"
    lag_values = []         # every numeric victim lag observed (for summary/diagnosis)
    lag_points = []         # (t_rel_from_catchup_start, lag) time series → lag_trajectory
    lag_none_seen = False   # victim lag reported null at least once
    # 心跳证据（plan §9.3）：leader 视角 victim 的「最近一次确认心跳」age。
    # 0.10 的独立 heartbeat worker 要证明的正是「快照 install 期间心跳不停」。
    rss_in_window_kb = None
    rss_baseline_kb = None
    projects_compared = 0
    hb_samples = 0
    hb_max_ms = None
    hb_max_in_window_ms = None
    chaos_done = []
    cfg_gate_diag = 0       # first few lag==0-but-gate-fail reasons (diagnosis only)
    transfer_seen_at = None
    chaos_target = rng.choice([1, 2]) if args.chaos_during_snapshot else 0
    while time.time() - catchup_t0 < args.catchup_timeout:
        # churn 注入：优先在 victim 的 transfer 窗口刚打开时打（这才是 C4「churn during
        # install」）；transfer 未出现时退回墙钟计划。
        if args.chaos_during_snapshot and transfer_seen_at is None:
            try:
                with open(victim.log_path, "rb") as f:
                    f.seek(victim_restart_offset)
                    tail = f.read().decode("utf-8", "replace").lower()
                if "full-snapshot transfer start" in tail:
                    transfer_seen_at = time.time()
                    log("  transfer window opened @ catchup+t%.1fs → 注入 churn"
                        % (transfer_seen_at - catchup_t0))
                    c = snapshot_chaos_once("transfer-window")
                    if c:
                        chaos_done.append(c)
            except OSError:
                pass
        if len(chaos_done) < chaos_target and chaos_schedule and time.time() - catchup_t0 >= chaos_schedule[0]:
            if catchup_seconds is None and (max_lag is None or max_lag > 0):
                c = snapshot_chaos_once("scheduled")
                if c:
                    chaos_done.append(c)
            chaos_schedule.pop(0)
        victim_lag = None
        leader = current_leader_obj() or leader
        md = members(leader.http_base(), None) if leader else None
        if md:
            for m in md.get("members", []):
                if str(m.get("node_id")) == str(victim_id):
                    hb_age = m.get("last_heartbeat_ms_ago")
                    if transfer_seen_at is not None and catchup_seconds is None:
                        rss_now = read_rss_kb(victim)
                        if isinstance(rss_now, int):
                            if rss_baseline_kb is None:
                                rss_baseline_kb = rss_now
                            rss_in_window_kb = (
                                rss_now if rss_in_window_kb is None
                                else max(rss_in_window_kb, rss_now)
                            )
                    if isinstance(hb_age, int) and not isinstance(hb_age, bool):
                        hb_samples += 1
                        hb_max_ms = hb_age if hb_max_ms is None else max(hb_max_ms, hb_age)
                        if transfer_seen_at is not None and catchup_seconds is None:
                            hb_max_in_window_ms = (
                                hb_age if hb_max_in_window_ms is None
                                else max(hb_max_in_window_ms, hb_age)
                            )
                    lag = m.get("lag")
                    if lag is None or isinstance(lag, bool):
                        if lag is None:
                            lag_none_seen = True
                    else:
                        lag_values.append(int(lag))
                        lag_points.append((round(time.time() - catchup_t0, 2), int(lag)))
                        max_lag = int(lag) if max_lag is None else max(max_lag, int(lag))
                        victim_lag = int(lag)
                    break
        # primary catch-up: victim healthz + victim lag == 0 (numeric) + **全部** snapshot 项目
        # 配置相等。复核指出只看 projects[0] 会乐观（一个项目相等不等于整份快照 restore 完成）；
        # 这里逐项目比对，最后再由 validate_final 对全节点全项目复核。
        if victim_lag == 0 and healthz(victim.http_base()):
            all_equal = True
            first_mismatch = None
            for proj in projects:
                lc = read_config(leader, token, proj) if leader else None
                vc = read_config(victim, token, proj)
                if not lc or not vc or not lc.get("value") or lc.get("value") != vc.get("value"):
                    all_equal = False
                    first_mismatch = proj
                    break
            if all_equal:
                catchup_seconds = time.time() - catchup_t0
                projects_compared = len(projects)
                break
            elif cfg_gate_diag < 3:
                cfg_gate_diag += 1
                log("  diag cfg-gate: project %s 仍未与 leader 相等" % (first_mismatch,))
            elif cfg_gate_diag < 3:
                # why the gate refuses to settle despite lag==0: which side unreadable,
                # or value mismatch (len+tail) — distinguishes harness/transport vs state.
                cfg_gate_diag += 1
                probe = http_get(leader.http_base() if leader else victim.http_base(),
                                 token,
                                 "/v1/projects/%s/branches/%s/config?format=json"
                                 % (projects[0], BRANCH), timeout=10.0)
                log("  diag cfg-gate: leader=%s lc=%s vc=%s tailL=%r tailV=%r probe=%r/%r/%r"
                    % ((leader.node_id if leader else None),
                       (None if lc is None else repr(sorted(lc.keys()))),
                       (None if vc is None else repr(sorted(vc.keys()))),
                       (None if not lc or not lc.get("value") else lc["value"][-24:]),
                       (None if not vc or not vc.get("value") else vc["value"][-24:]),
                       probe["status"], probe.get("err"), str(probe.get("data"))[:160]))
        time.sleep(0.4)

    # C4 证据：victim 峰值 RSS（VmHWM）+ 快照字节数 + 追赶 episode（含 retry）
    victim_peak_rss_kb = read_peak_rss_kb(victim)
    snapshot_bytes = latest_snapshot_bytes(victim)
    episode = snapshot_episode(victim, victim_restart_offset)
    events = election_events(victim, victim_restart_offset)
    attr = classify_elections(episode, events, chaos_done)
    victim_elections = attr["unexplained"]
    interrupted_by_election = victim_elections > 0

    return {"victim_id": victim_id, "catchup_seconds": catchup_seconds,
            "max_lag": max_lag, "lag_none_seen": lag_none_seen, "lag_values": lag_values,
            "victim_peak_rss_kb": victim_peak_rss_kb,
            "snapshot_bytes": snapshot_bytes,
            "snapshot_bytes_mtime": snapshot_bytes_mtime(victim),
            "snapshot_install_seconds": episode["episode_seconds"],
            "snapshot_last_attempt_seconds": episode["last_attempt_seconds"],
            "snapshot_attempts": episode["attempts"],
            "snapshot_transfer_ts": episode["transfer_ts"],
            "snapshot_done_ts": episode["done_ts"],
            "snapshot_elections_in_episode": attr["in_episode"],
            "snapshot_elections_during_chaos": attr["during_chaos"],
            "snapshot_elections_unexplained": attr["unexplained"],
            "snapshot_elections_since_restart": attr["since_restart"],
            "victim_rss_max_in_window_kb": rss_in_window_kb,
            "victim_rss_baseline_kb": rss_baseline_kb,
            "victim_heartbeat_samples": hb_samples,
            "victim_heartbeat_max_ms": hb_max_ms,
            "victim_heartbeat_max_in_window_ms": hb_max_in_window_ms,
            "lag_points": lag_points,
            "lag_trajectory": _sample_lag_points(lag_points),
            "interrupted_by_election": interrupted_by_election,
            "victim_elections": victim_elections,
            "victim_elections_since_restart": attr["since_restart"],
            "install_windows": 1 if episode["episode_seconds"] else 0,
            "snapshot_chaos_injections": len(chaos_done),
            "snapshot_chaos_windows": chaos_done,
            "snapshot_projects_compared": projects_compared,
            "bulk_seconds": bulk_seconds, "leader_node": leader,
            "projects": projects}, victim


# ---------------------------------------------------------------- quiesce + final validation


def quiesce(timeout=30):
    """Wait until every node is healthz AND the leader's replication view shows every live
    VOTER lag == a numeric 0.

    lag semantics (dsh-observability cluster_members_json / replication_lag):
      - lag is only populated when queried from the CURRENT LEADER (replication map exists
        on the leader only). We therefore read members from the leader node.
      - lag == null on a NON-leader voter means its matched log index is not yet established
        on the leader → replication not built → NOT caught up. Do NOT treat null as settled.
      - lag == null on the LEADER's own member entry is expected (openraft's replication map
        excludes the leader itself) → tolerated only for is_leader == true.

    Returns True on settle, False on timeout (caller fails).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = all(healthz(n.http_base()) for n in CLUSTER.nodes)
        lid = wait_leader(1)
        leader = CLUSTER.by_id(lid) if lid else None
        lag_ok = False
        if leader:
            md = members(leader.http_base(), None)
            if md:
                lag_ok = True
                for m in md.get("members", []):
                    if not m.get("is_voter"):
                        continue  # non-voters don't gate quorum settle
                    if m.get("is_leader"):
                        continue  # leader's own entry: replication excludes self → null OK
                    lag = m.get("lag")
                    # require an explicit numeric 0; null or >0 = not settled
                    if isinstance(lag, bool) or not isinstance(lag, int) or lag != 0:
                        lag_ok = False
                        break
        if ok and lag_ok:
            return True
        time.sleep(0.5)
    return False


def c3_errors(args, stats):
    """C3（3+ 节点 HTTP leader kill / 换人）断言，纯函数便于单测。

    心跳 age 只作数据记录（leader 侧 ack age 会被 install 负载拉高，不能当通过/失败阈值；
    复核指出 3s 预算 > election_timeout_max 600ms，证明不了任何东西）。真正的不变式是
    「leader 可达期间 follower 不竞选」，由 C4 的 episode 选举归属断言负责。
    """
    errs = []
    if args.leader_kills > 0 and stats.leader_kills == 0:
        errs.append("C3: 配置了 --leader-kills=%d 但一次都没杀到自认 leader 的节点（target_not_leader=%d）"
                    % (args.leader_kills, stats.leader_kill_target_not_leader))
    if stats.leader_kills > 0:
        if stats.leader_kill_targets_verified < stats.leader_kills:
            errs.append("C3: %d 次 kill 但只有 %d 次确认目标是 leader（其余是陈旧视图）"
                        % (stats.leader_kills, stats.leader_kill_targets_verified))
        if stats.observed_leader_changes < stats.leader_kills:
            errs.append("C3: %d 次 leader kill 但只观测到 %d 次 leader 变化（missing=%d）"
                        % (stats.leader_kills, stats.observed_leader_changes,
                           stats.leader_change_missing))
    return errs


def c4_errors(args, snapshot_info, victim_is_voter):
    """C4（voter 大快照 / churn 在 episode 内 / install 预算 / 选举归属 / RSS）断言。

    episode = 第一次 transfer → 最终 done（含 churn 打断后的 retry）。复核 blocker：只取
    「最后一个完整窗口」会把被打断那段里的选举藏起来，所以所有判定都基于 episode。
    """
    errs = []
    info = snapshot_info or {}
    if info.get("install_windows", 0) < 1:
        errs.append("C4: victim 没有完成 full-snapshot（episode 缺 transfer 或 done）——"
                    "日志追平/learner 路径不算 C4")
        return errs
    ep = info.get("snapshot_install_seconds")
    if ep is None:
        errs.append("C4: 无法量测 episode 端到端耗时")
    elif ep > args.max_install_seconds:
        errs.append("C4: episode 端到端 %ss 超过预算 %ss（attempts=%s，last_attempt=%ss）"
                    % (ep, args.max_install_seconds, info.get("snapshot_attempts"),
                       info.get("snapshot_last_attempt_seconds")))
    sb = info.get("snapshot_bytes")
    if sb is None:
        errs.append("C4: victim 没有落盘快照文件（snapshot_bytes=None）")
    else:
        if sb < args.min_snapshot_bytes:
            errs.append("C4: 快照只有 %s 字节 < 下限 %s（规模不足以覆盖大快照场景）"
                        % (sb, args.min_snapshot_bytes))
        t0 = info.get("snapshot_transfer_ts")
        mt = info.get("snapshot_bytes_mtime")
        if t0 is not None and mt is not None and mt < t0.timestamp() - 1.0:
            errs.append("C4: 快照文件 mtime 早于 episode transfer——可能是历史残留")
    if victim_is_voter is not True:
        errs.append("C4: victim node %s 不是 voter（is_voter=%s）——learner 追平不能算 C4"
                    % (info.get("victim_id"), victim_is_voter))

    if args.chaos_during_snapshot:
        windows = info.get("snapshot_chaos_windows") or []
        t0, t1 = info.get("snapshot_transfer_ts"), info.get("snapshot_done_ts")
        inside = [
            w for w in windows
            if t0 is not None and t1 is not None and w.get("at") is not None
            and t0.timestamp() <= w["at"] <= t1.timestamp()
        ]
        if not windows:
            errs.append("C4: --chaos-during-snapshot 开了但一次注入都没发生")
        elif not inside:
            errs.append("C4: %d 次 churn 注入没有一次落在 episode 内" % len(windows))

    unexplained = info.get("snapshot_elections_unexplained")
    if unexplained is None:
        errs.append("C4: 缺选举归属统计（snapshot_elections_unexplained=None）")
    elif unexplained > 0:
        errs.append("C4: episode 内出现 %d 次无法用 churn 解释的竞选（in_episode=%s, during_chaos=%s）"
                    "——0.9 失败模式：install 期间心跳被抑制"
                    % (unexplained, info.get("snapshot_elections_in_episode"),
                       info.get("snapshot_elections_during_chaos")))

    rss_kb = info.get("victim_rss_max_in_window_kb")
    base_kb = info.get("victim_rss_baseline_kb") or 0
    if rss_kb is None:
        errs.append("C4: 缺 install 窗口 RSS 采样（victim_rss_max_in_window_kb=None）")
    elif sb:
        net_kb = max(0, rss_kb - int(base_kb))
        ratio = (net_kb * 1024.0) / float(sb)
        if ratio > args.max_rss_ratio:
            errs.append("C4: install 窗口净增量 RSS %.2fx 快照 > 预算 %.2fx"
                        "（rss=%dKiB baseline=%dKiB snapshot=%dB）"
                        % (ratio, args.max_rss_ratio, rss_kb, int(base_kb), sb))
    return errs


def validate_final(args, token, writers, snapshot_info, errors):
    # leader
    lid = wait_leader(10)
    leader = CLUSTER.by_id(lid) if lid else None
    if leader is None or not leader.alive():
        errors.append("no live leader at final check")
        return

    all_projects = [w.project for w in writers] + SNAPSHOT_PROJECTS

    # 1+2: per project, leader config version >= acked_version; marker ∈ all_markers;
    #      every node config version == leader version, marker == leader marker
    leader_cfg = {}
    for p in all_projects:
        lc = mgmt_config(leader, token, p)
        leader_cfg[p] = lc
        if lc is None:
            errors.append("leader config unreadable for project %s" % p)

    for w in writers:
        lc = leader_cfg.get(w.project)
        if lc and w.acked_version:
            ver = lc.get("version")
            if ver is not None and ver < w.acked_version:
                errors.append("leader version %s < acked_version %s (project %s)"
                              % (ver, w.acked_version, w.project))
        if lc and lc.get("value"):
            if not w.owner_ok(lc["value"]):
                errors.append("leader value not owned by writer w%d (project %s, value tail=%s)"
                              % (w.wid, w.project, lc["value"][-40:]))

    for n in CLUSTER.nodes:
        if not n.alive():
            errors.append("node %d down at final check" % n.node_id)
            continue
        if n.node_id == leader.node_id:
            continue
        for p in all_projects:
            nc = mgmt_config(n, token, p)
            lc = leader_cfg.get(p)
            if nc is None or lc is None:
                continue
            if nc.get("version") != lc.get("version"):
                errors.append("node %d version %s != leader %s (project %s)"
                              % (n.node_id, nc.get("version"), lc.get("version"), p))
                break
            if nc.get("value") != lc.get("value"):
                errors.append("node %d marker/value != leader (project %s)" % (n.node_id, p))
                break

    # 3: exactly one leader consistent across all nodes; 5 voters; none down
    voters_seen = set()
    leaders_seen = set()
    for n in CLUSTER.nodes:
        md = members(n.http_base(), None)
        if md is None:
            errors.append("node %d members unreadable" % n.node_id)
            continue
        cl = md.get("current_leader")
        if cl is not None:
            leaders_seen.add(cl)
        vs = [m for m in md.get("members", []) if m.get("is_voter")]
        if len(vs) >= 1:
            voters_seen.add(tuple(sorted(str(x.get("node_id")) for x in vs)))
    if len(leaders_seen) > 1:
        errors.append("leader disagreement across nodes: %s" % leaders_seen)

    # 3b: leader replication lag — every live VOTER must show a numeric lag == 0 in the
    #     leader's replication view. lag == null on a non-leader voter means its matched log
    #     index is not established on the leader (replication not built / not caught up), which
    #     is NOT settled. The leader's own member entry is null by design → tolerated.
    md = members(leader.http_base(), None)
    if md:
        for m in md.get("members", []):
            if not m.get("is_voter") or m.get("is_leader"):
                continue
            lag = m.get("lag")
            if isinstance(lag, bool) or not isinstance(lag, int) or lag != 0:
                errors.append("voter node %s not settled: lag=%r (null=replication/catch-up "
                              "not established)" % (m.get("node_id"), lag))

    # 4: log scan — no forbidden tokens in THIS run's log files
    for n in CLUSTER.nodes:
        with open(n.log_path, "rb") as f:
            blob = f.read().decode("utf-8", "replace").lower()
        for tok in FORBIDDEN:
            if tok in blob:
                errors.append("forbidden token %r in node %d log" % (tok, n.node_id))

    # 5: reader monotonicity — no per (project,node) seq regression
    for rd in READERS:
        for v in rd.violations:
            errors.append("reader monotonicity regression %s" % (v,))
            break

    # 6: snapshot catch-up assertions
    if snapshot_info is not None:
        cs = snapshot_info.get("catchup_seconds")
        if cs is None:
            errors.append("follower catch-up not reached: victim lag never settled to numeric 0 "
                          "within %ss (lag_none_seen=%s, max_lag=%s; %s)"
                          % (args.catchup_timeout, snapshot_info.get("lag_none_seen"),
                             snapshot_info.get("max_lag"),
                             describe_lag_trend(snapshot_info.get("lag_points") or [])))
        elif cs >= args.catchup_timeout:
            errors.append("catch_up_seconds %s >= timeout %s" % (cs, args.catchup_timeout))
        if snapshot_info.get("max_lag") is None and snapshot_info.get("lag_none_seen"):
            errors.append("victim lag stayed null throughout (InstallSnapshot never established)")
        # ---- 溯源断言：--no-build 时二进制不得旧于源码
        bm, sm = GLOBAL.get("bin_mtime"), GLOBAL.get("source_newest_mtime")
        if bm and sm and bm < sm:
            errors.append("provenance: 二进制 mtime 早于最新源码（陈旧 binary，先 rebuild）")

        # ---- C3/C4 断言（纯函数，单测覆盖 scripts/stress/test_leader_storm.py）----
        errors.extend(c3_errors(args, STATS))
        md = members(leader.http_base(), None) if leader else None
        voter_ok = None
        if md:
            for m in md.get("members", []):
                if str(m.get("node_id")) == str(snapshot_info.get("victim_id")):
                    voter_ok = bool(m.get("is_voter"))
                    break
        errors.extend(c4_errors(args, snapshot_info, voter_ok))

        # P3 churn 并发 invariant: 正在被 full-snapshot 追赶的 follower（含 leader kill/freeze
        # churn 期间）在 **transfer→install→done 窗口内** 必须一次选举/预投票都不发起。
        # 窗口外的选举（重启后尚未连上 leader）只做诊断输出。
        log("  victim elections: in_episode=%s during_chaos=%s unexplained=%s since_restart=%s attempts=%s"
            % (snapshot_info.get("snapshot_elections_in_episode"),
               snapshot_info.get("snapshot_elections_during_chaos"),
               snapshot_info.get("snapshot_elections_unexplained"),
               snapshot_info.get("victim_elections_since_restart"),
               snapshot_info.get("snapshot_attempts")))
        if snapshot_info.get("snapshot_elections_unexplained", 0) > 0:
            errors.append("victim node %s 在 episode（含 retry）内发起 %d 次无法用 churn 解释的竞选"
                          "（0.9 失败模式：install 期间心跳被抑制）"
                          % (snapshot_info.get("victim_id"),
                             snapshot_info.get("snapshot_elections_unexplained")))
        if snapshot_info["victim_id"] is not None:
            vid = snapshot_info["victim_id"]
            with open(CLUSTER.by_id(vid).log_path, "rb") as f:
                blob = f.read().decode("utf-8", "replace").lower()
            for tok in ("length limit exceeded", "installsnapshot rpc timed out",
                        "msgpack decode", "msgpack encode", "crc mismatch",
                        "frame too large", "snapshot too large"):
                if tok in blob:
                    errors.append("victim node %d log shows %r (snapshot catch-up regression)"
                                  % (vid, tok))

def _cfg_version(cfg):
    return cfg.get("version")


# ---------------------------------------------------------------- main


def parse_args():
    p = argparse.ArgumentParser(prog="leader_storm.py", description="cluster storm harness")
    p.add_argument("--profile", default="storm", choices=sorted(PROFILES))
    p.add_argument("--bin", default=None)
    p.add_argument("--keep", action="store_true")
    p.add_argument("--nodes", type=int)
    p.add_argument("--workers", type=int)
    p.add_argument("--duration", type=float)
    p.add_argument("--leader-kills", type=int)
    p.add_argument("--snapshot-writes", type=int)
    p.add_argument("--value-bytes", type=int, default=60000)
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--catchup-timeout", type=float, default=60.0)
    p.add_argument("--max-install-seconds", type=float, default=30.0,
                   help="C4 预算：full-snapshot 端到端安装时间（transfer→apply 完成）上限，默认 30s")
    p.add_argument("--max-rss-ratio", type=float, default=2.0,
                   help="C4 内存预算：install 窗口内 victim 峰值 RSS / 快照字节数（§8.5 默认 2.0）")
    p.add_argument("--min-snapshot-bytes", type=int, default=100_000_000,
                   help="C4 规模下限：victim 实际落盘快照字节数，默认 100MB（153MB/12k 目标的下限保护）")
    p.add_argument("--chaos-during-snapshot", action="store_true",
                   default=os.environ.get("CHAOS_DURING_SNAPSHOT", "0") in ("1", "true", "yes"),
                   help="churn 并发: inject 1-2 leader kill/freeze while the victim is catching "
                        "up via full-snapshot (victim must stay follower: 0 elections)")
    return p.parse_args()


def main():
    args = parse_args()
    GLOBAL["git_rev"] = git_rev(args.bin)
    GLOBAL["git_status"] = git_status(args.bin)
    try:
        GLOBAL["bin_mtime"] = os.path.getmtime(args.bin) if args.bin else None
    except OSError:
        GLOBAL["bin_mtime"] = None
    GLOBAL["source_newest_mtime"] = source_newest_mtime(repo_root(args.bin))
    prof = dict(PROFILES[args.profile])
    for key in ("nodes", "workers", "duration", "leader_kills", "snapshot_writes"):
        v = getattr(args, key.replace("-", "_"))
        if v is not None:
            prof[key] = v
    args.nodes = prof["nodes"]
    args.workers = prof["workers"]
    args.duration = prof["duration"]
    args.leader_kills = prof["leader_kills"]
    args.snapshot_writes = prof["snapshot_writes"]

    bin_path = args.bin or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "..", "server", "target", "debug", "defing")
    if not (os.path.isfile(bin_path) and os.access(bin_path, os.X_OK)):
        log("FAIL: binary not executable: %s (build first / pass --bin)" % bin_path)
        return 2

    rng = random.Random(args.seed)
    random.seed(args.seed)

    work = None
    nodes = []
    exit_code = 0
    try:
        import tempfile
        work = tempfile.mkdtemp(prefix="defing-storm.")
        node_objs = []
        for i in range(1, args.nodes + 1):
            d = os.path.join(work, "n%d" % i)
            os.makedirs(d)
            node_objs.append(Node(i, bin_path, d, os.path.join(work, "n%d.log" % i)))
        # bootstrap seed map: every node passes the SAME map id@raft@http
        seed_map = ",".join("%d@%s@%s" % (n.node_id, n.raft_addr, n.http_addr) for n in node_objs)
        globals()["CLUSTER"] = Cluster(node_objs, seed_map)
        nodes.extend(node_objs)  # track for finally cleanup


        t_boot = time.time()
        for n in node_objs:
            n.spawn(seed_map)
        # wait all healthz
        boot_ok = all(wait_healthz(n, 30) for n in node_objs)
        if not boot_ok:
            for n in node_objs:
                if not healthz(n.http_base()):
                    log("  boot fail node %d; log tail:" % n.node_id)
                    for line in n.tail(30):
                        log("    " + line)
            return 2
        # 启动参数断点: 每个节点 log 的 effective Raft config must equal dev_config()
        if not assert_raft_config(node_objs):
            return 2
        # login once (login 免 Bearer; auto-forwards to leader)
        tok_data = None
        for attempt in range(20):
            r = http_json(node_objs[0].http_base(), None, "POST", "/api/v1/login",
                          {"password": ADMIN_PASSWORD}, timeout=10.0)
            if r["status"] == 200 and isinstance(r.get("data"), dict) and r["data"].get("token"):
                tok_data = r["data"]["token"]
                break
            time.sleep(0.5)
        if not tok_data:
            log("FAIL: admin login failed")
            return 2
        globals()["SESSION_TOKEN"] = tok_data
        # wait cluster fully forms: 5 voters + a leader (members now auth'd via SESSION_TOKEN)
        formed = None
        for _ in range(60):
            lid = wait_leader(1)
            if lid is not None:
                md = members(node_objs[0].http_base())
                if md and len([m for m in md.get("members", []) if m.get("is_voter")]) >= args.nodes:
                    formed = lid
                    break
            time.sleep(0.3)
        if formed is None:
            md = members(node_objs[0].http_base())
            log("  cluster not formed (need %d voters + leader); members=%s" % (args.nodes, md))
            return 2
        # all nodes must see the session token persisted via Raft (cluster-wide)
        if not wait_healthz(node_objs[0], 10):
            return 2
        log("  cluster up (%d nodes, %.1fs), leader=%s, logged in (token persisted)"
            % (args.nodes, time.time() - t_boot, formed))

        # prebuild writer projects + snapshot projects
        n_projects = max(args.workers, 8)
        writer_projects = ["storm-%d" % i for i in range(n_projects)]
        globals()["SNAPSHOT_PROJECTS"] = ["snp-%d" % i for i in range(8)] if args.snapshot_writes > 0 else []
        deadline_boot = time.time() + 40
        for proj in writer_projects + SNAPSHOT_PROJECTS:
            if not ensure_project(tok_data, proj, deadline_boot):
                log("FAIL: prebuild project %s" % proj)
                return 2
        log("  prebuilt %d writer + %d snapshot projects" % (len(writer_projects), len(SNAPSHOT_PROJECTS)))

        # stats + threads
        globals()["STATS"] = Stats(node_objs)
        globals()["STATS_LOCK"] = threading.Lock()
        writers = []
        readers = []
        globals()["READERS"] = readers
        load_deadline = time.time() + args.duration
        START_TS_G = time.time()
        globals()["START_TS"] = START_TS_G

        for wi in range(args.workers):
            w = Writer(wi, writer_projects[wi % n_projects], args, STATS, tok_data)
            w.t = threading.Thread(target=w.run, args=(load_deadline,), name="w%d" % wi)
            writers.append(w)
        nr = max(4, args.workers // 2)
        for ri in range(nr):
            r = Reader(ri, writer_projects, args, STATS, tok_data)
            r.t = threading.Thread(target=r.run, args=(load_deadline,), name="rd%d" % ri)
            readers.append(r)
        chaos = Chaos(args, STATS, rng)
        chaos.t = threading.Thread(target=chaos.run, args=(load_deadline,), name="chaos")

        t_load = time.time()
        for w in writers:
            w.t.start()
        for r in readers:
            r.t.start()
        chaos.t.start()
        log("== load/chaos: duration=%.0fs workers=%d readers=%d leader_kills<=%d =="
            % (args.duration, args.workers, nr, args.leader_kills))
        # wait until load deadline
        while time.time() < load_deadline:
            time.sleep(0.2)
        for w in writers:
            w.stop.set()
        for r in readers:
            r.stop.set()
        chaos.stop.set()
        for w in writers:
            w.t.join(timeout=15)
        for r in readers:
            r.t.join(timeout=15)
        chaos.t.join(timeout=30)
        load_seconds = time.time() - t_load
        log("  load/chaos done in %.1fs" % load_seconds)

        # snapshot catch-up phase (skip in quick / snapshot_writes==0)
        snapshot_info = None
        if args.snapshot_writes > 0:
            info = snapshot_phase(args, STATS, tok_data, rng)
            if info:
                snapshot_info = info[0]
        t_quiesce = time.time()
        if not quiesce(40):
            log("  WARN: cluster not fully settled before final validation")
        quiesce_seconds = round(time.time() - t_quiesce, 1)

        # final validation
        errors = []
        validate_final(args, tok_data, writers, snapshot_info, errors)

        # summary
        acked = sum(len(w.all_markers) for w in writers)
        success_rate = (STATS.publish_ok / STATS.attempts) if STATS.attempts else 0.0
        summary = {
            "profile": args.profile,
            "duration_load_s": round(load_seconds, 1),
            "acked_writes": acked,
            "attempts": STATS.attempts,
            "publish_ok": STATS.publish_ok,
            "success_rate": round(success_rate, 4),
            "retries": STATS.retries,
            "transient_errors": STATS.transient,
            "leader_kills": STATS.leader_kills,
            "observed_leader_changes": STATS.observed_leader_changes,
            "freeze_count": STATS.freeze_count,
            "follower_kills": STATS.follower_kills,
            "leader_kill_targets_verified": STATS.leader_kill_targets_verified,
            "leader_kill_target_not_leader": STATS.leader_kill_target_not_leader,
            "leader_change_missing": STATS.leader_change_missing,
            "quiesce_seconds": quiesce_seconds,
            "snapshot_projects_compared": len(SNAPSHOT_PROJECTS),
            "snapshot_catchup_seconds": (snapshot_info or {}).get("catchup_seconds"),
            "snapshot_victim_peak_rss_kb": (snapshot_info or {}).get("victim_peak_rss_kb"),
            "snapshot_bytes": (snapshot_info or {}).get("snapshot_bytes"),
            "snapshot_install_seconds": (snapshot_info or {}).get("snapshot_install_seconds"),
            "max_lag": (snapshot_info or {}).get("max_lag"),
            # (t_rel_since_catchup_start, victim lag) sampled; null for quick/no-snapshot
            "lag_trajectory": (snapshot_info or {}).get("lag_trajectory"),
            "per_node_requests": STATS.per_node,
        }
        # only meaningful when the snapshot catch-up phase actually ran (not quick)
        if snapshot_info is not None:
            summary["victim_lag_none_seen"] = bool(snapshot_info.get("lag_none_seen"))
            summary["snapshot_interrupted_by_election"] = bool(snapshot_info.get("interrupted_by_election"))
            # P3 churn 并发: victim must campaign 0x even under leader kill/freeze injections
            summary["chaos_during_snapshot"] = bool(args.chaos_during_snapshot)
            summary["victim_elections_in_episode"] = snapshot_info.get("snapshot_elections_in_episode")
            summary["victim_elections_during_chaos"] = snapshot_info.get("snapshot_elections_during_chaos")
            summary["victim_elections_unexplained"] = snapshot_info.get("snapshot_elections_unexplained")
            summary["victim_elections_since_restart"] = snapshot_info.get("victim_elections_since_restart")
            summary["victim_rss_max_in_window_kb"] = snapshot_info.get("victim_rss_max_in_window_kb")
            summary["victim_rss_baseline_kb"] = snapshot_info.get("victim_rss_baseline_kb")
            summary["victim_heartbeat_samples"] = snapshot_info.get("victim_heartbeat_samples")
            summary["victim_heartbeat_max_in_window_ms"] = snapshot_info.get(
                "victim_heartbeat_max_in_window_ms")
            summary["max_rss_ratio_budget"] = args.max_rss_ratio
            summary["snapshot_attempts"] = snapshot_info.get("snapshot_attempts")
            summary["snapshot_last_attempt_seconds"] = snapshot_info.get("snapshot_last_attempt_seconds")
            summary["snapshot_bytes_mtime"] = snapshot_info.get("snapshot_bytes_mtime")
            summary["max_install_seconds_budget"] = args.max_install_seconds
            summary["min_snapshot_bytes"] = args.min_snapshot_bytes
            summary["snapshot_chaos_injections"] = snapshot_info.get("snapshot_chaos_injections")
        # 证据溯源（复核要求：日志必须能定位到源码版本与二进制）
        summary["git_rev"] = GLOBAL.get("git_rev")
        summary["git_status_porcelain"] = GLOBAL.get("git_status")
        summary["bin_sha256"] = sha256_of(args.bin)
        summary["bin_mtime"] = GLOBAL.get("bin_mtime")
        summary["source_newest_mtime"] = GLOBAL.get("source_newest_mtime")
        summary["tmpdir"] = os.environ.get("TMPDIR")
        summary["argv"] = sys.argv[1:]
        log("== SUMMARY ==")
        for k, v in summary.items():
            log("  %-24s %s" % (k + ":", v))
        if errors:
            log("== FAILURES (%d) ==" % len(errors))
            for e in errors[:20]:
                log("  ✗ " + e)
            exit_code = 1
        else:
            log("  ✓ 全部 invariant 通过")
        # one-line JSON
        log("JSON " + json.dumps({"ok": not errors, **summary}, ensure_ascii=False))

        # failure → print lag trajectory tail (distinguish 活锁 constant-stuck vs slow descent)
        # then victim/leader tails
        if errors:
            if snapshot_info is not None:
                pts = snapshot_info.get("lag_points") or []
                if pts:
                    log("--- victim %s lag 轨迹: last %d of %d points ---"
                        % (snapshot_info.get("victim_id"), min(10, len(pts)), len(pts)))
                    for t, lag in pts[-10:]:
                        log("  t=%6.2fs lag=%s" % (t, lag))
                    log("  trend: " + describe_lag_trend(pts))
                else:
                    log("--- victim lag: no numeric observations (lag stayed null) ---")
            for n in node_objs:
                if any(("node %d " % n.node_id) in e for e in errors):
                    log("--- node %d log tail ---" % n.node_id)
                    for line in n.tail(20):
                        log("  " + line)
        return exit_code
    except KeyboardInterrupt:
        log("interrupted")
        return 1
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 2
    finally:
        # cleanup ALL our processes + temp dir (never pkill defing, only our Popen)
        for n in nodes:
            try:
                if n.proc and n.proc.poll() is None:
                    # SIGKILL even if frozen (SIGSTOP'd node ignores SIGTERM)
                    n.proc.kill()
                    try:
                        n.proc.wait(timeout=8)
                    except Exception:
                        pass
                n.close_log()
            except Exception:
                pass
        if work and not args.keep:
            import shutil
            shutil.rmtree(work, ignore_errors=True)
        elif args.keep and work:
            log("kept data-dir + logs: %s" % work)


if __name__ == "__main__":
    sys.exit(main())
