#!/usr/bin/env python3
"""leader_storm.py 的 parser + C3/C4 判定单测（stdlib unittest）。

复核要求（迁移/测试）：
- 「证据生产者」必须有解析单测：episode（含 aborted→retry）、选举事件聚类、快照字节/mtime、RSS；
- 「没走快照 / episode 超预算 / 快照太小或过期 / churn 没落在 episode 内 / 无法解释的竞选 /
  缺 RSS 采样」必须判失败。

运行： python3 -m unittest scripts/stress/test_leader_storm.py -v
"""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import leader_storm as ls  # noqa: E402

BASE = datetime(2026, 10, 29, 1, 0, 0)


def t(sec, frac=0):
    return (BASE + timedelta(seconds=sec, microseconds=frac)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def write_log(lines):
    fd, path = tempfile.mkstemp(suffix=".log")
    with os.fdopen(fd, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


def node(path):
    return SimpleNamespace(log_path=path, data_dir=tempfile.gettempdir())


def args(**kw):
    base = dict(
        leader_kills=5,
        max_install_seconds=30.0,
        min_snapshot_bytes=100_000_000,
        max_rss_delta_mb=0.0,  # P5：0=auto（192MiB+payload）；测试里按需收紧
        chaos_during_snapshot=True,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def stats(**kw):
    base = dict(
        leader_kills=0,
        leader_kill_targets_verified=0,
        leader_kill_target_not_leader=0,
        observed_leader_changes=0,
        leader_change_missing=0,
    )
    base.update(kw)
    return SimpleNamespace(**base)


class EpisodeTest(unittest.TestCase):
    def test_aborted_transfer_then_retry_counts_attempts_and_covers_first_transfer(self):
        path = write_log([
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % t(1),
            "%s INFO openraft::core::raft_core: election timeout expired, triggering election" % t(2),
            "%s INFO openraft::core::raft_core: trigger pre-vote" % t(2, 1000),
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % t(3),
            "%s INFO openraft::following_handler: install full snapshot, meta: x" % t(4),
            "%s INFO openraft::core::sm::worker: Done install complete snapshot, meta: x" % t(6),
        ])
        ep = ls.snapshot_episode(node(path), 0)
        self.assertEqual(ep["attempts"], 2, "两次 transfer = 一次被打断 + 一次重试")
        self.assertAlmostEqual(ep["episode_seconds"], 5.0, places=3, msg="1s→6s，含 retry 间隔")
        self.assertAlmostEqual(ep["last_attempt_seconds"], 3.0, places=3, msg="3s→6s")

    def test_single_attempt(self):
        path = write_log([
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % t(1),
            "%s INFO openraft::following_handler: install full snapshot, meta: x" % t(2),
            "%s INFO openraft::core::sm::worker: Done install complete snapshot, meta: x" % t(4),
        ])
        ep = ls.snapshot_episode(node(path), 0)
        self.assertEqual(ep["attempts"], 1)
        self.assertAlmostEqual(ep["episode_seconds"], 3.0, places=3)

    def test_missing_done_means_no_episode(self):
        path = write_log(["%s INFO dsh_raft::snapshot: full-snapshot transfer start" % t(1)])
        ep = ls.snapshot_episode(node(path), 0)
        self.assertIsNone(ep["episode_seconds"])
        self.assertEqual(ep["attempts"], 1)


class ElectionEventsTest(unittest.TestCase):
    def test_markers_within_gap_are_one_event(self):
        path = write_log([
            "%s INFO raft_core: election timeout expired, triggering election" % t(1),
            "%s INFO raft_core: trigger pre-vote" % t(1, 1000),
            "%s INFO raft_core: trigger election" % t(1, 2000),
            "%s INFO raft_core: trigger election" % t(5),
        ])
        ev = ls.election_events(node(path), 0)
        self.assertEqual(len(ev), 2, "同一次竞选的多行 marker 必须聚成一个事件")

    def test_classify_in_episode_outside_chaos_is_unexplained(self):
        ep = {"transfer_ts": BASE + timedelta(seconds=1), "done_ts": BASE + timedelta(seconds=10)}
        events = [BASE + timedelta(seconds=3), BASE + timedelta(seconds=20)]
        r = ls.classify_elections(ep, events, [])
        self.assertEqual(r["in_episode"], 1, "只统计 episode 内的事件")
        self.assertEqual(r["unexplained"], 1)

    def test_classify_inside_chaos_window_is_explained_with_grace(self):
        ep = {"transfer_ts": BASE + timedelta(seconds=1), "done_ts": BASE + timedelta(seconds=10)}
        chaos = [{"kind": "freeze", "at": (BASE + timedelta(seconds=2)).timestamp(),
                  "resumed_at": (BASE + timedelta(seconds=4)).timestamp()}]
        r1 = ls.classify_elections(ep, [BASE + timedelta(seconds=3)], chaos)
        r2 = ls.classify_elections(ep, [BASE + timedelta(seconds=4, milliseconds=400)], chaos)
        r3 = ls.classify_elections(ep, [BASE + timedelta(seconds=6)], chaos)
        self.assertEqual((r1["during_chaos"], r1["unexplained"]), (1, 0))
        self.assertEqual((r2["during_chaos"], r2["unexplained"]), (1, 0), "恢复后 600ms grace")
        self.assertEqual((r3["during_chaos"], r3["unexplained"]), (0, 1))


class SnapshotBytesTest(unittest.TestCase):
    def test_picks_largest_snap_and_reports_mtime(self):
        with tempfile.TemporaryDirectory() as d:
            snap = os.path.join(d, "snapshots")
            os.makedirs(snap)
            for name, size in [("a.snap", 10), ("b.snap", 40), ("x.part", 999)]:
                with open(os.path.join(snap, name), "wb") as f:
                    f.write(b"x" * size)
            n = SimpleNamespace(data_dir=d)
            self.assertEqual(ls.latest_snapshot_bytes(n), 40, "只认 *.snap，取最大")
            self.assertIsNotNone(ls.snapshot_bytes_mtime(n))

    def test_missing_dir(self):
        with tempfile.TemporaryDirectory() as d:
            n = SimpleNamespace(data_dir=d)
            self.assertIsNone(ls.latest_snapshot_bytes(n))
            self.assertIsNone(ls.snapshot_bytes_mtime(n))


def good_info():
    return {
        "victim_id": 3,
        "install_windows": 1,
        "snapshot_attempts": 2,
        "snapshot_last_attempt_seconds": 1.5,
        "snapshot_install_seconds": 2.0,
        "snapshot_bytes": 250_000_000,
        "snapshot_transfer_ts": BASE,
        "snapshot_done_ts": BASE + timedelta(seconds=2),
        "snapshot_bytes_mtime": (BASE + timedelta(seconds=1)).timestamp(),
        "snapshot_chaos_windows": [
            {"kind": "freeze", "at": (BASE + timedelta(seconds=1)).timestamp(),
             "resumed_at": (BASE + timedelta(seconds=2)).timestamp()}
        ],
        "snapshot_elections_in_episode": 0,
        "snapshot_elections_during_chaos": 0,
        "snapshot_elections_unexplained": 0,
        "victim_rss_max_in_window_kb": 400_000,
        "victim_peak_rss_kb": 400_000,
        "victim_rss_baseline_kb": 100_000,
        # P4 口径：断言 = 绝对净增量（max(窗口采样, VmHWM) - 基线）= 300MiB < 320MiB；
        # db_file/payload 比值只入 summary 供对照。
        "victim_db_file_bytes": 300_000_000,
    }


class C4AssertionsTest(unittest.TestCase):
    def test_good_case_passes(self):
        self.assertEqual(ls.c4_errors(args(), good_info(), True), [])

    def test_no_episode_fails(self):
        info = good_info()
        info["install_windows"] = 0
        errs = ls.c4_errors(args(), info, True)
        self.assertTrue(any("episode" in e for e in errs), errs)

    def test_over_budget_fails(self):
        info = good_info()
        info["snapshot_install_seconds"] = 31.0
        self.assertTrue(any("超过预算" in e for e in ls.c4_errors(args(), info, True)))

    def test_small_snapshot_fails(self):
        info = good_info()
        info["snapshot_bytes"] = 10
        self.assertTrue(any("下限" in e for e in ls.c4_errors(args(), info, True)))

    def test_stale_snapshot_file_fails(self):
        info = good_info()
        info["snapshot_bytes_mtime"] = (BASE - timedelta(seconds=60)).timestamp()
        self.assertTrue(any("历史残留" in e for e in ls.c4_errors(args(), info, True)))

    def test_learner_victim_fails(self):
        self.assertTrue(any("不是 voter" in e for e in ls.c4_errors(args(), good_info(), False)))

    def test_no_chaos_injection_fails(self):
        info = good_info()
        info["snapshot_chaos_windows"] = []
        self.assertTrue(any("一次注入都没发生" in e for e in ls.c4_errors(args(), info, True)))

    def test_chaos_outside_episode_fails(self):
        info = good_info()
        info["snapshot_chaos_windows"] = [
            {"kind": "kill", "at": (BASE + timedelta(seconds=30)).timestamp(),
             "resumed_at": (BASE + timedelta(seconds=31)).timestamp()}
        ]
        self.assertTrue(any("没有一次落在 episode 内" in e
                            for e in ls.c4_errors(args(), info, True)))

    def test_unexplained_election_fails(self):
        info = good_info()
        info["snapshot_elections_unexplained"] = 1
        self.assertTrue(any("无法用 churn 解释" in e for e in ls.c4_errors(args(), info, True)))

    def test_missing_rss_fails(self):
        info = good_info()
        info["victim_rss_max_in_window_kb"] = None
        self.assertTrue(any("缺 install 窗口 RSS" in e for e in ls.c4_errors(args(), info, True)))

    def test_rss_over_budget_fails(self):
        info = good_info()
        info["victim_rss_max_in_window_kb"] = 1_000_000
        self.assertTrue(any("净增量 RSS" in e for e in ls.c4_errors(args(), info, True)))

    def test_missing_db_file_bytes_fails(self):
        """观测基准（redb 尺寸）缺失判失败，避免 summary/比值静默缺失。"""
        info = good_info()
        info["victim_db_file_bytes"] = None
        self.assertTrue(any("redb 尺寸" in e for e in ls.c4_errors(args(), info, True)))

    def test_rss_budget_scales_with_payload(self):
        """预算 = 192MiB + payload（随规模放大；flag 只能收紧）：
        - payload 386MB（v1）→ 预算 ~560MiB；delta 350MiB → 通过（大 payload 合法内存更高）
        - payload 95MB（v2）→ 预算 ~283MiB；同一 delta 350MiB → 失败
        - payload 95MB、delta 300MiB > 283 → 失败；delta 250MiB → 通过
        """
        info = good_info()
        info["victim_rss_baseline_kb"] = 100_000
        # v1 大 payload：同一 delta 合法
        info["snapshot_bytes"] = 386_000_000
        info["victim_rss_max_in_window_kb"] = 450_000  # delta 350MiB < 560
        info["victim_peak_rss_kb"] = 450_000
        self.assertEqual(ls.c4_errors(args(), info, True), [])
        # v2 小 payload：同一 delta 超预算
        info["snapshot_bytes"] = 95_000_000
        self.assertTrue(any("净增量 RSS" in e for e in ls.c4_errors(
            args(min_snapshot_bytes=0), info, True)))
        # delta 300MiB > 283 预算
        info["victim_rss_max_in_window_kb"] = 400_000
        info["victim_peak_rss_kb"] = 400_000
        self.assertTrue(any("净增量 RSS" in e for e in ls.c4_errors(
            args(min_snapshot_bytes=0), info, True)))
        # delta 250MiB → 通过
        info["victim_rss_max_in_window_kb"] = 350_000
        info["victim_peak_rss_kb"] = 350_000
        self.assertEqual(ls.c4_errors(args(min_snapshot_bytes=0), info, True), [])

    def test_rss_budget_flag_can_tighten_but_not_loosen(self):
        """flag > 0 只能收紧：min(flag, 192+payload)；flag 小于 auto 时按 flag 判。"""
        info = good_info()
        info["victim_rss_baseline_kb"] = 100_000
        info["snapshot_bytes"] = 95_000_000        # auto = 192+90.6 = 282.6
        info["victim_rss_max_in_window_kb"] = 350_000  # delta 250MiB
        info["victim_peak_rss_kb"] = 350_000
        self.assertEqual(ls.c4_errors(args(min_snapshot_bytes=0), info, True), [])
        # 收紧到 200 → 250 超预算
        self.assertTrue(any("净增量 RSS" in e for e in ls.c4_errors(
            args(min_snapshot_bytes=0, max_rss_delta_mb=200.0), info, True)))
        # 放宽到 1000 → 仍按 auto（282.6），250 通过
        self.assertEqual(ls.c4_errors(
            args(min_snapshot_bytes=0, max_rss_delta_mb=1000.0), info, True), [])

    def test_rss_budget_catches_whole_payload_residency(self):
        """整份 payload 额外驻留（+1x payload）必须超预算——这是预算公式的核心目的。"""
        info = good_info()
        info["snapshot_bytes"] = 95_000_000          # ~90.6MiB
        info["victim_rss_baseline_kb"] = 100_000
        # 合法增量 200MiB + 额外一份 90.6MiB ≈ 291MiB > min(320, 192+90.6=282.6)
        info["victim_rss_max_in_window_kb"] = 100_000 + 291 * 1024
        self.assertTrue(any("净增量 RSS" in e for e in ls.c4_errors(
            args(min_snapshot_bytes=0), info, True)))

    def test_rss_uses_vmhwm_when_window_sample_lower(self):
        """窗口采样漏采峰值时用 VmHWM：window 150MiB 但 VmHWM 350MiB → 超预算判失败。"""
        info = good_info()
        info["victim_rss_max_in_window_kb"] = 250_000   # delta 150MiB（看起来 OK）
        info["victim_peak_rss_kb"] = 700_000            # delta 600MiB（真实峰值 > auto 430）
        self.assertTrue(any("净增量 RSS" in e for e in ls.c4_errors(args(), info, True)))

class C3AssertionsTest(unittest.TestCase):
    def test_good_case(self):
        self.assertEqual(
            ls.c3_errors(args(), stats(leader_kills=5, leader_kill_targets_verified=5,
                                       observed_leader_changes=5)), [])

    def test_no_kill_when_requested(self):
        self.assertTrue(any("一次都没杀到" in e for e in
                            ls.c3_errors(args(), stats(leader_kill_target_not_leader=9))))

    def test_unverified_target(self):
        errs = ls.c3_errors(args(), stats(leader_kills=5, leader_kill_targets_verified=3,
                                          observed_leader_changes=5))
        self.assertTrue(any("确认目标是 leader" in e for e in errs))

    def test_missing_leader_change(self):
        errs = ls.c3_errors(args(), stats(leader_kills=5, leader_kill_targets_verified=5,
                                          observed_leader_changes=1, leader_change_missing=4))
        self.assertTrue(any("leader 变化" in e for e in errs))

    def test_zero_kills_when_not_requested(self):
        self.assertEqual(ls.c3_errors(args(leader_kills=0), stats()), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
