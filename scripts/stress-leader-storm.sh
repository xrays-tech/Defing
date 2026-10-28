#!/usr/bin/env bash
# Thin wrapper for the leader-storm cluster stress harness (scripts/stress/leader_storm.py).
# ALWAYS rebuilds `defing` (debug, incremental) before running — a stale binary silently
# testing old code was the failure mode this guards (only-build-if-missing was wrong).
# Dynamic ports + temp data-dirs are handled inside Python (socket bind 0) — no fixed ports,
# no fixed tmp dirs (avoids collisions with chaos-test.sh / bench.sh / cluster-demo.sh).
#
# Usage: scripts/stress-leader-storm.sh [--profile quick|storm|extreme] [--bin PATH]
#                                       [--no-build] [--keep] [extra flags...]
#   --no-build (or env NO_BUILD=1)  skip cargo build (external pre-build / CI); binary
#                    must already exist & be executable, else exit 2.
#   --bin PATH     explicit binary (must exist & be executable when --no-build; the cargo
#                    build still refreshes the default debug target, PATH wins for the run).
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PY="$HERE/stress/leader_storm.py"
DEFAULT_BIN="$ROOT/server/target/debug/defing"

if [ ! -f "$PY" ]; then
  echo "FAIL: harness not found: $PY" >&2
  exit 2
fi

# resolve --bin for build/validate logic; --no-build consumed here (Python doesn't know it),
# everything else forwarded verbatim.
NO_BUILD="${NO_BUILD:-0}"
BIN=""
prev=""
FWD=()
for a in "$@"; do
  if [ "$prev" = "--bin" ]; then
    BIN="$a"
    prev=""
    FWD+=("$a")
    continue
  fi
  prev="$a"
  if [ "$a" = "--no-build" ]; then
    NO_BUILD=1
    continue
  fi
  FWD+=("$a")
done
[ -n "$BIN" ] || BIN="$DEFAULT_BIN"

if [ "$NO_BUILD" = "1" ]; then
  echo "NO_BUILD=1 — skipping cargo build (external pre-build assumed)"
else
  echo "building defing (debug, incremental) ..."
  if ! ( cd "$ROOT/server" && cargo build --bin defing ); then
    echo "FAIL: cargo build --bin defing failed" >&2
    exit 2
  fi
  if [ "$BIN" != "$DEFAULT_BIN" ]; then
    echo "note: --bin override active — running $BIN (cargo build refreshed $DEFAULT_BIN, unused)"
  fi
fi

if [ ! -x "$BIN" ]; then
  echo "FAIL: binary not executable: $BIN (--no-build requires a pre-built binary; pass --bin or drop --no-build)" >&2
  exit 2
fi

echo "using binary: $BIN (mtime: $(stat -c '%y' "$BIN" 2>/dev/null || echo '?'))"

exec python3 "$PY" "${FWD[@]}"
