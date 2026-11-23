"""
Micro-benchmark of the admit.lua script: Redis server time per call, and
pipelined throughput, with sticky sessions off and in each sticky outcome.

Runs against the local Redis (make db-up) and FLUSHES IT: never point it at a
shared instance.

    uv run python scripts/bench_admit.py
    uv run python scripts/bench_admit.py --baseline-ref firstv2   # + pre-sticky script

`usec/call` is Redis' own INFO commandstats figure for EVALSHA, measured over
the admit phase only (settles run afterwards, outside the measurement).  On the
single-threaded Redis core, 1e6 / usec_per_call is the admit ceiling.  The
pipelined admits/s figure includes this Python client and is a lower bound.

Scenarios (each admit walks a shuffled list of --candidates backends):
  baseline  the script at --baseline-ref, pre-sticky KEYS/ARGV layout
  off       sticky_ttl_sec=0
  new       no session yet: walk router order, SET the pin
  hit       session pinned to a random candidate with capacity: GET, scan, probe
  repinned  session pinned to a backend no longer in the candidate list
  spill     session pinned to a full candidate: probe it, then walk
"""

import argparse
import random
import subprocess
import time
from pathlib import Path

from redis import Redis

from first_gateway import Settings
from first_gateway.database.redis.admission import (
    _ADMIT_LUA,
    _SETTLE_LUA,
    _STICKY_OFF_KEY,
)
from first_gateway.database.redis.keys import Keys

MODEL = "bench/model"
ADMIT_LUA_PATH = "packages/gateway/first_gateway/database/redis/lua/admit.lua"
STICKY_TTL_SEC = 600
FULL_BACKEND = "b-full"


def _backends(n: int) -> list[str]:
    return [f"b{i}" for i in range(n)]


def _admit_call(
    scenario: str, rid: str, user: str, candidates: list[str]
) -> tuple[list[str], list[str | int | float]]:
    legacy = scenario == "baseline"
    sticky = scenario not in ("baseline", "off")
    keys: list[str] = [
        Keys.user_rate_limit(MODEL, user, "tokens"),
        Keys.user_rate_limit(MODEL, user, "rpm"),
        Keys.user_inflight(MODEL, user),
        Keys.model_inflight(MODEL),
        Keys.model_rejects(MODEL),
        Keys.deadlines(),
        Keys.reservation(rid),
    ]
    if not legacy:
        keys.append(
            Keys.sticky_session(MODEL, None, user, "s") if sticky else _STICKY_OFF_KEY
        )
    for uid in candidates:
        keys.append(Keys.backend_errors(uid))
        keys.append(Keys.backend_inflight(MODEL, uid))
    # Quotas are on (the GCRA buckets are evaluated) but never bind.
    args: list[str | int | float] = [
        rid,
        MODEL,
        user,
        500,  # est_tokens
        1_000_000,  # max_user_concurrency
        1e9,  # tokens_per_sec
        1e12,  # burst_tokens
        1e6,  # requests_per_sec
        1e9,  # burst_requests
        30,  # lease_sec
    ]
    if not legacy:
        args.append(STICKY_TTL_SEC if sticky else 0)
    for uid in candidates:
        args.extend([uid, 0 if uid == FULL_BACKEND else 1_000_000, 3])
    return keys, args


def _seed_sessions(
    r: Redis, scenario: str, users: list[str], backends: list[str]
) -> None:
    pipe = r.pipeline(transaction=False)
    for user in users:
        key = Keys.sticky_session(MODEL, None, user, "s")
        if scenario == "hit":
            pipe.set(key, random.choice(backends), ex=STICKY_TTL_SEC)
        elif scenario == "repinned":
            pipe.set(key, "b-retired", ex=STICKY_TTL_SEC)
        elif scenario == "spill":
            pipe.set(key, FULL_BACKEND, ex=STICKY_TTL_SEC)
    pipe.execute()


def run(
    r: Redis,
    scenario: str,
    admit_sha: str,
    settle_sha: str,
    *,
    n: int,
    batch: int,
    num_users: int,
    num_candidates: int,
) -> tuple[float, float]:
    r.flushdb()
    # An admit re-pins its session, so scenarios measuring a first encounter
    # (no session, or a pin to an unusable backend) give each request its own
    # user; the rest share --users users.
    first_encounter = scenario in ("new", "repinned", "spill")
    users = [f"u{i}" for i in range(n if first_encounter else num_users)]
    backends = _backends(num_candidates)
    if scenario == "spill":
        backends = backends[:-1] + [FULL_BACKEND]
    _seed_sessions(r, scenario, users, backends)

    calls = []
    for i in range(n):
        candidates = backends[:]
        random.shuffle(candidates)
        if scenario == "spill":
            # Keep the full backend off the front so its probe is extra work.
            candidates.remove(FULL_BACKEND)
            candidates.append(FULL_BACKEND)
        calls.append(
            (f"r{i}", users[i % len(users)])
            + _admit_call(scenario, f"r{i}", users[i % len(users)], candidates)
        )

    r.config_resetstat()
    t0 = time.perf_counter()
    chosen: list[tuple[str, str, str]] = []
    for start in range(0, n, batch):
        pipe = r.pipeline(transaction=False)
        chunk = calls[start : start + batch]
        for _, _, keys, args in chunk:
            pipe.evalsha(admit_sha, len(keys), *keys, *args)
        for (rid, user, _, _), res in zip(chunk, pipe.execute()):
            assert int(res[0]) == 1, (scenario, res)
            if scenario != "baseline":
                expected = "hit" if scenario == "hit" else scenario
                if scenario == "spill":
                    expected = "repinned"
                assert res[2] == expected, (scenario, res)
            chosen.append((rid, user, res[1]))
    elapsed = time.perf_counter() - t0
    stats = r.info("commandstats")["cmdstat_evalsha"]
    usec_per_call = float(stats["usec_per_call"])

    for start in range(0, n, batch):
        pipe = r.pipeline(transaction=False)
        for rid, user, backend in chosen[start : start + batch]:
            keys = [
                Keys.reservation(rid),
                Keys.deadlines(),
                Keys.backend_inflight(MODEL, backend),
                Keys.user_inflight(MODEL, user),
                Keys.user_rate_limit(MODEL, user, "tokens"),
                Keys.model_inflight(MODEL),
                Keys.usage_ewma(MODEL, user, "input_token_chars"),
                Keys.usage_ewma(MODEL, user, "output_token"),
            ]
            pipe.evalsha(settle_sha, len(keys), *keys, 0, rid, "", "", 0.3, 60)
        pipe.execute()
    return usec_per_call, n / elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--n", type=int, default=50_000, help="admits per run")
    parser.add_argument("--batch", type=int, default=500, help="pipeline depth")
    parser.add_argument("--users", type=int, default=500)
    parser.add_argument("--candidates", type=int, default=8)
    parser.add_argument("--repeat", type=int, default=3, help="runs per scenario")
    parser.add_argument(
        "--baseline-ref", help="git ref whose admit.lua to bench as 'baseline'"
    )
    parser.add_argument("--redis-url", default=Settings().redis_url)
    opts = parser.parse_args()

    r = Redis.from_url(opts.redis_url, decode_responses=True)
    settle_sha = r.script_load(_SETTLE_LUA)
    shas = {"off": r.script_load(_ADMIT_LUA)}
    scenarios = ["off", "new", "hit", "repinned", "spill"]
    if opts.baseline_ref:
        root = Path(__file__).parents[1]
        legacy = subprocess.run(
            ["git", "show", f"{opts.baseline_ref}:{ADMIT_LUA_PATH}"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        shas["baseline"] = r.script_load(legacy)
        scenarios.insert(0, "baseline")

    print(
        f"{opts.n:,} admits/run, pipeline {opts.batch}, {opts.users} users, "
        f"{opts.candidates} candidates, best of {opts.repeat}\n"
    )
    print(f"{'scenario':<10} {'usec/call':>10} {'ceiling/s':>10} {'pipelined/s':>12}")
    for scenario in scenarios:
        best_usec, best_rate = float("inf"), 0.0
        for _ in range(opts.repeat):
            usec, rate = run(
                r,
                scenario,
                shas.get(scenario, shas["off"]),
                settle_sha,
                n=opts.n,
                batch=opts.batch,
                num_users=opts.users,
                num_candidates=opts.candidates,
            )
            best_usec, best_rate = min(best_usec, usec), max(best_rate, rate)
        print(
            f"{scenario:<10} {best_usec:>10.2f} {1e6 / best_usec:>10,.0f} "
            f"{best_rate:>12,.0f}"
        )
    r.flushdb()


if __name__ == "__main__":
    main()
