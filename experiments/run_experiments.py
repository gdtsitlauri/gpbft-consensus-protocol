#!/usr/bin/env python3
"""PBFT vs. GPBFT in SymBChainSim: messages, data volume, throughput, latency.

    python experiments/run_experiments.py <SymBChainSim checkout with GPBFT installed> [--jobs N]

Protocols: PBFT, GPBFT-2 (two managers per group, the default) and GPBFT-1
(one manager per group). Every configuration runs once per seed in SEEDS, with
the default SymBChainSim workload (120 tx/s); the tables give the mean and the
standard deviation over the seeds. Every point-to-point message the network
delivers is counted, by message type; a broadcast to n-1 nodes counts as n-1
messages. Experiments:
  1. fault-free, 4 to 64 nodes
  2. 16 nodes, 3 of them crashing and recovering (behaviour_config.yaml)
  3. GPBFT-2 with longer aggregation windows, 16 and 64 nodes
Writes results/results.csv (one row per run) and results/results.md.
"""
import csv
import io
import json
import os
import random
import statistics as st
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
SIZES = [4, 8, 16, 32, 64]
SIM_TIME = 300
SEEDS = [1837413, 2718281, 3141592, 1414213, 1732050]
VARIANTS = ["PBFT", "GPBFT-2", "GPBFT-1"]
WINDOWS = [0.01, 0.05, 0.1]  # seconds; 0.01 is the default in GPBFT_config.yaml
WINDOW_SIZES = [16, 64]


def run_one(sim_root: Path, variant: str, n: int, seed: int, faulty: int = 0, window: float = 0.0) -> dict:
    """Runs one simulation in a fresh interpreter (the simulator keeps global state)."""
    args = [sys.executable, __file__, str(sim_root), "--one", variant, str(n), str(seed), str(faulty), str(window)]
    r = subprocess.run(args, capture_output=True, text=True)
    if r.returncode:
        sys.exit(f"{variant} n={n} seed={seed} failed:\n{r.stderr[-3000:]}")
    return json.loads(r.stdout.strip().splitlines()[-1])


def simulate(variant: str, n: int, seed: int, faulty: int, window: float) -> dict:
    cp = variant.split("-")[0]
    import numpy as np
    from Manager.Manager import Manager
    from Utils.Metrics import Metrics
    from Chain.Network import Network

    counts, volume = {}, {}
    original = Network._message.__func__ if hasattr(Network._message, "__func__") else Network._message

    def counting(sender, receiver, msg):
        kind = msg.payload.get("type", "?")
        if kind in ("vote", "group_aggregate", "certificate"):
            kind = f"{kind}:{msg.payload['phase']}"
        counts[kind] = counts.get(kind, 0) + 1
        volume[kind] = volume.get(kind, 0.0) + Network.size(msg)
        original(sender, receiver, msg)

    Network._message = staticmethod(counting)
    try:
        random.seed(seed)
        np.random.seed(seed)
        overrides = {
            "simulation.init_cp": cp, "application.num_nodes": n, "simulation.sim_time": SIM_TIME,
            "simulation.snapshot_interval": -1, "simulation.print_every": 10**9,
            "reconfiguration.reconfigure": False,
            "network.gossip": False,  # direct links: point-to-point messages are not flooded
        }
        if cp == "GPBFT":
            overrides["GPBFT.managers_per_group"] = int(variant.split("-")[1])
            if window:
                overrides["GPBFT.aggregation_window"] = window
        if faulty:  # behaviour_config.yaml: 3 faulty nodes that crash and recover
            overrides["behaviour.use"] = True
            overrides["behaviour.print_updates"] = False
        manager = Manager()
        manager.load_params("base.yaml", overrides)
        manager.set_up()
        with redirect_stdout(io.StringIO()):
            manager.run()
            Metrics.measure_all(manager.sim)
        nodes = [x.id for x in manager.sim.nodes]
        blocks = min(x.blockchain_length() for x in manager.sim.nodes)
        mean = lambda vals: st.mean(v for v in vals if v is not None) if any(v is not None for v in vals) else 0.0
        consensus = {k: v for k, v in counts.items() if k != "round_change"}
        return {
            "protocol": variant, "nodes": n, "faulty": faulty, "window_s": window, "seed": seed, "blocks": blocks,
            "throughput_tx_s": round(mean([Metrics.throughput[i] for i in nodes]), 2),
            "latency_s": round(mean([Metrics.latency[i]["AVG"] for i in nodes]), 3),
            "block_time_s": round(mean([Metrics.blocktime[i]["AVG"] for i in nodes]), 3),
            "messages": sum(counts.values()),
            "messages_per_block": round(sum(consensus.values()) / max(blocks, 1), 1),
            "MB_per_block": round(sum(v for k, v in volume.items() if k != "round_change") / max(blocks, 1), 3),
            "round_changes": counts.get("round_change", 0),
            "by_type": ";".join(f"{k}={v}" for k, v in sorted(counts.items())),
        }
    finally:
        Network._message = staticmethod(original)


def ms(vals, digits=1):
    """mean ± standard deviation over the seeds"""
    m = st.mean(vals)
    s = st.stdev(vals) if len(vals) > 1 else 0.0
    return f"{m:.{digits}f} ± {s:.{digits}f}"


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    sim_root = Path(sys.argv[1]).resolve()
    if len(sys.argv) == 8 and sys.argv[2] == "--one":
        sim_dir = sim_root / "src" / "Simulator"
        os.chdir(sim_dir)
        sys.path.insert(0, str(sim_dir))
        variant, n, seed, faulty, window = sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]), float(sys.argv[7])
        with redirect_stdout(io.StringIO()):
            res = simulate(variant, n, seed, faulty, window)
        print(json.dumps(res))
        return
    jobs = int(sys.argv[sys.argv.index("--jobs") + 1]) if "--jobs" in sys.argv else (os.cpu_count() or 2)

    configs = [(v, n, 0, 0.0) for n in SIZES for v in VARIANTS]
    configs += [(v, 16, 3, 0.0) for v in VARIANTS]
    configs += [("GPBFT-2", n, 0, w) for n in WINDOW_SIZES for w in WINDOWS if w != WINDOWS[0]]
    tasks = [(c, seed) for c in configs for seed in SEEDS]
    print(f"{len(tasks)} simulations, {jobs} at a time", flush=True)

    def work(task):
        (v, n, faulty, window), seed = task
        r = run_one(sim_root, v, n, seed, faulty, window)
        print(f"{v:8s} n={n:3d} faulty={faulty} window={window or 'default'} seed={seed}  blocks={r['blocks']}  "
              f"msgs/block={r['messages_per_block']}  latency={r['latency_s']}s", flush=True)
        return r

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        rows = list(pool.map(work, tasks))

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    def runs(v, n, faulty=0, window=0.0):
        return [r for r in rows if r["protocol"] == v and r["nodes"] == n and r["faulty"] == faulty and r["window_s"] == window]

    def col(v, n, key, faulty=0, window=0.0):
        return [r[key] for r in runs(v, n, faulty, window)]

    md = ["# PBFT vs. GPBFT in SymBChainSim", "",
          f"Generated by `experiments/run_experiments.py`: {SIM_TIME} s of simulated time per run, "
          f"{len(SEEDS)} seeds per configuration ({', '.join(map(str, SEEDS))}); every cell is the mean ± the "
          "standard deviation over the seeds. Default SymBChainSim workload (120 tx/s) and network model, with "
          "gossip off for all protocols (direct links, as PBFT assumes; with gossip on, every point-to-point message "
          "would be flooded). GPBFT-2 has two managers per group (default), GPBFT-1 one. Messages are point-to-point "
          "deliveries; round-change messages are excluded from the per-block figures. In SymBChainSim's PBFT, "
          "prepare and commit messages carry the whole block, while GPBFT votes carry only the block id, so the "
          "MB/block column favours GPBFT beyond the message count. One row per run: `results.csv`.", "",
          "## Messages per block (fault-free)", "",
          "| nodes | PBFT | GPBFT-2 | change | GPBFT-1 | change |", "| ---: | ---: | ---: | ---: | ---: | ---: |"]
    change = lambda p, g: f"{100 * (st.mean(g) / st.mean(p) - 1):+.0f}%"
    for n in SIZES:
        p, g2, g1 = (col(v, n, "messages_per_block") for v in VARIANTS)
        md.append(f"| {n} | {ms(p)} | {ms(g2)} | {change(p, g2)} | {ms(g1)} | {change(p, g1)} |")
    md += ["", "## Data, latency and throughput (fault-free)", "",
           "| nodes | protocol | MB/block | latency (s) | block time (s) | tx/s | blocks |",
           "| ---: | --- | ---: | ---: | ---: | ---: | ---: |"]
    for n in SIZES:
        for v in VARIANTS:
            md.append(f"| {n} | {v} | {ms(col(v, n, 'MB_per_block'), 2)} | {ms(col(v, n, 'latency_s'), 3)} | "
                      f"{ms(col(v, n, 'block_time_s'), 3)} | {ms(col(v, n, 'throughput_tx_s'))} | {ms(col(v, n, 'blocks'), 0)} |")
    md += ["", "## 16 nodes, 3 of them crashing and recovering", "",
           "| protocol | blocks | tx/s | latency (s) | round-change messages |", "| --- | ---: | ---: | ---: | ---: |"]
    for v in VARIANTS:
        md.append(f"| {v} | {ms(col(v, 16, 'blocks', 3), 0)} | {ms(col(v, 16, 'throughput_tx_s', 3))} | "
                  f"{ms(col(v, 16, 'latency_s', 3), 3)} | {ms(col(v, 16, 'round_changes', 3), 0)} |")
    md += ["", "## Aggregation window (GPBFT-2, fault-free)", "",
           "A manager sends its group aggregate when the whole group has voted or when the aggregation window "
           "expires. A longer window means fewer partial aggregates (fewer messages) but a longer wait.", "",
           "| nodes | window (s) | msgs/block | latency (s) | blocks |", "| ---: | ---: | ---: | ---: | ---: |"]
    for n in WINDOW_SIZES:
        for wdw in WINDOWS:
            key = 0.0 if wdw == WINDOWS[0] else wdw
            label = f"{wdw} (default)" if wdw == WINDOWS[0] else f"{wdw}"
            md.append(f"| {n} | {label} | {ms(col('GPBFT-2', n, 'messages_per_block', 0, key))} | "
                      f"{ms(col('GPBFT-2', n, 'latency_s', 0, key), 3)} | {ms(col('GPBFT-2', n, 'blocks', 0, key), 0)} |")
    md += ["", f"## Messages by type (16 nodes, fault-free, whole run, seed {SEEDS[0]})", ""]
    for v in VARIANTS:
        r = next(x for x in runs(v, 16) if x["seed"] == SEEDS[0])
        md.append(f"- {v}: " + ", ".join(r["by_type"].split(";")))
    (out / "results.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()
