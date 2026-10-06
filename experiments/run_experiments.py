#!/usr/bin/env python3
"""PBFT vs. GPBFT in SymBChainSim: messages, data volume, throughput, latency.

    python experiments/run_experiments.py <SymBChainSim checkout with GPBFT installed>

Protocols: PBFT, GPBFT-2 (two managers per group, the default) and GPBFT-1
(one manager per group). For every network size and protocol, runs one
simulation with the default SymBChainSim workload (120 tx/s) and counts every
point-to-point message that the network delivers, by message type; a broadcast
to n-1 nodes counts as n-1 messages. A second experiment lets 3 of 16 nodes
crash and recover. Writes results/results.csv and results/results.md.
"""
import csv
import io
import json
import os
import subprocess
import random
import statistics as st
import sys
from contextlib import redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
SIZES = [4, 8, 16, 32, 64]
SIM_TIME = 300
SEED = 1837413
VARIANTS = ["PBFT", "GPBFT-2", "GPBFT-1"]


def run_one(sim_dir: Path, cp: str, n: int, faulty: int = 0) -> dict:
    """Runs one simulation in a fresh interpreter (the simulator keeps global state)."""
    r = subprocess.run([sys.executable, __file__, str(sim_dir.parent.parent), "--one", cp, str(n), str(faulty)],
                       capture_output=True, text=True)
    if r.returncode:
        sys.exit(f"{cp} n={n} failed:\n{r.stderr[-3000:]}")
    return json.loads(r.stdout.strip().splitlines()[-1])


def simulate(variant: str, n: int, faulty: int = 0) -> dict:
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
        random.seed(SEED)
        np.random.seed(SEED)
        overrides = {
            "simulation.init_cp": cp, "application.num_nodes": n, "simulation.sim_time": SIM_TIME,
            "simulation.snapshot_interval": -1, "simulation.print_every": 10**9,
            "reconfiguration.reconfigure": False,
            "network.gossip": False,  # direct links: point-to-point messages are not flooded
        }
        if cp == "GPBFT":
            overrides["GPBFT.managers_per_group"] = int(variant.split("-")[1])
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
        consensus = {k: v for k, v in counts.items() if k not in ("round_change",)}
        return {
            "protocol": variant, "nodes": n, "faulty": faulty, "blocks": blocks,
            "throughput_tx_s": round(mean([Metrics.throughput[i] for i in nodes]), 1),
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


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    sim_dir = Path(sys.argv[1]).resolve() / "src" / "Simulator"
    if len(sys.argv) == 6 and sys.argv[2] == "--one":
        os.chdir(sim_dir)
        sys.path.insert(0, str(sim_dir))
        with redirect_stdout(io.StringIO()):
            res = simulate(sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))
        print(json.dumps(res))
        return
    rows = []
    for n in SIZES:
        for v in VARIANTS:
            r = run_one(sim_dir, v, n)
            rows.append(r)
            print(f"{v:8s} n={n:3d}  blocks={r['blocks']:5d}  msgs/block={r['messages_per_block']:8.1f}  "
                  f"MB/block={r['MB_per_block']:8.3f}  tx/s={r['throughput_tx_s']:6.1f}  latency={r['latency_s']:.3f}s", flush=True)
    for v in VARIANTS:  # crash faults: 3 of 16 nodes crash and recover repeatedly
        r = run_one(sim_dir, v, 16, faulty=3)
        rows.append(r)
        print(f"{v:8s} n= 16 with 3 crashing nodes  blocks={r['blocks']}  tx/s={r['throughput_tx_s']}  "
              f"latency={r['latency_s']}s  round changes={r['round_changes']}", flush=True)

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    def find(v, n, faulty=0):
        return next(r for r in rows if r["protocol"] == v and r["nodes"] == n and r["faulty"] == faulty)

    md = ["# PBFT vs. GPBFT in SymBChainSim", "",
          f"Generated by `experiments/run_experiments.py`: {SIM_TIME} s of simulated time per run, seed {SEED}, "
          "default SymBChainSim workload (120 tx/s) and network model, with gossip off for all protocols "
          "(direct links, as PBFT assumes; with gossip on, every point-to-point message would be flooded). "
          "GPBFT-2 has two managers per group (default), GPBFT-1 one. Messages are point-to-point deliveries; "
          "round-change messages are excluded from the per-block figures. In SymBChainSim's PBFT, prepare and "
          "commit messages carry the whole block, while GPBFT votes carry only the block id, so the MB/block "
          "column favours GPBFT beyond the message count.", "",
          "## Messages per block (fault-free)", "",
          "| nodes | PBFT | GPBFT-2 | reduction | GPBFT-1 | reduction |", "| ---: | ---: | ---: | ---: | ---: | ---: |"]
    red = lambda p, g: 100 * (1 - g / p) if p else 0
    for n in SIZES:
        p, g2, g1 = find("PBFT", n), find("GPBFT-2", n), find("GPBFT-1", n)
        md.append(f"| {n} | {p['messages_per_block']} | {g2['messages_per_block']} | {red(p['messages_per_block'], g2['messages_per_block']):.0f}% | "
                  f"{g1['messages_per_block']} | {red(p['messages_per_block'], g1['messages_per_block']):.0f}% |")
    md += ["", "## Data, latency and throughput (fault-free)", "",
           "| nodes | protocol | MB/block | latency (s) | block time (s) | tx/s | blocks |", "| ---: | --- | ---: | ---: | ---: | ---: | ---: |"]
    for n in SIZES:
        for v in VARIANTS:
            r = find(v, n)
            md.append(f"| {n} | {v} | {r['MB_per_block']} | {r['latency_s']} | {r['block_time_s']} | {r['throughput_tx_s']} | {r['blocks']} |")
    md += ["", "## 16 nodes, 3 of them crashing and recovering", "",
           "| protocol | blocks | tx/s | latency (s) | round-change messages |", "| --- | ---: | ---: | ---: | ---: |"]
    for v in VARIANTS:
        r = find(v, 16, 3)
        md.append(f"| {v} | {r['blocks']} | {r['throughput_tx_s']} | {r['latency_s']} | {r['round_changes']} |")
    md += ["", "## Messages by type (16 nodes, fault-free, whole run)", ""]
    for v in VARIANTS:
        md.append(f"- {v}: " + ", ".join(find(v, 16)["by_type"].split(";")))
    (out / "results.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
