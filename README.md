# GPBFT Consensus Protocol

**Can grouping nodes and aggregating their votes cut the communication of PBFT without losing its throughput or its tolerance to failures?**

GPBFT (group-based Practical Byzantine Fault Tolerance) is implemented here as a consensus protocol of the
[SymBChainSim](https://github.com/GiorgDiama/SymBChainSim) blockchain simulator and compared with the
simulator's own PBFT. The design follows Yu et al., "GPBFT: A Practical Byzantine Fault-Tolerant Consensus
Algorithm Based on Dual Administrator Short Group Signatures", *Security and Communication Networks*, 2022
([doi:10.1155/2022/8311821](https://doi.org/10.1155/2022/8311821)): group managers aggregate the votes of
their groups with short group signatures.

| part | what it does |
| --- | --- |
| groups | nodes are split into groups of about sqrt(n); each group has two managers per round (one with `managers_per_group: 1`), rotating every round |
| votes | instead of broadcasting prepare and commit votes to everyone (about 2n^2 messages per block in PBFT), nodes send them to their group managers |
| group signatures | a manager aggregates the votes of its group into one group signature (modelled as a signed set of voter ids, with signing and verification delays) and sends it to the managers of the other groups only |
| certificates | a manager holding 2f prepare votes (prepared) or 2f+1 commit votes (committed) sends one certificate to its members |
| aggregation window | a manager does not wait forever for a slow or crashed member: after a short window it sends a partial aggregate, and later votes follow |
| failures | the second manager keeps a group working when one manager crashes; if a whole round fails, the round-change protocol of SymBChainSim moves to the next round, with other managers |

## Main results

300 s of simulated time per run, 120 tx/s, the default SymBChainSim network model with direct links (gossip
off for all protocols, as PBFT assumes). All numbers are in `results/results.md`.

| nodes | PBFT msgs/block | GPBFT-2 msgs/block | GPBFT-1 msgs/block | PBFT latency (s) | GPBFT-2 latency (s) | GPBFT-1 latency (s) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 27.0 | 43.3 | 19.2 | 1.13 | 0.97 | 1.14 |
| 8 | 119.0 | 141.5 | 56.9 | 1.20 | 1.10 | 1.22 |
| 16 | 496.6 | 423.9 | 155.2 | 1.31 | 1.26 | 1.73 |
| 32 | 2015.0 | 1198.7 | 411.5 | 1.20 | 1.49 | 1.84 |
| 64 | 8127.0 | 2951.9 | 970.9 | 1.26 | 1.42 | 1.78 |

1. **Grouping cuts messages more as the network grows.** With one manager per group (GPBFT-1), messages per
   block fall by 29% at 4 nodes and by 88% at 64 nodes. With two managers (GPBFT-2) the duplicated votes
   and aggregates cost more: GPBFT-2 sends more messages than PBFT up to 8 nodes, 15% fewer at 16 and 64%
   fewer at 64 nodes.
2. **Throughput is unchanged.** Every protocol keeps up with the 120 tx/s workload at every size.
3. **Latency grows with the extra hops.** Votes travel member, manager, other managers, members, so a round
   takes longer: at 64 nodes, 1.42 s (GPBFT-2) and 1.78 s (GPBFT-1) against 1.26 s for PBFT. A longer
   aggregation window trades messages for latency (`results/aggregation_window.md`).
4. **Two managers per group are needed under failures.** With 3 of 16 nodes crashing and recovering,
   GPBFT-1 confirms only 58 blocks, because a crashed manager leaves its group without certificates;
   GPBFT-2 confirms 138 blocks against 112 for PBFT, with the same latency (7.0 s).

## Limitations (reported as such)

- Group signatures are modelled, not computed: an aggregate is a signed set of voter ids with fixed signing and
  verification delays, and its size is that of the id set, not of a real short group signature.
- Of the paper's design, the grouping, the two managers per group and the vote aggregation are implemented.
  The credit-based choice of the primary and the managers by a certification authority, and the Trace phase
  that identifies and revokes malicious nodes, are not.
- Only crash faults are evaluated; SymBChainSim has no model of Byzantine behaviour yet, so a manager that
  forges or withholds aggregates is not simulated.
- The comparison uses direct links. With gossip on (the SymBChainSim default), every point-to-point message
  would be flooded through the network and the grouping would lose its benefit.
- In SymBChainSim's PBFT, prepare and commit messages carry the whole block, while GPBFT votes carry only the
  block id, so the data-volume figures favour GPBFT beyond the message counts. The message counts are the
  fair comparison.

## Folder map

```
gpbft-consensus-protocol/
  GPBFT/                    the protocol: GPBFT_state.py, GPBFT_transition.py, GPBFT_messages.py,
                            GPBFT_timeouts.py, GPBFT_config.yaml
  install.py                adds GPBFT to a SymBChainSim checkout
  experiments/              run_experiments.py (PBFT vs. GPBFT-2 vs. GPBFT-1)
  results/                  results.md and results.csv, aggregation_window.md
  legacy/                   the first version and its documents
```

## Running

Python 3.12 or later with `pyyaml`, `numpy` and `matplotlib`.

```bash
git clone https://github.com/GiorgDiama/SymBChainSim.git
git -C SymBChainSim checkout 8241944            # version used for the results
python install.py SymBChainSim
cd SymBChainSim/src/Simulator
python Blockchain.py --cp GPBFT --set network.gossip=false --set application.num_nodes=16
```

Reproducing all results (about three minutes):

```bash
python experiments/run_experiments.py SymBChainSim
```

## The first version

`legacy/` holds the first version of this project. It was written for an older SymBChainSim API and,
although it declared `group_sign` and `trace` phases, nothing in it sent those messages, so it behaved as
PBFT; its test notes drew conclusions from an always-empty list of group signatures. The current version
implements the grouping, aggregation and certificates and measures them.

## Author

George David Tsitlauri, University of Thessaly.
