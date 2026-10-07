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
off for all protocols, as PBFT assumes). Every configuration ran with 5 seeds; the numbers are means, with
the standard deviation over the seeds where it matters. All tables are in `results/results.md`, every run in
`results/results.csv`.

| nodes | PBFT msgs/block | GPBFT-2 msgs/block | GPBFT-1 msgs/block | PBFT latency (s) | GPBFT-2 latency (s) | GPBFT-1 latency (s) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 27.1 | 44.1 | 19.6 | 1.12 | 0.99 | 1.21 |
| 8 | 119.1 | 143.0 | 57.0 | 1.14 | 1.04 | 1.17 |
| 16 | 495.5 | 409.5 | 151.1 | 1.23 | 1.19 | 1.50 |
| 32 | 2017.7 | 1184.4 | 403.3 | 1.22 | 1.28 | 1.69 |
| 64 | 8129.8 | 2977.9 | 974.8 | 1.24 | 1.38 | 1.76 |

1. **Grouping cuts messages more as the network grows.** With one manager per group (GPBFT-1), messages per
   block fall by 28% at 4 nodes and by 88% at 64 nodes. With two managers (GPBFT-2) the duplicated votes
   and aggregates cost more: GPBFT-2 sends more messages than PBFT up to 8 nodes (+63% at 4, +20% at 8),
   17% fewer at 16 and 63% fewer at 64 nodes. Their standard deviation over the seeds is at most 3% of the
   mean.
2. **Transactions keep flowing, but GPBFT-1 makes fewer, larger blocks.** Every protocol keeps up with the
   120 tx/s workload at every size. From 16 nodes on, however, GPBFT-1 takes 1.1 to 1.3 s per block instead
   of about 1.0 s and confirms 234 to 266 blocks in 300 s against about 298 for PBFT; GPBFT-2 stays within
   4% of PBFT.
3. **Latency grows with the extra hops.** Votes travel member, manager, other managers, members, so a round
   takes longer at scale: at 64 nodes, 1.38 s (GPBFT-2) and 1.76 s (GPBFT-1) against 1.24 s for PBFT. Up to
   16 nodes GPBFT-2 is as fast as PBFT or faster. A longer aggregation window trades messages for latency:
   at 64 nodes, a 0.1 s window cuts GPBFT-2 to 1,639 messages per block at 1.62 s.
4. **Under crashes, two managers per group are needed, and GPBFT-2 is the steadiest.** With 3 of 16 nodes
   crashing and recovering, GPBFT-1 confirms only 73 ± 19 blocks, because a crashed manager leaves its group
   without certificates. GPBFT-2 confirms 142 ± 10 blocks against 124 ± 59 for PBFT, with a latency of
   5.9 ± 1.3 s against 8.4 ± 7.9 s. Because PBFT varies so much from seed to seed, the difference in the
   means is not conclusive; what the runs show is that GPBFT-2 does at least as well as PBFT and varies much
   less.

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
- Five seeds per configuration: enough to show which differences are stable (the message counts) and which
  are not (block counts under crashes), not enough for formal significance tests.

## Folder map

```
gpbft-consensus-protocol/
  GPBFT/                    the protocol: GPBFT_state.py, GPBFT_transition.py, GPBFT_messages.py,
                            GPBFT_timeouts.py, GPBFT_config.yaml
  install.py                adds GPBFT to a SymBChainSim checkout
  experiments/              run_experiments.py: all experiments (sizes, crashes, aggregation window)
  results/                  results.md (tables), results.csv (one row per run)
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

Reproducing all results (110 simulations, a few minutes with several cores):

```bash
python experiments/run_experiments.py SymBChainSim --jobs 8
```

## The first version

`legacy/` holds the first version of this project. It was written for an older SymBChainSim API and,
although it declared `group_sign` and `trace` phases, nothing in it sent those messages, so it behaved as
PBFT; its test notes drew conclusions from an always-empty list of group signatures. The current version
implements the grouping, aggregation and certificates and measures them.

## Author and license

George David Tsitlauri, University of Thessaly. MIT license ([LICENSE](LICENSE)).
