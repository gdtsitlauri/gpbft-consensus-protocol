# Aggregation window (GPBFT-2, fault-free)

A manager sends its group aggregate when the whole group has voted or when the
aggregation window expires. A longer window means fewer partial aggregates
(fewer messages) but a longer wait (higher latency). Same setup as results.md.

| window | nodes | msgs/block | latency (s) | blocks in 300 s |
| ---: | ---: | ---: | ---: | ---: |
| 0.01 s (default) | 16 | 423.9 | 1.256 | 289 |
| 0.05 s | 16 | 362.2 | 1.349 | 289 |
| 0.1 s | 16 | 312.5 | 1.504 | 271 |
| 0.01 s (default) | 64 | 2951.9 | 1.420 | 284 |
| 0.05 s | 64 | 2053.9 | 1.555 | 268 |
| 0.1 s | 64 | 1631.9 | 1.637 | 255 |
