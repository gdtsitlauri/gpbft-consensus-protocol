# GPBFT Consensus Protocol

An implementation of GPBFT (Group-based Practical Byzantine Fault Tolerance), an enhancement over PBFT with group structuring and group signatures. Designed to reduce communication overhead and increase scalability in distributed consensus systems.

This protocol was tested in the [SymBChainSim](https://github.com/GiorgDiama/SymBChainSim) blockchain simulation environment.

## 📂 Contents

- `GPBFT.py` – Main implementation of the GPBFT protocol
- `GPBFT_config.yaml` – Configuration for nodes and parameters
- `Testings.pdf` – Testing scenarios and logs
- `GPBFT.pdf` – Full project documentation and design analysis

## 🛠️ Technologies

- Python 3.x
- YAML configuration
- SymBChainSim (external simulator)

## 🚀 How to Run

1. Adjust node parameters in `GPBFT_config.yaml`
2. Run the protocol with:
   ```bash
   python3 GPBFT.py
3. Simulation and verification can be done using SymBChainSim
