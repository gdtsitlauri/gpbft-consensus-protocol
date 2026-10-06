#!/usr/bin/env python3
"""Adds GPBFT to a SymBChainSim checkout.

    git clone https://github.com/GiorgDiama/SymBChainSim.git
    git -C SymBChainSim checkout 8241944        # version GPBFT was developed and evaluated on
    python install.py SymBChainSim

Copies GPBFT/ into src/Simulator/Chain/Consensus/GPBFT, registers the protocol
in Protocols.py, loads its configuration in Parameters.py and adds it to the
consensus section of the config files. Running it twice is harmless.
"""
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def patch(path: Path, anchor: str, addition: str, marker: str) -> None:
    text = path.read_text(encoding="utf-8")
    if marker in text:
        return
    if anchor not in text:
        sys.exit(f"{path}: expected text not found:\n{anchor}")
    path.write_text(text.replace(anchor, anchor + addition, 1), encoding="utf-8")


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    root = Path(sys.argv[1]).resolve()
    sim = root / "src" / "Simulator"
    if not (sim / "Blockchain.py").exists():
        sys.exit(f"{root} is not a SymBChainSim checkout")

    dest = sim / "Chain" / "Consensus" / "GPBFT"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(HERE / "GPBFT", dest, ignore=shutil.ignore_patterns("__pycache__"))

    patch(sim / "Chain" / "Consensus" / "Protocols.py",
          "from Chain.Consensus.Tendermint.TM_state import Tendermint\n",
          "from Chain.Consensus.GPBFT.GPBFT_state import GPBFT\n", "GPBFT_state")
    patch(sim / "Chain" / "Consensus" / "Protocols.py",
          "    Tendermint.NAME: Tendermint,\n", "    GPBFT.NAME: GPBFT,\n", "GPBFT.NAME")
    patch(sim / "Parameters.py", "    PBFT = {}\n", "    GPBFT = {}\n", "GPBFT = {}")
    patch(sim / "Parameters.py",
          '        Parameters.Tendermint = read_yaml(params["consensus"]["Tendermint"])\n',
          '        Parameters.GPBFT = read_yaml(params["consensus"].get("GPBFT", "Chain/Consensus/GPBFT/GPBFT_config.yaml"))\n',
          'Parameters.GPBFT = read_yaml')
    for cfg in (root / "src" / "Configs").glob("*.yaml"):
        text = cfg.read_text(encoding="utf-8")
        if "Tendermint: Chain/Consensus/Tendermint/TM_config.yaml" in text and "GPBFT:" not in text:
            patch(cfg, "  Tendermint: Chain/Consensus/Tendermint/TM_config.yaml\n",
                  "  GPBFT: Chain/Consensus/GPBFT/GPBFT_config.yaml\n", "GPBFT:")
    print(f"GPBFT installed in {root}")
    print(f"try: cd {sim} && python Blockchain.py --cp GPBFT")


if __name__ == "__main__":
    main()
