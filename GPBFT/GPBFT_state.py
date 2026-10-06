"""
GPBFT: group-based Practical Byzantine Fault Tolerance for SymBChainSim.

Nodes are split into groups of about sqrt(n) nodes. In every round each group
has two managers (the "dual administrators" of GPBFT; one with
managers_per_group: 1), rotating with the round. Instead of broadcasting their
prepare and commit votes to everybody, as in PBFT, nodes send them to their
group managers. A manager aggregates the votes of its group into one group
signature (modelled as a signed set of voter ids) and exchanges it with the
managers of the other groups only. When a manager holds enough votes in total
it sends one certificate to its own members. Messages per round fall from
about 2n^2 in PBFT to O(n + g^2) for g groups; the second manager keeps a
group working when one manager crashes, at the cost of duplicate messages.

Phases of a round
    pre_prepare      primary -> all nodes (block), as in PBFT
    prepare          member -> its managers
    group_prepare    manager -> managers of the other groups (aggregated prepare votes)
    prepared_cert    manager -> its members (2f prepare votes, i.e. prepared)
    commit           member -> its managers
    group_commit     manager -> managers of the other groups (aggregated commit votes)
    commit_cert      manager -> its members (2f+1 commit votes, i.e. committed)
    new_block        primary -> all nodes, as in PBFT

A manager sends its group aggregate when it has the votes of its whole group,
or after a short aggregation window, so that a crashed member cannot block the
group; votes that arrive later are sent in a follow-up aggregate. Managers
rotate every round; if all managers of a group fail, the round-change protocol
of SymBChainSim moves to the next round, which has other managers.
"""

from Parameters import Parameters

from Chain.Block import Block
from Chain.TransactionFactory import TransactionFactory
from Chain.Consensus import Rounds
from Chain.Consensus.GPBFT import GPBFT_transition as state_transitions
from Chain.Consensus.GPBFT import GPBFT_timeouts as timeouts
from Chain.Consensus.GPBFT import GPBFT_messages as messages
from Chain.Consensus import ConsensusProtocol

from Engine.Handler import handle_backlog

from math import ceil, sqrt
from random import randint
from typing import Optional, TYPE_CHECKING, List, Dict, Set
import logging

if TYPE_CHECKING:
    from Engine.Event import Event
    from Node import Node

logger = logging.getLogger(__name__.split(".")[-1])


def group_size(n: int) -> int:
    """Group size: the configured value, or ceil(sqrt(n)) when it is 0."""
    s = Parameters.GPBFT.get("group_size", 0)
    return s if s and s > 0 else max(1, ceil(sqrt(n)))


def groups(n: int) -> List[List[int]]:
    """Partitions node ids 0..n-1 into consecutive groups."""
    s = group_size(n)
    return [list(range(i, min(i + s, n))) for i in range(0, n, s)]


class GPBFT(ConsensusProtocol.ConsensusProtocol):
    """GPBFT protocol state of one node (see the module docstring)."""

    NAME = "GPBFT"

    def __init__(self, node: "Node") -> None:
        self.rounds: Rounds.RoundChangeState = Rounds.init_round_change_state()
        self.state: str = ""
        self.miner: str = ""
        self.msgs: Dict[str, List[str]] = {}
        self.timeout: "Event" = None
        self.block: "Block" = None
        self.node: "Node" = node
        self.reset_group_state()

    # ----------------------------------------------------------- groups
    def my_group(self) -> List[int]:
        for g in groups(Parameters.application["num_nodes"]):
            if self.node.id in g:
                return g
        raise ValueError(f"node {self.node.id} is in no group")

    def managers_of(self, group: List[int]) -> List[int]:
        """The managers of a group in the current round (they rotate with the round)."""
        k = max(1, min(Parameters.GPBFT.get("managers_per_group", 2), len(group)))
        first = self.rounds.round % len(group) if Parameters.GPBFT.get("rotate_managers", True) else 0
        return [group[(first + j) % len(group)] for j in range(k)]

    def my_managers(self) -> List[int]:
        return self.managers_of(self.my_group())

    def is_manager(self) -> bool:
        return self.node.id in self.my_managers()

    def other_group_managers(self) -> List[int]:
        mine = set(self.my_group())
        return [m for g in groups(Parameters.application["num_nodes"]) if not set(g) & mine for m in self.managers_of(g)]

    def reset_group_state(self) -> None:
        # manager-side aggregation state
        self.group_votes: Dict[str, Set[int]] = {"prepare": set(), "commit": set()}   # votes of my group
        self.sent_votes: Dict[str, Set[int]] = {"prepare": set(), "commit": set()}    # already aggregated
        self.all_votes: Dict[str, Set[int]] = {"prepare": set(), "commit": set()}     # all groups
        self.window_open: Dict[str, bool] = {"prepare": False, "commit": False}
        self.cert_sent: Dict[str, bool] = {"prepare": False, "commit": False}
        self.voted: Dict[str, bool] = {"prepare": False, "commit": False}

    # ----------------------------------------------------------- protocol interface
    def set_state(self) -> None:
        self.rounds = Rounds.init_round_change_state()
        self.state = ""
        self.miner = ""
        self.msgs = {"prepare": [], "commit": []}
        self.timeout = None
        self.block = None
        self.reset_group_state()

    def state_to_string(self) -> str:
        return (
            f"round: {self.rounds.round} | node_state: {self.state} | miner: {self.miner} | "
            f"managers: {self.my_managers()} | block: {self.block.id if self.block is not None else -1} | "
            f"prepare votes: {len(self.all_votes['prepare'])} | commit votes: {len(self.all_votes['commit'])}"
        )

    def reset_msgs(self) -> None:
        self.msgs = {"prepare": [], "commit": []}
        self.reset_group_state()
        Rounds.reset_votes(self.node)

    def validate_message(self, event: "Event") -> tuple[bool, Optional[str]]:
        round, current_round = event.payload["round"], self.rounds.round
        if round < current_round:
            return False, None
        elif round == current_round:
            return True, None
        else:
            return True, "backlog"

    def validate_block(self, block: "Block", time: float) -> str:
        result = self.node.validate_block(block)
        match result:
            case "invalid":
                Rounds.change_round(self.node, time)
                return "invalid"
            case "future_block" | "future_conf":
                return "backlog"
            case "valid":
                return "valid"
            case _:
                raise ValueError(f"Node block validation returned unexpected: {result}")

    def init(self, time: float, starting_round: int) -> None:
        self.set_state()
        self.start(time, starting_round)

    def get_miner(self) -> None:
        if Parameters.execution["proposer_selection"] == "round_robin":
            self.miner = self.rounds.round % Parameters.application["num_nodes"]
        elif Parameters.execution["proposer_selection"] == "hash":
            self.miner = (self.node.last_block.id + self.rounds.round) % Parameters.application["num_nodes"]
        else:
            raise ValueError(f"No such 'proposer_selection {Parameters.execution['proposer_selection']}")

    def create_block(self, time: float) -> tuple[Optional["Block"], float]:
        block = Block(
            depth=len(self.node.blockchain),
            id=randint(1, 10000),
            previous=self.node.last_block.id,
            time_created=time,
            miner=self.node.id,
            consensus=GPBFT.NAME,
        )
        block.extra_data = {
            "proposer": self.node.id,
            "round": self.rounds.round,
            "configuration_depth": self.node.reconfiguration_state.confchain[-1].depth,
        }
        transactions, size = TransactionFactory.execute_transactions(self.node.reconfiguration_state.configuration, self.node.pool, time)
        if transactions:
            block.transactions = transactions
            block.size = size + Parameters.data["base_block_size"]
            time += Parameters.execution["creation_time"]
            time += len(transactions) * Parameters.execution["time_per_tx"]
            return block, time
        return None, time

    def start(self, time: float, new_round: int) -> Optional[int]:
        if self.node.update(time):
            return 0
        self.state = "new_round"
        self.rounds.round = new_round
        self.reset_msgs()
        self.block = None
        self.get_miner()

        time += self.node.reconfiguration_state.configuration.block_time
        timeouts.schedule_timeout(self, time)

        if self.miner == self.node.id:
            messages.schedule_propose(self, time)
        else:
            handle_backlog(self.node, time)
        return None

    def init_round_change(self, time: float) -> None:
        timeouts.schedule_timeout(self, time, add_time=True)

    # ----------------------------------------------------------- handler
    @staticmethod
    def handle_event(event: "Event") -> str:
        if not event.actor or not event.actor.cp or event.actor.cp.NAME != GPBFT.NAME:
            return "different protocol"
        cp = event.actor.cp
        match event.payload["type"]:
            case "propose":
                return state_transitions.propose(cp, event)
            case "pre_prepare":
                return state_transitions.pre_prepare(cp, event)
            case "vote":
                return state_transitions.vote(cp, event)
            case "aggregate_window":
                return state_transitions.aggregate_window(cp, event)
            case "group_aggregate":
                return state_transitions.group_aggregate(cp, event)
            case "certificate":
                return state_transitions.certificate(cp, event)
            case "timeout":
                return timeouts.handle_timeout(cp, event)
            case "new_block":
                return state_transitions.new_block(cp, event)
            case _:
                return "unhandled"
