"""
Message helpers for GPBFT: broadcasts as in PBFT, plus unicast and
multicast to the group manager, the other managers and the group members.
"""

from Engine.Scheduler import Scheduler
from Engine.Event import Event, MessageEvent
from Chain.Network import Network

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from Chain.Consensus.GPBFT.GPBFT_state import GPBFT
    from Chain.Block import Block


def _node(node_id: int):
    for n in Network.nodes:
        if n.id == node_id:
            return n
    raise ValueError(f"no node {node_id}")


def send_to(state: "GPBFT", time: float, payload: dict, receivers: Iterable[int]) -> None:
    """Sends one message to each receiver (point to point, no gossip)."""
    for rid in receivers:
        if rid == state.node.id:
            continue
        event = Event(state.handle_event, state.node, time, payload)
        Network._message(state.node, _node(rid), MessageEvent.from_Event(event, _node(rid)))


def _payload(state: "GPBFT", kind: str, **extra) -> dict:
    p = {"type": kind, "round": state.rounds.round, "CP": state.NAME}
    p.update(extra)
    return p


def schedule_propose(state: "GPBFT", time: float) -> None:
    Scheduler.schedule_event(state.node, time, _payload(state, "propose"), state.handle_event)


def schedule_aggregate_window(state: "GPBFT", time: float, phase: str) -> None:
    Scheduler.schedule_event(state.node, time, _payload(state, "aggregate_window", phase=phase), state.handle_event)


def broadcast_pre_prepare(state: "GPBFT", time: float, block: "Block") -> None:
    Scheduler.schedule_broadcast_message(state.node, time, _payload(state, "pre_prepare", block=block), state.handle_event)


def broadcast_new_block(state: "GPBFT", time: float, block: "Block") -> None:
    Scheduler.schedule_broadcast_message(state.node, time, _payload(state, "new_block", block=block), state.handle_event)


def send_vote(state: "GPBFT", time: float, phase: str, block: "Block") -> None:
    """A node's prepare or commit vote, to the managers of its group (not to itself)."""
    send_to(state, time, _payload(state, "vote", phase=phase, block_id=block.id), state.my_managers())


def send_group_aggregate(state: "GPBFT", time: float, phase: str, voters: set) -> None:
    """A manager's aggregated group signature, to the managers of the other groups."""
    payload = _payload(state, "group_aggregate", phase=phase, voters=frozenset(voters), block_id=state.block.id)
    send_to(state, time, payload, state.other_group_managers())


def send_certificate(state: "GPBFT", time: float, phase: str, voters: set) -> None:
    """A quorum certificate (2f or 2f+1 votes), from a manager to its members."""
    payload = _payload(state, "certificate", phase=phase, voters=frozenset(voters), block_id=state.block.id)
    send_to(state, time, payload, state.my_group())
