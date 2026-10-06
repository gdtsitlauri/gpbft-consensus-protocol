"""
State transitions of GPBFT (see GPBFT_state for the protocol description).
"""

from Parameters import Parameters

from Chain.Network import Network
from Chain.Consensus.GPBFT import GPBFT_messages as msgs

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from Chain.Consensus.GPBFT.GPBFT_state import GPBFT
    from Engine.Event import Event

import logging

logger = logging.getLogger(__name__.split(".")[-1])


def _new_votes_record(state: "GPBFT", event: "Event", time: float) -> None:
    state.block.extra_data["votes"] = {"pre_prepare": [], "prepare": [], "commit": []}
    state.block.extra_data["votes"]["pre_prepare"].append((event.creator.id, time, Network.size(event)))


# ---------------------------------------------------------------- proposer
def propose(state: "GPBFT", event: "Event") -> str:
    time = event.time
    block, creation_time = state.create_block(time)
    if block is None:
        if creation_time + 1 + Parameters.execution["creation_time"] <= state.timeout.time:
            msgs.schedule_propose(state, creation_time + 1)
        return "no transactions - rescheduled"
    time = creation_time
    state.state = "pre_prepared"
    state.block = block.copy()
    _new_votes_record(state, event, time)
    msgs.broadcast_pre_prepare(state, time, block)
    return "proposed block"


def pre_prepare(state: "GPBFT", event: "Event") -> str:
    time = event.time
    block = event.payload["block"]
    valid, future = state.validate_message(event)
    if not valid:
        return "invalid"
    if future is not None:
        return future
    time += Parameters.execution["msg_val_delay"]
    if state.state != "new_round":
        return "invalid"
    time += Parameters.execution["block_val_delay"]
    if (ret := state.validate_block(block, time)) != "valid":
        return ret
    state.block = block.copy()
    _new_votes_record(state, event, time)
    state.state = "pre_prepared"
    cast_vote(state, time, "prepare")
    return "new_state"


# ---------------------------------------------------------------- members
def cast_vote(state: "GPBFT", time: float, phase: str) -> None:
    """Sends this node's prepare/commit vote to its group's managers (recording it if it is one of them)."""
    if state.voted[phase]:
        return
    state.voted[phase] = True
    msgs.send_vote(state, time, phase, state.block)  # skips this node itself
    if state.is_manager():
        record_group_vote(state, time, phase, state.node.id)


def certificate(state: "GPBFT", event: "Event") -> str:
    """A manager's quorum certificate reaches one of its members."""
    valid, future = state.validate_message(event)
    if not valid:
        return "invalid"
    if future is not None:
        return future
    if state.state in ("new_round",):
        return "backlog"  # block not received yet
    if state.state == "round_change" or state.block is None or event.payload["block_id"] != state.block.id:
        return "invalid"
    time = event.time + Parameters.execution["msg_val_delay"] + Parameters.GPBFT["sig_verify_delay"]
    return apply_certificate(state, time, event.payload["phase"])


def apply_certificate(state: "GPBFT", time: float, phase: str) -> str:
    if phase == "prepare":
        if state.state != "pre_prepared":
            return "invalid"
        state.state = "prepared"
        cast_vote(state, time, "commit")
        return "new_state"
    # commit certificate: the block is decided
    if state.state not in ("pre_prepared", "prepared"):
        return "invalid"
    state.node.add_block(state.block, time)
    Parameters.simulation["blockchain"] = Parameters.simulation.get("blockchain", dict())
    if state.block.id not in Parameters.simulation["blockchain"]:
        Parameters.simulation["blockchain"][state.block.id] = state.block.copy()
    if state.node.id == state.miner:
        msgs.broadcast_new_block(state, time, state.block)
    state.start(time, state.rounds.round + 1)
    return "new_state"


# ---------------------------------------------------------------- managers
def vote(state: "GPBFT", event: "Event") -> str:
    """A member's vote reaches its manager."""
    valid, future = state.validate_message(event)
    if not valid:
        return "invalid"
    if future is not None:
        return future
    if not state.is_manager():
        return "invalid"
    if state.state == "new_round":
        return "backlog"  # the manager has not received the block yet
    if state.state == "round_change" or state.block is None or event.payload["block_id"] != state.block.id:
        return "invalid"
    time = event.time + Parameters.execution["msg_val_delay"]
    record_group_vote(state, time, event.payload["phase"], event.creator.id)
    return "handled"


def _eligible(state: "GPBFT", phase: str) -> set:
    """Members expected to vote in a phase (the proposer casts no prepare vote, as in PBFT)."""
    members = set(state.my_group())
    if phase == "prepare":
        members.discard(state.miner)
    return members


def record_group_vote(state: "GPBFT", time: float, phase: str, voter: int) -> None:
    state.group_votes[phase].add(voter)
    pending = state.group_votes[phase] - state.sent_votes[phase]
    if state.group_votes[phase] >= _eligible(state, phase):
        flush_group(state, time, phase)  # whole group has voted
    elif pending and not state.window_open[phase]:
        state.window_open[phase] = True
        msgs.schedule_aggregate_window(state, time + Parameters.GPBFT["aggregation_window"], phase)


def aggregate_window(state: "GPBFT", event: "Event") -> str:
    if event.payload["round"] != state.rounds.round or state.state in ("new_round", "round_change"):
        return "invalid"
    state.window_open[event.payload["phase"]] = False
    flush_group(state, event.time, event.payload["phase"])
    return "handled"


def flush_group(state: "GPBFT", time: float, phase: str) -> None:
    """Sends the not-yet-aggregated votes of the group to the other managers."""
    new = state.group_votes[phase] - state.sent_votes[phase]
    if not new:
        return
    state.sent_votes[phase] |= new
    time += Parameters.GPBFT["sig_aggregate_delay"]
    msgs.send_group_aggregate(state, time, phase, new)
    add_votes(state, time, phase, new)


def group_aggregate(state: "GPBFT", event: "Event") -> str:
    """Another manager's group signature reaches this manager."""
    valid, future = state.validate_message(event)
    if not valid:
        return "invalid"
    if future is not None:
        return future
    if not state.is_manager():
        return "invalid"
    if state.state == "new_round":
        return "backlog"
    if state.state == "round_change" or state.block is None or event.payload["block_id"] != state.block.id:
        return "invalid"
    time = event.time + Parameters.execution["msg_val_delay"] + Parameters.GPBFT["sig_verify_delay"]
    add_votes(state, time, event.payload["phase"], set(event.payload["voters"]))
    return "handled"


def add_votes(state: "GPBFT", time: float, phase: str, voters: set) -> None:
    state.all_votes[phase] |= voters
    need = Parameters.application["required_messages"] - (1 if phase == "prepare" else 0)
    if len(state.all_votes[phase]) >= need and not state.cert_sent[phase]:
        state.cert_sent[phase] = True
        msgs.send_certificate(state, time, phase, state.all_votes[phase])
        apply_certificate(state, time, phase)


# ---------------------------------------------------------------- new block (as in PBFT)
def new_block(state: "GPBFT", event: "Event") -> str:
    block = event.payload["block"]
    time = event.time + Parameters.execution["msg_val_delay"] + Parameters.execution["block_val_delay"]
    if block.depth <= state.node.blockchain[-1].depth:
        return "invalid"
    if block.depth > state.node.blockchain[-1].depth + 1:
        state.node.attempt_sync(time=time, sync_node=event.creator)
        return "detected_desync"
    state.node.add_block(block.copy(), time)
    state.start(time, block.extra_data["round"] + 1)
    return "new_state"
