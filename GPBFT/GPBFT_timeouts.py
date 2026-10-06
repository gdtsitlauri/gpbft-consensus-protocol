"""
Round timeouts of GPBFT (same logic as PBFT).
"""

from Parameters import Parameters

from Engine.Scheduler import Scheduler
from Chain.Consensus import Rounds

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from Chain.Consensus.GPBFT.GPBFT_state import GPBFT
    from Engine.Event import Event


def handle_timeout(state: "GPBFT", event: "Event") -> str:
    if event.payload["round"] != state.rounds.round:
        return "invalid"
    if state.node.update(event.time):
        return "changed_cp"
    if event.actor.attempt_sync(event.time):
        return "detected_desync"
    Rounds.change_round(state.node, event.time)
    return "handled"


def schedule_timeout(state: "GPBFT", time: float, add_time: bool = True) -> None:
    if add_time:
        time += Parameters.GPBFT["timeout"]
    payload = {"type": "timeout", "round": state.rounds.round, "CP": state.NAME}
    if state.timeout is not None and Parameters.simulation["debugging_mode"]:
        state.node.queue.remove_event(state.timeout)
    state.timeout = Scheduler.schedule_event(state.node, time, payload, state.handle_event)
