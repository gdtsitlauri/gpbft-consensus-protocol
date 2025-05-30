'''
    Group Practical Byzantine Fault Tolerance Consensus Protocol

    GPBFT State:
        round - current round
        change_to - candidate round to change to
        state - GPBFT node state (new_round, pre-prepared, prepared, committed, group_signed, traced, round_change)]
        msgs: list of messages received from other nodes
        timeout - reference to latest timeout event (when node state updates it is used to find event and delete from event queue)
        block - the current proposed block
        group_signatures - list of group signatures received
        trace - tracking information for malicious nodes
'''

from Chain.Block import Block
from Chain.Parameters import Parameters

import Chain.Consensus.Rounds as Rounds
import Chain.Consensus.HighLevelSync as Sync

from types import SimpleNamespace
from random import randint
from sys import modules
from copy import copy

########################## PROTOCOL CHARACTERISTICS ###########################

NAME = "GPBFT"

def set_state(node):
    node.state.cp = modules[__name__]
    node.state.cp_state = SimpleNamespace(
        round=Rounds.round_change_state(),
        state="",
        miner="",
        msgs={'prepare': [], 'commit': [], 'group_signatures': []},
        timeout=None,
        block=None,
        trace=None,
    )

def state_to_string(node):
    s = (f"{Rounds.state_to_string(node)} | CP_state: {node.state.cp_state.state} | "
         f"block: {node.state.cp_state.block.id if node.state.cp_state.block is not None else -1} | "
         f"msgs: {node.state.cp_state.msgs} | "
         f"TO: {round(node.state.cp_state.timeout.time, 3) if node.state.cp_state.timeout is not None else -1}")
    return s

def reset_msgs(node):
    node.state.cp_state.msgs = {'prepare': [], 'commit': [], 'group_signatures': []}
    Rounds.reset_votes(node)

def get_miner(node, round_robin=True):
    if round_robin:
        node.state.cp_state.miner = node.state.cp_state.round.round % Parameters.application["Nn"]
    else:
        node.state.cp_state.miner = int(node.state.blockchain.last_block().hash, 16) % Parameters.application["Nn"]

def init(node, time=0, starting_round=0):
    set_state(node)
    start(node, starting_round, time)

def create_GPBFT_block(node, time):
    time += Parameters.data["block_interval"] + Parameters.execution["creation_time"]

    block = Block(
        depth=len(node.blockchain),
        id=randint(1, 10000),
        previous=node.last_block.id,
        time_created=time,
        miner=node.id,
        consensus=modules[__name__]
    )
    block.extra_data = {
        'proposer': node.id,
        'round': node.state.cp_state.round.round
    }

    current_pool = [t for t in node.pool if t.timestamp <= time]
    timeout_time = node.state.cp_state.timeout.time

    while not current_pool and time + 1 < timeout_time:
        time += 1
        current_pool = [t for t in node.pool if t.timestamp <= time]

    if current_pool and time < timeout_time:
        block.transactions, block.size = Parameters.simulation["txion_model"].execute_transactions(current_pool)
        return block, time
    else:
        return -1, -1

########################## HANDLER ###########################

def handle_event(event):
    if event.payload['type'] == 'pre_prepare':
        return pre_prepare(event)
    elif event.payload['type'] == 'prepare':
        return prepare(event)
    elif event.payload['type'] == 'commit':
        return commit(event)
    elif event.payload['type'] == 'group_sign':
        return group_sign(event)
    elif event.payload['type'] == 'trace':
        return trace(event)
    elif event.payload['type'] == 'timeout':
        return timeout(event)
    elif event.payload['type'] == 'new_block':
        return new_block(event)
    else:
        return 'unhandled'

########################## PROTOCOL COMMUNICATION ###########################

def validate_message(event, node):
    node_state, cp_state = node.state, node.state.cp_state
    payload = event.payload

    if payload['round'] < cp_state.round.round:
        return False

    return True

def process_vote(node, type, sender):
    node.state.cp_state.msgs[type] += [sender.id]

def pre_prepare(event):
    node = event.receiver
    time = event.time
    state = node.state.cp_state
    block = event.payload['block']
    
    time += Parameters.execution["msg_val_delay"]

    if state.state == 'new_round':
        if block.depth - 1 == node.last_block.depth and block.extra_data["round"] == state.round.round:
            time += Parameters.execution["block_val_delay"]

            state.block = event.payload['block'].copy()
            block = state.block

            state.state = 'pre_prepared'
            state.block = block

            payload = {
                'type': 'prepare',
                'block': block,
                'round': state.round.round,
            }
            node.scheduler.schedule_broadcast_message(node, time, payload, handle_event)

            process_vote(node, 'prepare', node)

            return 'new_state'
        else:
            Rounds.change_round(node, time)
        return 'handled'
    return 'unhandled'

def prepare(event):
    node = event.receiver
    time = event.time
    state = node.state.cp_state
    block = event.payload['block']

    if not validate_message(event, node):
        return "invalid"
    
    time += Parameters.execution["msg_val_delay"]

    if state.state == 'pre_prepared':
        process_vote(node, 'prepare', event.creator)

        if len(state.msgs['prepare']) == Parameters.application["required_messages"] - 1:
            state.state = 'prepared'

            payload = {
                'type': 'commit',
                'block': block,
                'round': state.round.round,
            }
            node.scheduler.schedule_broadcast_message(node, time, payload, handle_event)

            process_vote(node, 'commit', node)
            return 'new_state'
        return 'handled'
    elif state.state == 'new_round':
        return 'backlog'
    elif state.state == "round_change":
        process_vote(node, 'prepare', event.creator)

        if len(state.msgs['prepare']) >= Parameters.application["required_messages"] - 2:
            time += Parameters.execution["block_val_delay"]

            if block.depth - 1 == node.last_block.depth:
                state.round.round = event.payload['round']

                state.block = event.payload['block'].copy()
                block = state.block

                state.state = 'pre_prepared'
                state.block = block

                payload = {
                    'type': 'prepare',
                    'block': block,
                    'round': state.round.round,
                }
                node.scheduler.schedule_broadcast_message(node, time, payload, handle_event)

                process_vote(node, 'prepare', node)

                return 'new_state'
            else:
                if node.state.synced:
                    node.state.synced = False
                    Sync.create_local_sync_event(node, event.creator, time)

                return "handled"
    return 'invalid'

def commit(event):
    node = event.receiver
    time = event.time
    state = node.state.cp_state
    block = event.payload['block'].copy()

    if not validate_message(event, node):
        return "invalid"
    time += Parameters.execution["msg_val_delay"]

    if state.state == 'prepared':
        process_vote(node, 'commit', event.creator)

        if len(state.msgs['commit']) >= Parameters.application["required_messages"]:
            payload = {
                'type': 'commit',
                'block': block,
                'round': state.round.round,
            }
            node.scheduler.schedule_broadcast_message(node, time, payload, handle_event)

            process_vote(node, 'commit', node)

            node.add_block(state.block, time)
            
            payload = {
                'type': 'new_block',
                'block': block,
                'round': state.round.round,
            }
            node.scheduler.schedule_broadcast_message(node, time, payload, handle_event)

            start(node, state.round.round + 1, time)

            return 'new_state'
        return 'handled'
    elif state.state == 'new_round' or state.state == "pre_prepared":
        return 'backlog'
    elif state.state == 'round_change':
        process_vote(node, 'commit', event.creator)

        if len(state.msgs['commit']) >= Parameters.application["required_messages"] - 1:
            time += Parameters.execution["block_val_delay"]

            if block.depth - 1 == node.last_block.depth:
                state.round.round = event.payload['round']

                payload = {
                    'type': 'commit',
                    'block': block,
                    'round': state.round.round,
                }
                node.scheduler.schedule_broadcast_message(node, time, payload, handle_event)

                process_vote(node, 'commit', node)

                node.add_block(block, time)

                payload = {
                    'type': 'new_block',
                    'block': block,
                    'round': state.round.round,
                }
                node.scheduler.schedule_broadcast_message(node, time, payload, handle_event)

                start(node, state.round.round + 1, time)

                return 'new_state'
            else:
                if node.state.synced:
                    node.state.synced = False
                    Sync.create_local_sync_event(node, event.creator, time)

                return "handled"
    return "invalid"

def group_sign(event):
    node = event.receiver
    state = node.state.cp_state

    if state.round.round == event.payload['round']:
        state.msgs['group_signatures'].append(event.payload)
        if len(state.msgs['group_signatures']) >= Parameters.application["required_messages"]:
            state.state = 'group_signed'
            reset_msgs(node)

def trace(event):
    node = event.receiver
    state = node.state.cp_state

    if state.round.round == event.payload['round']:
        state.trace = event.payload['trace']
        # Implement tracing logic here to identify and remove malicious nodes
        state.state = 'traced'
        reset_msgs(node)

def new_block(event):
    node = event.receiver
    block = event.payload['block']
    time = event.time

    if not validate_message(event, node):
        return "invalid"
    time += Parameters.execution["msg_val_delay"]

    time += Parameters.execution["block_val_delay"]

    if block.depth <= node.blockchain[-1].depth:
        return "invalid"
    elif block.depth > node.blockchain[-1].depth + 1:
        if node.state.synced:
            node.state.synced = False
            Sync.create_local_sync_event(node, event.creator, time)
            return "handled"
    else:
        if event.payload['round'] > node.state.cp_state.round.round:
            node.state.cp_state.round.round = event.payload['round']
        node.add_block(block, time)
        start(node, event.payload['round']+1, time)
        return "handled"

########################## ROUND CHANGE ###########################

def init_round_change(node, time):
    schedule_timeout(node, time, remove=True, add_time=True)

def start(node, new_round, time):
    if node.update(time):
        return 0

    state = node.state.cp_state

    state.state = 'new_round'
    node.backlog = []

    reset_msgs(node)

    state.round.round = new_round
    state.block = None

    get_miner(node)

    if state.miner == node.id:
        schedule_timeout(node, Parameters.data["block_interval"] + time)

        block, creation_time = create_GPBFT_block(node, time)

        if creation_time == -1:
            print(f"Block creation failed at {time} for CP {NAME}")
            return 0

        state.state = 'pre_prepared'
        state.block = block.copy()
        
        payload = {
            'type': 'pre_prepare',
            'block': block,
            'round': new_round,
        }   

        node.scheduler.schedule_broadcast_message(node, creation_time, payload, handle_event)
    else:
        schedule_timeout(node, Parameters.data["block_interval"] + time)

########################## TIMEOUTS ###########################

def timeout(event):
    node = event.creator

    if event.payload['round'] == node.state.cp_state.round.round:
        if event.actor.update(event.time):
            return 0

        if node.state.synced:
            synced, in_sync_neighbour = node.synced_with_neighbours()
            if not synced:
                node.state.synced = False
                Sync.create_local_sync_event(node, in_sync_neighbour, event.time)

        Rounds.change_round(node, event.time)
        return "handled"
    return "invalid"

def schedule_timeout(node, time, remove=True, add_time=True):
    if node.state.cp_state.timeout is not None and remove:
        try:
            node.remove_event(node.state.cp_state.timeout)
        except ValueError:
            pass

    if add_time:
        time += Parameters.PBFT['timeout']

    payload = {
        'type': 'timeout',
        'round': node.state.cp_state.round.round,
    }

    event = node.scheduler.schedule_event(node, time, payload, handle_event)
    node.state.cp_state.timeout = event

########################## RESYNC CP SPECIFIC ACTIONS ###########################

def resync(node, payload, time):
    set_state(node)
    if node.state.cp_state.round.round < payload['blocks'][-1].extra_data['round']:
        node.state.cp_state.round.round = payload['blocks'][-1].extra_data['round']

    schedule_timeout(node, time=time)

######################### OTHER #################################################

def clean_up(node):
    for event in node.queue.event_list:
        if event.payload["CP"] == NAME:
            node.queue.remove_event(event)