"""A more efficient job poll mechanism for local platforms.

This continually polls jobs via the shared filesystem, it is more efficient
than conventional polling because:
* It doesn't spawn job poll subprocesses every time jobs need polling.
* It keeps the job status file open for reading rather than opening & closing
  it with each poll.

The caveat being that it only works for the "localhost" install target.

The added advantage of this approach is that Cylc does not need to be installed
on the remote host. This greatly simplifies containerised deployments as it
avoids the need to shoehorn Cylc into the job container and removes the need to
open TCP sockets.

At present, only the "cylc message" command is supported, however, others,
notably "cylc broadcast" could be supported in this way too.
"""

import asyncio
from pathlib import Path
from typing import Callable, Dict, Set, TypedDict, TYPE_CHECKING

from cylc.flow import LOG
from cylc.flow.async_util import make_async
from cylc.flow.main_loop import startup, periodic, shutdown
from cylc.flow.pathutil import get_workflow_run_job_dir
from cylc.flow.task_state import TASK_STATUS_SUBMITTED, TASK_STATUS_RUNNING
from cylc.flow.network.resolvers import TaskMsg

if TYPE_CHECKING:
    from cylc.flow.scheduler import Scheduler
    from cylc.flow.id import Tokens

    NamedPipeState = TypedDict('NamedPipeState', {
        'tasks': Set[Tokens],
        'pollers': Dict[Tokens, asyncio.Task],
        'signals': Dict[Tokens, asyncio.Future],
        'implementation': Callable,
    })


@startup
async def init(scheduler: 'Scheduler', state: 'NamedPipeState'):
    # set of tokens for the active tasks we are tracking
    state['tasks'] = set()
    # dict of poller tasks
    state['pollers'] = {}
    # dict of futures used to signal tasks to stop
    state['signals'] = {}
    # implementation to use to poll task statues
    state['implementation'] = status_file_poller


@periodic
async def update(scheduler: 'Scheduler', state: 'NamedPipeState'):
    # get all active tasks in the pool
    new_tasks = {
        itask.tokens.duplicate(job=f'{itask.submit_num:02}')
        for itask in scheduler.pool.get_tasks()
        if itask.state(TASK_STATUS_SUBMITTED, TASK_STATUS_RUNNING)
    }

    # start pollers for any newly active tasks
    for tokens in new_tasks - state['tasks']:
        start_poller(
            scheduler.message_queue,
            state,
            tokens,
        )

    # stop pollers for any tasks that are no longer active
    for tokens in state['tasks'] - new_tasks:
        stop_poller(state['signals'], tokens)

    # tidy up pollers which have returned
    purge_pollers(state)

    state['tasks'] = new_tasks


@shutdown
async def stop(scheduler: 'Scheduler', state: 'NamedPipeState'):
    # tell all pollers to stop
    for signal in state['signals'].values():
        signal.set_result(True)

    # wait for them to return
    await asyncio.gather(*state['pollers'].values())


def purge_pollers(state: 'NamedPipeState'):
    """Remove stopped pollers and signals from the state."""
    # tidy dangling pollers
    for tokens in list(state['pollers']):
        if state['pollers'][tokens].done():
            del state['pollers'][tokens]
            del state['signals'][tokens]


def start_poller(message_queue, state: 'NamedPipeState', tokens):
    """Add a new poller."""
    LOG.warning(f'starting poller: {tokens.task}')
    stop_signal = asyncio.Future()
    state['pollers'][tokens] = (
        asyncio.create_task(
            # named_pipe_poller(
            state['implementation'](
                message_queue,
                stop_signal,
                tokens,
            )
        )
    )
    state['signals'][tokens] = stop_signal


def stop_poller(signals, tokens):
    """Tell a poller to stop."""
    LOG.warning(f'stopping poller: {tokens.task}')
    signals[tokens].set_result(True)


async def status_file_poller(
    message_queue,
    stop_signal,
    tokens,
    max_interval=1,
):
    """Regular old file poller aka "tail -f" implementation."""
    status_file_path = Path(get_workflow_run_job_dir(
        tokens['workflow'],
        tokens['cycle'],
        tokens['task'],
        tokens['job'],
        'job.status',
    ))
    while True:
        try:
            with open(status_file_path, 'r') as status_file:
                while True:
                    line = status_file.readline().strip()
                    if line:
                        if line.startswith('CYLC_MESSAGE'):
                            time, severity, message = line[13:].split('|')
                            task_msg = TaskMsg(
                                tokens.task,
                                time[:-2],
                                severity,
                                message,
                            )
                            message_queue.put(task_msg)
                    else:
                        # nothing to read, wait for a while to avoid hammering
                        # the system
                        if stop_signal.done():
                            return
                        await asyncio.sleep(max_interval)
        except FileNotFoundError:
            # wait for file to be created
            await asyncio.sleep(5)


async def named_pipe_poller(
    message_queue,
    stop_signal,
    tokens,
    max_interval=1,
):
    """Read from a named pipe in the job log directory.

    Unfortunately named pipes appear to be a kernel feature not a filesystem
    feature so this approach will not work with remote hosts which completely
    defeats the object.
    """
    named_pipe = Path(get_workflow_run_job_dir(
        tokens['workflow'],
        tokens['cycle'],
        tokens['task'],
        tokens['job'],
        'job.pipe'
    ))
    while True:
        try:
            with open(named_pipe, 'r') as named_pipe:
                while True:
                    msg = await make_async(named_pipe.readline)()  # TODO
                    msg = msg.strip()
                    if msg:
                        time, severity, message = msg.split(':::', 2)
                        task_msg = TaskMsg(
                            tokens.task,
                            time[:-2],
                            severity,
                            message,
                        )
                        message_queue.put(task_msg)
                    else:
                        # nothing to read, wait for a while to avoid hammering
                        # the system
                        if stop_signal.done():
                            return
                        await asyncio.sleep(max_interval)
        except FileNotFoundError:
            # wait for file to be created
            await asyncio.sleep(5)
