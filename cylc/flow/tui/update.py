import asyncio
from multiprocessing import Process
from types import SimpleNamespace
from queue import Queue

from cylc.flow.exceptions import (
    ClientError,
    ClientTimeout,
    WorkflowStopped
)
from cylc.flow.network.client_factory import get_client
from cylc.flow.tui.data import (
    QUERY
)
from cylc.flow.tui.util import (
    compute_tree,
    dummy_flow,
)


class Updater(Process):
    def __init__(
        self,
        w_id,
        timeout=1,
        sleep_time=1,
    ):
        super(Process, self).__init__()
        self.w_id = w_id
        self.queues = SimpleNamespace(
            data=Queue(),
            filter=Queue(),
        )
        self.timeout = timeout
        self.sleep_time = sleep_time
        self.task = None

    def run(self):
        # self.task = asyncio.create_task(
        asyncio.run(
            _update(
                self.w_id,
                self.queues,
                timeout=self.timeout,
                sleep_time=self.sleep_time,
            )
        )
        # asyncio.get_event_loop().run_until_complete(self.task)

    def terminate(self):
        self.task.cancel()
        super(Process, self).terminate()


async def _update(
    w_id,
    queues,
    timeout=1,
    sleep_time=1,
):
    client = None
    filter_states = None
    while True:
        message = None
        tree = None

        if not queues.filter.empty():
            filter_states = queues.filter.get()
        try:
            if not client:
                client = get_client(w_id, timeout=timeout)
            data = await client.async_request(
                'graphql',
                {
                    'request_string': QUERY,
                    'variables': {
                        # list of task states we want to see
                        'taskStates': [
                            state
                            for state, is_on in filter_states.items()
                            if is_on
                        ]
                    }
                }
            )
        except WorkflowStopped:
            client = None
            tree = dummy_flow({
                'name': w_id,
                'id': w_id,
                'status': 'stopped',
                'stateTotals': {}
            })
        except (ClientError, ClientTimeout) as exc:
            # catch network / client errors
            message = [('workflow_error', str(exc))]
        else:
            if isinstance(data, list):
                # catch GraphQL errors
                try:
                    message = data[0]['error']['message']
                except (IndexError, KeyError):
                    message = str(data)
                message = [('workflow_error', message)]

            if len(data['workflows']) != 1:
                # multiple workflows in returned data - shouldn't happen
                raise ValueError()

            tree = compute_tree(data['workflows'][0])

        queues.data.put((message, tree), timeout=1)
        print(queues.data.qsize())

        await asyncio.sleep(sleep_time)
