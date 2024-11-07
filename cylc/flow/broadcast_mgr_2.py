# THIS FILE IS PART OF THE CYLC WORKFLOW ENGINE.
# Copyright (C) NIWA & British Crown (Met Office) & Contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Manage broadcast (and external trigger broadcast)."""

import json
from threading import RLock
from typing import (
    TYPE_CHECKING,
)

from cylc.flow.parsec.util import (
    pdeepcopy,
    poverride,
)
from cylc.flow.wallclock import (
    get_current_time_string,
)

if TYPE_CHECKING:
    from cylc.flow.config import WorkflowConfig
    from cylc.flow.id import Tokens
    from cylc.flow.task_proxy import TaskProxy
    from cylc.flow.workflow_db_mgr import WorkflowDatabaseManager


serialise_settings = json.dumps
deserialise_settings = json.loads


def addict(target, source):
    """Recursively add source dict to target dict."""
    for key, val in source.items():
        if isinstance(val, dict):
            if key not in target:
                target[key] = {}
            addict(target[key], val)
        else:
            target[key] = val


class BroadcastMgr:
    workflow_db_mgr: 'WorkflowDatabaseManager'
    # data_store_mgr
    config: 'WorkflowConfig'

    def __init__(self, workflow_db_mgr, data_store_mgr, config):
        self.workflow_db_mgr = workflow_db_mgr
        self.data_store_mgr = data_store_mgr
        self.config = config
        self.broadcasts = {}
        # self.ext_triggers = {}  # Can use collections.Counter in future
        self.lock = RLock()

        self._min_cycle = '1000'
        self._max_cycle = '3000'

        self.reload_config()

    # XTRIGS

    def check_ext_triggers(self, itask, ext_trigger_queue):
        pass  # TODO

    def _match_ext_trigger(self, itask):
        pass  # TODO
        # return bool

    # INTERFACES

    def put_broadcast(self, cycle=None, namespace=None, settings=None):
        # TODO: make the IDs sequential, not strictly necessary, we could rely on:
        # * timestamp
        # * insertion order
        # but nicer.
        time = get_current_time_string(display_sub_seconds=True)

        if self._is_cycle_in_window(cycle):
            self.broadcasts.setdefault(cycle, {}).setdefault(namespace, {})[
                time
            ] = settings

        self.workflow_db_mgr.put_broadcast_2(
            time, cycle, namespace, serialise_settings(settings)
        )

        # return modified_settings, bad_options

    def clear_broadcast(
        self,
        point_strings=None,
        namespace=None,
        cancel_settings=None,
        events=None,
    ):
        if events:
            # lookup events by ID (i.e. event time)
            opts = {'events': events}
        else:
            # lookup events matching the provided cycle/namespace/settings
            opts = {
                'cycle': point_strings,
                'namespace': namespace,
                'settings': serialise_settings(cancel_settings)
                if cancel_settings
                else None,
            }

        # get matching events
        broadcasts = list(
            self.workflow_db_mgr.pri_dao.select_broadcasts_2(**opts)
        )

        # extract event IDs from matches if not provided
        events = events or [time for time, *_ in broadcasts]

        # compute changes
        ret: dict = {}
        for _time, cycle, namespace, settings in broadcasts:
            addict(ret, {cycle: {namespace: settings}})
            pass

        # remove events from the DB
        self.workflow_db_mgr.drop_broadcast_2(events)

        # remove events from memory (avoid refreshing as this requires an extra DB call)
        for cycle, namespaces in tuple(self.broadcasts.items()):
            for namespace, broadcasted_settings in tuple(namespaces.items()):
                for time, settings in tuple(broadcasted_settings.items()):
                    if time in events:
                        broadcasted_settings.pop(time)
                if not broadcasted_settings:
                    # remove entry if no broadcasts are left
                    namespaces.pop(namespace)
            if not namespaces:
                # remove entry if no broadcasts are left
                self.broadcasts.pop(cycle)

        bad_options = []
        return ret, bad_options

    def get_broadcast(self, tokens: 'Tokens') -> dict:
        broadcasts = []
        if self._is_cycle_in_window(tokens['cycle']):
            for cycle, namespace in self._iter_broadcast_hierarchy(
                tokens['cycle'], tokens['task']
            ):
                settings = self.broadcasts.get(cycle, {}).get(namespace, {})
                if settings:
                    broadcasts.extend([*settings.values()])  # TODO: order
        else:
            for cycle, namespace in self._iter_broadcast_hierarchy(
                tokens['cycle'], tokens['task']
            ):
                broadcasts.extend(
                    [
                        deserialise_settings(settings)
                        for _time, _cycle, _namespace, settings in self.workflow_db_mgr.pri_dao.select_broadcasts_2(
                            cycle=cycle,
                            namespace=namespace,
                        )
                    ]
                )  # TODO: order

        ret: dict = {}
        for broadcast in broadcasts:
            addict(ret, broadcast)

        return ret

    def get_updated_rtconfig(self, itask: 'TaskProxy') -> dict:
        """Retrieve updated rtconfig for a single task proxy"""
        overrides = self.get_broadcast(itask.tokens)
        if overrides:
            rtconfig = pdeepcopy(itask.tdef.rtconfig)
            poverride(rtconfig, overrides, prepend=True)
        else:
            rtconfig = itask.tdef.rtconfig
        return rtconfig

    # NEW

    def _iter_broadcast_hierarchy(self, cycle, namespace):
        for cycle in ('*', cycle):
            for namespace in reversed(self.linearized_ancestors[namespace]):
                yield (cycle, namespace)

    def _is_cycle_in_window(self, cycle):
        cycle = str(cycle)
        return cycle >= self._min_cycle and cycle <= self._max_cycle

    def reload_broadcasts(self):
        self.broadcasts = {}  # TODO
        for (
            time,
            cycle,
            namespace,
            settings,
        ) in self.workflow_db_mgr.pri_dao.select_broadcasts_2(
            min_cycle=self._min_cycle,
            max_cycle=self._max_cycle,
        ):
            self.broadcasts.setdefault(cycle, {}).setdefault(namespace, {})[
                time
            ] = deserialise_settings(settings)

    def reload_config(self):
        self.linearized_ancestors = self.config.get_linearized_ancestors()

    def set_window(self, min_cycle, max_cycle):
        self._min_cycle = min_cycle
        self._max_cycle = max_cycle
        self.reload_broadcasts()
