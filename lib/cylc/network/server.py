#!/usr/bin/env python3

# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2019 NIWA & British Crown (Met Office) & Contributors.
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
"""HTTP(S) server, and suite runtime API service facade exposed.

Implementation via ZMQ.
"""

import getpass
import re
from time import time, sleep

from threading import Thread

from jose import jwt
import zmq

from cylc import LOG
from cylc.cfgspec.glbl_cfg import glbl_cfg
from cylc.network import NO_PASSPHRASE, Priv, encrypt, decrypt, get_secret
from cylc.version import CYLC_VERSION
from cylc.wallclock import RE_DATE_TIME_FORMAT_EXTENDED


class ZMQServer(object):
    """A simple ZeroMQ request-response server for a multi-endpoint API.

    NOTE: Security is provided via the encode / decode interface.

    Args:
        encode_method (function): Translates incomming message strings into
            Python data-structures (e.g. json.loads).
            ``encode_method(json, secret) -> str``
        decode_method (function): Translates outgoing Python data-structures
            into strings (e.g. json.dumps).
            ``encode_method(str, secret) -> json``
        secret_method (function): Return the secret for use with the
            endode/decode methods.

    Endpoints are methods decorated using the expose decorator:
        class MyServer(ZMQServer):
            @staticmethod
            @ZMQServer.expose
            def my_method(required_arg, optional_arg=None):
                return {}

    Accepts requests of the format:
        {
            "command": COMMAND,
            "args": { ... }
        }

    Returns responces of the format:
        {
            "data": { ... }
        }

    Or error in the format:
        {
            "error": {
                "message": MESSAGE
            }
        }
    """

    def __init__(self, encode_method, decode_method, secret_method):
        self.port = None
        self.socket = None
        self.endpoints = None
        self.thread = None
        self.encode = encode_method
        self.decode = decode_method
        self.secret = secret_method

    def start(self, ports):
        # create socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)

        # pick port
        for port in ports:
            try:
                self.socket.bind('tcp://*:%d' % port)
            except zmq.error.ZMQError:
                pass
            else:
                self.port = port
                break
        else:
            raise Exception('No room at the inn, all ports occupied.')

        # register exposed methods
        self.endpoints = {name: obj
                          for name, obj in self.__class__.__dict__.items()
                          if hasattr(obj, 'exposed')}

        # start accepting requests
        # TODO: this in asyncio
        self.thread = Thread(target=self._listener)
        self.thread.start()

    def stop(self):
        # TODO: cancel the recv_string so that this works
        self.port = None
        self.thread.join()

    def _listener(self):
        while self.port:
            msg = self.socket.recv_string()

            try:
                message = self.decode(msg, self.secret())
                LOG.info('recieved request: %s from %s' % (
                    message['command'], message['user']))
            except Exception as exc:
                # failed to decode message, possibly resulting from failed
                # authentication
                response = self.encode(
                    {'error': {'message': str(exc)}}, self.secret())
            else:
                res = self._reciever(message)
                response = self.encode(res, self.secret())
            self.socket.send_string(response)
            sleep(0.1)

    def _reciever(self, message):
        """Recieve JSON process as applicable, send JSON."""
        # determine the server method to call
        try:
            method = getattr(self, message['command'])
            args = message['args']
            args.update({'user': message['user']})
        except KeyError:
            # malformed message
            return {'error': {
                'message': 'Request missing required field(s).'}}
        except AttributeError:
            # no exposed method by that name
            return {'error': {
                'message': 'No method by the name "%s"' % message['command']}}

        # generate response
        try:
            response = method(**args)
        except Exception as exc:
            # includes incorrect arguments (TypeError)
            return {'error': {'message': str(exc)}}

        return {'data': response}

    @staticmethod
    def expose(func=None, alias=None):
        func.exposed = True
        return func


def authorise(req_priv_level):
    def wrapper(fcn):
        def _authorise(self, *args, user=None, **kwargs):
            usr_priv_level = self.get_priv_level(user)
            if usr_priv_level < req_priv_level:
                LOG.info(
                    SuiteRuntimeServer.CONNECT_DENIED_PRIV_TMPL,
                    usr_priv_level, req_priv_level, user, 'TODO-host',
                    'TODO-progname', 'TODO-uuid')
                raise Exception('Authorisation failire')
            LOG.info(
                SuiteRuntimeServer.LOG_COMMAND_TMPL, fcn.__name__,
                user, 'TODO-host', 'TODO-progname', 'TODO-uuid')
            return fcn(self, *args, **kwargs)
        return _authorise
    return wrapper


class SuiteRuntimeServer(ZMQServer):
    """Suite runtime service API facade exposed via zmq."""

    CONNECT_DENIED_PRIV_TMPL = (
        "[client-connect] DENIED (privilege '%s' < '%s') %s@%s:%s %s")
    LOG_COMMAND_TMPL = '[client-command] %s %s@%s:%s %s'
    #LOG_IDENTIFY_TMPL = '[client-identify] %d id requests in PT%dS'
    #LOG_FORGET_TMPL = '[client-forget] %s'
    #LOG_CONNECT_ALLOWED_TMPL = "[client-connect] %s@%s:%s privilege='%s' %s"
    RE_MESSAGE_TIME = re.compile(
        r'\A(.+) at (' + RE_DATE_TIME_FORMAT_EXTENDED + r')\Z', re.DOTALL)

    def __init__(self, schd):
        ZMQServer.__init__(
            self,
            encrypt,
            decrypt, 
            lambda: get_secret(schd.suite)
        )
        self.schd = schd

    def get_priv_level(self, user):
        if user == getpass.getuser():
            return Priv.CONTROL
        elif self.schd.config.cfg['cylc']['authentication']['public']:
            return self.schd.config.cfg['cylc']['authentication']['public']
        else:
            return glbl_cfg().get(['authentication', 'public'])


    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def clear_broadcast(
            self, point_strings=None, namespaces=None, cancel_settings=None):
        """Clear settings globally, or for listed namespaces and/or points.

        Return a tuple (modified_settings, bad_options), where:
        * modified_settings is similar to the return value of the "put" method,
          but for removed settings.
        * bad_options is a dict in the form:
              {"point_strings": ["20020202", ..."], ...}
          The dict is only populated if there are options not associated with
          previous broadcasts. The keys can be:
          * point_strings: a list of bad point strings.
          * namespaces: a list of bad namespaces.
          * cancel: a list of tuples. Each tuple contains the keys of a bad
            setting.
        """
        return self.schd.task_events_mgr.broadcast_mgr.clear_broadcast(
            point_strings, namespaces, cancel_settings)

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def dry_run_tasks(self, items, check_syntax=True):
        """Prepare job file for a task.

        items[0] is an identifier for matching a task proxy.
        """
        if not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(('dry_run_tasks', (items,),
                                    {'check_syntax': check_syntax}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def expire_broadcast(self, cutoff=None):
        """Clear all settings targeting cycle points earlier than cutoff."""
        return self.schd.task_events_mgr.broadcast_mgr.expire_broadcast(cutoff)

    @authorise(Priv.READ)
    @ZMQServer.expose
    def get_broadcast(self, task_id=None):
        """Retrieve all broadcast variables that target a given task ID."""
        return self.schd.task_events_mgr.broadcast_mgr.get_broadcast(task_id)

    @authorise(Priv.IDENTITY)
    @ZMQServer.expose
    def get_cylc_version(self):
        """Return the cylc version running this suite."""
        return CYLC_VERSION

    @authorise(Priv.READ)
    @ZMQServer.expose
    def get_graph_raw(self, start_point_string, stop_point_string,
                      group_nodes=None, ungroup_nodes=None,
                      ungroup_recursive=False, group_all=False,
                      ungroup_all=False):
        """Return raw suite graph."""
        # Ensure that a "None" str is converted to the None value.
        if stop_point_string is not None:
            stop_point_string = str(stop_point_string)
        return self.schd.info_get_graph_raw(
            start_point_string, stop_point_string,
            group_nodes=group_nodes,
            ungroup_nodes=ungroup_nodes,
            ungroup_recursive=ungroup_recursive,
            group_all=group_all,
            ungroup_all=ungroup_all)

    @authorise(Priv.READ)
    @ZMQServer.expose
    def get_latest_state(self, full_mode=False):
        """Return latest suite state (suitable for a GUI update)."""
        return self.schd.info_get_latest_state(client_info, full_mode)

    @authorise(Priv.DESCRIPTION)
    @ZMQServer.expose
    def get_suite_info(self):
        """Return a dict containing the suite title and description."""
        return self.schd.info_get_suite_info()

    @authorise(Priv.READ)
    @ZMQServer.expose
    def get_suite_state_summary(self):
        """Return the global, task, and family summary data structures."""
        return self.schd.info_get_suite_state_summary()

    @authorise(Priv.READ)
    @ZMQServer.expose
    def get_task_info(self, names):
        """Return info of a task."""
        if not isinstance(names, list):
            names = [names]
        return self.schd.info_get_task_info(names)

    @authorise(Priv.READ)
    @ZMQServer.expose
    def get_task_jobfile_path(self, task_id):
        """Return task job file path."""
        return self.schd.info_get_task_jobfile_path(task_id)

    @authorise(Priv.READ)
    @ZMQServer.expose
    def get_task_requisites(self, items=None, list_prereqs=False):
        """Return prerequisites of a task."""
        if not isinstance(items, list):
            items = [items]
        return self.schd.info_get_task_requisites(
            items, list_prereqs=(list_prereqs in [True, 'True']))

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def hold_after_point_string(self, point_string):
        """Set hold point of suite."""
        self.schd.command_queue.put(
            ("hold_after_point_string", (point_string,), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def hold_suite(self):
        """Hold the suite."""
        self.schd.command_queue.put(("hold_suite", (), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def hold_tasks(self, items):
        """Hold tasks.

        items is a list of identifiers for matching task proxies.
        """
        if not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(("hold_tasks", (items,), {}))
        return (True, 'Command queued')

    # @authorise(Priv.IDENTIFY)  # TODO: split method into auth zones
    # @ZMQServer.expose
    # def identify(self):
    #     """Return suite identity, (description, (states))."""
    #     # TODO
    #     privileges = []
    #     for privilege in PRIVILEGE_LEVELS[0:3]:
    #         #if self._access_priv_ok(privilege):
    #         privileges.append(privilege)
    #     return self.schd.info_get_identity(privileges)

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def insert_tasks(self, items, stop_point_string=None, no_check=False):
        """Insert task proxies.

        items is a list of identifiers of (families of) task instances.
        """
        if not isinstance(items, list):
            items = [items]
        if stop_point_string == "None":
            stop_point_string = None
        self.schd.command_queue.put((
            "insert_tasks",
            (items,),
            {"stop_point_string": stop_point_string,
             "no_check": no_check in ['True', True]}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def kill_tasks(self, items):
        """Kill task jobs.

        items is a list of identifiers for matching task proxies.
        """
        if not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(("kill_tasks", (items,), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def nudge(self):
        """Tell suite to try task processing."""
        self.schd.command_queue.put(("nudge", (), {}))
        return (True, 'Command queued')

    @authorise(Priv.IDENTITY)
    @ZMQServer.expose
    def ping_suite(self):
        """Return True."""
        return True

    @authorise(Priv.READ)
    @ZMQServer.expose
    def ping_task(self, task_id, exists_only=False):
        """Return True if task_id exists (and running)."""
        return self.schd.info_ping_task(task_id, exists_only=exists_only)

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def poll_tasks(self, items=None, poll_succ=False):
        """Poll task jobs.

        items is a list of identifiers for matching task proxies.
        """
        if items is not None and not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(
            ("poll_tasks", (items,),
                {"poll_succ": poll_succ in ['True', True]}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def put_broadcast(
            self, point_strings=None, namespaces=None, settings=None):
        """Add new broadcast settings (server side interface).

        Return a tuple (modified_settings, bad_options) where:
          modified_settings is list of modified settings in the form:
            [("20200202", "foo", {"command scripting": "true"}, ...]
          bad_options is as described in the docstring for self.clear().
        """
        return self.schd.task_events_mgr.broadcast_mgr.put_broadcast(
            point_strings, namespaces, settings)

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def put_ext_trigger(self, event_message, event_id):
        """Server-side external event trigger interface."""
        self.schd.ext_trigger_queue.put((event_message, event_id))
        return (True, 'Event queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def put_message(self, task_id, severity, message):
        """(Compat) Put task message.

        Arguments:
            task_id (str): Task ID in the form "TASK_NAME.CYCLE".
            severity (str): Severity level of message.
            message (str): Content of message.
        """
        match = self.RE_MESSAGE_TIME.match(message)
        event_time = None
        if match:
            message, event_time = match.groups()
        self.schd.message_queue.put(
            (task_id, event_time, severity, message))
        return (True, 'Message queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def put_messages(self, task_job=None, event_time=None, messages=None):
        """Put task messages in queue for processing later by the main loop.

        Arguments:
            task_job (str): Task job in the form "CYCLE/TASK_NAME/SUBMIT_NUM".
            event_time (str): Event time as string.
            messages (list): List in the form [[severity, message], ...].
        """
        for severity, message in messages:
            self.schd.message_queue.put(
                (task_job, event_time, severity, message))
        return (True, 'Messages queued: %d' % len(messages))

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def reload_suite(self):
        """Tell suite to reload the suite definition."""
        self.schd.command_queue.put(("reload_suite", (), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def release_suite(self):
        """Unhold suite."""
        self.schd.command_queue.put(("release_suite", (), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def release_tasks(self, items):
        """Unhold tasks.

        items is a list of identifiers for matching task proxies.
        """
        if not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(("release_tasks", (items,), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def remove_cycle(self, point_string, spawn=False):
        """Remove tasks in a cycle from task pool."""
        self.schd.command_queue.put(
            ("remove_tasks", ('%s/*' % point_string,), {"spawn": spawn}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def remove_tasks(self, items, spawn=False):
        """Remove tasks from task pool.

        items is a list of identifiers for matching task proxies.
        """
        if not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(
            ("remove_tasks", (items,), {"spawn": spawn}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def reset_task_states(self, items, state=None, outputs=None):
        """Reset statuses tasks.

        items is a list of identifiers for matching task proxies.
        """
        if not isinstance(items, list):
            items = [items]
        if outputs and not isinstance(outputs, list):
            outputs = [outputs]
        self.schd.command_queue.put((
            "reset_task_states",
            (items,), {"state": state, "outputs": outputs}))
        return (True, 'Command queued')

    @authorise(Priv.SHUTDOWN)
    @ZMQServer.expose
    def set_stop_after_clock_time(self, datetime_string):
        """Set suite to stop after wallclock time."""
        self.schd.command_queue.put(
            ("set_stop_after_clock_time", (datetime_string,), {}))
        return (True, 'Command queued')

    @authorise(Priv.SHUTDOWN)
    @ZMQServer.expose
    def set_stop_after_point(self, point_string):
        """Set suite to stop after cycle point."""
        self.schd.command_queue.put(
            ("set_stop_after_point", (point_string,), {}))
        return (True, 'Command queued')

    @authorise(Priv.SHUTDOWN)
    @ZMQServer.expose
    def set_stop_after_task(self, task_id):
        """Set suite to stop after an instance of a task."""
        self.schd.command_queue.put(
            ("set_stop_after_task", (task_id,), {}))
        return (True, 'Command queued')

    @authorise(Priv.SHUTDOWN)
    @ZMQServer.expose
    def set_stop_cleanly(self, kill_active_tasks=False):
        """Set suite to stop cleanly or after kill active tasks."""
        self.schd.command_queue.put(
            ("set_stop_cleanly", (), {"kill_active_tasks": kill_active_tasks}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def set_verbosity(self, level):
        """Set suite verbosity to new level."""
        self.schd.command_queue.put(("set_verbosity", (level,), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def spawn_tasks(self, items):
        """Spawn tasks.

        items is a list of identifiers for matching task proxies.
        """
        if not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(("spawn_tasks", (items,), {}))
        return (True, 'Command queued')

    @authorise(Priv.SHUTDOWN)
    @ZMQServer.expose
    def stop_now(self, terminate=False):
        """Stop suite on event handler completion, or terminate right away."""
        self.schd.command_queue.put(("stop_now", (), {"terminate": terminate}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def take_checkpoints(self, items):
        """Checkpoint current task pool.

        items[0] is the name of the checkpoint.
        """
        if not isinstance(items, list):
            items = [items]
        self.schd.command_queue.put(("take_checkpoints", (items,), {}))
        return (True, 'Command queued')

    @authorise(Priv.CONTROL)
    @ZMQServer.expose
    def trigger_tasks(self, items, back_out=False):
        """Trigger submission of task jobs where possible.

        items is a list of identifiers for matching task proxies.
        """
        if not isinstance(items, list):
            items = [items]
        items = [str(item) for item in items]
        self.schd.command_queue.put(
            ("trigger_tasks", (items,), {"back_out": back_out}))
        return (True, 'Command queued')
