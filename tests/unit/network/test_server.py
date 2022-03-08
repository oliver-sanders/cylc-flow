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

from types import SimpleNamespace

from cylc.flow.network.server import WorkflowRuntimeServer as Server


async def test_receiver():
    """Test the receiver with different message objects."""
    receiver = Server._receiver

    # simulate an endpoint that always succeeds
    def _method(*args, **kwargs):
        return 'response'

    server = SimpleNamespace(api=_method)

    # start with a message that works
    msg = {'command': 'api', 'user': '', 'args': {}}
    assert 'error' not in receiver(server, msg)
    assert 'data' in receiver(server, msg)

    # remove the user field - should error
    msg2 = dict(msg)
    msg2.pop('user')
    assert 'error' in receiver(server, msg2)

    # remove the command field - should error
    msg3 = dict(msg)
    msg3.pop('command')
    assert 'error' in receiver(server, msg3)

    # provide an invalid command - should error
    msg4 = {**msg, 'command': 'foobar'}
    assert 'error' in receiver(server, msg4)

    # simulate a command failure with the original message
    # (the one which worked earlier) - should error
    def _method(*args, **kwargs):
        raise Exception('foo')

    server.api = _method
    assert 'error' in receiver(server, msg)
