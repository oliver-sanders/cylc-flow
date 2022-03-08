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

"""Test workflow shutdown logic."""

import asyncio

from async_timeout import timeout
import pytest


async def test_server_shutdown(one, start):
    """Test the server can shut down successfully when sent the stop signal."""
    async with start(one):
        one.server.queue.put(one.server._STOP)
        async with timeout(2):
            # wait for the server to consume the STOP item from the queue
            while True:
                if one.server.queue.empty():
                    break
                await asyncio.sleep(0.01)
        # ensure the server is "closed"
        with pytest.raises(ValueError):
            one.server.queue.put('foobar')
            one.server._listener()
