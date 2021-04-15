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
"""Package for network interfaces to Cylc scheduler objects."""

import getpass
import json

API = 5  # cylc API version
MSG_TIMEOUT = "TIMEOUT"


def encode_(message):
    """Convert the structure holding a message field from JSON to a string."""
    try:
        return json.dumps(message)
    except TypeError as exc:
        return json.dumps({'errors': [{'message': str(exc)}]})


def decode_(message):
    """Convert an encoded message string to JSON with an added 'user' field."""
    msg = json.loads(message)
    msg['user'] = getpass.getuser()  # assume this is the user
    return msg
