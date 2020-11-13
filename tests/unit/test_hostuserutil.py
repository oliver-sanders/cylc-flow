# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
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
"""Test aspects of "cylc.flow.hostuserutil"."""

import os

import pytest

from cylc.flow.hostuserutil import (
    get_fqdn_by_host,
    get_host,
    get_user,
    get_user_home,
    is_remote_host,
    is_remote_user
)


def test_is_remote_user_on_current_user():
    """is_remote_user with current user."""
    assert not is_remote_user(None)
    assert not is_remote_user(os.getenv('USER'))


def test_is_remote_host_on_localhost():
    """is_remote_host with localhost."""
    assert not is_remote_host(None)
    assert not is_remote_host('localhost')
    assert not is_remote_host(os.getenv('HOSTNAME'))
    assert not is_remote_host(get_host())


def test_get_fqdn_by_host_on_bad_host():
    """get_fqdn_by_host bad host."""
    bad_host = 'nosuchhost.nosuchdomain.org'
    with pytest.raises(
        (IOError, OSError),
        match=(
            r"(\[Errno -2\] Name or service|"
            r"\[Errno 8\] nodename nor servname provided, or)"
            rf" not known: '{bad_host}'"
        )
    ) as ctx:
        get_fqdn_by_host(bad_host)
    assert ctx.value.filename == bad_host


def test_get_user():
    """get_user."""
    assert os.getenv('USER') == get_user()


def test_get_user_home():
    """get_user_home."""
    assert os.getenv('HOME') == get_user_home()
