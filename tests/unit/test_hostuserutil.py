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

import os
from time import sleep

import pytest

from cylc.flow.hostuserutil import (
    get_hostname,
    is_remote_host,
    fqdn,
    primary_host_name,
    lru_cache
)


def test_is_remote_host_on_localhost():
    """is_remote_host with localhost."""
    assert not is_remote_host(None)
    assert not is_remote_host('localhost')
    assert not is_remote_host(os.getenv('HOSTNAME'))
    assert not is_remote_host(get_hostname())


def test_is_remote_host_invalid():
    """Unresolvable hosts should be considered remote hosts."""
    assert is_remote_host('nosuchhost.nosuchdomain.org')


@pytest.mark.parametrize('method', [fqdn, primary_host_name])
def test_get_hostname_on_bad_host(method):
    """get_hostname bad host.

    Warning:
        This test can fail due to ISP/network configuration
        (for example ISP may reroute failed DNS to custom search page)
        e.g: https://www.virginmedia.com/help/advanced-network-error-search

    """
    bad_host = 'nosuchhost.nosuchdomain.org'
    with pytest.raises(IOError) as exc:
        method(bad_host)
    assert exc.value.errno in [2, 8]
    assert exc.value.filename == bad_host


def test_lru_cache():
    """It should store results from previous invocations."""
    hits = []

    @lru_cache
    def test_method(*args, **kwargs):
        nonlocal hits
        hits.append((args, kwargs))

    # call the method once
    test_method()
    assert len(hits) == 1

    # when we call again the result should be cached
    test_method()
    assert len(hits) == 1
    assert hits == [(tuple(), {})]

    hits[:] = []

    # it should work with args and kwargs
    test_method('a', 1, foo='bar')
    assert hits == [(('a', 1), {'foo': 'bar'})]
    test_method('a', 1, foo='bar')
    assert hits == [(('a', 1), {'foo': 'bar'})]
    test_method('b')
    test_method('b')
    assert hits == [(('a', 1), {'foo': 'bar'}), (('b',), {})]


def test_cache_expiry():
    """It should expire the cache after a configured period."""
    hits = []

    @lru_cache(expires=1)
    def test_method(*args, **kwargs):
        hits.append((args, kwargs))

    # call the method once
    test_method()
    assert len(hits) == 1

    # when we call again the result should be cached
    test_method()
    assert len(hits) == 1

    # wait for the cache to expire
    sleep(1)

    # when we call again the result should be recomputed
    test_method()
    assert len(hits) == 2
