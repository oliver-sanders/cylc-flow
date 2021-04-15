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

from time import sleep

from cylc.flow.util import elru_cache


def test_elru_caching():
    """It should store results from previous invocations."""
    hits = []

    @elru_cache
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


def test_elru_expiry():
    """It should expire the cache after a configured period."""
    hits = []

    @elru_cache(expires=1)
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
