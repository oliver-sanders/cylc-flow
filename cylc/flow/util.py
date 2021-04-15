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

import functools
from time import time
from typing import Callable


def _make_key(args, kwargs):
    """Return a hashable key from args + kwargs.

    Example:
        >>> _make_key(('a', 1), {'b': 2})
        ('a', 1, ('b', 2))

    """
    return args + tuple(
        item
        for item in kwargs.items()
    )


def elru_cache(fcn: Callable = None, expires: int = 3600):
    """A least recently used cache implementation with an expiry.

    Args:
        fcn:
            The function to cache results from.
        expires:
            The maximum cache validity period.

            After this period the cache will be wiped on the next call.

    """

    def _elru_cache(fcn, expires):
        fcn.cache = {}
        fcn.cache_age = time()

        @functools.wraps(fcn)
        def _inner(*args, **kwargs):
            nonlocal fcn, expires

            cache = fcn.cache
            age = fcn.cache_age

            # check if the cache has expired
            if time() - age > expires:
                cache = {}
                age = time()

            # check if the value is in the cache ...
            key = _make_key(args, kwargs)
            if key not in cache:
                # ... no -> compute it
                cache[key] = fcn(*args, **kwargs)

            return cache[key]

        return _inner

    if fcn:
        # @elru_cache - no brackets (use default exipres value)
        return _elru_cache(fcn, expires)

    else:
        # @elru_cache() - brackets (use provided expires value)
        def _inner(fcn):
            return _elru_cache(fcn, expires)

        return _inner
