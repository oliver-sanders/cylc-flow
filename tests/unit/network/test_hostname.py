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

import os
import re

import pytest

from cylc.flow.network.hostname import (
    get_hostname,
    is_remote_host,
    is_remote_platform,
    fqdn,
    primary_host_name,
    _get_local_ip_address,
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


@pytest.mark.parametrize(
    'hosts,ret', [
        ([get_hostname()], False),
        (['nosuchhost.nosuchdomain.org'], True),
        (['localhost', 'nosuchhost.nosuchdomain.org'], True)
    ]
)
def test_is_remote_platform(hosts, ret):
    assert is_remote_platform({'hosts': hosts}) == ret


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
    assert exc.value.errno in [-2, 2, 8]
    assert exc.value.filename == bad_host


def test_get_local_ip_address():
    """It returns something that looks roughly like an IP address.

    Note the result could be IPv6 in any of its esoteric forms.
    """
    assert re.match(
        r'^[0-9a-fA-F:\.]{3,}$',
        _get_local_ip_address(get_hostname())
    )
