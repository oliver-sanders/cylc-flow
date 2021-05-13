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

"""Test the cylc.flow.host_select module.

NOTE: these are functional tests, for unit tests see the docstrings in
      the host_select module.

"""
import socket

import pytest

from cylc.flow.exceptions import HostSelectException
from cylc.flow.host_select import (
    select_host,
    select_workflow_host
)
from cylc.flow.network.hostname import (
    LOCALHOST,
    LOCALHOST_ALIASES,
    get_host_from_name,
)
from cylc.flow.parsec.exceptions import ListValueError


LOCALHOST_ALIASES = [
    alias
    for alias in LOCALHOST_ALIASES
    if alias != LOCALHOST
]


LOCALHOST_SHORT = None
try:
    if get_host_from_name(socket.gethostname()) == LOCALHOST:
        LOCALHOST_SHORT = socket.gethostname()
except IOError:
    pass


def test_localhost():
    """Basic test with one host to choose from."""
    short, fqdn = select_host([LOCALHOST])
    assert get_host_from_name(short) == LOCALHOST
    assert fqdn == LOCALHOST


def test_unique():
    """Basic test choosing from multiple forms of localhost"""
    name, fqdn = select_host(
        LOCALHOST_ALIASES + [LOCALHOST]
    )
    assert name in LOCALHOST_ALIASES + [LOCALHOST]
    assert fqdn == LOCALHOST


def test_filter():
    """Test that hosts are filtered out if specified."""
    message = 'Localhost not allowed'
    with pytest.raises(HostSelectException) as excinfo:
        select_host(
            [LOCALHOST],
            blacklist=[LOCALHOST],
            blacklist_name='Localhost not allowed'
        )
    assert message in str(excinfo.value)


def test_rankings():
    """Positive test that rankings are evaluated.

    (doesn't prove anything by itself hence test_unreasonable_rankings)
    """
    assert select_host(
        [LOCALHOST],
        ranking_string='''
            # if this test fails due to race conditions
            # then you have bigger issues than a test failure
            virtual_memory().available > 1
            getloadavg()[0] < 500
            cpu_count() > 1
            disk_usage('/').free > 1
        '''
    )[1] == LOCALHOST


def test_unreasonable_rankings():
    """Negative test that rankings are evaluated.

    (doesn't prove anything by itself hence test_rankings)
    """
    with pytest.raises(HostSelectException) as excinfo:
        select_host(
            [LOCALHOST],
            ranking_string='''
                # if this test fails due to race conditions
                # then you are very lucky
                virtual_memory().available > 123456789123456789
                getloadavg()[0] < 1
                cpu_count() > 512
                disk_usage('/').free > 123456789123456789
            '''
        )
    assert (
        'virtual_memory().available > 123456789123456789: False'
    ) in str(excinfo.value)


def test_metric_command_failure():
    """If the psutil command (or SSH) fails ensure the host is excluded."""
    with pytest.raises(HostSelectException) as excinfo:
        select_host(
            [LOCALHOST],
            ranking_string='''
                # elephant is not a psutil attribute
                # so will cause the command to fail
                elephant
            '''
        )
    assert excinfo.value.data[LOCALHOST]['get_metrics'] == (
        'Command failed (exit: 1)'
    )


@pytest.mark.skipif(
    not LOCALHOST_SHORT,
    reason='require short form of localhost'
)
def test_workflow_host_select(mock_glbl_cfg):
    """Run the workflow_host_select mechanism."""
    mock_glbl_cfg(
        'cylc.flow.host_select.glbl_cfg',
        f'''
            [scheduler]
                [[run hosts]]
                    available= {LOCALHOST_SHORT}
        '''
    )
    assert select_workflow_host() == (LOCALHOST_SHORT, LOCALHOST)


def test_workflow_host_select_default(mock_glbl_cfg):
    """Ensure "localhost" is provided as a default host."""
    mock_glbl_cfg(
        'cylc.flow.host_select.glbl_cfg',
        '''
            [scheduler]
                [[run hosts]]
                    available =
        '''
    )
    hostname, host_fqdn = select_workflow_host()
    assert hostname in LOCALHOST_ALIASES + [LOCALHOST]
    assert get_host_from_name(hostname) == LOCALHOST


@pytest.mark.skipif(
    not LOCALHOST_SHORT,
    reason='require short form of localhost'
)
def test_workflow_host_select_condemned(mock_glbl_cfg):
    """Ensure condemned hosts are filtered out."""
    mock_glbl_cfg(
        'cylc.flow.host_select.glbl_cfg',
        f'''
            [scheduler]
                [[run hosts]]
                    available = {LOCALHOST_SHORT}
                    condemned = {LOCALHOST}
        '''
    )
    with pytest.raises(HostSelectException) as excinfo:
        select_workflow_host()
    assert 'blacklisted' in str(excinfo.value)
    assert 'condemned host' in str(excinfo.value)


def test_condemned_host_ambiguous(mock_glbl_cfg):
    """Test the [scheduler]condemend host coercer

    Not actually host_select code but related functionality.
    """
    with pytest.raises(ListValueError) as excinfo:
        mock_glbl_cfg(
            'cylc.flow.host_select.glbl_cfg',
            f'''
                [scheduler]
                    [[run hosts]]
                        available = {LOCALHOST}
                        condemned = localhost
            '''
        )
    assert 'ambiguous host' in excinfo.value.msg
