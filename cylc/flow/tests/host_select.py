"""Test the cylc.flow.host_select module.

NOTE: these are functional tests, for unit tests see the docstrings in
      the host_select module.

"""
import socket

import pytest

from cylc.flow.exceptions import HostSelectException
from cylc.flow.host_select import (
    select_host,
    select_suite_host
)
from cylc.flow.hostuserutil import get_fqdn_by_host
from cylc.flow.parsec.exceptions import ListValueError
from cylc.flow.tests.util import mock_glbl_cfg


localhost, localhost_aliases, _ = socket.gethostbyname_ex('localhost')
localhost_fqdn = get_fqdn_by_host(localhost)


def test_hostname_checking():
    """Check that unknown hosts raise an error"""
    with pytest.raises(socket.gaierror):
        select_host(['beefwellington'])


def test_localhost():
    """Basic test with one host to choose from."""
    assert select_host([localhost]) == (
        localhost,
        localhost_fqdn
    )


def test_unique():
    """Basic test choosing from multiple forms of localhost"""
    assert select_host(
        localhost_aliases + [localhost]
    ) == (
        localhost,
        localhost_fqdn
    )


def test_filter():
    """Test that hosts are filtered out if specified."""
    message = 'Localhost not allowed'
    with pytest.raises(HostSelectException) as excinfo:
        select_host(
            [localhost],
            blacklist=[localhost],
            blacklist_name='Localhost not allowed'
        )
    assert message in str(excinfo.value)


def test_thresholds():
    """Positive test that thresholds are evaluated.

    (doesn't prove anything by itself hence test_unreasonable_thresholds)
    """
    assert select_host(
        [localhost],
        threshold_string='''
            # if this test fails due to race conditions
            # then you have bigger issues than a test failure
            virtual_memory().available > 1
            getloadavg()[0] < 500
            cpu_count() > 1
            disk_usage('/').free > 1
        '''
    ) == (localhost, localhost_fqdn)


def test_unreasonable_thresholds():
    """Negative test that thresholds are evaluated.

    (doesn't prove anything by itself hence test_thresholds)
    """
    with pytest.raises(HostSelectException) as excinfo:
        select_host(
            [localhost],
            threshold_string='''
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
            [localhost],
            threshold_string='''
                # elephant is not a psutil attribute
                # so will cause the command to fail
                elephant
            '''
        )
    assert excinfo.value.data[localhost_fqdn]['get_metrics'] == (
        'Command failed (exit: 1)'
    )


def test_suite_host_select(mock_glbl_cfg):
    """Run the suite_host_select mechanism."""
    mock_glbl_cfg(
        'cylc.flow.host_select.glbl_cfg',
        f'''
            [suite servers]
                run hosts = {localhost}
        '''
    )
    assert select_suite_host() == (localhost, localhost_fqdn)


def test_suite_host_select_invalid_host(mock_glbl_cfg):
    """Ensure hosts are parsed before evaluation."""
    mock_glbl_cfg(
        'cylc.flow.host_select.glbl_cfg',
        '''
            [suite servers]
                run hosts = elephant
        '''
    )
    with pytest.raises(socket.gaierror):
        select_suite_host()


def test_suite_host_select_default(mock_glbl_cfg):
    """Ensure "localhost" is provided as a default host."""
    mock_glbl_cfg(
        'cylc.flow.host_select.glbl_cfg',
        '''
            [suite servers]
                run hosts =
        '''
    )
    hostname, host_fqdn = select_suite_host()
    assert hostname in localhost_aliases + [localhost]
    assert host_fqdn == localhost_fqdn


def test_suite_host_select_condemned(mock_glbl_cfg):
    """Ensure condemned hosts are filtered out."""
    mock_glbl_cfg(
        'cylc.flow.host_select.glbl_cfg',
        f'''
            [suite servers]
                run hosts = {localhost}
                condemned hosts = {localhost_fqdn}
        '''
    )
    with pytest.raises(HostSelectException) as excinfo:
        select_suite_host()
    assert 'blacklisted' in str(excinfo.value)
    assert 'condemned host' in str(excinfo.value)


def test_condemned_host_ambiguous(mock_glbl_cfg):
    """Test the [suite servers]condemend host coercer

    Not actually host_select code but related functionality.
    """
    with pytest.raises(ListValueError) as excinfo:
        mock_glbl_cfg(
            'cylc.flow.host_select.glbl_cfg',
            f'''
                [suite servers]
                    run hosts = {localhost}
                    condemned hosts = {localhost}
            '''
        )
    assert 'ambiguous host' in excinfo.value.msg
