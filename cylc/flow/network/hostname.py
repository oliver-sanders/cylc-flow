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
"""Host name utilities"""

from contextlib import suppress
import socket
from typing import Callable, Dict, Set

from cylc.flow.cfgspec.glbl_cfg import glbl_cfg
from cylc.flow.exceptions import UserInputError
from cylc.flow.util import elru_cache


@expiring_lru_cache
def address() -> str:
    """Return IP address of target.

    This finds the external address of the particular network adapter
    responsible for connecting to the target.

    The address is configured by
    :cylc:conf:`global.cylc[scheduler][DNS][self identification]address`.

    If your host sees the internet, a common address such as ``google.com``
    will do; otherwise choose a host visible on your intranet.

    .. note::

       Although no connection is made to the target, the target must be
       reachable on the network (or just recorded in the DNS) or the function
       will hang and time out after a few seconds.

    .. admonition:: explanation
       :class: hint

       Computers can contain a large set of network devices, each connected to
       different and independent sub-networks. Additionally there might be
       available a number of devices, to be utilized in the manner of network
       devices to exchange data with external systems.

       However, if properly configured, your operating system knows what device
       has to be utilized. Querying results depend on target addresses and
       routing information. In our solution we are utilizing the features of
       the local operating system to determine the correct network device. It
       is the same step we will get the associated network address.

       To reach this goal we will utilize the UDP protocol. Unlike TCP/IP, UDP
       is a stateless networking protocol to transfer single data packages. You
       do not have to open a point-to-point connection to a service running at
       the target host. We have to provide the target address to enable the
       operating system to find the correct device. Due to the nature of UDP
       you are not required to choose a valid target address. You just have to
       make sure your are choosing an arbitrary address from the correct
       subnet.
    """
    return _get_local_ip_address(
        glbl_cfg().get(
            ['scheduler', 'DNS', 'self identification', 'address']
        )
     )


def _get_local_ip_address(target: str) -> str:
    """Returns the IP address (either v4/6) of localhost."""
    # ATTRIBUTION:
    # http://www.linux-support.com/cms/get-local-ip-address-with-python/
    ipaddr = ""
    with suppress(IOError):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((target, 8000))
            ipaddr = sock.getsockname()[0]
    return ipaddr


def hardwired() -> str:
    """Hardwire the hostname or IP.

    The host or IP is configured by
    :cylc:conf:`global.cylc[scheduler][DNS][self identification]hardwired`.

    .. warning::

       The method configured by
       :cylc:conf:`global.cylc[scheduler][DNS][network identification]method`
       should resolve to the hardcoded value.
    """
    return (
        glbl_cfg().get(
            ['scheduler', 'DNS', 'self identification', 'hardwired']
        )
    )


@expiring_lru_cache
def local_fqdn() -> str:
    """Uses the :py:func:`socket.getfqdn` method to resolve the hostname."""
    fqdn = socket.getfqdn()
    if fqdn == (
        '1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0'
        '.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa'
    ):
        fqdn = socket.gethostname()
    return fqdn


@elru_cache
def fqdn(target: str) -> str:
    """Uses the :py:func:`socket.getfqdn` method to resolve the hostname.

    .. note::

       To ensure the return hostname will resolve on the network it is
       then passed through :py:func:`socket.gethostbyname_ex`.

    .. note::

       To support Mac OS default DNS configuration from Catalina (10.15)
       onwards the result ``1.0...0.ip6.arpa`` is interpreted as localhost
       and :py:func:`socket.gethostname`, which returns the short hostname,
       is used in its place. This should return the same value as
       ``hostname -f`` in this configuration.
    """
    fqdn = socket.getfqdn(target)
    # TODO: remove this!?
    if fqdn == (
        '1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0'
        '.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa'
    ):
        fqdn = socket.gethostname()
    # check the fqdn is valid by calling socket.gethostbyname_ex
    primary_host_name(fqdn)
    return fqdn


@elru_cache
def primary_host_name(target: str) -> str:
    """Uses the primary host name from :py:func:`socket.gethostbyname_ex`.

    .. note::

       This was the primary method used for network hostname resolution by
       Cylc7.

    .. warning::

       Does not support IPv6 name resolution.
    """
    try:
        return socket.gethostbyname_ex(target)[0]
    except IOError as exc:
        if not exc.filename:
            exc.filename = target
        raise


def _get_host_from_name(method: Callable) -> Callable:
    """Default behaviour for all network identification methods.

    * If hostname is a localhost alias return the preferred localhost name.
    """
    def _inner(target: str) -> str:
        nonlocal method
        if target in LOCALHOST_ALIASES:
            return LOCALHOST
        return method(target)
    return _inner


def is_remote_host(name: str) -> bool:
    """Return True if name has different IP address than the current host.

    Return False if name is None.
    Return True if host is unknown.
    """
    if not name or name in LOCALHOST_ALIASES:
        # e.g. localhost.localdomain
        return False
    else:
        try:
            return get_host_from_name(name) != LOCALHOST
        except IOError:
            # if the host name does not resolve assume it is a remote host
            return True


def is_remote_platform(platform: dict) -> bool:
    """Return True if any job host in platform have different IP address
    to the current host.

    Return False if name is None.
    Return True if host is unknown.

    Todo:
        Should this fail miserably if some hosts are remote and some are
        not?
    """
    if not platform:
        return False
    for host in platform['hosts']:
        if is_remote_host(host):
            return True
    return False


EXPORTED_METHODS: Dict[str, Callable] = {
    'address': address,
    'fqdn': fqdn,
    'hardwired': hardwired,
    'local_fqdn': local_fqdn,
    'primary host name': primary_host_name,
}


def _get_method(ident: str) -> Callable:
    method = glbl_cfg().get(['scheduler', 'DNS', ident, 'method'])
    try:
        method = EXPORTED_METHODS[method]
    except KeyError:
        raise UserInputError(f'Invalid hostname method: {method}')

    if ident == 'network identification':
        method = _get_host_from_name(method)

    return method


# NOTE: these methods cannot change during the live of the Scheduler
# by a global config reload (because that wouldn't make sense)
get_hostname: Callable = _get_method('self identification')
get_host_from_name: Callable = _get_method('network identification')

# preferred localhost identifier
LOCALHOST: str = get_hostname()
# aliases of localhost (e.g. localhost2, localhost.localdomain)
LOCALHOST_ALIASES: Set[str] = set(socket.gethostbyname_ex('localhost')[1])
LOCALHOST_ALIASES.add(LOCALHOST)
