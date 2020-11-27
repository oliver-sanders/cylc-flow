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

"""Cylc install."""

import os
import logging
from cylc.flow.loggingutil import CylcLogFormatter
from cylc.flow.pathutil import (
    get_install_log_name)
from cylc.flow.wallclock import (
    get_current_time_string)

INSTALL_LOG = logging.getLogger('cylc-install')
INSTALL_LOG.addHandler(logging.NullHandler())
INSTALL_LOG.setLevel(logging.INFO)


def _open_install_log(reg, is_reload=False):
    """Open Cylc log handlers for an install."""
    time_str = get_current_time_string(
        override_use_utc=True, use_basic_format=True,
        display_sub_seconds=False
    )
    if is_reload:
        load_type = "reload"
    else:
        load_type = "install"
    log_path = get_install_log_name(reg, f"{time_str}-{load_type}.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    handler = logging.FileHandler(log_path)
    handler.setFormatter(CylcLogFormatter())
    INSTALL_LOG.addHandler(handler)


def _close_install_log():
    """Close Cylc log handlers for a flow run."""
    for handler in INSTALL_LOG.handlers:
        try:
            handler.close()
        except IOError:
            pass

def install(reg=None, source=None, redirect=False, rundir=None):
    _open_install_log(reg)
    INSTALL_LOG.info("mooooooooooo")
    _close_install_log()
  