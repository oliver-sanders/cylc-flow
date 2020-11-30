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

from cylc.flow.suite_files import check_nested_run_dirs
import os
import re
from glob import glob
from pathlib import Path
import logging
from cylc.flow.loggingutil import CylcLogFormatter
from cylc.flow.pathutil import (
    get_install_log_name, get_next_rundir_number)
from cylc.flow.wallclock import (
    get_current_time_string)


import aiofiles

from cylc.flow.exceptions import SuiteServiceFileError
from cylc.flow.pathutil import get_workflow_run_dir
from cylc.flow.platforms import get_platform
from cylc.flow.hostuserutil import (
    get_user,
    is_remote_host,
    is_remote_user
)
from cylc.flow.unicode_rules import SuiteNameValidator



INSTALL_LOG = logging.getLogger('cylc-install')
INSTALL_LOG.addHandler(logging.NullHandler())
INSTALL_LOG.setLevel(logging.INFO)

FAIL_IF_EXIST_DIR = ['log', 'share', 'work', '_cylc-install']

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


def _symlink_run_N(workflow_space):
    """Symlinks runN to the appropriate run directory"""
    # workflow_space = ~/cylc-run/$(basename $PWD)
    run_num = get_next_rundir_number(workflow_space)
    nn_path = os.path.join(workflow_space, 'runN')
    if run_num != 1:
        os.unlink(nn_path)
    new_path = os.path.join(workflow_space, f'run{run_num}')
    os.symlink(new_path, nn_path)


def rsync_rund(src, dst, restart=False):
    """Create and return the rsync command used for cylc install/re-install.
        Args: 
            src (str): file path location of source directory
            dst (str): file path location of destination directory
            restart (bool): indicate restart (--delete option added)

        Return: rsync_cmd: command used for rsync.

    """

    rsync_cmd = ['rsync -a']
    if restart:
        rsync_cmd += ['--delete']
    ignore_dirs = ['.git', '.svn']
    for exclude in ignore_dirs:
        rsync_cmd += [f" --exclude={exclude}"]
    rsync_cmd += [src]
    rsync_cmd += [dst]

    INSTALL_LOG.info(f'Command used for RSYNC - {rsync_cmd} ')
    return rsync_cmd


def install(flow_name=None, source=None, run_name=None redirect=False, directory=None, no_run_dir=False):
    """Install a suite, or renew its installation.

    Create suite service directory and symlink to suite source location.

    Args:
        flow_name (str): workflow name, default basename($PWD).
        source (str): directory location of flow.cylc file, default $PWD.
        redirect (bool): allow reuse of existing name and run directory.
        rundir (str): for overriding the default cylc-run directory.

    Return:
        str: The installed suite name (which may be computed here).

    Raise:
        SuiteServiceFileError:
            No flow.cylc file found in source location.
            Illegal name (can look like a relative path, but not absolute).
            Another suite already has this name (unless --redirect).
            Trying to install a workflow that is nested inside of another.
    """



    # --directory=/path/to/flow (-C ...) implies install the workflow found in /path/to/flow (rather than $PWD).

    if flow_name is None:
        flow_name = (Path.cwd().stem)
    is_valid, message = SuiteNameValidator.validate(flow_name)
    if not is_valid:
        raise SuiteServiceFileError(f'Invalid workflow name - {message}')
    if Path.is_absolute(Path(flow_name)):
        raise SuiteServiceFileError(
            f'Workflow name cannot be an absolute path: {flow_name}')
    if run_name == '_cylc-install':
        raise SuiteServiceFileError(
            f'Run name cannot be _cylc-install. Choose another run name.')
    check_nested_run_dirs(flow_name)
    source_dir = Path(get_workflow_run_dir(flow_name))
    for dir_ in FAIL_IF_EXIST_DIR:
        if source_dir.joinpath('dir_').exists:
            raise SuiteServiceFileError(
                f'Installation failed. - {dir_} exists in source directory.')

    # Workflow setup is not illegal - install should go ahead

    _open_install_log(flow_name)


    if no-run
    
    # flow.cylc must exist so we can detect accidentally reversed args.
    source = os.path.abspath(source)
    flow_file_path = os.path.join(source, SuiteFiles.FLOW_FILE)
    if not os.path.isfile(flow_file_path):
        # If using deprecated suite.rc, symlink it into flow.cylc:
        suite_rc_path = os.path.join(source, SuiteFiles.SUITE_RC)
        if os.path.isfile(suite_rc_path):
            os.symlink(suite_rc_path, flow_file_path)
            INSTALL_LOG.warning(
                f'The filename "{SuiteFiles.SUITE_RC}" is deprecated in favor '
                f'of "{SuiteFiles.FLOW_FILE}". Symlink created.')
        else:
            raise SuiteServiceFileError(
                f'no flow.cylc or suite.rc in {source}')

    # Create service dir if necessary.
    srv_d = get_suite_srv_dir(flow_name)
    if rundir is None:
        os.makedirs(srv_d, exist_ok=True)
    else:
        suite_run_d, srv_d_name = os.path.split(srv_d)
        alt_suite_run_d = os.path.join(rundir, flow_name)
        alt_srv_d = os.path.join(rundir, flow_name, srv_d_name)
        os.makedirs(alt_srv_d, exist_ok=True)
        os.makedirs(os.path.dirname(suite_run_d), exist_ok=True)
        if os.path.islink(suite_run_d) and not os.path.exists(suite_run_d):
            # Remove a bad symlink.
            os.unlink(suite_run_d)
        if not os.path.exists(suite_run_d):
            os.symlink(alt_suite_run_d, suite_run_d)
        elif not os.path.islink(suite_run_d):
            raise SuiteServiceFileError(
                f"Run directory '{suite_run_d}' already exists.")
        elif alt_suite_run_d != os.readlink(suite_run_d):
            target = os.readlink(suite_run_d)
            raise SuiteServiceFileError(
                f"Symlink '{suite_run_d}' already points to {target}.")
        # (else already the right symlink)

    # See if suite already has a source or not
    try:
        orig_source = os.readlink(
            os.path.join(srv_d, SuiteFiles.Service.SOURCE))
    except OSError:
        orig_source = None
    else:
        if not os.path.isabs(orig_source):
            orig_source = os.path.normpath(
                os.path.join(srv_d, orig_source))
    if orig_source is not None and source != orig_source:
        if not redirect:
            raise SuiteServiceFileError(
                f"the name '{flow_name}' already points to {orig_source}.\nUse "
                "--redirect to re-use an existing name and run directory.")
        INSTALL_LOG.warning(
            f"the name '{flow_name}' points to {orig_source}.\nIt will now be "
            f"redirected to {source}.\nFiles in the existing {flow_name} run "
            "directory will be overwritten.\n")
        # Remove symlink to the original suite.
        os.unlink(os.path.join(srv_d, SuiteFiles.Service.SOURCE))

    rsync_rund()
    # Create symlink to the suite, if it doesn't already exist.
    if orig_source is None or source != orig_source:
        target = os.path.join(srv_d, SuiteFiles.Service.SOURCE)
        if (os.path.abspath(source) ==
                os.path.abspath(os.path.dirname(srv_d))):
            # If source happens to be the run directory,
            # create .service/source -> ..
            source_str = ".."
        else:
            source_str = source
        os.symlink(source_str, target)

    INSTALL_LOG(f'INSTALLED {flow_name} -> {source}')
    _close_install_log()
    return flow_name

