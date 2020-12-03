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

from genericpath import exists
from subprocess import Popen, PIPE
from cylc.flow.suite_files import SuiteFiles, check_nested_run_dirs, get_suite_srv_dir
import os
import re
from glob import glob
from pathlib import Path
import logging
from cylc.flow.loggingutil import CylcLogFormatter
from cylc.flow.pathutil import (
    get_install_log_name, get_next_rundir_number, get_source_dir, make_localhost_symlinks)
from cylc.flow.wallclock import (
    get_current_time_string)



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

def _open_install_log(reg, rund, is_reload=False):
    """Open Cylc log handlers for an install."""
    time_str = get_current_time_string(
        override_use_utc=True, use_basic_format=True,
        display_sub_seconds=False
    )
    if is_reload:
        load_type = "reload"
    else:
        load_type = "install"
    rund = os.path.expanduser(rund)
    log_path = os.path.join(rund, 'log', 'install',f"{time_str}-{load_type}.log")
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


def get_rsync_rund_cmd(src, dst, restart=False):
    """Create and return the rsync command used for cylc install/re-install.
        Args: 
            src (str): file path location of source directory
            dst (str): file path location of destination directory
            restart (bool): indicate restart (--delete option added)

        Return: rsync_cmd: command used for rsync.

    """

    rsync_cmd = ["rsync"]
    rsync_cmd.append("-av")
    if restart:
        rsync_cmd.append('--delete')
    ignore_dirs = ['.git', '.svn', '.cylcignore']
    for exclude in ignore_dirs:
        if src.joinpath(exclude).exists():
            rsync_cmd.append(f"--exclude={exclude}")
    if src.joinpath('.cylcignore').exists():
        rsync_cmd.append("--exclude-from=.cylcignore")
    rsync_cmd.append(f"{src}/")
    rsync_cmd.append(f"{dst}")

    return rsync_cmd


def install(flow_name=None,source=None, run_name=None,
    no_run_name=False, no_symlinks=False, reinstall=False):
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
    if not source: 
        source = Path.cwd()
    if not flow_name:
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

    validate_source_dir(source)
    # Workflow setup is not illegal - install should go ahead
    basename_cwd = Path.cwd().stem
    run_path_base = os.path.expanduser(f'~/cylc-run/{basename_cwd}')
    if run_name:
        run_path_base = run_path_base + f'/{run_name}'
    run_n = os.path.expanduser(os.path.join(run_path_base, 'runN'))
    run_num = get_next_rundir_number(run_path_base)
    rundir = os.path.join(run_path_base, f'run{run_num}')
    if run_num == 1 and os.path.exists(rundir):
        INSTALL_LOG.error(f"This path: {rundir} exists. Try using --run-name")
    unlink_runN(run_n)
    try: 
        os.makedirs(os.path.expanduser(rundir), exist_ok=False)
    except OSError as e:
        if e.strerror == "File exists":
            raise SuiteServiceFileError(f"Run directory already exists : {e}")
    link_runN(rundir)
    _open_install_log(flow_name, rundir)
    if no_symlinks:
        make_localhost_symlinks(flow_name, log_type=INSTALL_LOG)
    # flow.cylc must exist so we can detect accidentally reversed args.
    INSTALL_LOG.info(f"source directory is {source}")
    flow_file_path = os.path.join(source, SuiteFiles.FLOW_FILE)
    if not os.path.isfile(flow_file_path):
        # If using deprecated suite.rc, symlink it into flow.cylc:
        suite_rc_path = os.path.join(source, SuiteFiles.SUITE_RC)
        if os.path.isfile(suite_rc_path):
            os.symlink(suite_rc_path, flow_file_path)
            INSTALL_LOG.warning(
                f'The filename "{SuiteFiles.SUITE_RC}" is deprecated in favour'
                f' of "{SuiteFiles.FLOW_FILE}". Symlink created.')
        else:
            raise SuiteServiceFileError(
                f'no flow.cylc or suite.rc in {source}')
    rsync_cmd = get_rsync_rund_cmd(source, os.path.expanduser(rundir))
    proc = Popen(rsync_cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = proc.communicate()
    if stderr:
        print(stderr)
    INSTALL_LOG.info(f"Copying files from {source} to {rundir}. {stdout}")
    if stderr:
        INSTALL_LOG.warning(f"An error occurred when copying files from {source} to {rundir}. {stderr}")
    INSTALL_LOG.info(f"Workflow {flow_name} sucessfully installed into {rundir}")
    _close_install_log()
    return





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

def validate_source_dir(source):
    """
    Ensure the source directory is valid.
    
    Args:
        source (path): Path to source directory
    Raises:
        SuiteServiceFileError:
            No flow.cylc file found in source location.
            Illegal name (can look like a relative path, but not absolute).
            If nested workflows.
    """
    # Ensure source dir does not contain log, share, work, _cylc_install
    for dir_ in FAIL_IF_EXIST_DIR:
        path_to_check = os.path.join(Path(source), dir_)
        if os.path.exists(path_to_check):
            raise SuiteServiceFileError(
                f'Installation failed. - {dir_} exists in source directory.')

    check_nested_run_dirs(source)


def unlink_runN(run_n):
    """Remove symlink runN"""
    try:
        os.unlink(run_n)
    except OSError:
        pass


def link_runN(latest_run):
    """Create symlink runN, pointing at the latest run"""
    run_n = os.path.expanduser(os.path.join((os.path.dirname(latest_run)), 'runN'))
    try:
        os.symlink(os.path.expanduser(latest_run), run_n)
    except OSError:
        pass