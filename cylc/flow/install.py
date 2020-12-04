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

import logging
import os
from pathlib import Path
from subprocess import PIPE, Popen
from cylc.flow.exceptions import SuiteServiceFileError
from cylc.flow.loggingutil import CylcLogFormatter
from cylc.flow.pathutil import (get_next_rundir_number, get_workflow_run_dir,
                                make_localhost_symlinks)
from cylc.flow.suite_files import (SuiteFiles,
                                   check_nested_run_dirs)
from cylc.flow.unicode_rules import SuiteNameValidator
from cylc.flow.wallclock import get_current_time_string

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
    rund = Path(rund).expanduser()
    log_path = Path(
        rund,
        'log',
        'install',
        f"{time_str}-{load_type}.log")
    log_parent_dir = log_path.parent
    log_parent_dir.mkdir(exist_ok=True, parents=True)
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
        if Path(src).joinpath(exclude).exists():
            rsync_cmd.append(f"--exclude={exclude}")
    if Path(src).joinpath('.cylcignore').exists():
        rsync_cmd.append("--exclude-from=.cylcignore")
    rsync_cmd.append(f"{src}/")
    rsync_cmd.append(f"{dst}")

    return rsync_cmd


def install(flow_name=None, source=None, run_name=None,
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
    source = Path(source).expanduser()
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
            'Run name cannot be "_cylc-install".'
            ' Please choose another run name.')
    validate_source_dir(source)
    basename_cwd = Path.cwd().stem
    run_path_base = os.path.expanduser(f'~/cylc-run/{basename_cwd}')
    if no_run_name:
        rundir = run_path_base
    else:
        if run_name:
            run_path_base = run_path_base + f'/{run_name}'
        run_n = os.path.expanduser(os.path.join(run_path_base, 'runN'))
        run_num = get_next_rundir_number(run_path_base)
        rundir = Path(run_path_base, f'run{run_num}')
        if run_num == 1 and os.path.exists(rundir):
            SuiteServiceFileError(
                f"This path: {rundir} exists. Try using --run-name")
        unlink_runN(run_n)
    check_nested_run_dirs(rundir)
    try:
        os.makedirs(os.path.expanduser(rundir), exist_ok=False)
    except OSError as e:
        if e.strerror == "File exists":
            raise SuiteServiceFileError(f"Run directory already exists : {e}")
    _open_install_log(flow_name, rundir)
    link_runN(rundir)
    if not no_symlinks:
        make_localhost_symlinks(rundir, flow_name, log_type=INSTALL_LOG)
    # flow.cylc must exist so we can detect accidentally reversed args.
    flow_file_path = os.path.expanduser(os.path.join(source, SuiteFiles.FLOW_FILE))
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
                f'no {SuiteFiles.FLOW_FILE} or {SuiteFiles.SUITE_RC} in {source}')
    rsync_cmd = get_rsync_rund_cmd(source, os.path.expanduser(rundir))
    proc = Popen(rsync_cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    stdout, stderr = proc.communicate()
    INSTALL_LOG.info(f"Copying files from {source} to {rundir}")
    INSTALL_LOG.info(f"{stdout}")
    if stderr:
        INSTALL_LOG.warning(
            f"An error occurred when copying files from {source} to {rundir}")
        INSTALL_LOG.warning(f" Error: {stderr}")
    cylc_install = Path(rundir, '_cylc_install')
    cylc_install.mkdir(parents=True)
    source_link = cylc_install.joinpath('source')
    INSTALL_LOG.info(f"Creating symlink from {source_link}")
    source_link.symlink_to(source)
    INSTALL_LOG.info(f'INSTALLED {flow_name} -> {source}')
    _close_install_log()
    return


def validate_source_dir(source):
    """Ensure the source directory is valid.

    Args:
        source (path): Path to source directory
    Raises:
        SuiteServiceFileError:
            If log, share, work or _cylc-install directories exist in the
            source directory.
    """
    # Ensure source dir does not contain log, share, work, _cylc_install
    for dir_ in FAIL_IF_EXIST_DIR:
        path_to_check = Path(source, dir_)
        if path_to_check.exists():
            raise SuiteServiceFileError(
                f'Installation failed. - {dir_} exists in source directory.')

def unlink_runN(run_n):
    """Remove symlink runN"""
    try:
        Path(run_n).unlink()
    except OSError:
        pass


def link_runN(latest_run):
    """Create symlink runN, pointing at the latest run"""
    latest_run = Path(latest_run).expanduser()
    run_n = Path(latest_run.parent,'runN')
    try:
        run_n.symlink_to(latest_run)
    except OSError:
        pass
