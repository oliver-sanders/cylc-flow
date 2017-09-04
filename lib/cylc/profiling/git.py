# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2017 NIWA
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
"""Provides python wrappers to certain git commands."""

import os
from subprocess import (Popen, PIPE, CalledProcessError, check_call)


class GitCheckoutError(Exception):
    """Exception to be raised if a git checkout command fails."""
    pass


def archive_cylc_version(ref, repo_path, clone_path):
    """Archive a cylc version at a particular version to a chosen dir.

    Wrapper which adds a VERSION file for cylc. See archive_branch for details.

    """
    archive_branch(ref, repo_path, clone_path)
    with open(os.path.join(clone_path, 'VERSION'), 'w+') as version_file:
        version_file.write(ref)


def archive_branch(ref, repo_path, clone_path):
    """Archive and untar a git repository at a chosen version to a chosen dir.

    Args:
        ref (str): Any valid git identifier e.g. a branch name.
        repo_path (str): The path the the repository to clone.
        clone_path (str): A path, the basename of which does not exist.

    Raises:
        GitCheckoutError: In the event of a non-zero return code.

    """
    os.mkdir(os.path.join(clone_path))
    cmd = ('git -C "{repo_path}" archive "{ref}" | (cd "{clone_path}" && '
           'tar -xf -)').format(repo_path=repo_path, ref=ref,
                                clone_path=clone_path)
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    if proc.wait():
        raise GitCheckoutError(proc.communicate()[1])


def describe(ref=None):
    """Returns stdout of the `git describe <COMMIT>` command."""
    try:
        cmd = ['git', 'describe']
        if ref:
            cmd.append(ref)
        return Popen(cmd, stdout=PIPE).communicate()[0].strip()
    except CalledProcessError:
        return None


def is_ancestor_commit(commit1, commit2):
    """Returns True if commit1 is an ancestor of commit2."""
    try:
        ancestor = Popen(['git', 'merge-base', commit1, commit2],
                         stdout=PIPE).communicate()[0].strip()
        return ancestor == commit1
    except CalledProcessError:
        return False


def checkout(branch, delete_pyc=False):
    """Checkouts the git branch with the provided name."""
    try:
        cmd = ['git', 'checkout', '-q', branch]
        print '$ ' + ' '.join(cmd)
        check_call(cmd)
    except CalledProcessError:
        raise GitCheckoutError()
    try:
        if delete_pyc:
            cmd = ['find', 'lib', '-name', r'\*.pyc', '-delete']
            print '$ ' + ' '.join(cmd)
            check_call(cmd, stdout=open(os.devnull, 'wb'))
    except CalledProcessError:
        pass


def get_commit_date(commit):
    """Returns the commit date (in unix time) of the profided commit."""
    proc = Popen(['git', 'show', '-s', '--format=%at', commit],
                 stdout=PIPE, stderr=PIPE)
    if proc.wait():
        raise KeyError()
    return proc.communicate()[0].split()[-1]


def order_versions_by_date(versions):
    """Orders a list of version objects by the date of the most recent
    commit."""
    versions.sort(key=lambda x: get_commit_date(x['id']))


def order_identifiers_by_date(versions):
    """Orders a list of git identifiers by the date of the most recent
    commit."""
    versions.sort(key=lambda x: get_commit_date(x))


def has_changes_to_be_committed():
    """Returns True if there are any un-committed changes to the working
    copy."""
    git_status = Popen(['git', 'status'], stdout=PIPE).communicate()[0]
    if 'Changes to be committed' in git_status:
        return True
    if 'Changed but not updated' in git_status:
        return True
    return False


def is_git_repo():
    """Returns true if we are currently within a git repository."""
    proc = Popen(['git', 'rev-parse', '--git-dir'], stdout=PIPE, stderr=PIPE,)
    return proc.wait() == 0
