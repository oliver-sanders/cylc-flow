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

from cylc.flow.suite_files import check_nested_run_dirs
import pytest
from unittest import mock

import os.path
from pathlib import Path
from cylc.flow import suite_files
from cylc.flow.exceptions import SuiteServiceFileError, WorkflowFilesError


@pytest.mark.parametrize(
    'path, expected',
    [('a/b/c', '/mock_cylc_dir/a/b/c'),
     ('/a/b/c', '/a/b/c')]
)
def test_get_cylc_run_abs_path(path, expected, monkeypatch):
    monkeypatch.setattr('cylc.flow.pathutil.get_platform',
                        lambda: {'run directory': '/mock_cylc_dir'})
    assert suite_files.get_cylc_run_abs_path(path) == expected


@pytest.mark.parametrize(
    'path, expected',
    [('service/dir/exists', True),
     ('flow/file/exists', False),  # Non-run dirs can still contain flow.cylc
     ('nothing/exists', False)]
)
@pytest.mark.parametrize('is_abs_path', [False, True])
def test_is_valid_run_dir(path, expected, is_abs_path, monkeypatch):
    """Test that a directory is correctly identified as a valid run dir when
    it contains a service dir.
    """
    prefix = os.sep if is_abs_path is True else 'mock_cylc_dir'
    flow_file = os.path.join(prefix, 'flow', 'file', 'exists', 'flow.cylc')
    serv_dir = os.path.join(prefix, 'service', 'dir', 'exists', '.service')
    monkeypatch.setattr('os.path.isfile', lambda x: x == flow_file)
    monkeypatch.setattr('os.path.isdir', lambda x: x == serv_dir)
    monkeypatch.setattr('cylc.flow.pathutil.get_platform',
                        lambda: {'run directory': 'mock_cylc_dir'})
    path = os.path.normpath(path)
    if is_abs_path:
        path = os.path.join(os.sep, path)

    assert suite_files.is_valid_run_dir(path) is expected, (
        f'Is "{path}" a valid run dir?')


@pytest.mark.parametrize('direction', ['parents', 'children'])
def test_nested_run_dirs_raise_error(direction, monkeypatch):
    """Test that a suite cannot be contained in a subdir of another suite."""
    monkeypatch.setattr('cylc.flow.suite_files.get_cylc_run_abs_path',
                        lambda x: x)
    if direction == "parents":
        monkeypatch.setattr('cylc.flow.suite_files.os.scandir', lambda x: [])
        monkeypatch.setattr('cylc.flow.suite_files.is_valid_run_dir',
                            lambda x: x == os.path.join('bright', 'falls'))
        # Not nested in run dir - ok:
        suite_files.check_nested_run_dirs('alan/wake')
        # It is itself a run dir - ok:
        suite_files.check_nested_run_dirs('bright/falls')
        # Nested in a run dir - bad:
        for path in ('bright/falls/light', 'bright/falls/light/and/power'):
            with pytest.raises(WorkflowFilesError) as exc:
                suite_files.check_nested_run_dirs(path)
            assert 'Nested run directories not allowed' in str(exc.value)
@pytest.mark.parametrize(
    'run_dir',
    [
        ('bright/falls/light'),
        ('bright/falls/light/dark')
    ]
)
def test_rundir_parent_that_does_not_contain_workflow_no_error(
        run_dir, monkeypatch):
    """Test that a workflow raises no error when a parent directory is not also
        a workflow directory."""

    monkeypatch.setattr('cylc.flow.suite_files.os.path.isdir',
                        lambda x: False if x.find('.service') > 0
                        else True)
    monkeypatch.setattr(
        'cylc.flow.suite_files.get_cylc_run_abs_path', lambda x: x)
    monkeypatch.setattr(
        'cylc.flow.suite_files.os.scandir', lambda x: [])

    try:
        suite_files.check_nested_run_dirs(run_dir, 'placeholder_flow')
    except Exception:
        pytest.fail("check_nested_run_dirs raised exception unexpectedly.")


@pytest.mark.parametrize(
    'run_dir, srv_dir',
    [
        ('bright/falls/light', 'bright/falls/.service'),
        ('bright/falls/light/dark', 'bright/falls/light/.service')
    ]
)
def test_rundir_parent_that_contains_workflow_raises_error(
        run_dir, srv_dir, monkeypatch):
    """Test that a workflow that contains another worfkflow raises error."""

    monkeypatch.setattr(
        'cylc.flow.suite_files.os.path.isdir', lambda x: x == srv_dir)
    monkeypatch.setattr(
        'cylc.flow.suite_files.get_cylc_run_abs_path', lambda x: x)
    monkeypatch.setattr(
        'cylc.flow.suite_files.os.scandir', lambda x: [])

    with pytest.raises(SuiteServiceFileError) as exc:
        suite_files.check_nested_run_dirs(run_dir, 'placeholder_flow')
    assert 'Nested run directories not allowed' in str(exc.value)


@pytest.mark.parametrize(
    'run_dir',
    [
        ('a'),
        ('d/a'),
        ('z/d/a/a')
    ]
)
def test_rundir_children_that_do_not_contain_workflows_no_error(
        run_dir, monkeypatch):
    """Test that a run directory that contains no other workflows does not
    raise an error."""

    monkeypatch.setattr('cylc.flow.suite_files.os.path.isdir',
                        lambda x: False if x.find('.service')
                        else True)
    monkeypatch.setattr(
        'cylc.flow.suite_files.get_cylc_run_abs_path',
        lambda x: x)
    monkeypatch.setattr('cylc.flow.suite_files.os.scandir',
                        lambda x: [
                            mock.Mock(path=run_dir[0:len(x) + 2],
                                      is_symlink=lambda: False)])
    try:
        suite_files.check_nested_run_dirs(run_dir, 'placeholder_flow')
    except Exception:
        pytest.fail("check_nested_run_dirs raised exception unexpectedly.")


@pytest.mark.parametrize(
    'run_dir, srv_dir',
    [
        ('a', 'a/R/.service'),
        ('d/a', 'd/a/a/R/.service'),
        ('z/d/a/a', 'z/d/a/a/R/.service')
    ]
)
def test_rundir_children_that_contain_workflows_raise_error(
        run_dir, srv_dir, monkeypatch):
    """Test that a workflow cannot be contained in a subdir of another
    workflow."""
    monkeypatch.setattr('cylc.flow.suite_files.os.path.isdir',
                        lambda x: False if (
                            x.find('.service') > 0 and x != srv_dir)
                        else True)
    monkeypatch.setattr(
        'cylc.flow.suite_files.get_cylc_run_abs_path',
        lambda x: x)
    monkeypatch.setattr('cylc.flow.suite_files.os.scandir',
                        lambda x: [
                            mock.Mock(path=srv_dir[0:len(x) + 2],
                                      is_symlink=lambda: False)])

    with pytest.raises(SuiteServiceFileError) as exc:
        check_nested_run_dirs(run_dir, 'placeholder_flow')
    assert 'Nested run directories not allowed' in str(exc.value)

@pytest.mark.parametrize(
    'reg, expected_err',
    [('foo/bar/', None),
     ('/foo/bar', SuiteServiceFileError)]
)
def test_validate_reg(reg, expected_err):
    if expected_err:
        with pytest.raises(expected_err) as exc:
            suite_files._validate_reg(reg)
        assert 'cannot be an absolute path' in str(exc.value)
    else:
        suite_files._validate_reg(reg)


@pytest.mark.parametrize(
    'reg, props',
    [
        ('foo/bar/', {}),
        ('foo', {'no dir': True}),
        ('foo/..', {
            'no dir': True,
            'err': WorkflowFilesError,
            'err msg': ('cannot be a path that points to the cylc-run '
                        'directory or above')
        }),
        ('foo/../..', {
            'no dir': True,
            'err': WorkflowFilesError,
            'err msg': ('cannot be a path that points to the cylc-run '
                        'directory or above')
        }),
        ('foo', {
            'not stopped': True,
            'err': SuiteServiceFileError,
            'err msg': 'Cannot remove running workflow'
        }),
        ('foo/bar', {
            'symlink dirs': {
                'log': 'sym-log',
                'share': 'sym-share',
                'share/cycle': 'sym-cycle',
                'work': 'sym-work'
            }
        }),
        ('foo', {
            'symlink dirs': {
                'run': 'sym-run',
                'log': 'sym-log',
                'share': 'sym-share',
                'share/cycle': 'sym-cycle',
                'work': 'sym-work'
            }
        }),
        ('foo', {
            'bad symlink': {
                'type': 'file',
                'path': 'sym-log/cylc-run/foo/meow.txt'
            },
            'err': WorkflowFilesError,
            'err msg': 'Target is not a directory'
        }),
        ('foo', {
            'bad symlink': {
                'type': 'dir',
                'path': 'sym-log/bad/path'
            },
            'err': WorkflowFilesError,
            'err msg': 'Expected target to end with "cylc-run/foo/log"'
        })
    ]
)
def test_clean(reg, props, monkeypatch, tmp_path):
    """Test the clean() function.

    Params:
        reg (str): Workflow name.
        props (dict): Possible values are (all optional):
            'err' (Exception): Expected error.
            'err msg' (str): Message that is expected to be in the exception.
            'no dir' (bool): If True, do not create run dir for this test case.
            'not stopped' (bool): If True, simulate that the workflow is
                still running.
            'symlink dirs' (dict): As you would find in the global config
                under [symlink dirs][platform].
            'bad symlink' (dict): Simulate an invalid log symlink dir:
                'type' (str): 'file' or 'dir'.
                'path' (str): Path of the symlink target relative to tmp_path.
    """
    # --- Setup ---
    tmp_path.joinpath('cylc-run').mkdir()
    run_dir = tmp_path.joinpath('cylc-run', reg)
    run_dir_top_parent = tmp_path.joinpath('cylc-run', Path(reg).parts[0])
    symlink_dirs = props.get('symlink dirs')
    bad_symlink = props.get('bad symlink')
    if not props.get('no dir') and (
            not symlink_dirs or 'run' not in symlink_dirs):
        run_dir.mkdir(parents=True)

    dirs_to_check = [run_dir_top_parent]
    if symlink_dirs:
        if 'run' in symlink_dirs:
            dst = tmp_path.joinpath(symlink_dirs['run'], 'cylc-run', reg)
            dst.mkdir(parents=True)
            run_dir.symlink_to(dst)
            dirs_to_check.append(dst)
            symlink_dirs.pop('run')
        for s, d in symlink_dirs.items():
            dst = tmp_path.joinpath(d, 'cylc-run', reg, s)
            dst.mkdir(parents=True)
            src = run_dir.joinpath(s)
            src.symlink_to(dst)
            dirs_to_check.append(dst.parent)
    if bad_symlink:
        dst = tmp_path.joinpath(bad_symlink['path'])
        if bad_symlink['type'] == 'file':
            dst.parent.mkdir(parents=True)
            dst.touch()
        else:
            dst.mkdir(parents=True)
        src = run_dir.joinpath('log')
        src.symlink_to(dst)

    def mocked_detect_old_contact_file(reg):
        if props.get('not stopped'):
            raise SuiteServiceFileError('Mocked error')

    monkeypatch.setattr('cylc.flow.suite_files.detect_old_contact_file',
                        mocked_detect_old_contact_file)
    monkeypatch.setattr('cylc.flow.suite_files.get_suite_run_dir',
                        lambda x: tmp_path.joinpath('cylc-run', x))

    # --- The actual test ---
    expected_err = props.get('err')
    if expected_err:
        with pytest.raises(expected_err) as exc:
            suite_files.clean(reg)
        expected_msg = props.get('err msg')
        if expected_msg:
            assert expected_msg in str(exc.value)
    else:
        suite_files.clean(reg)
        for d in dirs_to_check:
            assert d.exists() is False
            assert d.is_symlink() is False


def test_remove_empty_reg_parents(tmp_path):
    """Test that _remove_empty_parents() doesn't remove parents containing a
    sibling."""
    reg = 'foo/bar/baz/qux'
    path = tmp_path.joinpath(reg)
    tmp_path.joinpath('foo/bar/baz').mkdir(parents=True)
    sibling_reg = 'foo/darmok'
    sibling_path = tmp_path.joinpath(sibling_reg)
    sibling_path.mkdir()
    suite_files._remove_empty_reg_parents(reg, path)
    assert tmp_path.joinpath('foo/bar').exists() is False
    assert tmp_path.joinpath('foo').exists() is True
    # Also path must be absolute
    with pytest.raises(ValueError) as exc:
        suite_files._remove_empty_reg_parents('foo/darmok', 'meow/foo/darmok')
    assert 'Path must be absolute' in str(exc.value)
    # Check it skips non-existent dirs, and stops at the right place too
    tmp_path.joinpath('foo/bar').mkdir()
    sibling_path.rmdir()
    suite_files._remove_empty_reg_parents(reg, path)
    assert tmp_path.joinpath('foo').exists() is False
    assert tmp_path.exists() is True

@pytest.mark.parametrize(
    'run_dir, srv_dir',
    [
        ('a', 'a/R/.service'),
        ('d/a', 'd/a/a/R/.service'),
        ('z/d/a/a', 'z/d/a/a/R/.service')
    ]
)
def test_symlinkrundir_children_that_contain_workflows_raise_error(
        run_dir, srv_dir, monkeypatch):
    """Test that a workflow cannot be contained in a subdir of another
    workflow."""
    monkeypatch.setattr('cylc.flow.suite_files.os.path.isdir',
                        lambda x: False if (
                            x.find('.service') > 0 and x != srv_dir)
                        else True)
    monkeypatch.setattr(
        'cylc.flow.suite_files.get_cylc_run_abs_path',
        lambda x: x)
    monkeypatch.setattr('cylc.flow.suite_files.os.scandir',
                        lambda x: [
                            mock.Mock(path=srv_dir[0:len(x) + 2],
                                      is_symlink=lambda: True)])

    try:
        check_nested_run_dirs(run_dir, 'placeholder_flow')
    except SuiteServiceFileError:
        pytest.fail("Unexpected SuiteServiceFileError, Check symlink logic.")
