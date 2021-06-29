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

from tempfile import (
    NamedTemporaryFile, SpooledTemporaryFile, TemporaryFile,
    TemporaryDirectory
)
import unittest
import pytest

from pathlib import Path
from types import SimpleNamespace

from cylc.flow import LOG
from cylc.flow.task_events_mgr import TaskJobLogsRetrieveContext
from cylc.flow.subprocctx import SubProcContext
from cylc.flow.subprocpool import SubProcPool, _XTRIG_FUNCS, get_func


class TestSubProcPool(unittest.TestCase):

    def test_get_temporary_file(self):
        """Test SubProcPool.get_temporary_file."""
        self.assertIsInstance(
            SubProcPool.get_temporary_file(), SpooledTemporaryFile)

    def test_run_command_returns_0(self):
        """Test basic usage, command returns 0"""
        ctx = SubProcContext('truth', ['true'])
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, '')
        self.assertEqual(ctx.ret_code, 0)

    def test_run_command_returns_1(self):
        """Test basic usage, command returns 1"""
        ctx = SubProcContext('lies', ['false'])
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, '')
        self.assertEqual(ctx.ret_code, 1)

    def test_run_command_writes_to_out(self):
        """Test basic usage, command writes to STDOUT"""
        ctx = SubProcContext('parrot', ['echo', 'pirate', 'urrrr'])
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, 'pirate urrrr\n')
        self.assertEqual(ctx.ret_code, 0)

    def test_run_command_writes_to_err(self):
        """Test basic usage, command writes to STDERR"""
        ctx = SubProcContext(
            'parrot2', ['bash', '-c', 'echo pirate errrr >&2'])
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, 'pirate errrr\n')
        self.assertEqual(ctx.out, '')
        self.assertEqual(ctx.ret_code, 0)

    def test_run_command_with_stdin_from_str(self):
        """Test STDIN from string"""
        ctx = SubProcContext('meow', ['cat'], stdin_str='catches mice.\n')
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, 'catches mice.\n')
        self.assertEqual(ctx.ret_code, 0)

    def test_run_command_with_stdin_from_unicode(self):
        """Test STDIN from string with Unicode"""
        ctx = SubProcContext('meow', ['cat'], stdin_str='喵\n')
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, '喵\n')
        self.assertEqual(ctx.ret_code, 0)

    def test_run_command_with_stdin_from_handle(self):
        """Test STDIN from a single opened file handle"""
        handle = TemporaryFile()
        handle.write('catches mice.\n'.encode('UTF-8'))
        handle.seek(0)
        ctx = SubProcContext('meow', ['cat'], stdin_files=[handle])
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, 'catches mice.\n')
        self.assertEqual(ctx.ret_code, 0)
        handle.close()

    def test_run_command_with_stdin_from_path(self):
        """Test STDIN from a single file path"""
        handle = NamedTemporaryFile()
        handle.write('catches mice.\n'.encode('UTF-8'))
        handle.seek(0)
        ctx = SubProcContext('meow', ['cat'], stdin_files=[handle.name])
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, 'catches mice.\n')
        self.assertEqual(ctx.ret_code, 0)
        handle.close()

    def test_run_command_with_stdin_from_handles(self):
        """Test STDIN from multiple file handles"""
        handles = []
        for txt in ['catches mice.\n', 'eat fish.\n']:
            handle = TemporaryFile()
            handle.write(txt.encode('UTF-8'))
            handle.seek(0)
            handles.append(handle)
        ctx = SubProcContext('meow', ['cat'], stdin_files=handles)
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, 'catches mice.\neat fish.\n')
        self.assertEqual(ctx.ret_code, 0)
        for handle in handles:
            handle.close()

    def test_run_command_with_stdin_from_paths(self):
        """Test STDIN from multiple file paths"""
        handles = []
        for txt in ['catches mice.\n', 'eat fish.\n']:
            handle = NamedTemporaryFile()
            handle.write(txt.encode('UTF-8'))
            handle.seek(0)
            handles.append(handle)
        ctx = SubProcContext(
            'meow', ['cat'], stdin_files=[handle.name for handle in handles])
        SubProcPool.run_command(ctx)
        self.assertEqual(ctx.err, '')
        self.assertEqual(ctx.out, 'catches mice.\neat fish.\n')
        self.assertEqual(ctx.ret_code, 0)
        for handle in handles:
            handle.close()

    def test_xfunction(self):
        """Test xtrigger function import."""
        with TemporaryDirectory() as temp_dir:
            python_dir = Path(temp_dir, "lib", "python")
            python_dir.mkdir(parents=True)
            the_answer_file = python_dir / "the_answer.py"
            with the_answer_file.open(mode="w") as f:
                f.write("""the_answer = lambda: 42""")
                f.flush()
            fn = get_func("the_answer", temp_dir)
            result = fn()
            self.assertEqual(42, result)

    def test_xfunction_cache(self):
        """Test xtrigger function import cache."""
        with TemporaryDirectory() as temp_dir:
            python_dir = Path(temp_dir, "lib", "python")
            python_dir.mkdir(parents=True)
            amandita_file = python_dir / "amandita.py"
            with amandita_file.open(mode="w") as f:
                f.write("""amandita = lambda: 'chocolate'""")
                f.flush()
            fn = get_func("amandita", temp_dir)
            result = fn()
            self.assertEqual('chocolate', result)

            # is in the cache
            self.assertTrue('amandita' in _XTRIG_FUNCS)
            # returned from cache
            self.assertEqual(fn, get_func("amandita", temp_dir))
            del _XTRIG_FUNCS['amandita']
            # is not in the cache
            self.assertFalse('amandita' in _XTRIG_FUNCS)

    def test_xfunction_import_error(self):
        """Test for error on importing a xtrigger function.

        To prevent the test eventually failing if the test function is added
        and successfully imported, we use an invalid module name as per Python
        spec.
        """
        with TemporaryDirectory() as temp_dir:
            with self.assertRaises(ModuleNotFoundError):
                get_func("invalid-module-name", temp_dir)

    def test_xfunction_attribute_error(self):
        """Test for error on looking for an attribute in a xtrigger script."""
        with TemporaryDirectory() as temp_dir:
            python_dir = Path(temp_dir, "lib", "python")
            python_dir.mkdir(parents=True)
            the_answer_file = python_dir / "the_sword.py"
            with the_answer_file.open(mode="w") as f:
                f.write("""the_droid = lambda: 'excalibur'""")
                f.flush()
            with self.assertRaises(AttributeError):
                get_func("the_sword", temp_dir)


if __name__ == '__main__':
    unittest.main()




@pytest.mark.parametrize(
    'expect, ret_code, cmd_key',
    [
        pytest.param('Test Callback Called', 0, 'cylc-cat', id="return 0"),
        pytest.param('Test Callback Called', 1, 'cylc-cat', id="return 1"),
        pytest.param(
            '"cylc-cat" failed because "mouse" is not available',
            255,
            'cylc-cat',
            id="return 255"
        ),
        pytest.param(
            '"cylc-ret" failed because "mouse" is not available',
            255,
            TaskJobLogsRetrieveContext('cylc-ret', None, None, None),
            id="return 255 (log-ret)"
        )
    ]
)
def test__run_command_exit(caplog, expect, ret_code, cmd_key):
    """It runs a callback
    """
    def _test_callback(ctx):
        LOG.error(expect)

    def _test_callback_255(ctx):
        LOG.error('255 substitute callback: ' + expect)

    ctx = SimpleNamespace(
        timestamp=None,
        ret_code=ret_code,
        host='mouse',
        cmd_key=cmd_key
    )
    SubProcPool._run_command_exit(
        ctx, callback=_test_callback, callback_255=_test_callback_255
    )
    assert expect in caplog.records[0].msg
    if ret_code == 255:
        assert f'255 substitute callback: {expect}' in caplog.records[1].msg


def test__run_command_exit_no_255_args(caplog):
    """It runs the 255 callback with the args of the callback.
    """
    def _test_callback(ctx, foo):
        LOG.error(f'Logging from callback {foo}')

    def _test_callback_255(ctx, foo):
        LOG.error(f'Logging from 255 callback {foo}')

    ctx = SimpleNamespace(
        timestamp=None,
        ret_code=255,
        host='mouse',
        cmd_key='my-command'
    )
    SubProcPool._run_command_exit(
        ctx, callback=_test_callback, callback_args=['Zaphod'],
        callback_255=_test_callback_255
    )
    assert '255' in caplog.records[1].msg

def test__run_command_exit_add_to_badhosts():
    """It updates the list of badhosts
    """
    badhosts = {'foo', 'bar'}
    ctx = SimpleNamespace(
        timestamp=None,
        ret_code=255,
        host='mouse',
        cmd_key='my-command'
    )
    SubProcPool._run_command_exit(
        ctx,
        bad_hosts=badhosts,
        callback=print,
        callback_args=['Welcome to Magrathea']
    )
    assert badhosts == {'foo', 'bar', 'mouse'}
