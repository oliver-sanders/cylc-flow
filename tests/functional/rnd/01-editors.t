#!/bin/bash
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
#-------------------------------------------------------------------------------
# Test that the cylc [editors] can be set via the config or envvars.
. "$(dirname "$0")/test_header"
set_test_number 6
#-------------------------------------------------------------------------------
echo '#1' >&2
cylc get-global-config --sparse &>2

# editors not set in the config or with envvars
TEST_NAME="$TEST_NAME_BASE-defaults"
export EDITOR=
export GEDITOR=
echo '#2' >&2
cylc get-global-config --sparse &>2
run_ok "$TEST_NAME" cylc get-global-config -i '[editors]'
cmp_ok "${TEST_NAME}.stdout" << __HERE__
terminal = vi
gui = gvim -fg
__HERE__

# editors set with envvars
TEST_NAME="$TEST_NAME_BASE-envvar-override"
export EDITOR=editor
export GEDITOR=geditor
echo '#3' >&2
cylc get-global-config --sparse &>2
run_ok "$TEST_NAME" cylc get-global-config -i '[editors]'
cmp_ok "${TEST_NAME}.stdout" << __HERE__
terminal = editor
gui = geditor
__HERE__

# editors set with envvars and the config (which should take precedence)
TEST_NAME="$TEST_NAME_BASE-config-override"
export EDITOR=editor
export GEDITOR=geditor
echo '#4' >&2
cylc get-global-config --sparse &>2
create_test_globalrc '' '
[editors]
    terminal = myeditor
    gui = mygeditor
'
run_ok "$TEST_NAME" cylc get-global-config -i '[editors]'
cmp_ok "${TEST_NAME}.stdout" << __HERE__
terminal = myeditor
gui = mygeditor
__HERE__

exit
