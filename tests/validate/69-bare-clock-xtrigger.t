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

# Test that undeclared zero-offset clock xtriggers are allowed.
. "$(dirname "$0")/test_header"

set_test_number 1

cat >'suite.rc' <<'__SUITE_RC__'
[scheduling]
    initial cycle point = now
    [[dependencies]]
        [[[ T00 ]]]
            graph = "@wall_clock => foo"
__SUITE_RC__

run_ok "${TEST_NAME_BASE}-val" cylc validate 'suite.rc'