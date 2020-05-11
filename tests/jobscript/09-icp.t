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
# Test for "cylc jobscript --icp=CYCLE_POINT".
. "$(dirname "${0}")/test_header"

set_test_number 3
init_suite "${TEST_NAME_BASE}" <<'__SUITE_RC__'
[cylc]
    UTC mode = True
[scheduling]
    [[dependencies]]
        [[[R1]]]
            graph = foo
[runtime]
    [[foo]]
        script = true
__SUITE_RC__

run_ok "${TEST_NAME_BASE}" \
    cylc jobscript --icp=20200101T0000Z "${SUITE_NAME}" 'foo.20200101T0000Z'
contains_ok "${TEST_NAME_BASE}.stdout" <<__OUT__
    export CYLC_SUITE_INITIAL_CYCLE_POINT="20200101T0000Z"
__OUT__
cmp_ok "${TEST_NAME_BASE}.stderr" <<__ERR__
Task Job Script Generated: ${SUITE_RUN_DIR}/log/job/20200101T0000Z/foo/01/job
__ERR__
purge_suite "${SUITE_NAME}"
exit
