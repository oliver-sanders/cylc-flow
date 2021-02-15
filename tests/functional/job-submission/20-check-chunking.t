#!/usr/bin/env bash
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
# Test that a job containing more than 100 tasks will split into batches.

. "$(dirname "$0")/test_header"
set_test_number 3

create_test_global_config '' "
[scheduler]
    process pool size = 1
[platforms]
    [[$CYLC_TEST_PLATFORM]]
        max batch submit size = 2
"

install_suite

run_ok "${TEST_NAME_BASE}-validate" cylc validate \
    -s "CYLC_TEST_PLATFORM='$CYLC_TEST_PLATFORM'" \
    "${SUITE_NAME}"

suite_run_ok "${TEST_NAME_BASE}-run" cylc run \
    -s "CYLC_TEST_PLATFORM='$CYLC_TEST_PLATFORM'" \
    --debug \
    --no-detach \
    --reference-test \
    "${SUITE_NAME}"

grep_ok \
    "# will invoke in batches, sizes=\[2, 2, 1\]" \
    "${SUITE_RUN_DIR}/log/suite/log"

# tidy up
purge
exit
