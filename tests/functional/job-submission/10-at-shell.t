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
# Test job submission via at, with SHELL set to tcsh
export REQUIRE_PLATFORM='batch:at'
. "$(dirname "$0")/test_header"
set_test_number 2
install_suite "${TEST_NAME_BASE}" "${TEST_NAME_BASE}"

create_test_global_config "" "
[platforms]
  [[atform]]
    batch system = at
    hosts = localhost
"

run_ok "${TEST_NAME_BASE}-validate" \
    cylc validate "${SUITE_NAME}"
# By setting "SHELL=/bin/tcsh", "at" would run its command under "/bin/tcsh",
# which would cause a failure of this test without the fix in #1749.
suite_run_ok "${TEST_NAME_BASE}-run" \
    env 'SHELL=/bin/tcsh' cylc run --reference-test --debug --no-detach "${SUITE_NAME}"

purge
exit
