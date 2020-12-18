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
# Test calling "cylc shutdown suite1" from suite2.
# See https://github.com/cylc/cylc-flow/issues/1843
. "$(dirname "$0")/test_header"

set_test_number 1
RUND="$RUN_DIR"
NAME1="cylctb-${CYLC_TEST_TIME_INIT}/${TEST_SOURCE_DIR_BASE}/${TEST_NAME_BASE}-1"
NAME2="cylctb-${CYLC_TEST_TIME_INIT}/${TEST_SOURCE_DIR_BASE}/${TEST_NAME_BASE}-2"
SUITE1_RUND="${RUND}/${NAME1}"
mkdir -p "${SUITE1_RUND}"
RND_SUITE_NAME=x$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c6)
RND_SUITE_SOURCE="$PWD/${RND_SUITE_NAME}"
mkdir -p "${RND_SUITE_SOURCE}"
cp -p "${TEST_SOURCE_DIR}/basic/flow.cylc" "${RND_SUITE_SOURCE}"
cylc install --flow-name="${NAME1}" --no-run-name --directory="${RND_SUITE_SOURCE}"
SUITE2_RUND="${RUND}/${NAME2}"
mkdir -p "${SUITE2_RUND}"
rm ${RND_SUITE_SOURCE}/flow.cylc
cat >"${RND_SUITE_SOURCE}/flow.cylc" <<__FLOW_CONFIG__
[scheduler]
    [[events]]
[scheduling]
    [[graph]]
        R1=t1
[runtime]
    [[t1]]
        script=cylc shutdown "${NAME1}"
__FLOW_CONFIG__
cylc install --flow-name="${NAME2}" --directory="${RND_SUITE_SOURCE}" --no-run-name
cylc run --no-detach "${NAME1}" 1>'1.out' 2>&1 &
SUITE_RUN_DIR="${SUITE1_RUND}" poll_suite_running
run_ok "${TEST_NAME_BASE}" cylc run --no-detach --abort-if-any-task-fails "${NAME2}"
cylc shutdown "${NAME1}" --max-polls=20 --interval=1 1>'/dev/null' 2>&1 || true
purge "${NAME1}"
purge "${NAME2}"
rm -rf ${RND_SUITE_SOURCE}
exit
