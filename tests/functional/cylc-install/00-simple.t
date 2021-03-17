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

#------------------------------------------------------------------------------
# Test workflow installation
. "$(dirname "$0")/test_header"
set_test_number 26

# Test default name: "cylc install" (suite in $PWD, no args)
TEST_NAME="${TEST_NAME_BASE}-basic"
make_rnd_suite
pushd "${RND_SUITE_SOURCE}" || exit 1
run_ok "${TEST_NAME}" cylc install 

contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run1
__OUT__
popd || exit 1
purge_rnd_suite

# Test default name: "cylc install" (suite in $PWD, flow.cylc given as arg)
TEST_NAME="${TEST_NAME_BASE}-basic"
make_rnd_suite
pushd "${RND_SUITE_SOURCE}" || exit 1
run_ok "${TEST_NAME}" cylc install flow.cylc

contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run1
__OUT__
popd || exit 1
purge_rnd_suite


# Test default path: "cylc install REG" (flow in $PWD)
TEST_NAME="${TEST_NAME_BASE}-pwd2"
make_rnd_suite
pushd "${RND_SUITE_SOURCE}" || exit 1
run_ok "${TEST_NAME}" cylc install "${RND_SUITE_NAME}"
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run1
__OUT__
popd || exit 1
purge_rnd_suite

# Test cylc install succeeds if suite.rc file in source dir

TEST_NAME="${TEST_NAME_BASE}-no-flow-file"
make_rnd_suite
rm -f "${RND_SUITE_SOURCE}/flow.cylc"
touch "${RND_SUITE_SOURCE}/suite.rc"
run_ok "${TEST_NAME}" cylc install --flow-name="${RND_SUITE_NAME}" -C "${RND_SUITE_SOURCE}"
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run1
__OUT__
# test symlink not made in source dir
exists_fail "flow.cylc"
# test symlink correctly made in run dir
pushd "${RND_SUITE_RUNDIR}/run1" || exit 1
exists_ok "flow.cylc"
if [[ $(readlink "${RND_SUITE_RUNDIR}/run1/flow.cylc") == "${RND_SUITE_RUNDIR}/run1/suite.rc" ]] ; then
    ok "symlink.suite.rc"
else
    fail "symlink.suite.rc"
fi

INSTALL_LOG="$(find "${RND_SUITE_RUNDIR}/run1/log/install" -type f -name '*.log')"
grep_ok "The filename \"suite.rc\" is deprecated in favour of \"flow.cylc\". Symlink created." "${INSTALL_LOG}" 
popd || exit 1

purge_rnd_suite

# Test default path: "cylc install REG" --no-run-name (flow in $PWD)
TEST_NAME="${TEST_NAME_BASE}-pwd-no-run-name"
make_rnd_suite
pushd "${RND_SUITE_SOURCE}" || exit 1
run_ok "${TEST_NAME}" cylc install "${RND_SUITE_NAME}" --no-run-name
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}
__OUT__
popd || exit 1
purge_rnd_suite

# Test "cylc install REG" flow-name given (flow in $PWD)
TEST_NAME="${TEST_NAME_BASE}-flow-name"
make_rnd_suite
pushd "${RND_SUITE_SOURCE}" || exit 1
run_ok "${TEST_NAME}" cylc install --flow-name="${RND_SUITE_NAME}-olaf" 
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED ${RND_SUITE_NAME}-olaf from ${RND_SUITE_SOURCE} -> ${RUN_DIR}/${RND_SUITE_NAME}-olaf/run1
__OUT__
popd || exit 1
rm -rf "${RUN_DIR}/${RND_SUITE_NAME}-olaf"
purge_rnd_suite

# Test "cylc install REG" flow-name given (flow in $PWD)
TEST_NAME="${TEST_NAME_BASE}-flow-name-no-run-name"
make_rnd_suite
pushd "${RND_SUITE_SOURCE}" || exit 1
run_ok "${TEST_NAME}" cylc install --flow-name="${RND_SUITE_NAME}-olaf" --no-run-name
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED ${RND_SUITE_NAME}-olaf from ${RND_SUITE_SOURCE} -> ${RUN_DIR}/${RND_SUITE_NAME}-olaf
__OUT__
popd || exit 1
rm -rf "${RUN_DIR}/${RND_SUITE_NAME}-olaf"
purge_rnd_suite

# Test "cylc install" --directory given (flow in --directory)
TEST_NAME="${TEST_NAME_BASE}-option--directory"
make_rnd_suite
run_ok "${TEST_NAME}" cylc install --flow-name="${RND_SUITE_NAME}" --directory="${RND_SUITE_SOURCE}"
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run1
__OUT__
purge_rnd_suite

# Test running cylc install twice increments run dirs correctly
TEST_NAME="${TEST_NAME_BASE}-install-twice"
make_rnd_suite
pushd "${RND_SUITE_SOURCE}" || exit 1
run_ok "${TEST_NAME}" cylc install "${RND_SUITE_NAME}"
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run1
__OUT__
run_ok "${TEST_NAME}" cylc install "${RND_SUITE_NAME}"
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run2
__OUT__
popd || exit 1
purge_rnd_suite

# Test -C option
TEST_NAME="${TEST_NAME_BASE}-option-C"
make_rnd_suite
run_ok "${TEST_NAME}" cylc install --flow-name="${RND_SUITE_NAME}" -C "${RND_SUITE_SOURCE}"
contains_ok "${TEST_NAME}.stdout" <<__OUT__
INSTALLED $RND_SUITE_NAME from ${RND_SUITE_SOURCE} -> ${RND_SUITE_RUNDIR}/run1
__OUT__
purge_rnd_suite

exit
