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
# test file installation to remote platforms

export REQUIRE_PLATFORM='loc:remote comms:tcp'
. "$(dirname "$0")/test_header"
set_test_number 8

create_files () {
    # dump some files into the run dir
    for DIR in "bin" "app" "etc" "lib" "dir1" "dir2"
    do
        mkdir -p "${SUITE_RUN_DIR}/${DIR}"
        touch "${SUITE_RUN_DIR}/${DIR}/moo"
    done
    for FILE in "file1" "file2"
    do
        touch "${SUITE_RUN_DIR}/${FILE}"
    done
}

# Test configured files/directories along with default files/directories
# (app, bin, etc, lib) are correctly installed on the remote platform.
TEST_NAME="${TEST_NAME_BASE}-default-paths"
init_suite "${TEST_NAME}" <<__FLOW_CONFIG__
[scheduling]
    [[graph]]
        R1 = foo
[runtime]
    [[foo]]
        platform = $CYLC_TEST_PLATFORM
__FLOW_CONFIG__

create_files

# run the flow
run_ok "${TEST_NAME}-validate" cylc validate "${SUITE_NAME}" \
    -s "CYLC_TEST_PLATFORM='${CYLC_TEST_PLATFORM}'"
suite_run_ok "${TEST_NAME}-run1" cylc run "${SUITE_NAME}" \
    --no-detach \
    -s "CYLC_TEST_PLATFORM='${CYLC_TEST_PLATFORM}'"

# ensure these files get installed on the remote platform
SSH="$(cylc get-global-config -i "[platforms][$CYLC_TEST_PLATFORM]ssh command")"
${SSH} "${CYLC_TEST_HOST}" \
    find "${SUITE_RUN_DIR}/"{app,bin,etc,lib} -type f | sort > 'find.out'
cmp_ok 'find.out'  <<__OUT__
${SUITE_RUN_DIR}/app/moo
${SUITE_RUN_DIR}/bin/moo
${SUITE_RUN_DIR}/etc/moo
${SUITE_RUN_DIR}/lib/moo
__OUT__

purge

# Test the [scheduler]install configuration
TEST_NAME="${TEST_NAME_BASE}-configured-paths"
init_suite "${TEST_NAME}" <<__FLOW_CONFIG__
[scheduler]
    install = dir1/, dir2/, file1, file2
[scheduling]
    [[graph]]
        R1 = foo
[runtime]
    [[foo]]
        platform = $CYLC_TEST_PLATFORM
__FLOW_CONFIG__

create_files

run_ok "${TEST_NAME}-validate" cylc validate "${SUITE_NAME}" \
    -s "CYLC_TEST_PLATFORM='${CYLC_TEST_PLATFORM}'"
suite_run_ok "${TEST_NAME}-run2" cylc run "${SUITE_NAME}" \
    --no-detach \
    -s "CYLC_TEST_PLATFORM='${CYLC_TEST_PLATFORM}'"

${SSH} "${CYLC_TEST_HOST}" \
    find "${SUITE_RUN_DIR}/"{app,bin,dir1,dir2,file1,file2,etc,lib} -type f | sort > 'find.out'
cmp_ok 'find.out'  <<__OUT__
${SUITE_RUN_DIR}/app/moo
${SUITE_RUN_DIR}/bin/moo
${SUITE_RUN_DIR}/dir1/moo
${SUITE_RUN_DIR}/dir2/moo
${SUITE_RUN_DIR}/etc/moo
${SUITE_RUN_DIR}/file1
${SUITE_RUN_DIR}/file2
${SUITE_RUN_DIR}/lib/moo
__OUT__

purge

if ! command -v xfs_mkfile; then
    skip 2
    exit
fi

# Test file install completes before dependent tasks are executed
TEST_NAME="${TEST_NAME_BASE}-installation-timing"
init_suite "${TEST_NAME}" <<__FLOW_CONFIG__
[scheduler]
    install = dir1/, dir2/
    [[events]]
        abort on stalled = true
        abort on inactivity = true

[scheduling]
    [[graph]]
        R1 = olaf => sven

[runtime]
    [[olaf]]
        # task dependent on file install already being complete
        script = cat \${CYLC_SUITE_RUN_DIR}/dir1/moo
        platform = $CYLC_TEST_PLATFORM

    [[sven]]
        # task dependent on file install already being complete
        script = rm -r \${CYLC_SUITE_RUN_DIR}/dir1 \${CYLC_SUITE_RUN_DIR}/dir2
        platform = $CYLC_TEST_PLATFORM

__FLOW_CONFIG__

# This generates a large file, ready for the file install. The aim is
# to slow rsync and ensure tasks do not start until file install has
# completed.
for DIR in "dir1" "dir2"; do
    mkdir -p "${SUITE_RUN_DIR}/${DIR}"
    xfs_mkfile 1024m "${SUITE_RUN_DIR}/${DIR}/moo"
done

run_ok "${TEST_NAME}-validate" cylc validate "${SUITE_NAME}"
suite_run_ok "${TEST_NAME}-run" \
    cylc run --debug --no-detach "${SUITE_NAME}"

purge
exit
