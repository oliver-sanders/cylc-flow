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
# Test restart from a checkpoint before a reload

# TODO: Remove named checkpointing - https://github.com/cylc/cylc-flow/issues/3891

. "$(dirname "$0")/test_header"

date-remove() {
    sed 's/[0-9]\+\(-[0-9]\{2\}\)\{2\}T[0-9]\{2\}\(:[0-9]\{2\}\)\{2\}Z/DATE/'
}

set_test_number 7

install_suite "${TEST_NAME_BASE}" "${TEST_NAME_BASE}"
cp -p 'flow.cylc' 'flow1.cylc'

run_ok "${TEST_NAME_BASE}-validate" cylc validate "${SUITE_NAME}"

# Suite reloads+inserts new task to mess up prerequisites - suite should stall
suite_run_fail "${TEST_NAME_BASE}-run" \
    timeout 120 cylc run "${SUITE_NAME}" --debug --no-detach
cylc ls-checkpoints "${SUITE_NAME}" | date-remove >'cylc-ls-checkpoints.out'
contains_ok 'cylc-ls-checkpoints.out' <<'__OUT__'
#######################################################################
# CHECKPOINT ID (ID|TIME|EVENT)
1|DATE|reload-init
2|DATE|reload-done
0|DATE|latest
__OUT__

cylc ls-checkpoints "${SUITE_NAME}" 1 | date-remove >'cylc-ls-checkpoints-1.out'
contains_ok 'cylc-ls-checkpoints-1.out' <<'__OUT__'
#######################################################################
# CHECKPOINT ID (ID|TIME|EVENT)
1|DATE|reload-init

# SUITE PARAMS (KEY|VALUE)
is_held|1

# BROADCAST STATES (POINT|NAMESPACE|KEY|VALUE)
2017|t1|script|true

# TASK POOL (CYCLE|NAME|STATUS|IS_HELD)
2017|t1|running|1
__OUT__
cylc ls-checkpoints "${SUITE_NAME}" 2 | date-remove >'cylc-ls-checkpoints-2.out'
contains_ok 'cylc-ls-checkpoints-2.out' <<'__OUT__'
#######################################################################
# CHECKPOINT ID (ID|TIME|EVENT)
2|DATE|reload-done

# SUITE PARAMS (KEY|VALUE)
is_held|1

# BROADCAST STATES (POINT|NAMESPACE|KEY|VALUE)
2017|t1|script|true

# TASK POOL (CYCLE|NAME|STATUS|IS_HELD)
2017|t1|running|1
__OUT__
cylc ls-checkpoints "${SUITE_NAME}" 0 | date-remove >'cylc-ls-checkpoints-0.out'
contains_ok 'cylc-ls-checkpoints-0.out' <<'__OUT__'
#######################################################################
# CHECKPOINT ID (ID|TIME|EVENT)
0|DATE|latest

# SUITE PARAMS (KEY|VALUE)

# BROADCAST STATES (POINT|NAMESPACE|KEY|VALUE)
2017|t1|script|true

# TASK POOL (CYCLE|NAME|STATUS|IS_HELD)
2017|t2|failed|0
__OUT__

# Restart should stall in exactly the same way
suite_run_fail "${TEST_NAME_BASE}-restart-1" \
    timeout 60 cylc restart "${SUITE_NAME}" --debug --no-detach

# # Restart from a checkpoint before the reload should allow the suite to proceed
# # normally. (disabled until named checkpointing and this whole test file removed)
# cp -p 'flow1.cylc' 'flow.cylc'
# suite_run_ok "${TEST_NAME_BASE}-restart-2" \
#     timeout 120 cylc restart "${SUITE_NAME}" \
#     --checkpoint=1 --debug --no-detach --reference-test

purge
exit
