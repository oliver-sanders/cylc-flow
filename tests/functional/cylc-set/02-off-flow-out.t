#!/usr/bin/env bash
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
#-------------------------------------------------------------------------------

# "cylc set" proposal examples.
# Set off-flow outputs to prevent a new flow from stalling.

. "$(dirname "$0")/test_header"
set_test_number 11

install_and_validate
reftest_run

# Check that we set:
#  - all the required outputs of a_cold
#  - the requested and implied outputs of b_cold and c_cold

grep_workflow_log_ok grep-a1 'implied output "submitted" of 1/a_cold'
grep_workflow_log_ok grep-a2 'implied output "started" of 1/a_cold'
grep_workflow_log_ok grep-a3 'completing output "succeeded" of 1/a_cold'

grep_workflow_log_ok grep-b1 'implied output "submitted" of 1/b_cold'
grep_workflow_log_ok grep-b2 'implied output "started" of 1/b_cold'
grep_workflow_log_ok grep-b3 'completing output "succeeded" of 1/b_cold'

grep_workflow_log_ok grep-c1 'implied output "submitted" of 1/c_cold'
grep_workflow_log_ok grep-c2 'implied output "started" of 1/c_cold'
grep_workflow_log_ok grep-c3 'completing output "succeeded" of 1/c_cold'

purge