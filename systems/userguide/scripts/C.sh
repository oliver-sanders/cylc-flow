#!/bin/bash

#         __________________________
#         |____C_O_P_Y_R_I_G_H_T___|
#         |                        |
#         |  (c) NIWA, 2008-2010   |
#         | Contact: Hilary Oliver |
#         |  h.oliver@niwa.co.nz   |
#         |    +64-4-386 0461      |
#         |________________________|


# CYLC USERGUIDE EXAMPLE SYSTEM. 
# Task C: a storm surge model.

# Depends on atmos surface pressure and winds, and own restart file.
# Generates two restart files, valid for the next two cycles.

# run length 120 minutes, scaled by $REAL_TIME_ACCEL 

# trap errors so that we need not check the success of basic operations.
set -e; trap 'cylc message --failed' ERR

# START MESSAGE
cylc message --started

# check environment
check-env.sh || exit 1

# check prerequisites
ONE=$TMPDIR/surface-winds-${CYCLE_TIME}.nc
TWO=$TMPDIR/surface-pressure-${CYCLE_TIME}.nc
THR=$TMPDIR/${TASK_NAME}-${CYCLE_TIME}.restart
for PRE in $ONE $TWO $THR; do
    if [[ ! -f $PRE ]]; then
        # FAILURE MESSAGE
        cylc message -p CRITICAL "file not found: $PRE"
        cylc message --failed
        exit 1
    fi
done

# EXECUTE THE MODEL ...

# create a restart file for the next cycle
sleep $(( 40 * 60 / $REAL_TIME_ACCEL )) 
touch $TMPDIR/${TASK_NAME}-${NEXT_CYCLE_TIME}.restart
cylc message --next-restart-completed

# create a restart file for the next next cycle
sleep $(( 40 * 60 / $REAL_TIME_ACCEL )) 
touch $TMPDIR/${TASK_NAME}-${NEXT_NEXT_CYCLE_TIME}.restart
cylc message --next-restart-completed

# create storm surge forecast output
sleep $(( 40 * 60 / $REAL_TIME_ACCEL )) 
touch $TMPDIR/storm-surge-${CYCLE_TIME}.nc
cylc message "storm surge fields ready for $CYCLE_TIME"

# SUCCESS MESSAGE
cylc message --succeeded
