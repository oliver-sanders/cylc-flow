#!/bin/bash

#         __________________________
#         |____C_O_P_Y_R_I_G_H_T___|
#         |                        |
#         |  (c) NIWA, 2008-2010   |
#         | Contact: Hilary Oliver |
#         |  h.oliver@niwa.co.nz   |
#         |    +64-4-386 0461      |
#         |________________________|


# cylc file-move example system, postprocess task

# run length 10 minutes

# trap errors so that we need not check the success of basic operations.
set -e; trap 'cylc message --failed' ERR

cylc message --started

# check environment
check-env.sh || exit 1

# check prerequistes
PRE=$TMPDIR/postproc/input/$CYCLE_TIME/forecast.nc
[[ ! -f $PRE ]] && {
    MSG="file not found: $PRE"
    echo "ERROR, postproc: $MSG"
    cylc message -p CRITICAL $MSG
    cylc message --failed
    exit 1
}

sleep $(( 10 * 60 / REAL_TIME_ACCEL ))

OUTDIR=$TMPDIR/postproc/output/$CYCLE_TIME
mkdir -p $OUTDIR
touch $OUTDIR/products.nc
cylc message "forecast products ready for $CYCLE_TIME"

cylc message --succeeded
