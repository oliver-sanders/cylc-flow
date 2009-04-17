#!/usr/bin/python

# YOU MUST EXPORT PYTHONPATH TO MAKE AVAILABLE:
# 1/ sequenz 'src' module directory
# 2/ system config module directory (for config.py and task_classes.py)

import sys, os

import dummy_mode_clock 
from master_control import main_switch
import pyro_ns_naming
import pyro_setup
import pimp_my_logger
import task_base
import task_manager
import dead_letter
import config

import logging
import re

import profile

global pyro_daemon

# scons will replace this with actual version at build/install time:
sequenz_version = "foo-bar-baz";

def print_banner():
    print "__________________________________________________________"
    print
    print "      .       Sequenz Dynamic Metascheduler       ."
    print "version: " + sequenz_version
    print "      .         Hilary Oliver, NIWA, 2008         ."
    print "              See repository documentation"
    print "      .    Pyro nameserver required: 'pyro-ns'    ."
    print "__________________________________________________________"

def clean_shutdown( reason ):
    global pyro_daemon
    log = logging.getLogger( 'main' )
    log.critical( 'System Halt: ' + reason )
    pyro_daemon.shutdown( True ) 
#    sys.exit(0)

def usage():
    print "sequenz [-r]"
    print "Options:"
    print "  + [-r] restart from state dump file (overriding"
    print "         the configured start time and task list)."

def main( argv ):

    if len( argv ) > 2:
        usage()
        sys.exit(1)

    if len( argv ) == 2 and argv[1] == '-r':
        restart = True
    else:
        restart = False

    print_banner()

    # create the Pyro daemon
    global pyro_daemon
    pyro_daemon = pyro_setup.create_daemon()

    # dummy mode accelerated clock
    if config.dummy_mode:
        dummy_clock = dummy_mode_clock.time_converter( config.start_time, config.dummy_clock_rate, config.dummy_clock_offset ) 
        pyro_daemon.connect( dummy_clock, pyro_ns_naming.name( 'dummy_clock' ) )
    else:
        dummy_clock = None

    print
    print 'Logging to ' + config.logging_dir
    if not os.path.exists( config.logging_dir ):
        os.makedirs( config.logging_dir )

    # top level logging
    log = logging.getLogger( 'main' )
    pimp_my_logger.pimp_it( log, 'main', dummy_clock )

    # remotely accessible control switch
    master = main_switch()
    pyro_daemon.connect( master, pyro_ns_naming.name( 'master' ) )

    # dead letter box for remote use
    dead_letter_box = dead_letter.letter_box()
    pyro_daemon.connect( dead_letter_box, pyro_ns_naming.name( 'dead_letter_box' ) )

    # initialize the task pool from general config file or state dump
    pool = task_manager.manager( pyro_daemon, restart, dummy_clock )
    pyro_daemon.connect( pool, pyro_ns_naming.name( 'god' ) )

    print
    print "Beginning task processing now"
    if config.dummy_mode:
        print "      (DUMMY MODE)"
    print
 
    while True: # MAIN LOOP

        if master.system_halt:
            clean_shutdown( 'remote request' )
	    return

        if task_base.state_changed and not master.system_pause:
            # PROCESS ALL TASKS whenever one has changed state
            # as a result of a remote task message coming in: 
            # interact, run, create new, and kill spent tasks
            #---
            pool.process_tasks()

            pool.dump_state()

            if pool.all_finished():
                clean_shutdown( "ALL TASKS FINISHED" )
                return

        # REMOTE METHOD HANDLING; with no timeout and single-
        # threaded pyro, handleRequests() returns after one or
        # more remote method invocations are processed (these 
        # are not just task messages, hence the use of the
        # state_changed variable above).
        #---
        task_base.state_changed = False
        pyro_daemon.handleRequests( timeout = None )

     # END MAIN LOOP

if __name__ == "__main__":
    
    if False:
	# Do this to get performance profiling information
	# This method has a big performance hit itself, so
	# maybe there is a better way to do it?!)
        profile.run( 'main( sys.argv )' )

    else:
    	main( sys.argv )

