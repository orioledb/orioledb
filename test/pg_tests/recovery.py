#!/usr/bin/env python3

import os
import pathlib
import shutil
import sys

from os import listdir
from os.path import isfile, join

from test.pg_tests.perl_base import generate_perl_test_calls
from test.pg_tests.utils import file_name_to_test_name

expectedFailures = [
	"003_recovery_targets.pl",
	"004_timeline_switch.pl",
	"005_replay_delay.pl",
	"006_logical_decoding.pl",
	"009_twophase.pl",
	"010_logical_decoding_timelines.pl",
	"012_subtransactions.pl",
	"014_unlogged_reinit.pl",
	"017_shm.pl",
	"018_wal_optimize.pl",
	"019_replslot_limit.pl",
	"021_row_visibility.pl",
	"023_pitr_prepared_xact.pl",
	"026_overwrite_contrecord.pl",
	"027_stream_regress.pl",
	"028_pitr_timelines.pl",
	"029_stats_restart.pl",
	"030_stats_cleanup_replica.pl",
	"031_recovery_conflict.pl",
	"032_relfilenode_reuse.pl",
	"035_standby_logical_decoding.pl",
	"040_standby_failover_slots_sync.pl",
	"041_checkpoint_at_promote.pl",
	"043_no_contrecord_switch.pl",
	"046_checkpoint_logical_slot.pl",
	"047_checkpoint_physical_slot.pl",
	"048_vacuum_horizon_floor.pl",
]

if not 'PG_SRC_PATH' in os.environ:
	sys.exit("PG_SRC_PATH env variable should be path to postgres sources")

script_path = os.path.dirname(os.path.realpath(__file__))
recoverydir = join(os.environ['PG_SRC_PATH'], 'src/test/recovery/t')
bindir = pathlib.Path(shutil.which("postgres")).parent.resolve()
include_path = join(script_path, "perl_include")

tests = [
    f for f in listdir(recoverydir)
    if isfile(join(recoverydir, f)) and f.endswith(".pl")
]

single_test_path = os.path.join(script_path, '_g_rec_single_test.py')
temp_config_path = os.path.join(script_path, 'perl_temp.conf')
generate_perl_test_calls(tests, single_test_path, include_path, recoverydir,
                         expectedFailures, temp_config_path)

from . import _g_rec_single_test

# shortcuts for calling single tests like this "test.pg_tests.recovery.test_replslot_limit"
for test in tests:
	globals()[file_name_to_test_name(
	    test)] = lambda testname=test: _g_rec_single_test.Run(
	        file_name_to_test_name(testname))

from ._g_rec_single_test import Run
