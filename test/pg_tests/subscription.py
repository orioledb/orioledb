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
	"001_rep_changes.pl",
	"002_types.pl",
	"003_constraints.pl",
	"004_sync.pl",
	"005_encoding.pl",
	"006_rewrite.pl",
	"009_matviews.pl",
	"010_truncate.pl",
	"012_collation.pl",
	"013_partition.pl",
	"014_binary.pl",
	"015_stream.pl",
	"016_stream_subxact.pl",
	"017_stream_ddl.pl",
	"018_stream_subxact_abort.pl",
	"019_stream_subxact_ddl_abort.pl",
	"021_twophase.pl",
	"022_twophase_cascade.pl",
	"023_twophase_stream.pl",
	"025_rep_changes_for_schema.pl",
	"027_nosuperuser.pl",
	"028_row_filter.pl",
	"029_on_error.pl",
	"030_origin.pl",
	"031_column_list.pl",
	"032_subscribe_use_index.pl",
	"033_run_as_table_owner.pl",
	"100_bugs.pl",
]

if not 'PG_SRC_PATH' in os.environ:
	sys.exit("PG_SRC_PATH env variable should be path to postgres sources")

script_path = os.path.dirname(os.path.realpath(__file__))
subscriptiondir = join(os.environ['PG_SRC_PATH'], 'src/test/subscription/t')
bindir = pathlib.Path(shutil.which("postgres")).parent.resolve()
include_path = join(script_path, "perl_include")

tests = [
    f for f in listdir(subscriptiondir)
    if isfile(join(subscriptiondir, f)) and f.endswith(".pl")
]

single_test_path = os.path.join(script_path, '_g_sub_single_test.py')
temp_config_path = os.path.join(script_path, 'perl_temp.conf')
generate_perl_test_calls(tests, single_test_path, include_path,
                         subscriptiondir, expectedFailures, temp_config_path)

from . import _g_sub_single_test

# shortcuts for calling single tests like this "test.pg_tests.subscription.test_matviews"
for test in tests:
	globals()[file_name_to_test_name(
	    test)] = lambda testname=test: _g_sub_single_test.Run(
	        file_name_to_test_name(testname))

from ._g_sub_single_test import Run
