#!/usr/bin/env python3

import os
import pathlib
import shutil
import sys

from os import listdir
from os.path import isfile, join

from test.pg_tests.perl_base import generate_perl_test_calls
from test.pg_tests.utils import file_name_to_test_name

expectedFailures = []

if not 'PG_SRC_PATH' in os.environ:
	print("PG_SRC_PATH env variable should be path to postgres sources",
	      file=sys.stderr)

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
