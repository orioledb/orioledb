import os
import re
import shutil
import subprocess

from subprocess import Popen
from testgres.node import PostgresNode


def parse_regress_schedule(schedule):
	with open(schedule, 'r') as f:
		groups = []
		depends = []
		for line in f:
			test_match = re.search(r"test:\s*(.*)", line)
			if test_match:
				groups += [test_match.group(1).strip().split(" ")]
			else:
				depends_match = re.search(
				    r"#\s*(.*) depends on\s*((\w+(, )?)+)", line)
				if depends_match:
					dependent = depends_match.group(1)
					dependencies = depends_match.group(2).strip().split(", ")
					depends += [[dependent, dependencies]]
		return (groups, dict(depends))


def pg_regress(node: PostgresNode,
               test,
               first=True,
               fail_ok=False,
               verbose=False,
               clean=True):
	regressdir = os.path.join(os.environ['PG_SRC_PATH'], 'src/test/regress')
	pg_regress_path = os.path.join(regressdir, 'pg_regress')
	results_path = os.path.join(node.base_dir, 'results')
	os.makedirs(results_path, exist_ok=True)
	outputdir = os.path.join(results_path, test)
	cmd = [
	    pg_regress_path, '--max-connections=1', f'--port={node.port}',
	    f'--inputdir={regressdir}', f'--dlpath={regressdir}',
	    f'--outputdir={outputdir}'
	]
	if not first:
		cmd += ["--use-existing"]
	else:
		cmd += ["--load-extension=orioledb"]
	# TODO: Use --expecteddir for orioledb specific expects, ln postgreses one
	cmd += [test]
	process = Popen(cmd,
	                cwd=regressdir,
	                stderr=subprocess.PIPE,
	                stdout=subprocess.PIPE)
	output, error = process.communicate()

	if verbose:
		print(output.decode("utf-8"))
		print(error.decode("utf-8"))
	if process.returncode != 0 and not fail_ok:
		raise Exception(f"error when running: {cmd}")
	elif clean:
		if os.path.exists(outputdir):
			shutil.rmtree(outputdir)


def parse_isolation_schedule(schedule):
	with open(schedule, 'r') as f:
		tests = []
		for line in f:
			test_match = re.search(r"test:\s*(.*)", line)
			if test_match:
				tests += test_match.group(1).strip().split(" ")
		return tests


def pg_isolation_regress(node: PostgresNode,
                         test,
                         timeout=30,
                         verbose=False,
                         clean=True):
	isolationdir = os.path.join(os.environ['PG_SRC_PATH'],
	                            'src/test/isolation')
	pg_regress_path = os.path.join(isolationdir, 'pg_isolation_regress')
	results_path = os.path.join(node.base_dir, 'results')
	os.makedirs(results_path, exist_ok=True)
	outputdir = os.path.join(results_path, test)
	cmd = [
	    pg_regress_path, '--max-connections=1', f'--port={node.port}',
	    f'--inputdir={isolationdir}', f'--dlpath={isolationdir}',
	    f'--outputdir={outputdir}'
	]
	cmd += ["--load-extension=orioledb"]
	# TODO: Use --expecteddir for orioledb specific expects, ln postgreses one
	cmd += [test]
	process = Popen(cmd,
	                cwd=isolationdir,
	                stderr=subprocess.PIPE,
	                stdout=subprocess.PIPE)
	output, error = process.communicate(timeout=timeout)

	if verbose:
		print(output.decode("utf-8"))
		print(error.decode("utf-8"))
	if process.returncode != 0:
		raise Exception(f"error when running: {cmd}")
	elif clean:
		if os.path.exists(outputdir):
			shutil.rmtree(outputdir)


def prove(node: PostgresNode,
          test,
          test_path,
          include_path,
          temp_config_path,
          timeout=30,
          verbose=False):
	regressdir = os.path.join(os.environ['PG_SRC_PATH'], 'src/test/regress')
	pg_regress_path = os.path.join(regressdir, 'pg_regress')
	prove_path = shutil.which("prove")
	cmd = [
	    prove_path,
	    "-I",
	    include_path,
	]
	cmd += [os.path.join(test_path, test)]
	cmd_env = os.environ.copy()
	cmd_env["PGPORT"] = str(node.port)
	cmd_env["TESTLOGDIR"] = node.logs_dir
	cmd_env["TESTDATADIR"] = node.base_dir
	cmd_env["PG_REGRESS"] = pg_regress_path
	cmd_env["TEMP_CONFIG"] = temp_config_path
	cmd_env["PG_TEST_INITDB_EXTRA_OPTS"] = "--no-locale --encoding=UTF8"
	process = Popen(cmd,
	                stderr=subprocess.PIPE,
	                stdout=subprocess.PIPE,
	                env=cmd_env)
	output, error = process.communicate(timeout=timeout)

	if verbose:
		print(output.decode("utf-8"))
		print(error.decode("utf-8"))
	if process.returncode != 0:
		raise Exception(f"error when running: {cmd}")


def normalize_name(name: str):
	name = name.replace('.', '_').replace('-', '_')
	return f"test_{name}"


def file_name_to_test_name(file: str):
	name = re.sub(r'^\d+_', '', file.split('.')[0])
	return f"test_{name}"
