#!/usr/bin/env python3
"""Sanity checks for a release tag: ci/check_release.py <tag> [--repo DIR].

Run it on the commit the tag points to.  All failures are collected and
printed one per line; the exit status is 1 if there are any.
"""
import argparse
import glob
import os
import re
import subprocess
import sys


def read(repo, path):
	with open(os.path.join(repo, path)) as f:
		return f.read()


def check_version_header(repo, version):
	m = re.search(r'^#define ORIOLEDB_VERSION "(.*)"',
	              read(repo, 'include/orioledb.h'), re.M)
	if not m:
		return ['include/orioledb.h: ORIOLEDB_VERSION not found']
	if m.group(1) != 'OrioleDB ' + version:
		return [
		    f'include/orioledb.h: ORIOLEDB_VERSION is "{m.group(1)}", '
		    f'expected "OrioleDB {version}"'
		]
	return []


def check_branch(repo, branch, published=True):
	errors = []
	ref = 'origin/' + branch
	# An unpublished stamp commit is ahead of the branch, not on it
	if published:
		res = subprocess.run(
		    ['git', '-C', repo, 'merge-base', '--is-ancestor', 'HEAD', ref],
		    capture_output=True)
		if res.returncode != 0:
			errors.append(f'HEAD is not an ancestor of {ref}')
	target = read(repo, '.backpatch-target').strip()
	if target != branch:
		errors.append(f'.backpatch-target is "{target}", expected "{branch}"')
	return errors


def check_pgtags(repo):
	majors = set()
	errors = []
	for line in read(repo, '.pgtags').splitlines():
		line = line.strip()
		if not line:
			continue
		m = re.match(r'^(\d+): patches(\d+)_\d+$', line)
		if not m:
			errors.append(f'.pgtags: bad line "{line}"')
		elif m.group(1) != m.group(2):
			errors.append(f'.pgtags: "{line}" is a tag of a different major')
		else:
			majors.add(m.group(1))
	for major in ('16', '17', '18'):
		if major not in majors:
			errors.append(f'.pgtags: PostgreSQL {major} is missing')
	return errors


def check_docs(repo, version):
	path = 'doc/contributing/docker-builds.mdx'
	if 'OrioleDB ' + version not in read(repo, path):
		return [f'{path}: "OrioleDB {version}" not found']
	return []


def check_sql(repo, short):
	errors = []

	m = re.search(r"^default_version\s*=\s*'(.*)'",
	              read(repo, 'orioledb.control'), re.M)
	if not m or m.group(1) != short:
		current = f"'{m.group(1)}'" if m else 'missing'
		errors.append(f"orioledb.control: default_version is {current}, "
		              f"expected '{short}'")

	sql = os.path.join(repo, 'sql')
	if not (glob.glob(os.path.join(sql, f'orioledb--*--{short}_prod.sql')) or
	        os.path.exists(os.path.join(sql, f'orioledb--{short}_prod.sql'))):
		errors.append(f'sql: no upgrade script to {short} found')

	for suffix in ('prod', 'dev'):
		for path in glob.glob(
		    os.path.join(sql, f'orioledb--{short}--*_{suffix}.sql')):
			errors.append(
			    f'sql: {os.path.basename(path)} upgrades past {short}')

	return errors


def run_checks(repo, tag, published=True):
	"""Return the list of failures for TAG checked out in REPO.

	PUBLISHED is False for a stamp commit that is not pushed yet.
	"""
	m = re.match(r'^v(\d+)\.(\d+)\.(\d+)$', tag)
	if not m:
		return [f'tag "{tag}" does not match vMAJOR.MINOR.PATCH']
	version = tag[1:]
	short = f'{m.group(1)}.{m.group(2)}'

	errors = []
	errors += check_version_header(repo, version)
	errors += check_branch(repo, 'v' + short, published)
	errors += check_pgtags(repo)
	errors += check_docs(repo, version)
	errors += check_sql(repo, short)
	return errors


def main():
	parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
	parser.add_argument('tag')
	parser.add_argument('--repo', default='.')
	args = parser.parse_args()

	errors = run_checks(args.repo, args.tag)
	for error in errors:
		print('FAIL: ' + error)
	if not errors:
		print(f'OK: {args.tag} passes all release checks')

	return 1 if errors else 0


if __name__ == '__main__':
	sys.exit(main())
