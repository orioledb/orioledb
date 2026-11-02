#!/usr/bin/env python3
"""Release helper: ci/release.py {prepare,notes,publish} vX.Y.Z.

Run it from the main checkout.  The release is prepared in a detached
worktree of origin/vX.Y, so no local branch is created or changed.  Nothing
is ever force-pushed.
"""
import argparse
import os
import re
import shutil
import subprocess
import sys

import check_release

REPO = 'orioledb/orioledb'
FORMAT_DEFINES = [
    'ORIOLEDB_BINARY_VERSION', 'ORIOLEDB_SYS_TREE_VERSION',
    'ORIOLEDB_PAGE_VERSION', 'ORIOLEDB_COMPRESS_VERSION',
    'ORIOLEDB_WAL_VERSION', 'ORIOLEDB_CHECKPOINT_CONTROL_VERSION'
]
NON_USER_DIRS = ('test/', '.github/', 'ci/', 'doc/', '.claude/')
HELPER_RE = re.compile(r'test|debug|stopevent|sys_xid', re.I)


def die(msg):
	print('error: ' + msg, file=sys.stderr)
	sys.exit(1)


def run(args, cwd=None, check=True):
	res = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
	if check and res.returncode != 0:
		err = (res.stderr.strip() or res.stdout.strip()).splitlines()
		cmd = ' '.join(args)
		reason = err[-1] if err else f'rc={res.returncode}'
		die(f'"{cmd}" failed: {reason}')
	return res.stdout.strip()


def git(*args, cwd=None, check=True):
	return run(['git'] + list(args), cwd=cwd, check=check)


class Release:

	def __init__(self, args):
		m = re.match(r'^v(\d+)\.(\d+)\.(\d+)$', args.tag)
		if not m:
			die(f'tag "{args.tag}" does not match vMAJOR.MINOR.PATCH')
		self.tag = args.tag
		self.version = args.tag[1:]
		self.minor = f'{m.group(1)}.{m.group(2)}'
		self.branch = 'v' + self.minor
		self.root = git('rev-parse', '--show-toplevel')
		self.worktree = os.path.abspath(args.worktree or os.path.join(
		    os.path.dirname(self.root), 'orioledb-release-' + self.minor))

	def tag_exists(self):
		if git('tag', '-l', self.tag, cwd=self.root):
			return 'locally'
		if git('ls-remote',
		       '--tags',
		       'origin',
		       'refs/tags/' + self.tag,
		       cwd=self.root):
			return 'on origin'
		return None


def read(d, path):
	with open(os.path.join(d, path)) as f:
		return f.read()


def write(d, path, text):
	with open(os.path.join(d, path), 'w') as f:
		f.write(text)


def sub_once(text, pattern, repl, what, flags=re.M):
	text, n = re.subn(pattern, repl, text, flags=flags)
	if n == 0:
		die(f'{what}: pattern not found')
	return text


def psql_table(header, value):
	w = max(len(header), len(value))
	left = (w - len(header)) // 2
	return [
	    ' ' + ' ' * left + header + ' ' * (w - len(header) - left) + ' ',
	    '-' * (w + 2), ' ' + value
	]


def edit_docs(release, old, new):
	path = 'doc/contributing/docker-builds.mdx'
	text = read(release.worktree, path)

	if old not in text:
		die(f'{path}: "{old}" not found')

	text = text.replace(old, new)

	lines = text.split('\n')
	idx = [i for i, l in enumerate(lines) if 'select orioledb_version();' in l]
	if len(idx) != 1 or lines[idx[0] + 4].strip() != '(1 row)':
		die(f'{path}: orioledb_version() output not found')
	i = idx[0]
	lines[i + 1:i + 4] = psql_table('orioledb_version', new)

	found = False
	for i, l in enumerate(lines):
		if l.startswith(' orioledb |'):
			cells = l.split('|')
			for c in (1, 2):
				cells[c] = (' ' + release.minor).ljust(len(cells[c]))
			lines[i] = '|'.join(cells)
			found = True
	if not found:
		die(f'{path}: \\dx row of orioledb not found')

	write(release.worktree, path, '\n'.join(lines))


def edit_files(release):
	path = 'include/orioledb.h'
	text = read(release.worktree, path)

	m = re.search(r'^#define ORIOLEDB_VERSION "(.*)"', text, re.M)
	if not m:
		die(f'{path}: ORIOLEDB_VERSION not found')

	old, new = m.group(1), 'OrioleDB ' + release.version
	write(
	    release.worktree, path,
	    sub_once(text, r'^(#define ORIOLEDB_VERSION ")(.*)(")',
	             lambda m: m.group(1) + new + m.group(3), path))

	for path in ('test/expected/tableam.out', 'test/expected/tableam_1.out'):
		text = read(release.worktree, path)
		if old not in text:
			die(f'{path}: "{old}" not found')
		write(release.worktree, path, text.replace(old, new))

	edit_docs(release, old, new)

	return old, new


def cmd_prepare(args):
	"""prepare subcommand"""
	release = Release(args)
	ref = 'origin/' + release.branch

	where = release.tag_exists()
	if where:
		die(f'tag {release.tag} already exists {where}')

	if os.path.exists(release.worktree):
		die(f'worktree directory {release.worktree} already exists')

	git('fetch', 'origin', '--tags', cwd=release.root)

	if git('rev-parse', '--verify', '-q', ref, cwd=release.root,
	       check=False) == '':
		die(f'new minor branch: create {release.branch} manually')

	for r in ('origin/main', ref):
		target = git('show', r + ':.backpatch-target',
		             cwd=release.root).strip()
		if target != release.branch:
			die(f'{release.branch} is not the supported branch (.backpatch-target on {r} is {target}'
			    )

	git('worktree', 'add', '--detach', release.worktree, ref, cwd=release.root)

	try:
		old, new = edit_files(release)
	except SystemExit:
		print(
		    f'worktree left at {release.worktree}; remove it with: git worktree remove --force {release.worktree}',
		    file=sys.stderr)
		raise

	errors = check_release.run_checks(release.worktree, release.tag)
	for e in errors:
		print('FAIL: ' + e)

	for f in sorted(os.listdir(os.path.join(release.worktree, 'sql'))):
		if not f.endswith('_prod.sql'):
			continue
		for n in re.findall(
		    r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([\w."]+)',
		    read(release.worktree, 'sql/' + f), re.I):
			if HELPER_RE.search(n):
				print(f'REVIEW: sql/{f}: {n}')

	print('worktree: ' + release.worktree)
	print(git('status', '--short', cwd=release.worktree))

	if errors:
		die(f'checks failed; worktree left at {release.worktree}; remove it with: git '
		    f'worktree remove --force {release.worktree}')

	print(f"review the changes, then commit them as 'Stamp {release.version}'")


def prev_tag(rel):
	cands = []
	for t in git('tag', '-l', cwd=rel.root).split():
		if re.match(r'^v\d+\.\d+\.\d+$', t):
			v = tuple(int(x) for x in t[1:].split('.'))
			if v < tuple(int(x) for x in rel.version.split('.')):
				cands.append((v, t))
	if not cands:
		die(f'no tag lower than {rel.tag} found')
	return max(cands)[1]


def define_values(d, ref):
	vals = {}
	for name in FORMAT_DEFINES:
		out = git('grep',
		          '-h',
		          '-E',
		          rf'^#define[[:space:]]+{name}[[:space:]]',
		          ref,
		          '--',
		          'include',
		          cwd=d,
		          check=False)
		m = re.search(rf'#define\s+{name}\s+\(?(\d+)\)?', out)
		if m:
			vals[name] = m.group(1)
	return vals


def control_version(d, ref):
	out = git('show', ref + ':orioledb.control', cwd=d, check=False)
	m = re.search(r"^default_version\s*=\s*'(.*)'", out, re.M)
	return m.group(1) if m else None


def cmd_notes(args):
	"""notes subcommand"""
	release = Release(args)

	if not os.path.isdir(release.worktree):
		die(f'worktree {release.worktree} does not exist: run prepare first')

	prev = prev_tag(release)

	bullets = []
	for line in git('log',
	                prev + '..HEAD',
	                '--no-merges',
	                '--format=%H%x09%s',
	                cwd=release.worktree).splitlines():
		h, subj = line.split('\t', 1)
		if subj.startswith('Stamp '):
			continue
		files = git('diff-tree',
		            '--no-commit-id',
		            '--name-only',
		            '-r',
		            h,
		            cwd=release.worktree).splitlines()
		if files and all(f.startswith(NON_USER_DIRS) for f in files):
			print(f'skipped: {h[:8]} {subj}', file=sys.stderr)
			continue
		subj = re.sub(r'\s*\(#\d+\)$', '', subj)
		bullets.append(f'- {subj} ({h[:8]})')

	old_v, new_v = control_version(release.worktree, prev), control_version(
	    release.worktree, 'HEAD')
	if old_v != new_v:
		sql = f'Extension SQL version changed: {old_v} -> {new_v}.'
	else:
		sql = f'Extension SQL version stays at {new_v}.'
		sql_files = git('diff',
		                '--name-only',
		                prev,
		                'HEAD',
		                '--',
		                'sql/*_prod.sql',
		                cwd=release.worktree).splitlines()
		if sql_files:
			sql += f' Changed SQL scripts: {", ".join(sql_files)}.'

	old_d, new_d = define_values(release.worktree,
	                             prev), define_values(release.worktree, 'HEAD')
	diffs = [
	    f'{n} {old_d[n]} -> {new_d[n]}' for n in FORMAT_DEFINES
	    if n in old_d and n in new_d and old_d[n] != new_d[n]
	]
	fmt = ('Storage/WAL formats: changed: ' +
	       '; '.join(diffs) if diffs else 'Storage/WAL formats: unchanged.')

	print("## What's Changed\n")
	print('\n'.join(bullets) if bullets else 'No user-visible changes.')
	print(f'\n{sql}\n{fmt}\n')
	print(f'**Full Changelog**: https://github.com/{REPO}/compare/'
	      f'{prev}...{release.tag}')


def cmd_publish(args):
	"""publish subcommand"""
	release = Release(args)

	if not os.path.isdir(release.worktree):
		die(f'worktree {release.worktree} does not exist: run prepare first')

	# gh runs in the worktree, so a relative path must not depend on the cwd
	args.notes = os.path.abspath(args.notes)
	if not os.path.isfile(args.notes) or os.path.getsize(args.notes) == 0:
		die(f'notes file {args.notes} is missing or empty')

	if shutil.which('gh') is None:
		die('gh is not found in PATH')

	if git('status', '--porcelain', cwd=release.worktree):
		die(f'worktree {release.worktree} is not clean')

	subj = git('log', '-1', '--format=%s', cwd=release.worktree)
	if subj != 'Stamp ' + release.version:
		die(f'HEAD subject is "{subj}", expected "Stamp {release.version}"')

	# The stamp commit may have been amended or fixed up after prepare
	errors = check_release.run_checks(release.worktree,
	                                  release.tag,
	                                  published=False)
	for e in errors:
		print(f'FAIL: {e}')
	if errors:
		die('release checks failed on the stamp commit')

	where = release.tag_exists()
	if where:
		die(f'tag {release.tag} already exists {where}')

	steps = [
	    ('branch pushed', ['git', 'push', 'origin',
	                       'HEAD:' + release.branch], release.worktree),
	    ('tag created', ['git', 'tag', release.tag], release.worktree),
	    ('tag pushed', ['git', 'push', 'origin',
	                    release.tag], release.worktree),
	    ('release created', [
	        'gh', 'release', 'create', release.tag, '--draft', '--verify-tag',
	        '--title', release.tag, '--notes-file', args.notes
	    ], release.worktree),
	    ('worktree removed', ['git', 'worktree', 'remove',
	                          release.worktree], release.root),
	]

	git('fetch', 'origin', cwd=release.worktree)
	res = subprocess.run([
	    'git', 'merge-base', '--is-ancestor', 'origin/' + release.branch,
	    'HEAD'
	],
	                     cwd=release.worktree)
	if res.returncode != 0:
		die(f'origin/{release.branch} is not an ancestor of HEAD '
		    '(not a fast-forward)')

	if not args.yes:
		print('checks passed, would run:')
		for _, cmd, cwd in steps:
			print(f'  (in {cwd}) {" ".join(cmd)}')
		print('dry run: nothing was pushed, rerun with --yes to publish')
		return

	done = []
	for label, cmd, cwd in steps:
		try:
			res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
			failed = res.returncode != 0
			reason = res.stderr.strip() or res.stdout.strip()
		except OSError as e:
			failed, reason = True, str(e)

		if failed:
			print(f'error: step "{label}" failed: {reason}', file=sys.stderr)
			completed = ', '.join(done) or 'none'
			print(f'completed steps: {completed}', file=sys.stderr)
			pending = ', '.join(l for l, _, _ in steps if l not in done)
			print(f'not done: {pending}', file=sys.stderr)
			sys.exit(1)
		done.append(label)
		if label == 'release created':
			print('draft release: ' + res.stdout.strip().splitlines()[-1])

	print('The "Release sanity check" workflow runs on the tag push: '
	      'gh run list --workflow release_check.yml')


def main():
	parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
	sub = parser.add_subparsers(dest='cmd', required=True)

	common = argparse.ArgumentParser(add_help=False)
	common.add_argument('tag')
	common.add_argument('--worktree')

	prepare = sub.add_parser('prepare',
	                         parents=[common],
	                         help='create the worktree and edit the files')
	prepare.set_defaults(fn=cmd_prepare)

	notes = sub.add_parser('notes',
	                       parents=[common],
	                       help='print draft release notes')
	notes.set_defaults(fn=cmd_notes)

	publish = sub.add_parser(
	    'publish',
	    parents=[common],
	    help='push the branch and the tag, draft the release')
	publish.add_argument('--notes', required=True)
	publish.add_argument('--yes',
	                     action='store_true',
	                     help='really publish; without it only print the plan')
	publish.set_defaults(fn=cmd_publish)

	args = parser.parse_args()
	args.fn(args)


if __name__ == '__main__':
	main()
