#!/usr/bin/env python3
import sys
import re


def main():
	if len(sys.argv) != 3:
		sys.exit(1)

	major_version = sys.argv[1]
	provided_version = sys.argv[2]

	# Read .pgtags file
	with open('.pgtags', 'r') as f:
		for line in f:
			if line.startswith(f'{major_version}:'):
				expected = line.split()[1].strip()
				break
		else:
			print(f"No version found for PostgreSQL {major_version}")
			sys.exit(1)

	# Extract numeric version if format is like "patches17_14"
	if '_' in expected:
		expected_num = expected.split('_')[1]
		tag_format = f"tag 'patches{major_version}_{expected_num}'"
	else:
		expected_num = expected
		tag_format = f"commit '{expected}'"

	# Check if provided version is numeric (not a hash)
	is_numeric = re.match(
	    r'^\d+$', provided_version) is not None and len(provided_version) < 6
	# Check if provided version is part of `git describe --tags`
	is_describe_tag = re.match(r'^\d+-\d+-g[0-9a-f]+$',
	                           provided_version) is not None

	# Compare appropriately
	if is_numeric or is_describe_tag:
		actual_mismatch = (expected_num != provided_version)
	else:
		actual_mismatch = (expected != provided_version)

	if actual_mismatch:
		print(
		    f"Wrong orioledb patchset version: expected {expected_num}, got {provided_version}"
		)
		print(
		    f"Rebuild and install patched orioledb/postgres using {tag_format}"
		)
		sys.exit(1)


if __name__ == '__main__':
	main()
