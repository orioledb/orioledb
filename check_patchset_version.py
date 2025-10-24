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
				parts = line.split()
				# Possible formats:
				# "17: patches17_14-4-gfd5baeb1b4    fd5baeb1b4af0affa698d480f46380aecae37ad1"
				# "17: patches17_14"
				expected_tag = parts[1].strip()
				expected_hash = parts[2].strip() if len(parts) >= 3 else None
				break
		else:
			print(f"No version found for PostgreSQL {major_version}")
			sys.exit(1)

	# Extract numeric version from git describe format
	# Examples:
	#   patches17_14 -> 14
	#   patches17_14-4-gfd5baeb1b4 -> 14-4-gfd5baeb1b4
	match = re.match(r'^patches\d+_(\d+(?:-\d+-g[0-9a-f]+)?)$', expected_tag)
	if match:
		expected_num = match.group(1)
	else:
		print(f"Wrong tag format {expected_tag}")
		sys.exit(1)

	# Check if provided version is numeric (not a hash)
	is_numeric = re.match(
	    r'^\d+$', provided_version) is not None and len(provided_version) < 6
	# Check if provided version is part of `git describe --tags`
	is_describe_tag = re.match(r'^\d+-\d+-g[0-9a-f]+$',
	                           provided_version) is not None
	# Check if provided version is a full commit hash
	is_hash = re.match(r'^[0-9a-f]{40}$', provided_version) is not None

	actual_mismatch = True
	# Compare appropriately
	if is_numeric or is_describe_tag:
		# Compare against the extracted numeric/describe version
		actual_mismatch = (expected_num != provided_version)
	elif is_hash and expected_hash:
		# Compare full hash against the one from .pgtags
		actual_mismatch = (expected_hash != provided_version)
	elif is_hash and not expected_hash:
		# Full hash provided but not in .pgtags - this is a mismatch
		actual_mismatch = True

	if actual_mismatch:
		print(
		    f"Wrong orioledb patchset version: expected {expected_num}, got {provided_version}"
		)
		print(
		    f"Rebuild and install patched orioledb/postgres using tag '{expected_tag}'"
		)
		sys.exit(1)


if __name__ == '__main__':
	main()
