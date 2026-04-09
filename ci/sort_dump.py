#!/usr/bin/env python3
"""Normalize a pg_dump directory-format dump for deterministic comparison.

Reads the TOC via pg_restore -l to map file IDs to table names, then
sorts rows in each .dat file and writes normalized files named by
schema.table into an output directory.

Usage: sort_dump.py <dump_dir> <output_dir>
"""

import os
import subprocess
import sys


def main():
    dump_dir = sys.argv[1]
    out_dir = sys.argv[2]

    os.makedirs(out_dir, exist_ok=True)

    result = subprocess.run(['pg_restore', '-l', dump_dir],
                            capture_output=True, text=True)

    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith(';'):
            continue
        if 'TABLE DATA' not in line:
            continue

        # Format: "ID; OFFSET OID TABLE DATA schema table owner"
        parts = line.split()
        file_id = parts[0].rstrip(';')
        td_idx = parts.index('TABLE')
        schema = parts[td_idx + 2]
        table = parts[td_idx + 3]

        dat_file = os.path.join(dump_dir, f"{file_id}.dat")
        if not os.path.exists(dat_file):
            continue

        with open(dat_file, 'r') as f:
            rows = f.readlines()
        rows.sort()

        out_file = os.path.join(out_dir, f"{schema}.{table}.dat")
        with open(out_file, 'w') as f:
            f.writelines(rows)


if __name__ == '__main__':
    main()
