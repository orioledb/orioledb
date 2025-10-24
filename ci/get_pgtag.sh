#!/bin/bash
set -e

# Usage: ./get_pgtag.sh <pg_version> <pgtags_file>
PG_VERSION=$1
PGTAGS_FILE=${2:-.pgtags}

# Extract the version/tag from .pgtags
PGTAG_LINE=$(grep "^${PG_VERSION}: " "$PGTAGS_FILE")

if [ -z "$PGTAG_LINE" ]; then
  echo "Error: No tag found for PostgreSQL version $PG_VERSION in $PGTAGS_FILE" >&2
  exit 1
fi

# Extract fields: first field after colon is git describe, second (optional) is full hash
# Use awk to handle variable whitespace properly
PGTAG_RAW=$(echo "$PGTAG_LINE" | awk '{print $2}')
FULL_HASH=$(echo "$PGTAG_LINE" | awk '{print $3}')

# If a full hash is provided (40 characters), use it
if [ -n "$FULL_HASH" ] && [ ${#FULL_HASH} -eq 40 ]; then
  PGTAG="$FULL_HASH"
  echo "Using full commit hash: $PGTAG" >&2
  echo "Git describe: $PGTAG_RAW" >&2
# Check if it's a git describe format (extract base tag)
elif [[ "$PGTAG_RAW" =~ ^(.+)_([0-9]+)-([0-9]+)-g[0-9a-f]+$ ]]; then
  echo "PGTAG_RAW: $PGTAG_RAW, FULL_HASH: $FULL_HASH"
  echo "Error: No full commit hash provided for tag: $PGTAG_RAW"
  exit 1
else
  # Regular tag
  PGTAG="$PGTAG_RAW"
  echo "Using ref: $PGTAG" >&2
fi

if [ -n "$GITHUB_ENV" ]; then
  echo "PGTAG=$PGTAG" >> "$GITHUB_ENV"
fi
