#!/bin/bash

# Run pgspot (https://github.com/timescale/pgspot) over the extension's SQL
# scripts as they are shipped to users (the *_prod.sql sources plus the
# hand-written upgrade scripts, never the *_dev.sql parts).  The generated
# sql/*.sql files are not used, so no build is required.
#
# Licensing: pgspot is PostgreSQL-licensed, but its dependency pglast is
# GPL-3.0-or-later.  Both are used only as a CI tool installed from PyPI; never
# vendor them into the repo or bundle them into shipped artifacts (docker
# images, packages), and do not add them to requirements.txt.

set -eu

cd "$(dirname "$0")/.."

# Prefer an explicit pgspot binary (e.g. from a venv), fall back to PATH.
PGSPOT="${PGSPOT:-pgspot}"

status=0

for f in $(ls sql/orioledb--*.sql | sort -V); do
	case "$f" in
		*_dev.sql) continue ;;
		*_prod.sql) ;;
		*) # hand-written script, unless it is generated from a _prod source
			[ -e "${f%.sql}_prod.sql" ] && continue ;;
	esac

	ignore=()
	case "$f" in
		sql/orioledb--1.0_prod.sql)
			# Known, accepted findings in the already released base script:
			# unqualified function references in four views.  The functions
			# are created earlier in the same script, in the target schema,
			# so the views cannot bind to anything else.  New scripts must
			# stay clean.
			ignore=(--ignore PS016 --ignore PS017)
			;;
	esac

	echo "pgspot: $f"
	"$PGSPOT" "${ignore[@]}" "$f" || status=1
done

exit $status
