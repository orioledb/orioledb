# orioledb performance benchmarks

Benchmarks in this directory are separate from the correctness test
suites.  `make installcheck` / `make check` do not invoke them; they
run only through `make perfcheck` (or when invoked directly), so CI
and normal test iteration are unaffected.

## Running

```sh
# All benchmarks under perf/*_perf.py:
make IS_DEV=1 USE_PGXS=1 perfcheck

# A single benchmark with custom parameters:
PATH="/usr/local/pgsql/bin:$PATH" \
    python3 -W ignore::DeprecationWarning \
    perf/<name>_perf.py --rows 100000

# Options for any benchmark:
python3 perf/<name>_perf.py --help
```

## Adding new benchmarks

Drop a `*_perf.py` file into this directory; it is picked up
automatically by the `PERFCHECKS = $(sort $(wildcard perf/*_perf.py))`
glob in the Makefile.  Conventions:

- Start the script with a short module docstring describing what it
  measures and how each configuration is produced.
- Print `# commit = ...` and the parameter settings before the first
  result row.
