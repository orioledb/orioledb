# OrioleDB ❤️ Antithesis

Defines antithesis-ready simulation tests for OrioleDB. Each simulation is defined by a combination of container images and docker-compose configuration.  The container images have instrumented binaries and property assertions to guide the simulation search.

## Components

### Images

Build via `make build` (see details under Usage)

- `orioledb-config:<sha>_pg<PG_MAJOR>_odb<ORIOLEDB_REF><config name>` - antithesis config image, contains dynamically built `docker-compose.yaml` and supporting files for a specific configuration set and OrioleDB version.
- `orioledb-antithesis:<sha>_pg<PG_MAJOR>_odb<ORIOLEDB_REF>` - patched postgres + `orioledb.so` instrumented to run under Antithesis simulation
    - The default OrioleDB is latest main branch `git@github.com:orioledb/orioledb.git`, but can be any git reference
- `jepsen:<sha>` - client workload
    - the append workload is a transactional correctness test for upsert operations. It hammers Postgres with concurrent multi-key transactions that append unique integers to CSV-encoded "lists." After a period of upsert writes and read operations, it reads back the state and analyzes the observed histories for anomalies.
    - because every appended element is unique and lists preserve order, the workload can reconstruct version history and detect transactional anomalies (cycles → serializability violations, lost updates, aborted reads, etc.)
- `health-checker:<sha>` - simulation ready signal (see Appendix)
- `sk-recovery-race-client:<sha>` - client workload
    - deterministically constructs the PK/SK checkpoint race fixed in [orioledb#855](https://github.com/orioledb/orioledb/issues/855): pins concurrent INSERT/UPDATE/DELETE backends at the PK-applied/SK-pending boundary via `pg_stopevent_set`, forces a `CHECKPOINT` through them, then holds the window open for `RACE_WINDOW_SECONDS` so Antithesis's fault injection has a real chance of landing inside it. Reports via Antithesis SDK assertions (`always`/`reachable`).
- `sk-recovery-race-chaos-client:<sha>` - client workload
    - best-effort variant of the above with no `pg_stopevent_set`: runs concurrent INSERT/UPDATE/DELETE bursts against the same table shape under a very short `checkpoint_timeout`, relying on chance overlap with an automatic checkpoint plus Antithesis's own fault injection. Reports via Antithesis SDK assertions (`always`/`sometimes`).
- flake1 - TODO

### Configurations

| Config  | Description  |
|:--|:--|
| [setup/s3](./config/setup/s3)  | additive; runs orioledb in s3 mode with local minio backend; not compatible with `setup/postgres`  |
| [setup/postgres](./config/setup/postgres)  | replaces orioledb with stock postgres image for sanity checking compatible workloads |
| [workload/jepsen-repeatable-read](./config/workload/jepsens-repeatable-read)  | adds a jepsen client with append/rr workload  |
| [workload/sk-recovery-race](./config/workload/sk-recovery-race)  | adds a client that deterministically constructs the orioledb#855 PK/SK checkpoint race via stopevents and checks consistency each iteration  |
| [workload/sk-recovery-race-chaos](./config/workload/sk-recovery-race-chaos)  | adds a client that stresses the same PK/SK checkpoint race with concurrent DML and frequent checkpoints, no stopevents  |
| flake repro  | todo  |

- individual configuration sets are merged with `config/docker-compose.base.yaml` to define a simulation
- a config follows simple conventions
    - named by a path inside the `config` folder (e.g. `setup/s3` points to ``<repo>/config/setup/s3`)
    - `compose.yaml` - merged with base compose file
    - `env` - enivronment customizations

### Test Suites

These examples assume a working [snouty](https://github.com/antithesishq/snouty).  Run `eval "$(mise activate bash)"` and `snouty doctor` for setup instructions.

> [!note]
> If a test suite fails to come up locally, run `make down [CFG='...']` and try again.


#### Jepsen Standalone

```
make build push CFG='workload/jepsen-repeatable-read'

# optional
snouty validate target/

# note the config image built above (make build push ...)
# TODO: add a convenience for this in the Makefile
snouty launch \
  --config-image "$(make config-image CFG='workload/jepsen-repeatable-read')"
  --test-name 'orioledb_jepsen' \
  --description 'pg17_odbmain_workload-jepsen-repeatable-read fixed health checker' \
  --duration 20m \
  --ephemeral \
  --webhook basic_test
```

#### sk-recovery-race[-chaos]

```
# ref before fix
make build push CFG='workload/sk-recovery-race' PG_MAJOR=17 ORIOLEDB_REF=ce4681c77e3a0b8e67f900704bb598838837171e

# optional
snouty validate target/

snouty launch \
  --config-image "$(make config-image CFG='workload/sk-recovery-race' PG_MAJOR=17 ORIOLEDB_REF=ce4681c77e3a0b8e67f900704bb598838837171e)" \
  --test-name 'sk-recovery-race' \
  --description 'sk-recovery-race trial' \
  --duration 20m \
  --ephemeral \
  --param custom.container_faults_enable=true \
  --param custom.container_faults_exclusion='sk-recovery-race-chaos-client' \
  --webhook supabase
```

## Usage

Before pushing to Antithesis, it's worth running your changes locally. 

```bash
# builds simulation containers for
# - PostgreSQL 18
# - OrioleDB feature branch mhamilton/perf-improvements
# - stock OrioleDB configuration, no s3 or undo rewind
# - jepsen RR workload
make build PG_MAJOR=18 ORIOLEDB_REF=mhamilton/perf-improvements # PG_MAJOR=17 and ORIOLEDB_REF=main are default

# Run jepsen workload against orioledb configured in s3 mode (minio)
make build CFG='setup/s3 workload/jepsen-repeatable-read'

# starts simulation locally
make up # [CFG='...']

# tears down running sim, volumes, and intermediate files
make down # [CFG='...']
```


### Pre-requisites

- [Docker](https://www.docker.com/) for building images, running workloads locally
- [mise](https://mise.jdx.dev/) (optional) installs snouty and other dev-local tools
- [snouty](https://github.com/antithesishq/snouty)
    - mise installs snouty
    - `.snouty.toml` has tenant coordinates
    - You need to define the `ANTITHESIS_API_KEY` env var
    - run `snouty doctor` if you have any errors executing snouty

# Appendix

## Reference

- [Getting started with Antithesis and docker compose](https://antithesis.com/docs/getting_started/setup/)
- [etcd test cluster example](https://antithesis.com/docs/tutorials/cluster-setup/)
- [SDK](https://antithesis.com/docs/using_antithesis/sdk/)
- [Testing best practices](https://antithesis.com/docs/best_practices/optimizing/)
- driving workloads
    - can use any container entrypoint or
    - have a dedicated test driver container and use [Test Composer](https://antithesis.com/docs/test_templates/)
        - [basics](https://antithesis.com/docs/test_templates/first_test/)
        - [reference](https://antithesis.com/docs/test_templates/test_composer_reference/)
        - [an entrypoint](https://github.com/DataDog/dd-profiling-antithesis/blob/main/runner/resources/entrypoint.sh) that does `sleep infinity` inside antithesis and automatically runs the test composer tests itself is useful for local testing.

## Debugging running processes

### Java debugger: Jepsen

JDWP must be enabled when the Jepsen JVM starts:

```bash
JEPSEN_JDWP_PORT=7896 \
  make up CFG='workload/jepsen-repeatable-read'
```

The port is bound inside the container, so run the debugger there:

```bash
docker exec -it jepsen-client jdb -attach 7896
```

Useful interactive JDB commands:

```text
threads
where all
catch java.lang.Throwable
stop in some.java.Class.someMethod
cont
locals
dump someVariable
exit
```

A non-interactive example suitable for scripts or Antithesis MVD:

```bash
docker exec jepsen-client sh -c \
  'printf "threads\nwhere all\nexit\n" | /opt/java/bin/jdb -attach 7896'
```

Other Java diagnostics do not require JDWP:

```bash
# Find the Jepsen JVM
docker exec jepsen-client jcmd -l

# Capture a thread dump
docker exec jepsen-client jcmd 8 Thread.print

# Display VM flags
docker exec jepsen-client jcmd 8 VM.flags

# Write a heap dump for extraction
docker exec jepsen-client jcmd 8 GC.heap_dump /tmp/jepsen.hprof

# Start a 30-second Flight Recorder recording
docker exec jepsen-client jcmd 8 JFR.start \
  name=jepsen-debug \
  duration=30s \
  filename=/tmp/jepsen.jfr
```

Replace `8` with the PID reported by `jcmd -l`.

### GDB: OrioleDB/PostgreSQL


> [!note]
> `gdb` attachment to an x64_86 process doesn't work on mac/OrbStack/Rosetta. You'll need native amd64 Linux.

List PostgreSQL processes:

```bash
docker exec orioledb pgrep -a postgres
```

Attach to the postmaster—the oldest PostgreSQL process:

```bash
docker exec --privileged -it orioledb sh -c \
  'exec gdb -q -p "$(pgrep -o -x postgres)"'
```

Useful interactive GDB commands:

```text
set pagination off
info threads
bt
bt full
frame 2
info args
info locals
break SomeOrioleDBFunction
continue
detach
quit
```

To attach to a particular client backend, first list sessions:

```bash
docker exec orioledb sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -x -c "
     SELECT pid,
            usename,
            application_name,
            state,
            wait_event_type,
            wait_event,
            left(query, 100) AS query
       FROM pg_stat_activity
      WHERE backend_type = '\''client backend'\''
      ORDER BY pid;
   "'
```

Then attach using the returned PID:

```bash
docker exec --privileged -it orioledb gdb -q -p <PID>
```

Non-interactive backtrace:

```bash
docker exec --privileged orioledb sh -c '
  pid=$(pgrep -o -x postgres)
  exec gdb -q -nx -batch -p "$pid" \
    -ex "set pagination off" \
    -ex "thread apply all bt full" \
    -ex "detach"
'
```

Core dump without terminating PostgreSQL:

```bash
docker exec --privileged orioledb sh -c '
  pid=$(pgrep -o -x postgres)
  gcore -o /tmp/postgres-core "$pid"
'
```
