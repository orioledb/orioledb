#!/usr/bin/env python3
"""Run upstream extension regression suites unmodified against an OrioleDB server.

Every suite is executed with the exact pg_regress arguments `make installcheck`
would use, derived from the module's own Makefile.  The only differences
from a stock run are default_table_access_method='orioledb', the
--load-extension=orioledb flag ci/check.sh also passes to PostgreSQL's own
suites (pg_regress builds its database from template0), and whatever the module
itself asks for through its --temp-config file (which temp-instance mode would
have applied for it).

Raw pg_regress output is kept untouched under <out>/<extension>/; report.py
reads it.  Nothing here rewrites expected files or filters diffs.
"""
import argparse
import glob
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
PRINT_MK = os.path.join(tempfile.gettempdir(), f"orioledb-ext-print-{os.getpid()}.mk")


def sh(cmd, **kw):
    kw.setdefault("stdout", subprocess.PIPE)
    kw.setdefault("stderr", subprocess.STDOUT)
    kw.setdefault("text", True)
    return subprocess.run(cmd, **kw)


def psql(sql, db="postgres", user=None):
    cmd = ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", sql]
    if user:
        cmd += ["-U", user]
    r = sh(cmd)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {sql}\n{r.stdout}")
    return r.stdout.strip()


# --- Makefile evaluation ----------------------------------------------------

VARS = ["REGRESS", "REGRESS_OPTS", "NO_INSTALLCHECK", "ENCODING", "NO_LOCALE",
        "ISOLATION", "TAP_TESTS", "EXTENSION", "MODULE_big", "MODULES"]


def make_available():
    return shutil.which("make") is not None and sh(["pg_config", "--pgxs"]).returncode == 0


def eval_makefile_with_make(moddir):
    """Let GNU make expand the variables exactly as `make installcheck` would."""
    if not os.path.exists(PRINT_MK):
        with open(PRINT_MK, "w") as f:
            f.write("print-%:\n\t@echo '$($*)'\n")
    cmd = ["make", "-s", "-C", moddir, "USE_PGXS=1", "-f", "Makefile", "-f", PRINT_MK]
    cmd += [f"print-{v}" for v in VARS]
    r = sh(cmd)
    if r.returncode != 0:
        return None, r.stdout
    # One line per variable, in order; empty values are empty lines, so only the
    # newline that terminates the last echo may be dropped.
    lines = r.stdout.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    if len(lines) < len(VARS):
        return None, r.stdout
    return dict(zip(VARS, [l.strip() for l in lines[-len(VARS):]])), r.stdout


def eval_makefile_regex(moddir):
    """Fallback used when make/pgxs is unavailable: handles `VAR = a b \\` and `+=`."""
    out = {v: "" for v in VARS}
    try:
        text = open(os.path.join(moddir, "Makefile")).read()
    except FileNotFoundError:
        return out
    text = re.sub(r"\\\n\s*", " ", text)
    for line in text.splitlines():
        m = re.match(r"^\s*(\w+)\s*([:+?]?=)\s*(.*?)\s*$", line)
        if not m or m.group(1) not in out:
            continue
        var, op, val = m.groups()
        val = re.sub(r"#.*", "", val).strip()
        out[var] = (out[var] + " " + val).strip() if op == "+=" else val
    return out


def parse_regress_opts(opts):
    """Split REGRESS_OPTS into (kept pg_regress args, temp-config files, inputdir)."""
    keep, temp_configs, inputdir, expecteddir = [], [], None, None
    toks = shlex.split(opts) if opts else []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in ("--temp-config", "--temp-instance") and i + 1 < len(toks):
            if t == "--temp-config":
                temp_configs.append(toks[i + 1])
            i += 2
            continue
        if t.startswith("--temp-config="):
            temp_configs.append(t.split("=", 1)[1]); i += 1; continue
        if t.startswith("--temp-instance="):
            i += 1; continue
        if t.startswith("--inputdir="):
            inputdir = t.split("=", 1)[1]
        if t.startswith("--expecteddir="):
            expecteddir = t.split("=", 1)[1]
        keep.append(t)
        i += 1
    return keep, temp_configs, inputdir, expecteddir


def read_conf(path):
    """postgresql.conf-style `key = value` lines from a module's temp-config."""
    conf = {}
    if not os.path.exists(path):
        return conf
    for line in open(path):
        line = line.split("#", 1)[0].strip()
        if not line or "=" not in line:
            continue
        k, v = [x.strip() for x in line.split("=", 1)]
        conf[k] = v.strip("'\"")
    return conf


# --- manifest and required server configuration -----------------------------

def load_manifest(path):
    doc = json.load(open(path))
    return doc, [e for e in doc["extensions"]]


def moddir_for(entry, src):
    d = os.path.join(src, entry["kind"], entry["name"])
    return os.path.join(d, entry["subdir"]) if entry.get("subdir") else d


def expand(value):
    """${EXT_DIR} in manifest values -> this directory (fixture scripts live here)."""
    return str(value).replace("${EXT_DIR}", HERE)


def suite_temp_config(entry, moddir, use_make):
    """Settings this module's own --temp-config file(s) and manifest entry ask for."""
    conf = {}
    if entry.get("preload"):
        conf["shared_preload_libraries"] = ",".join(entry["preload"])
    for k, v in entry.get("server_config", {}).items():
        conf[k] = expand(v)
    if not os.path.isdir(moddir):
        return conf
    mk = None
    if use_make:
        mk, _ = eval_makefile_with_make(moddir)
    if mk is None:
        mk = eval_makefile_regex(moddir)
    _, temp_configs, _, _ = parse_regress_opts(mk.get("REGRESS_OPTS", ""))
    for tc in temp_configs:
        # $(top_srcdir)/contrib/x/y.conf -> the file next to the Makefile
        tc = re.sub(r"\$\([^)]*\)/contrib/[^/]+/", "", tc)
        conf.update(read_conf(os.path.join(moddir, os.path.basename(tc))))
    return conf


# Settings that only take effect at server start.  Everything else a suite asks
# for is applied for that suite alone (ALTER SYSTEM + reload) and reset after,
# so one module's temp-config cannot leak into another's output.
POSTMASTER_SETTINGS = {
    "shared_preload_libraries", "wal_level", "max_replication_slots", "max_wal_senders",
    "max_prepared_transactions", "archive_mode", "max_connections", "max_worker_processes",
    "max_locks_per_transaction", "track_commit_timestamp", "huge_pages", "port",
}


def split_settings(conf):
    """Start-time settings vs ones that can be applied per suite.  Extension
    settings (dotted names, e.g. pgsodium.getkey_script) count as start-time:
    a preloaded library reads them while the postmaster starts."""
    glob_, local = {}, {}
    for k, v in conf.items():
        (glob_ if k in POSTMASTER_SETTINGS or "." in k else local)[k] = v
    return glob_, local


BASE_SETTINGS = {"default_table_access_method": "orioledb", "shared_preload_libraries": "orioledb"}


def group_settings(entry, src, use_make):
    """Start-time settings this suite needs, baseline included: the server it must run on."""
    glob_, _ = split_settings(suite_temp_config(entry, moddir_for(entry, src), use_make))
    settings = dict(BASE_SETTINGS)
    for k, v in glob_.items():
        if k == "shared_preload_libraries":
            libs = ["orioledb"] + [x.strip() for x in v.split(",") if x.strip() and x.strip() != "orioledb"]
            settings[k] = ",".join(libs)
        else:
            settings[k] = v
    return settings


def build_groups(entries, src, use_make):
    """Suites that need the same start-time settings share a server.  Modules
    without a temp-config form group 0 (plain server, only orioledb preloaded);
    every distinct settings set gets its own group, exactly as `make check`
    would give each such module its own temp instance."""
    groups = []
    index = {}
    for e in entries:
        if e.get("skip"):
            continue
        settings = group_settings(e, src, use_make)
        key = tuple(sorted(settings.items()))
        if key not in index:
            index[key] = len(groups)
            groups.append({"id": len(groups), "settings": settings, "suites": []})
        groups[index[key]]["suites"].append(e["name"])
    # Keep the plain server first so the report reads top-down from "no extra config".
    groups.sort(key=lambda g: (g["settings"] != BASE_SETTINGS, g["id"]))
    for i, g in enumerate(groups):
        g["id"] = i
    return groups


def required_config(entries, src, use_make):
    """Union of start-time settings the selected suites need.  Reports conflicts, never resolves them silently."""
    settings = {"default_table_access_method": "orioledb"}
    preload = ["orioledb"]
    conflicts = []

    # Settings where one value satisfies every weaker request.
    ORDERED = {"wal_level": ["minimal", "replica", "logical"]}

    def add(k, v, who):
        if k in ORDERED and k in settings:
            order = ORDERED[k]
            if v in order and settings[k] in order:
                settings[k] = order[max(order.index(v), order.index(settings[k]))]
                return
        if k == "shared_preload_libraries":
            for lib in [x.strip() for x in v.split(",") if x.strip()]:
                if lib not in preload:
                    preload.append(lib)
            return
        if k in settings and settings[k] != v:
            conflicts.append((k, settings[k], v, who))
        settings[k] = v

    for e in entries:
        if e.get("skip"):
            continue
        glob_, _ = split_settings(suite_temp_config(e, moddir_for(e, src), use_make))
        for k, v in glob_.items():
            add(k, v, e["name"])
    settings["shared_preload_libraries"] = ",".join(preload)
    return settings, conflicts


def apply_suite_settings(local, user):
    """ALTER SYSTEM the suite-local settings; return the names actually applied."""
    applied = []
    for k, v in local.items():
        # Comma-separated values are list GUCs: a single quoted string would be
        # taken as one element, so pass the elements individually.
        sql_value = ", ".join(f"'{x.strip()}'" for x in v.split(",")) if "," in v else f"'{v}'"
        try:
            psql(f"ALTER SYSTEM SET {k} = {sql_value}", user=user)
            applied.append(k)
        except RuntimeError as ex:
            print(f"    warning: could not set {k}={v}: {str(ex).splitlines()[-1]}", flush=True)
    if applied:
        psql("SELECT pg_reload_conf()", user=user)
        time.sleep(0.5)
    return applied


def wait_for_server(user, seconds=120):
    """True once the server accepts connections; used to notice a crash-restart."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            psql("SELECT 1", user=user)
            return True
        except RuntimeError:
            time.sleep(1)
    return False


def reset_suite_settings(applied, user):
    for k in applied:
        try:
            psql(f"ALTER SYSTEM RESET {k}", user=user)
        except RuntimeError:
            pass
    if applied:
        try:
            psql("SELECT pg_reload_conf()", user=user)
        except RuntimeError:
            pass
        time.sleep(0.5)


def check_config(settings):
    bad = []
    for k, v in settings.items():
        cur = psql(f"SHOW {k}")
        if k == "shared_preload_libraries":
            have = {x.strip() for x in cur.split(",")}
            missing = [x for x in v.split(",") if x not in have]
            if missing:
                bad.append((k, cur, v))
        elif cur.lower() != v.lower():
            bad.append((k, cur, v))
    return bad


# --- running one suite ------------------------------------------------------

def pg_regress_path():
    pkglib = sh(["pg_config", "--pkglibdir"]).stdout.strip()
    cand = os.path.join(pkglib, "pgxs", "src", "test", "regress", "pg_regress")
    return cand if os.path.exists(cand) else shutil.which("pg_regress")


def pg_major():
    return sh(["pg_config", "--version"]).stdout.split()[1].split(".")[0]


def pgrx_version(moddir):
    """The pgrx version a crate pins (`pgrx = "=0.16.1"` or `{ version = ... }`);
    cargo-pgrx must match it exactly."""
    try:
        text = open(os.path.join(moddir, "Cargo.toml")).read()
    except FileNotFoundError:
        return None
    m = re.search(r'^pgrx\s*=\s*(?:"|\{[^}]*version\s*=\s*")=?([0-9][^"]*)"', text, re.M)
    return m.group(1) if m else None


def build_commands(entry, moddir):
    """The build an extension's own README/CI runs, by build system."""
    system = entry.get("build", "none")
    args = entry.get("build_args", [])
    pg_config = shutil.which("pg_config")
    if system == "pgxs":
        return [["make", "-C", moddir, "USE_PGXS=1", "-j4"] + args,
                ["make", "-C", moddir, "USE_PGXS=1", "install"] + args]
    if system == "pgrx":
        # cargo pgrx init once per pg_config (idempotent), then install the crate
        # for the server's major, as the extension's own CI does.  Extra cargo
        # features (e.g. wrappers' helloworld_fdw) come from build_args.
        features = ",".join([f"pg{pg_major()}"] + args)
        cmds = []
        want = pgrx_version(moddir)
        have = sh(["cargo", "pgrx", "--version"]).stdout.split()[-1] if shutil.which("cargo") else ""
        if want and want != have:
            cmds.append(["cargo", "install", "--locked", "cargo-pgrx", "--version", want])
        return cmds + [["cargo", "pgrx", "init", f"--pg{pg_major()}", pg_config],
                ["cargo", "pgrx", "install", "--release", "--no-default-features",
                 "--features", features, "-c", pg_config],
                ]
    if system == "postgis":
        return [["sh", "-c", f"cd {shlex.quote(moddir)} && ./autogen.sh"],
                ["sh", "-c", f"cd {shlex.quote(moddir)} && ./configure --with-pgconfig={pg_config} "
                             + " ".join(args)],
                ["make", "-C", moddir, "-j4"],
                ["make", "-C", moddir, "install"]]
    raise ValueError(f"unknown build system {system!r} for {entry['name']}")


def build_external(entry, moddir, outdir):
    log = os.path.join(outdir, "build.log")
    marker = os.path.join(outdir, "build.ok")
    if entry.get("build", "none") == "none":
        return "not-needed", log
    if os.path.exists(marker):
        return "ok", log
    tool = {"pgrx": "cargo"}.get(entry.get("build"), "make")
    if not shutil.which(tool):
        open(log, "w").write(f"{tool} not available; build skipped\n")
        return "skipped", log
    with open(log, "w") as f:
        for target in build_commands(entry, moddir):
            f.write("$ " + " ".join(target) + "\n")
            r = sh(target, cwd=moddir)
            f.write(r.stdout)
            if r.returncode != 0:
                return "failed", log
    open(marker, "w").write("")
    return "ok", log


def build_all(entries, src, build_dir):
    """Build and install every external (dependencies first).  Runs before any
    server starts, because a suite's server may have to preload the library.
    build_dir keeps build.log / build.ok per extension; it is separate from the
    results so a results reset does not force a rebuild."""
    by_name = {e["name"]: e for e in entries}
    done, results = set(), {}

    def build(e):
        if e["name"] in done or e.get("skip") or e["kind"] != "external":
            return
        for dep in e.get("depends_on", []):
            if dep in by_name:
                build(by_name[dep])
        moddir = moddir_for(e, src)
        bdir = os.path.join(build_dir, e["name"])
        os.makedirs(bdir, exist_ok=True)
        if not os.path.isdir(moddir):
            results[e["name"]] = "not-fetched"
        else:
            results[e["name"]], _ = build_external(e, moddir, bdir)
        done.add(e["name"])
        print(f"    build {e['name']}: {results[e['name']]}", flush=True)

    for e in entries:
        build(e)
    return results


def run_suite(entry, src, out, pg_regress, user, timeout, use_make, by_name, build_dir):
    name = entry["name"]
    moddir = moddir_for(entry, src)
    outdir = os.path.join(out, name)
    os.makedirs(outdir, exist_ok=True)
    tag = entry.get("tag", "latest" if entry["kind"] == "external" else None)
    resolved = os.path.join(src, entry["kind"], name, ".resolved_tag")   # repo root, not subdir
    if tag == "latest" and os.path.exists(resolved):
        tag = open(resolved).read().strip() + " (latest)"
    meta = {"name": name, "kind": entry["kind"], "tag": tag,
            "notes": entry.get("notes", ""), "started": datetime.now(timezone.utc).isoformat()}

    if entry.get("skip"):
        meta["status"] = "skipped"; meta["reason"] = entry["skip"]
        return meta
    if not os.path.isdir(moddir):
        meta["status"] = "missing"; meta["reason"] = f"{moddir} not fetched"
        return meta

    if entry["kind"] == "external":
        # Build and install what the manifest says this suite depends on, the
        # way the extension's own CI installs them before `make installcheck`.
        meta["depends_on"] = {}
        for dep_name in entry.get("depends_on", []):
            dep = by_name.get(dep_name)
            if not dep:
                meta["status"] = "missing"; meta["reason"] = f"dependency {dep_name} not in manifest"
                return meta
            dep_dir = moddir_for(dep, src)
            os.makedirs(os.path.join(build_dir, dep_name), exist_ok=True)
            if not os.path.isdir(dep_dir):
                meta["status"] = "missing"; meta["reason"] = f"dependency {dep_name} not fetched"
                return meta
            dep_status, _ = build_external(dep, dep_dir, os.path.join(build_dir, dep_name))
            meta["depends_on"][dep_name] = dep_status
            if dep_status == "failed":
                meta["status"] = "build-failed"; meta["reason"] = f"dependency {dep_name} failed to build"
                return meta
        os.makedirs(os.path.join(build_dir, name), exist_ok=True)
        meta["build"], meta["build_log"] = build_external(entry, moddir, os.path.join(build_dir, name))
        if meta["build"] == "failed":
            meta["status"] = "build-failed"
            return meta

    runner = entry.get("runner", "pg_regress")
    meta["runner"] = runner
    _, local_settings = split_settings(suite_temp_config(entry, moddir, use_make))
    meta["suite_settings"] = local_settings
    dbname = entry.get("dbname", "contrib_regression")
    use_existing = runner == "pgtap"
    cmd = None

    if runner in ("pg_regress", "smoke"):
        if entry.get("regress"):
            # Suites without a PGXS Makefile (pgrx crates run pg_regress from a
            # script; smoke checks live in this directory): the manifest states
            # the REGRESS list and options that script would use.
            spec = entry["regress"]
            tests = spec["tests"]
            if isinstance(tests, str) and tests.startswith("glob:"):
                tests = " ".join(sorted(os.path.splitext(os.path.basename(p))[0]
                                        for p in glob.glob(os.path.join(moddir, tests[5:]))))
            elif isinstance(tests, list):
                tests = " ".join(tests)
            mk = {"REGRESS": tests, "REGRESS_OPTS": expand(spec.get("opts", ""))}
            meta["makefile_eval"] = "manifest"
        else:
            mk = None
            if use_make:
                mk, make_out = eval_makefile_with_make(moddir)
                meta["makefile_eval"] = "make"
            if mk is None:
                mk = eval_makefile_regex(moddir)
                meta["makefile_eval"] = "regex"
        meta["makefile"] = {k: mk.get(k, "") for k in VARS}

        tests = mk.get("REGRESS", "").split()
        if not tests:
            meta["status"] = "no-suite"
            meta["reason"] = "Makefile defines no REGRESS tests" + (
                " (ISOLATION/TAP only)" if mk.get("ISOLATION") or mk.get("TAP_TESTS") else "")
            return meta

        keep, temp_configs, inputdir, expecteddir = parse_regress_opts(mk.get("REGRESS_OPTS", ""))
        meta["temp_configs"] = temp_configs
        meta["no_installcheck"] = bool(mk.get("NO_INSTALLCHECK"))

        # pgxs.mk appends --dbname=contrib_regression to REGRESS_OPTS; when the
        # Makefile could not be evaluated with make, mirror that here so the suite
        # runs in the database its own installcheck would use.
        if meta["makefile_eval"] == "regex" and not any(t.startswith("--dbname") for t in keep):
            keep.append("--dbname=contrib_regression")
        dbnames = [t.split("=", 1)[1] for t in keep if t.startswith("--dbname=")]
        dbname = dbnames[-1] if dbnames else "regression"
        use_existing = "--use-existing" in keep
        # Same flag ci/check.sh passes to PostgreSQL's own suites: pg_regress creates
        # its database from template0, so the access method has to be loaded into it.
        cmd = [pg_regress, f"--inputdir={inputdir or '.'}", "--outputdir=.",
               "--bindir=" + sh(["pg_config", "--bindir"]).stdout.strip(),
               "--load-extension=orioledb"]
        if user:
            cmd.append(f"--user={user}")
        # pg_regress resolves expected/ against the CWD unless told otherwise.  When a
        # module keeps sql/ and expected/ together under --inputdir (pgvector's test/),
        # point expecteddir there, which is what its own installcheck relies on.
        if inputdir and not expecteddir and os.path.isdir(os.path.join(moddir, inputdir, "expected")):
            cmd.append(f"--expecteddir={inputdir}")
        if mk.get("ENCODING"):
            cmd.append(f"--encoding={mk['ENCODING']}")
        if mk.get("NO_LOCALE"):
            cmd.append("--no-locale")
        cmd += keep + tests
    elif runner == "installcheck":
        # The module's own `make installcheck`, untouched; EXTRA_REGRESS_OPTS is the
        # pgxs hook for adding pg_regress options, used for the same
        # --load-extension flag as above.
        cmd = ["make", "-C", moddir, "USE_PGXS=1", "installcheck"]
    elif runner in ("pgtap", "postgis"):
        pass
    else:
        meta["status"] = "no-suite"
        meta["reason"] = f"unknown runner {runner}"
        return meta
    meta["dbname"] = dbname
    if cmd:
        meta["command"] = " ".join(shlex.quote(c) for c in cmd)
    # pg_regress looks for sql/<test>.sql and expected/<test>.out under
    # --outputdir before --inputdir (a vpath convenience), so the smoke checks
    # run from their own directory: pg_net's tree has a sql/pg_net.sql that
    # would otherwise be picked up.
    rundir = expand("${EXT_DIR}/smoke") if runner == "smoke" else moddir


    # Start from a clean database.  With --use-existing pg_regress expects the
    # caller to have prepared it (that is what these extensions' own CI does:
    # createdb, then load fixtures), so do that here; --load-extension does not
    # apply in that mode, so the access method is created explicitly.  pgTAP
    # suites are plain psql sessions and need the same preparation.
    try:
        # FORCE: a preloaded worker (pg_net) may still hold a session on the
        # database from the previous suite on this server.
        psql(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)", user=user)
        if use_existing:
            psql(f"CREATE DATABASE {dbname}", user=user)
            psql("CREATE EXTENSION IF NOT EXISTS orioledb", db=dbname, user=user)
    except RuntimeError as ex:
        meta["warning"] = str(ex)
    for d in entry.get("pre_dirs", []):
        os.makedirs(d, exist_ok=True)
        if os.geteuid() == 0:
            sh(["chown", "postgres", d])
    meta["pre_sql"] = []
    for step in entry.get("pre_sql", []):
        try:
            if "file" in step:
                r = sh(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", dbname] + (["-U", user] if user else [])
                       + ["-f", os.path.join(moddir, step["file"])])
                if r.returncode != 0:
                    raise RuntimeError(r.stdout)
                meta["pre_sql"].append({"file": step["file"], "ok": True})
            else:
                psql(step["sql"], db=dbname, user=user)
                meta["pre_sql"].append({"sql": step["sql"], "ok": True})
        except RuntimeError as ex:
            meta["pre_sql"].append({**step, "ok": False, "error": str(ex)[-300:]})

    applied = apply_suite_settings(local_settings, user)
    for stale in ("regression.out", "regression.diffs", "results", "log"):
        path = os.path.join(rundir, stale)
        shutil.rmtree(path, ignore_errors=True) if os.path.isdir(path) else (os.remove(path) if os.path.exists(path) else None)
    env = dict(os.environ)
    if user:
        env["PGUSER"] = user
    env["EXTRA_REGRESS_OPTS"] = "--load-extension=orioledb"
    log_path = os.path.join(outdir, "pg_regress.log")
    t0 = time.time()
    try:
        if cmd:
            r = subprocess.run(cmd, cwd=rundir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, timeout=timeout, env=env)
            meta["exit_code"] = r.returncode
            open(log_path, "w").write(r.stdout)
        elif runner == "pgtap":
            meta["tap_files"] = run_pgtap(entry, moddir, outdir, dbname, user, timeout)
        elif runner == "postgis":
            meta["exit_code"] = run_postgis(moddir, outdir, env, timeout)
        meta["status"] = "ran"
    except subprocess.TimeoutExpired as ex:
        meta["status"] = "timeout"
        open(log_path, "w").write((ex.stdout or "") + "\n[timeout]\n")
    finally:
        # A backend crash makes the postmaster restart and refuse connections
        # while it recovers; pg_regress reports the remaining tests as
        # "test process exited with exit code 2".  Record that explicitly: it
        # is the most important thing a suite can tell us.
        log_text = open(log_path, errors="replace").read() if os.path.exists(log_path) else ""
        if "test process exited with exit code 2" in log_text or not wait_for_server(user, 5):
            meta["server_crash"] = True
            if not wait_for_server(user):
                meta["status"] = "server-down"
        reset_suite_settings(applied, user)
        # pg_regress wrote into the module directory like installcheck does.  Copy
        # the raw output next to this suite's other artifacts, but leave the
        # originals where regression.diffs points at them: the drift filter
        # re-reads expected/ and results/ through those paths.
        for produced in ("regression.out", "regression.diffs", "results"):
            src_p, dst_p = os.path.join(rundir, produced), os.path.join(outdir, produced)
            if not os.path.exists(src_p):
                continue
            if os.path.isdir(src_p):
                shutil.rmtree(dst_p, ignore_errors=True)
                shutil.copytree(src_p, dst_p)
            else:
                shutil.copy2(src_p, dst_p)
    meta["duration_s"] = round(time.time() - t0, 1)
    return meta


# --- runners that are not pg_regress ----------------------------------------
#
# Their verdicts are written in pg_regress's own regression.out / regression.diffs
# shape so report.py reads every suite the same way.  Test names with '/' are
# flattened with '__' because pg_regress result paths are one level deep.

def write_regress_files(outdir, results):
    """results: [(name, ok, ms, failure_text_or_None)]"""
    with open(os.path.join(outdir, "regression.out"), "w") as f:
        for i, (name, ok, ms, _) in enumerate(results, 1):
            f.write(f"{'ok' if ok else 'not ok'} {i:5} {'-' if ok else '+'} {name:36} {ms:8} ms\n")
    failures = [(n, t) for n, ok, _, t in results if not ok]
    if failures:
        with open(os.path.join(outdir, "regression.diffs"), "w") as f:
            for n, t in failures:
                f.write(f"diff -U3 expected/{n}.out ./results/{n}.out\n--- expected/{n}.out\n+++ ./results/{n}.out\n")
                f.write("".join("+" + l + "\n" for l in (t or "(no output)").splitlines()))
                f.write("\n")


def parse_tap(text):
    """TAP as pg_prove reads it: plan `1..N` (first or, with no_plan(), last),
    `ok N`, `not ok N`, `#` diagnostics following a failure."""
    planned, passed, failed = None, 0, []
    for line in text.splitlines():
        line = line.rstrip()
        if line.startswith("1.."):
            try:
                planned = int(line[3:].split()[0])
            except (ValueError, IndexError):
                pass
        elif line.startswith("not ok"):
            failed.append(line)
        elif line.startswith("ok"):
            passed += 1
        elif line.startswith("#") and failed:
            failed[-1] += "\n" + line
    return planned, passed, failed


def run_pgtap(entry, moddir, outdir, dbname, user, timeout):
    """Each TAP file through psql, as pg_prove does.  Every upstream file wraps
    itself in BEGIN ... ROLLBACK, so one database serves the whole suite."""
    files = []
    for pattern in entry.get("tap_tests", ["test/*.sql"]):
        files += sorted(glob.glob(os.path.join(moddir, pattern)))
    tapdir = os.path.join(outdir, "tap")
    os.makedirs(tapdir, exist_ok=True)
    results = []
    for path in files:
        name = os.path.relpath(path, moddir)[:-4].replace("/", "__")
        cmd = ["psql", "-X", "-qAt", "--pset", "pager=off", "-v", "ON_ERROR_STOP=1", "-d", dbname, "-f", path]
        if user:
            cmd += ["-U", user]
        t0 = time.time()
        try:
            r = subprocess.run(cmd, cwd=os.path.dirname(path), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, timeout=timeout)
            out, rc = r.stdout, r.returncode
        except subprocess.TimeoutExpired as ex:
            out, rc = (ex.stdout or "") + "\n[timeout]\n", -1
        ms = int((time.time() - t0) * 1000)
        open(os.path.join(tapdir, name + ".tap"), "w").write(out)
        planned, passed, failed = parse_tap(out)
        problems = list(failed)
        if rc != 0:
            problems.append(f"psql exited {rc}:\n" + "\n".join(
                l for l in out.splitlines() if l.startswith("psql:") or "ERROR" in l or "FATAL" in l or "PANIC" in l)[-2000:])
        if planned is None:
            problems.append("no TAP plan line emitted")
        elif planned != passed + len(failed):
            problems.append(f"plan mismatch: declared {planned}, ran {passed + len(failed)}")
        ok = not problems
        results.append((name, ok, ms, None if ok else "\n".join(problems)))
        print(f"    {'ok' if ok else 'FAILED'}  {name}  (plan={planned} ok={passed} not_ok={len(failed)})", flush=True)
    write_regress_files(outdir, results)
    return len(files)


POSTGIS_LINE = re.compile(r"^\s*(\S+)\s+\.+\s+(ok|failed|skipped)(.*)$")


def run_postgis(moddir, outdir, env, timeout):
    """PostGIS's `make installcheck-base`: regress/run_test.pl --extension over the
    installed extension, then the same list again with --upgrade (upstream
    target).  Each line of run_test.pl output is one test."""
    # run_test.pl creates its database from template0, so the access method is
    # loaded through its --after-create-db-script hook (RUNTESTFLAGS is the
    # documented way to pass run_test.pl options through make).
    cmd = ["make", "-C", moddir, "installcheck-base",
           "RUNTESTFLAGS=--after-create-db-script " + os.path.join(HERE, "load-orioledb.sql")]
    r = subprocess.run(cmd, cwd=moddir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       text=True, timeout=timeout, env=env)
    open(os.path.join(outdir, "pg_regress.log"), "w").write(r.stdout)
    results, prefix = [], ""
    for line in r.stdout.splitlines():
        if "Running upgrade test" in line:
            prefix = "upgrade__"
            continue
        m = POSTGIS_LINE.match(line)
        if not m or m.group(1) in ("make",):
            continue
        name, verdict, rest = m.groups()
        name = prefix + name.replace("/", "__")
        ms = 0
        mm = re.search(r"\((\d+)\)|in (\d+) ms", rest)
        if mm:
            ms = int(mm.group(1) or mm.group(2))
        if verdict == "ok":
            results.append((name, True, ms, None))
            continue
        detail = rest.strip()
        dm = re.search(r"(/\S+_diff)", rest)
        if dm and os.path.exists(dm.group(1)):
            detail += "\n" + open(dm.group(1), errors="replace").read()
        # run_test.pl skips tests whose optional component is not built; the
        # "skipped (" text lets report.py file it under environment.
        results.append((name, False, ms, detail))
    write_regress_files(outdir, results)
    return r.returncode


def server_info():
    info = {"version": psql("SELECT version()")}
    try:
        info["orioledb_version"] = psql("SELECT orioledb_version()")
        info["orioledb_commit"] = psql("SELECT orioledb_commit_hash()")
    except RuntimeError:
        info["orioledb_version"] = None
    return info


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", default=os.path.join(HERE, "extensions.json"))
    ap.add_argument("--src", default=os.path.join(HERE, "work", "src"))
    ap.add_argument("--out", default=os.path.join(HERE, "work", "results"))
    ap.add_argument("--build-dir", help="where build logs and done-markers live (default: --out)")
    ap.add_argument("--only", help="comma-separated extension names")
    ap.add_argument("--kind", choices=["contrib", "external"], help="restrict to one kind")
    ap.add_argument("--user", default=os.environ.get("PGUSER") or None,
                    help="database superuser for psql/pg_regress (default: current OS user)")
    ap.add_argument("--timeout", type=int, default=900, help="seconds per suite")
    ap.add_argument("--build-only", action="store_true",
                    help="build and install all external extensions (dependencies first) and exit; needs no server")
    ap.add_argument("--list-groups", action="store_true",
                    help="print, as JSON, the groups of suites that share start-time settings, and exit")
    ap.add_argument("--group", type=int, help="run only the suites of this group (see --list-groups)")
    ap.add_argument("--config-only", action="store_true",
                    help="print the server settings the selected suites (or --group) need and exit")
    ap.add_argument("--no-config-check", action="store_true")
    args = ap.parse_args()

    doc, entries = load_manifest(args.manifest)
    by_name = {e["name"]: e for e in entries}
    if args.only:
        wanted = set(args.only.split(","))
        entries = [e for e in entries if e["name"] in wanted]
    if args.kind:
        entries = [e for e in entries if e["kind"] == args.kind]

    use_make = make_available()
    build_dir = args.build_dir or args.out
    if args.build_only:
        os.makedirs(build_dir, exist_ok=True)
        results = build_all(entries, args.src, build_dir)
        return 1 if any(v == "failed" for v in results.values()) else 0
    groups = build_groups(entries, args.src, use_make)
    if args.list_groups:
        print(json.dumps(groups, indent=2))
        return 0
    if args.group is not None:
        chosen = [g for g in groups if g["id"] == args.group]
        if not chosen:
            print(f"no group {args.group}; see --list-groups", file=sys.stderr)
            return 2
        entries = [e for e in entries if e["name"] in chosen[0]["suites"]]
        settings, conflicts = chosen[0]["settings"], []
    else:
        settings, conflicts = required_config(entries, args.src, use_make)
    if args.config_only:
        for k, v in settings.items():
            print(f"{k}={v}")
        for k, a, b, who in conflicts:
            print(f"# conflict: {k}: {a} vs {b} (from {who})", file=sys.stderr)
        return 0

    os.makedirs(args.out, exist_ok=True)
    if not args.no_config_check:
        bad = check_config(settings)
        if bad:
            print("server configuration does not match what the selected suites need:", file=sys.stderr)
            for k, cur, want in bad:
                print(f"  {k}: have '{cur}', need '{want}'", file=sys.stderr)
            print("apply them (postgres -c key=value / ALTER SYSTEM + restart) or pass --no-config-check", file=sys.stderr)
            return 2
    pg_regress = pg_regress_path()
    if not pg_regress:
        print("pg_regress not found", file=sys.stderr)
        return 2

    run_file = os.path.join(args.out, "run.json" if args.group is None else f"run-group{args.group}.json")
    run = {"generated": datetime.now(timezone.utc).isoformat(), "group": args.group,
           "pg_tag": open(os.path.join(args.src, "PG_TAG")).read().strip() if os.path.exists(os.path.join(args.src, "PG_TAG")) else None,
           "server": server_info(), "settings": settings, "conflicts": conflicts,
           "pg_regress": pg_regress, "makefile_eval": "make" if use_make else "regex",
           "suites": []}
    for e in entries:
        print(f"==> {e['kind']}/{e['name']}", flush=True)
        meta = run_suite(e, args.src, args.out, pg_regress, args.user, args.timeout, use_make, by_name, build_dir)
        meta["server_settings"] = settings
        print(f"    {meta['status']}" + (f" ({meta.get('reason')})" if meta.get("reason") else "")
              + (f" exit={meta.get('exit_code', '-')} {meta['duration_s']}s" if meta.get("status") == "ran" else ""), flush=True)
        json.dump(meta, open(os.path.join(args.out, e["name"], "meta.json"), "w"), indent=2) \
            if os.path.isdir(os.path.join(args.out, e["name"])) else None
        run["suites"].append(meta)
        json.dump(run, open(run_file, "w"), indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
