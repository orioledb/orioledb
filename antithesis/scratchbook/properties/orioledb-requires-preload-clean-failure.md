# orioledb-requires-preload-clean-failure

## Origin

Attention focus: Lifecycle Transitions. This follows up the task's specific question about binary-compatibility.mdx and "switching between patched and vanilla Postgres binaries against the same data directory." **That specific documented claim does not exist** — see the negative-finding note below — so this property is constructed from code-level defensive checks rather than a stated guarantee, and is flagged accordingly.

## What was actually checked, and what wasn't found

- Read `doc/contributing/binary-compatibility.mdx` in full. It only discusses **cross-architecture** portability ("a database initialized on one machine/architecture will work on a machine with a different architecture... might not work even if... same endianness, alignment, and float format") and the four version constants (`ORIOLEDB_BINARY_VERSION`, `ORIOLEDB_SYS_TREE_VERSION`, `ORIOLEDB_PAGE_VERSION`, `ORIOLEDB_COMPRESS_VERSION`). **It says nothing about vanilla-vs-patched Postgres binaries, nor about converting orioledb tables back to heap before any such switch.**
- Grepped `doc/` broadly for "vanilla", "downgrade", "USING heap", "convert", "migrat" — the only hit is `doc/usage/getting-started.mdx:288`, about `pg_rewind` (the *tool*, unrelated to the orioledb rewind feature) copying orioledb tables completely rather than incrementally — not about swapping binaries.
- **Conclusion: this is a documentation gap, not a documented ordering assumption.** No claim exists to validate or refute here; per validating-claims.md, an absent claim earns nothing either way — it just means this property is built from first-principles code reading, not from a stated guarantee, and should be labeled as such rather than presented as testing a product claim.

## Code evidence for what *does* happen

- `src/orioledb.c:1565`: `ereport(ERROR, ... errmsg("orioledb must be loaded via shared_preload_libraries"))` — reached when SQL-level access to orioledb functionality happens without the module having been preloaded.
- `src/orioledb.c`, `orioledb_check_shmem()`: `ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("orioledb must be loaded via shared_preload_libraries")))`, guarded by `if (!shared_segment_initialized)`. This is a defensive, explicit, clean-error check — not a crash, not silent misbehavior — that fires when orioledb-dependent code executes without the extension's shared-memory setup having run (which only happens via `shared_preload_libraries`, set once, at postmaster start).
- A genuinely **vanilla** (unpatched) Postgres binary cannot load `orioledb.so` at all in the first place — the custom resource-manager registration (`rmgr = { .rm_name = "OrioleDB resource manager", ... }`, `src/orioledb.c:399-409`) and the several patched-core hook points (`get_xidless_commit_lsn_hook`, `snapshot_register_hook`, `xact_redo_hook`, `CheckPoint_hook`, etc. — sut-analysis §1, confirmed against `/Users/artur/supabase/orioledb_postgres`) don't exist as symbols/hook slots in vanilla Postgres. This means the more likely real-world scenario isn't "vanilla Postgres somehow loads orioledb.so and misbehaves" — it's "an operator points a *patched* Postgres binary's data directory at a config that's missing `shared_preload_libraries = 'orioledb'`" (a misconfiguration, not a binary swap), or points a genuinely vanilla binary at the data directory and gets an immediate, unrelated failure (missing `orioledb.so` to load, or — if `shared_preload_libraries` isn't even set — no failure at load time at all, just silent absence of the extension until something touches an orioledb table).

## Why this matters (as a defensively-coded property, not a documented guarantee)

The genuinely interesting lifecycle-transition risk isn't "does orioledb detect its own absence" (it clearly does, via the two checks above) — it's whether **every** access path to orioledb state goes through a check that requires `shared_segment_initialized`, or whether some path (e.g., the tableam handler lookup itself, `pg_class.relam` resolution, catalog access to an orioledb table's metadata) could be reached *before* that check, in a config where `shared_preload_libraries` was forgotten but a patched Postgres binary is still running (so the .so *could* theoretically be dynamically loaded on first use in some Postgres versions/configs, bypassing the "always preloaded" assumption). This wasn't traced to a conclusion in this pass — it's the actual open question worth carrying forward.

## What goes wrong if the property is violated

If some access path reaches orioledb table data without the `shared_segment_initialized` gate having run, the failure mode could range from a Postgres-level crash (accessing uninitialized shared memory / an unregistered tableam) to, worse, a misleading partial success (e.g. metadata visible via catalog but actual table access failing inconsistently) rather than the clean, immediate, well-messaged `ERROR` the two known checks provide.

## Antithesis SDK angle

- **Type recommendation:** Safety — `Always()`: any attempt to access an orioledb-backed table or call an orioledb SQL function without `orioledb` present in `shared_preload_libraries` results in a clean, well-formed `ERROR`/`FATAL` (never a crash, never silent misbehavior, never inconsistent partial success).
- This is a config-mutation-style property (start Postgres with a config that omits `shared_preload_libraries`, or with a different/vanilla binary, against a data directory that already has orioledb tables) rather than a runtime-fault-injection property — it fits the "migration/switching" framing in the Lifecycle Transitions focus description ("migration steps that assume no concurrent traffic") loosely, but is really more of a "startup configuration mismatch" property. Flagged as lower-priority relative to the S3/rewind properties above, since it protects a check that's already demonstrably present and working, rather than surfacing a suspected gap.
- No existing SUT-side or workload-side coverage found for this scenario in `test/antithesis/`.

## Open Questions

- Is there any orioledb table-access code path that could execute *before* `orioledb_check_shmem()`/the `shared_preload_libraries` check fires (e.g., planner-time catalog lookups on `pg_class`/`pg_am` that don't need the tableam's runtime methods yet, versus executor-time access that does)? Not traced to a conclusion in this pass. `(needs follow-up code reading in src/tableam/handler.c and the planner-hook call sites before this property is implemented, to make sure the workload actually targets the right access point)`
- The task's original framing ("must all orioledb tables be converted back to heap before switching [to vanilla Postgres], and what happens if a crash interrupts that conversion") presupposes a documented conversion-back-to-heap workflow. **No such workflow was found anywhere in `doc/`** — there is no `ALTER TABLE ... SET ACCESS METHOD heap`-style migration path described for orioledb tables in the docs read this pass. Whether such a workflow exists undocumented (e.g., as a manual `CREATE TABLE ... USING heap; INSERT INTO ... SELECT ...` dance) or doesn't exist as a supported operation at all is unresolved. `(needs human input: confirm with the team whether an orioledb-to-heap conversion path is supported/planned at all, since if it doesn't exist, the crash-during-conversion sub-question in the task is moot)`

## Investigation Log

### Is there a documented claim about switching between patched and vanilla Postgres binaries against the same data directory?

- Examined: `doc/contributing/binary-compatibility.mdx` (full text), broad grep of `doc/` for "vanilla", "downgrade", "USING heap", "convert", "migrat", "uninstall", "DROP EXTENSION".
- Found: `binary-compatibility.mdx` addresses only cross-architecture portability; the one "vanilla" hit found (`doc/intro.mdx`, `doc/usage/getting-started.mdx`) refers to running the OrioleDB *Docker image* to get a patched Postgres, not to swapping binaries against an existing data directory. No conversion-back-to-heap workflow was found anywhere in `doc/`.
- Not found: any documented ordering assumption, migration guarantee, or crash-safety claim for this scenario.
- Conclusion: the task's premise (that such a claim exists in binary-compatibility.mdx) does not hold up — recorded as a validated absence, and the property above was reconstructed from code-level defensive checks instead, with that reconstruction explicitly labeled as such.
