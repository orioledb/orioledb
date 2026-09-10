#!/usr/bin/env python3
# coding: utf-8

import os

from test.t.base_test import BaseTest
from testgres.enums import NodeStatus
from testgres.exceptions import QueryException

GUARD_ERR_NEEDLE = "does not match the system catalog"


class ProcCacheGuardTest(BaseTest):
	"""
	Regression coverage for forged procedure-cache native-code loading.

	OrioleDB's procedure cache is backed by an on-disk system tree whose
	contents can be tampered with independently of the authoritative pg_proc
	catalog.  o_proc_cache_fill_finfo() must therefore re-read the real
	pg_proc row (bypassing OrioleDB's hook on the alive node, where the hook
	is not installed) and require the cached prolang / prosrc / probin to
	match before it loads native code for a non-builtin C or internal-language
	function; a mismatch is rejected with a controlled ERROR instead of
	dlopen()-ing / fmgr_lookupByName()-ing an attacker-selected symbol.

	Builtin functions (fixed bootstrap OIDs, served by fmgr_isbuiltin()) are
	inherently immune -- their function pointer comes from a hardcoded table,
	not the cache -- so the guard only matters for non-builtin procedures.
	Core range support procs (e.g. range_cmp) are builtins, so the test
	builds a custom btree opclass whose comparator is a user-defined C
	function o_test_cmp (a fresh, non-builtin OID) that points at an existing
	exported orioledb symbol.  A btree index on that opclass drives the guard.

	The IS_DEV helper orioledb_test_corrupt_proc_cache() populates the legit
	entry from the catalog and then overwrites this backend's in-memory copy
	in place, simulating on-disk tampering that survived a restart.  Because
	the procedure cache lives in per-process TopMemoryContext and a comparator
	resolution is cached per backend, the corruption must run in the same
	backend that performs the index build and before any descriptor fill has
	cached the comparator; the forge tests hold a single autocommit
	connection open for both.
	"""

	def _setup(self, node):
		"""Create the extension, a table and a non-builtin C comparator
		opclass on int4."""
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE proc_guard (
				id int PRIMARY KEY,
				k int4
			) USING orioledb;
		""")
		node.safe_psql("""
			CREATE FUNCTION o_test_cmp(a int4, b int4) RETURNS int4
			AS '$libdir/orioledb', 'orioledb_int4range_immutable'
			LANGUAGE C IMMUTABLE;

			CREATE OPERATOR CLASS o_test_int4_ops FOR TYPE int4 USING btree AS
				OPERATOR 1 <,
				OPERATOR 2 <=,
				OPERATOR 3 =,
				OPERATOR 4 >=,
				OPERATOR 5 >,
				FUNCTION 1 o_test_cmp(int4, int4);
		""")

	def _test_cmp_meta(self, con):
		"""Return (probin, prosrc, prolang) of the real o_test_cmp row."""
		return con.execute(
		    "SELECT probin::text, prosrc, prolang::oid FROM pg_proc "
		    "WHERE proname = 'o_test_cmp';")[0]

	def _internal_lang_oid(self, con):
		return con.execute(
		    "SELECT oid FROM pg_language WHERE lanname = 'internal';")[0][0]

	def _assert_no_crash(self, node):
		self.assertEqual(
		    node.status(), NodeStatus.Running,
		    "node crashed while rejecting forged proc-cache "
		    "metadata")
		with open(os.path.join(node.logs_dir, 'postgresql.log')) as f:
			log = f.read()
		self.assertNotIn("TRAP: failed Assert", log)
		self.assertNotIn("Segmentation fault", log)
		self.assertNotIn("terminated by signal", log)

	def test_legit_non_builtin_c_comparator_index_builds(self):
		"""A legitimate non-builtin C comparator must not trip the guard."""
		node = self.node
		node.start()
		self._setup(node)
		# Empty table: the comparator is resolved (guard verifies the legit
		# entry) but never called during the build.
		node.safe_psql(
		    "CREATE INDEX proc_guard_k_idx ON proc_guard (k o_test_int4_ops);")
		valid = node.execute(
		    "SELECT indisvalid FROM pg_index WHERE indexrelid = "
		    "'proc_guard_k_idx'::regclass;")[0][0]
		self.assertTrue(valid)
		node.stop()

	def _forge_and_expect_error(self, corrupt):
		"""In one fresh backend: populate+forge the o_test_cmp cache entry,
		then build a btree index on the custom opclass and assert the guard
		rejects it without crashing.  ``corrupt`` maps the real (probin,
		prosrc, prolang, con) to the forged triple."""
		node = self.node
		node.start()
		self._setup(node)

		# One persistent autocommit backend for the forge + the index build.
		# The corruption must precede any descriptor fill in this backend so
		# that the comparator resolution (cached per backend) reads the
		# forged entry rather than a previously cached legit one.
		con = node.connect(autocommit=True)
		try:
			probin, prosrc, prolang = self._test_cmp_meta(con)
			fprobin, fprosrc, fprolang = corrupt(probin, prosrc, prolang, con)

			oid = con.execute(
			    "SELECT oid FROM pg_proc WHERE proname = 'o_test_cmp';")[0][0]
			con.execute(
			    "SELECT orioledb_test_corrupt_proc_cache(%s, %s, %s, %s);",
			    oid, fprobin, fprosrc, fprolang)

			raised = False
			try:
				con.execute("CREATE INDEX proc_guard_k_idx ON proc_guard "
				            "(k o_test_int4_ops);")
			except Exception as e:
				raised = True
				self.assertIn(
				    GUARD_ERR_NEEDLE, str(e),
				    "unexpected error for forged proc-cache "
				    "metadata: " + str(e))
			self.assertTrue(
			    raised, "forged proc-cache metadata was loaded as native code "
			    "without being rejected")
			self._assert_no_crash(node)
		finally:
			con.close()
		node.stop()

	def test_forged_prosrc_rejected(self):
		"""A forged source symbol must be rejected before dlopen()."""
		self._forge_and_expect_error(lambda p, s, l, c: (p, 'evil_symbol', l))

	def test_forged_probin_rejected(self):
		"""A forged library path must be rejected before dlopen()."""
		self._forge_and_expect_error(lambda p, s, l, c:
		                             ('/tmp/evil_path', s, l))

	def test_forged_prolang_rejected(self):
		"""A forged language (C -> internal) must be rejected."""

		def corrupt(p, s, l, c):
			return (p, s, self._internal_lang_oid(c))

		self._forge_and_expect_error(corrupt)
