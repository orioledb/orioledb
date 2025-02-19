# contrib/orioledb/Makefile

MODULE_big = orioledb
EXTENSION = orioledb
PGFILEDESC = "orioledb - orioledb transactional storage engine via TableAm"
SHLIB_LINK += -lzstd -lcurl -lssl -lcrypto

DATA_built = $(patsubst %_prod.sql,%.sql,$(wildcard sql/*_prod.sql))
DATA = $(filter-out $(wildcard sql/*_*.sql) $(DATA_built), $(wildcard sql/*sql))

EXTRA_CLEAN = include/utils/stopevents_defs.h \
			  include/utils/stopevents_data.h
OBJS = src/btree/btree.o \
	   src/btree/build.o \
	   src/btree/check.o \
	   src/btree/find.o \
	   src/btree/insert.o \
	   src/btree/io.o \
	   src/btree/iterator.o \
	   src/btree/merge.o \
	   src/btree/modify.o \
	   src/btree/page_chunks.o \
	   src/btree/page_contents.o \
	   src/btree/page_state.o \
	   src/btree/print.o \
	   src/btree/scan.o \
	   src/btree/split.o \
	   src/btree/undo.o \
	   src/catalog/ddl.o \
	   src/catalog/free_extents.o \
	   src/catalog/indices.o \
	   src/catalog/o_aggregate_cache.o \
	   src/catalog/o_amop_cache.o \
	   src/catalog/o_amproc_cache.o \
	   src/catalog/o_class_cache.o \
	   src/catalog/o_enum_cache.o \
	   src/catalog/o_collation_cache.o \
	   src/catalog/o_database_cache.o \
	   src/catalog/o_indices.o \
	   src/catalog/o_operator_cache.o \
	   src/catalog/o_opclass_cache.o \
	   src/catalog/o_proc_cache.o \
	   src/catalog/o_range_cache.o \
	   src/catalog/o_sys_cache.o \
	   src/catalog/o_tables.o \
	   src/catalog/o_type_cache.o \
	   src/catalog/sys_trees.o \
	   src/checkpoint/checkpoint.o \
	   src/checkpoint/control.o \
	   src/indexam/handler.o \
	   src/orioledb.o \
	   src/recovery/logical.o \
	   src/recovery/recovery.o \
	   src/recovery/wal.o \
	   src/recovery/worker.o \
	   src/s3/archive.o \
	   src/s3/checkpoint.o \
	   src/s3/control.o \
	   src/s3/checksum.o \
	   src/s3/headers.o \
	   src/s3/queue.o \
	   src/s3/requests.o \
	   src/s3/worker.o \
	   src/tableam/bitmap_scan.o \
	   src/tableam/descr.o \
	   src/tableam/func.o \
	   src/tableam/handler.o \
	   src/tableam/index_scan.o \
	   src/tableam/key_range.o \
	   src/tableam/key_bitmap.o \
	   src/tableam/operations.o \
	   src/tableam/scan.o \
	   src/tableam/tree.o \
	   src/transam/undo.o \
	   src/transam/oxid.o \
	   src/tuple/format.o \
	   src/tuple/toast.o \
	   src/tuple/slot.o \
	   src/tuple/sort.o \
	   src/workers/bgwriter.o \
	   src/utils/compress.o \
	   src/utils/o_buffers.o \
	   src/utils/page_pool.o \
	   src/utils/planner.o \
	   src/utils/seq_buf.o \
	   src/utils/stopevent.o \
	   src/utils/ucm.o \
	   $(WIN32RES)

REGRESSCHECKS = btree_sys_check \
				alter_type \
				bitmap_scan \
				btree_compression \
				btree_print \
				createas \
				ddl \
				explain \
				fillfactor \
				foreign_keys \
				generated \
				getsomeattrs \
				indices \
				indices_build \
				inherits \
				ioc \
				joins \
				nulls \
				opclass \
				parallel_scan \
				partial \
				partition \
				primary_key \
				row_level_locks \
				row_security \
				sanitizers \
				subquery \
				subtransactions \
				tableam \
				temp \
				toast \
				trigger \
				types
ISOLATIONCHECKS = bitmap_hist_scan \
				  btree_iterate \
				  btree_print_backend_id \
				  concurrent_update_delete \
				  fkeys \
				  included \
				  insert_fails \
				  ioc_deadlock \
				  ioc_lost_update \
				  isol_ddl \
				  isol_rc \
				  isol_rr \
				  isol_rr_bscan \
				  isol_rr_seqscan \
				  load_refind_page \
				  merge \
				  partition_move \
				  rightlink \
				  rll \
				  rll_deadlock \
				  rll_mix \
				  rll_subtrans \
				  table_lock_test \
				  uniq
TESTGRESCHECKS_PART_1 = test/t/checkpointer_test.py \
						test/t/eviction_bgwriter_test.py \
						test/t/eviction_compression_test.py \
						test/t/eviction_test.py \
						test/t/file_operations_test.py \
						test/t/files_test.py \
						test/t/incomplete_split_test.py \
						test/t/merge_test.py \
						test/t/o_tables_test.py \
						test/t/o_tables_2_test.py \
						test/t/recovery_test.py \
						test/t/recovery_opclass_test.py \
						test/t/recovery_worker_test.py \
						test/t/replication_test.py \
						test/t/types_test.py \
						test/t/undo_eviction_test.py
TESTGRESCHECKS_PART_2 = test/t/checkpoint_concurrent_test.py \
						test/t/checkpoint_eviction_test.py \
						test/t/checkpoint_same_trx_test.py \
						test/t/checkpoint_split1_test.py \
						test/t/checkpoint_split2_test.py \
						test/t/checkpoint_split3_test.py \
						test/t/checkpoint_update_compress_test.py \
						test/t/checkpoint_update_test.py \
						test/t/ddl_test.py \
						test/t/eviction_full_memory_test.py \
						test/t/include_indices_test.py \
						test/t/indices_build_test.py \
						test/t/logical_test.py \
						test/t/not_supported_yet_test.py \
						test/t/parallel_test.py \
						test/t/reindex_test.py \
						test/t/s3_test.py \
						test/t/schema_test.py \
						test/t/toast_index_test.py \
						test/t/trigger_test.py \
						test/t/unlogged_test.py \
						test/t/vacuum_test.py

PG_REGRESS_ARGS=--no-locale --inputdir=test --outputdir=test --temp-instance=./test/tmp_check
PG_ISOLATION_REGRESS_ARGS=--no-locale --inputdir=test --outputdir=test/output_iso --temp-instance=./test/tmp_check_iso

ifdef IS_DEV
sql/%.sql:
	@cat sql/$*_prod.sql sql/$*_dev.sql > $@
else
sql/%.sql:
	@cat sql/$*_prod.sql > $@
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
override PG_CPPFLAGS += -I$(CURDIR)/include
include $(PGXS)

ifeq ($(shell expr $(MAJORVERSION) \>= 14), 1)
  REGRESSCHECKS += toast_column_compress
endif

ifeq ($(shell expr $(MAJORVERSION) \>= 15), 1)
  TESTGRESCHECKS_PART_2 += test/t/merge_into_test.py
  ISOLATIONCHECKS += isol_merge
endif

# Control installation dependency and temporary install wrapper based on SKIP_INSTALL.
# In environments like Docker, we skip the installation of the extension.
# This mechanism is also useful for verifying orioledb extensions installed through
# package managers (apt, yum, etc.) or for running regression tests against production installations.
ifdef SKIP_INSTALL
INSTALL_DEP =
TEMP_INSTALL_WRAPPER =
else
INSTALL_DEP = | install
TEMP_INSTALL_WRAPPER = $(with_temp_install)
endif

regresscheck: $(INSTALL_DEP)
	$(pg_regress_check) \
		--temp-config test/orioledb_regression.conf \
		$(PG_REGRESS_ARGS) \
		$(REGRESSCHECKS)

isolationcheck: $(INSTALL_DEP)
	$(pg_isolation_regress_check) \
		--temp-config test/orioledb_isolation.conf \
		$(PG_ISOLATION_REGRESS_ARGS) \
		$(ISOLATIONCHECKS)

$(TESTGRESCHECKS_PART_1) $(TESTGRESCHECKS_PART_2): $(INSTALL_DEP)
	$(TEMP_INSTALL_WRAPPER) python3 -W ignore::DeprecationWarning -m unittest -v $@

installcheck: regresscheck isolationcheck testgrescheck
	echo "All checks are successful!"

else
subdir = contrib/orioledb
top_builddir = ../..
override PG_CPPFLAGS += -I$(top_srcdir)/$(subdir)/include
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

regresscheck: | submake-regress submake-orioledb temp-install
	$(pg_regress_check) \
		--temp-config $(top_srcdir)/contrib/orioledb/test/orioledb_regression.conf \
		$(PG_REGRESS_ARGS) \
		$(REGRESSCHECKS)

isolationcheck: | submake-isolation submake-orioledb temp-install
	$(pg_isolation_regress_check) \
		--temp-config $(top_srcdir)/contrib/orioledb/test/orioledb_isolation.conf \
		$(PG_ISOLATION_REGRESS_ARGS) \
		$(ISOLATIONCHECKS)

$(TESTGRESCHECKS_PART_1) $(TESTGRESCHECKS_PART_2): | submake-orioledb temp-install
	PG_CONFIG="$(abs_top_builddir)/tmp_install$(bindir)/pg_config" \
		$(with_temp_install) \
		python3 -m unittest -v $@

check: regresscheck isolationcheck testgrescheck
	echo "All checks are successful!"
endif

# Retrieve the current commit hash from the Git repository.
# If the .git environment does not exist (e.g., in a Docker environment or a non-Git setup),
# fallback to a default "fake" commit hash (all zeros) to avoid errors.
COMMIT_HASH := $(shell git rev-parse HEAD 2>/dev/null)
ifeq ($(strip $(COMMIT_HASH)),)
	COMMIT_HASH := 0000000000000000000000000000000000000000
endif
override CFLAGS_SL += -DCOMMIT_HASH=$(COMMIT_HASH) -Wno-error=deprecated-declarations

ifdef VALGRIND
override with_temp_install += PGCTLTIMEOUT=3000 \
	valgrind --vgdb=yes --leak-check=no --gen-suppressions=all \
	--suppressions=valgrind.supp --time-stamp=yes \
	--log-file=pid-%p.log --trace-children=yes \
	--trace-children-skip=*/initdb
else
override with_temp_install += PGCTLTIMEOUT=900
endif

include/utils/stopevents_data.h: include/utils/stopevents_defs.h

include/utils/stopevents_defs.h: stopevents.txt stopevents_gen.py
	python3 stopevents_gen.py


ifndef ORIOLEDB_PATCHSET_VERSION
ORIOLEDB_PATCHSET_VERSION=1
endif
CUR_ORIOLEDB_PATCHSET_VERSION := $(shell grep '^$(MAJORVERSION):' .pgtags | cut -d'_' -f2)

check_patchset_version:
	@if [ $(CUR_ORIOLEDB_PATCHSET_VERSION) != $(ORIOLEDB_PATCHSET_VERSION) ]; then \
		echo "Wrong orioledb patchset version:"\
				"expected $(CUR_ORIOLEDB_PATCHSET_VERSION),"\
				"got $(ORIOLEDB_PATCHSET_VERSION)"; \
		echo "Rebuild and install patched orioledb/postgres using tag"\
				"'patches$(MAJORVERSION)_$(CUR_ORIOLEDB_PATCHSET_VERSION)'"; \
		false; \
	fi

$(OBJS): include/utils/stopevents_defs.h check_patchset_version

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-orioledb:
	$(MAKE) -C $(top_builddir)/contrib/orioledb

testgrescheck: $(TESTGRESCHECKS_PART_1) $(TESTGRESCHECKS_PART_2)

testgrescheck_part_1: $(TESTGRESCHECKS_PART_1)

testgrescheck_part_2: $(TESTGRESCHECKS_PART_2)

temp-install: EXTRA_INSTALL=contrib/orioledb

orioledb.typedefs: $(OBJS)
	./typedefs_gen.py

pgindent: orioledb.typedefs
	pgindent --typedefs=orioledb.typedefs \
	src/*.c \
	src/*/*.c \
	include/*.h \
	include/*/*.h

yapf:
	yapf -i test/t/*.py
	yapf -i *.py

.PHONY: submake-orioledb submake-regress check \
	regresscheck isolationcheck testgrescheck pgindent \
	$(TESTGRESCHECKS_PART_1) $(TESTGRESCHECKS_PART_2)
