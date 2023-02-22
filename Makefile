# contrib/orioledb/Makefile

MODULE_big = orioledb
EXTENSION = orioledb
DATA = orioledb--1.0.sql
PGFILEDESC = "orioledb - orioledb transactional storage engine via TableAm"
SHLIB_LINK += -lzstd

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
	   src/orioledb.o \
	   src/recovery/recovery.o \
	   src/recovery/wal.o \
	   src/recovery/worker.o \
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
				bitmap_scan \
				btree_compression \
				btree_print \
				collate \
				createas \
				ddl \
				explain \
				foreign_keys \
				getsomeattrs \
				indices \
				indices_build \
				inherits \
				ioc \
				joins \
				opclass \
				parallel_scan \
				partial \
				partition \
				primary_key \
				row_level_locks \
				subquery \
				subtransactions \
				tableam \
				toast \
				trigger \
				types
ISOLATIONCHECKS = bitmap_hist_scan \
				  btree_iterate \
				  btree_print_backend_id \
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
TESTGRESCHECKS_PART_1 = t/checkpointer_test.py \
						t/eviction_bgwriter_test.py \
						t/eviction_compression_test.py \
						t/eviction_test.py \
						t/file_operations_test.py \
						t/files_test.py \
						t/incomplete_split_test.py \
						t/merge_test.py \
						t/o_tables_test.py \
						t/o_tables_2_test.py \
						t/recovery_test.py \
						t/recovery_opclass_test.py \
						t/recovery_worker_test.py \
						t/replication_test.py \
						t/types_test.py \
						t/types_2_test.py \
						t/undo_eviction_test.py \
						t/toast_index_test.py
TESTGRESCHECKS_PART_2 = t/checkpoint_concurrent_test.py \
						t/checkpoint_eviction_test.py \
						t/checkpoint_same_trx_test.py \
						t/checkpoint_split1_test.py \
						t/checkpoint_split2_test.py \
						t/checkpoint_split3_test.py \
						t/checkpoint_update_compress_test.py \
						t/checkpoint_update_test.py \
						t/ddl_test.py \
						t/eviction_full_memory_test.py \
						t/include_indices_test.py \
						t/indices_build_test.py \
						t/reindex_test.py \
						t/schema_test.py \
						t/trigger_test.py \
						t/vacuum_test.py

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
override PG_CPPFLAGS += -I$(CURDIR)/include
include $(PGXS)

ifeq ($(shell expr $(MAJORVERSION) \>= 14), 1)
  REGRESSCHECKS += toast_column_compress
endif

ifeq ($(shell expr $(MAJORVERSION) \>= 15), 1)
  TESTGRESCHECKS_PART_2 += t/merge_into_test.py
  ISOLATIONCHECKS += isol_merge
endif

regresscheck: | install
	$(pg_regress_check) \
		--temp-config orioledb_regression.conf \
		$(REGRESSCHECKS)

isolationcheck: | install
	$(pg_isolation_regress_check) \
		--temp-config orioledb_isolation.conf \
		$(ISOLATIONCHECKS)

$(TESTGRESCHECKS_PART_1) $(TESTGRESCHECKS_PART_2): | install
	$(with_temp_install) \
	python3 -W ignore::DeprecationWarning -m unittest -v $@

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
		--temp-config $(top_srcdir)/contrib/orioledb/orioledb_regression.conf \
		$(REGRESSCHECKS)

isolationcheck: | submake-isolation submake-orioledb temp-install
	$(pg_isolation_regress_check) \
		--temp-config $(top_srcdir)/contrib/orioledb/orioledb_isolation.conf \
		$(ISOLATIONCHECKS)

$(TESTGRESCHECKS_PART_1) $(TESTGRESCHECKS_PART_2): | submake-orioledb temp-install
	PG_CONFIG="$(abs_top_builddir)/tmp_install$(bindir)/pg_config" \
		$(with_temp_install) \
		python3 -m unittest -v $@

check: regresscheck isolationcheck testgrescheck
	echo "All checks are successful!"
endif

COMMIT_HASH = $(shell git rev-parse HEAD)
override CFLAGS_SL += -DCOMMIT_HASH=$(COMMIT_HASH)

ifdef VALGRIND
override with_temp_install += PGCTLTIMEOUT=1200 \
	valgrind --leak-check=no --gen-suppressions=all \
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
CUR_ORIOLEDB_PATCHSET_VERSION := $(shell grep '$(MAJORVERSION)' .pgtags | cut -d'_' -f2)

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

.PHONY: submake-orioledb submake-regress check \
	regresscheck isolationcheck testgrescheck pgindent \
	$(TESTGRESCHECKS_PART_1) $(TESTGRESCHECKS_PART_2)
