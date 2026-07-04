/*-------------------------------------------------------------------------
 *
 * scan.h
 *		Declarations for sequential scan of OrioleDB B-tree.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/scan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_SCAN_H__
#define __BTREE_SCAN_H__

#include "btree/btree.h"
#include "btree/page_contents.h"

#include "executor/tuptable.h"
#include "utils/sampling.h"

typedef struct
{
	int			pageLoadTrancheId,
				downlinksPublishTrancheId;
} BTreeScanShmem;

typedef struct BTreeSeqScan BTreeSeqScan;

typedef struct BTreeSeqScanCallbacks
{
	bool		(*isRangeValid) (OTuple low, OTuple high, void *arg);
	bool		(*getNextKey) (OFixedKey *key, bool inclusive, void *arg);

	/*
	 * Optional (may be NULL).  Given the just-finished internal page's hikey
	 * in key->tuple (an exclusive upper bound: every key on that page is less
	 * than it; a NULL tuple means "from the start of the tree"), rewrite key
	 * with the smallest key the scan still needs that is >= the hikey, as a
	 * non-leaf key usable for a fresh descent.  Returns false when nothing is
	 * left, letting the sequential scan skip whole internal pages that hold
	 * no wanted keys instead of stepping through every one.
	 */
	bool		(*getNextPageKey) (OFixedKey *key, void *arg);
} BTreeSeqScanCallbacks;

extern BTreeScanShmem *btreeScanShmem;

extern Size btree_scan_shmem_needs(void);
extern void btree_scan_init_shmem(Pointer ptr, bool found);
extern BTreeSeqScan *make_btree_seq_scan(BTreeDescr *desc,
										 OSnapshot *oSnapshot,
										 void *poscan);
extern BTreeSeqScan *make_btree_seq_scan_cb(BTreeDescr *desc,
											OSnapshot *oSnapshot,
											BTreeSeqScanCallbacks *cb,
											void *arg);
extern BTreeSeqScan *make_btree_sampling_scan(BTreeDescr *desc,
											  BlockSampler sampler);
extern OTuple btree_seq_scan_getnext(BTreeSeqScan *scan, MemoryContext mctx,
									 CommitSeqNo *tupleCsn,
									 BTreeLocationHint *hint);
extern OTuple btree_seq_scan_getnext_raw(BTreeSeqScan *scan, MemoryContext mctx,
										 bool *end, BTreeLocationHint *hint);
extern void free_btree_seq_scan(BTreeSeqScan *scan);
extern void seq_scans_cleanup(void);
extern int	meta_page_get_num_seq_scans(OInMemoryBlkno metaPageBlkno);

#endif							/* __BTREE_SCAN_H__ */
