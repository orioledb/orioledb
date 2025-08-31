/*-------------------------------------------------------------------------
 *
 * scan.h
 *		Declarations for sequential scan of OrioleDB B-tree.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
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

#endif							/* __BTREE_SCAN_H__ */
