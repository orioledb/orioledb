/*-------------------------------------------------------------------------
 *
 * io.h
 *		Declarations for orioledb B-tree IO.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/io.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_IO_H__
#define __BTREE_IO_H__

#include "btree.h"
#include "btree/find.h"

typedef enum OWalkPageResult
{
	OWalkPageSkipped,
	OWalkPageWritten,
	OWalkPageEvicted,
	OWalkPageMerged,
} OWalkPageResult;

extern Size btree_io_shmem_needs(void);
extern void btree_io_shmem_init(Pointer buf, bool found);
extern void btree_io_error_cleanup(void);
extern void request_btree_io_lwlocks(void);
extern int	assign_io_num(OInMemoryBlkno blkno, OffsetNumber offnum);
extern OWalkPageResult walk_page(OInMemoryBlkno blkno, bool evict);
extern void unlock_io(int ionum);
extern void wait_for_io_completion(int ionum);
extern bool cleanup_btree_files(Oid datoid, Oid relnode, bool fsync);
extern bool fsync_btree_files(Oid datoid, Oid relnode);
extern int	OFileRead(File file, char *buffer, int amount, off_t offset,
					  uint32 wait_event_info);
extern int	OFileWrite(File file, char *buffer, int amount, off_t offset,
					   uint32 wait_event_info);
extern void btree_init_smgr(BTreeDescr *descr);
extern void btree_open_smgr(BTreeDescr *descr);
extern void btree_close_smgr(BTreeDescr *descr);
extern char *btree_filename(Oid datoid, Oid relnode, int segno, uint32 chkpNum);
extern char *btree_smgr_filename(BTreeDescr *desc, off_t offset,
								 uint32 chkpNum);
extern int	btree_smgr_read(BTreeDescr *desc, char *buffer, uint32 chkpNum,
							int amount, off_t offset);
extern void btree_smgr_writeback(BTreeDescr *desc, uint32 chkpNum,
								 off_t offset, int amount);
extern void btree_smgr_sync(BTreeDescr *desc, uint32 chkpNum, off_t length);
extern void btree_smgr_punch_hole(BTreeDescr *desc, uint32 chkpNum,
								  off_t offset, int length);
extern void init_btree_io_lwlocks(void);
extern bool read_page_from_disk(BTreeDescr *desc, Pointer img, uint64 downlink, FileExtent *extent);
extern void load_page(OBTreeFindPageContext *context);
extern uint64 perform_page_io(BTreeDescr *desc, OInMemoryBlkno blkno,
							  Page img, uint32 checkpoint_number,
							  bool copy_blkno, bool *dirty_parent);
extern uint64 perform_page_io_autonomous(BTreeDescr *desc, uint32 chkpNum,
										 Page img, FileExtent *extent);
extern uint64 perform_page_io_build(BTreeDescr *desc, Page img,
									FileExtent *extent, BTreeMetaPage *metaPageBlkno);
extern BTreeDescr *index_oids_get_btree_descr(ORelOids oids, OIndexType type);
extern void try_to_punch_holes(BTreeDescr *desc);

#endif							/* __BTREE_IO_H__ */
