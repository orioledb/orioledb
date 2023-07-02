/*-------------------------------------------------------------------------
 *
 * recovery.h
 *		External declarations for orioledb engine recovery.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/recovery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __RECOVERY_H__
#define __RECOVERY_H__

#include "btree/btree.h"
#include "recovery/internal.h"

extern void o_recovery_start_hook(void);
#if PG_VERSION_NUM >= 150000
extern void orioledb_redo(XLogReaderState *record);
#else
extern void o_recovery_logicalmsg_redo_hook(XLogReaderState *record);
#endif
extern void o_xact_redo_hook(TransactionId xid, XLogRecPtr lsn);
extern void o_recovery_finish_hook(bool cleanup);

extern Size recovery_shmem_needs(void);
extern void recovery_shmem_init(Pointer ptr, bool found);
extern bool is_recovery_process(void);
extern CommitSeqNo recovery_map_oxid_csn(OXid oxid, bool *found);
extern void worker_send_msg(int worker_id, Pointer msg, uint64 msg_size);
extern void worker_queue_flush(int worker_id);
extern void idx_workers_shutdown(void);
extern void recovery_send_oids(ORelOids oids, OIndexNumber ix_num, uint32 o_table_version, int nindices,
							   bool send_to_leader);
extern void workers_send_finish(bool send_to_idx_pool);
extern void update_proc_retain_undo_location(int worker_id);

static inline bool
is_recovery_in_progress(void)
{
	return is_recovery_process() || RecoveryInProgress();
}

extern XLogRecPtr recovery_get_current_ptr(void);
extern int	recovery_queue_size_guc;
extern int	recovery_pool_size_guc;
extern int	recovery_idx_pool_size_guc;
extern OXid recovery_oxid;

typedef struct BTreeDescr BTreeDescr;

extern OTuple recovery_rec_insert(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size);
extern OTuple recovery_rec_update(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size);
extern OTuple recovery_rec_delete(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size);
extern OTuple recovery_rec_delete_key(BTreeDescr *desc, OTuple key, bool *allocated, int *size);

extern void recovery_cleanup_old_files(uint32 max_chkp_num,
									   bool before_recovery);


#endif							/* __RECOVERY_H__ */
