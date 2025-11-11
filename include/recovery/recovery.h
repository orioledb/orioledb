/*-------------------------------------------------------------------------
 *
 * recovery.h
 *		External declarations for orioledb engine recovery.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
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
#include "btree/page_contents.h"

extern void o_recovery_start_hook(void);
extern void orioledb_redo(XLogReaderState *record);
extern void o_xact_redo_hook(TransactionId xid, XLogRecPtr lsn);
extern void o_recovery_finish_hook(bool cleanup);

extern Size recovery_shmem_needs(void);
extern void recovery_shmem_init(Pointer ptr, bool found);
extern bool is_recovery_process(void);
extern CommitSeqNo recovery_map_oxid_csn(OXid oxid, bool *found);
extern void worker_send_msg(int worker_id, Pointer msg, uint64 msg_size);
extern void worker_queue_flush(int worker_id);
extern void idx_workers_shutdown(void);
extern void recovery_send_leader_oids(ORelOids oids, OIndexNumber ix_num,
									  uint32 o_table_version,
									  ORelOids old_oids, uint32 old_o_table_version,
									  bool isrebuild);
extern void recovery_send_worker_oids(dsm_handle seg_handle);
extern void workers_send_finish(bool send_to_idx_pool);
extern void update_proc_retain_undo_location(int worker_id);

static inline bool
is_recovery_in_progress(void)
{
	return is_recovery_process() || RecoveryInProgress();
}

extern XLogRecPtr recovery_get_current_ptr(void);
extern XLogRecPtr recovery_get_effective_replay_ptr(void);

extern bool orioledb_recovery_stops_before_hook(XLogReaderState *record,
												TransactionId *recordXid,
												TimestampTz *recordXtime);

extern int	recovery_queue_size_guc;
extern int	recovery_pool_size_guc;
extern int	recovery_idx_pool_size_guc;
extern OXid recovery_oxid;

typedef struct BTreeDescr BTreeDescr;

extern OTuple recovery_rec_insert(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size);
extern OTuple recovery_rec_update(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size);
extern OTuple recovery_rec_delete(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size, char relreplident);
extern OTuple recovery_rec_delete_key(BTreeDescr *desc, OTuple key, bool *allocated, int *size);

extern void recovery_cleanup_old_files(uint32 max_chkp_num,
									   bool before_recovery);

extern void recovery_load_state_from_file(int worker_id, uint32 chkpnum, bool shutdown);
extern bool check_recovery_workers_finished(void);

#endif							/* __RECOVERY_H__ */
