/*-------------------------------------------------------------------------
 *
 * oxid.h
 *		Decalarations for transaction management routines.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/transam/oxid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __OXID_H__
#define __OXID_H__

typedef struct
{
	pg_atomic_uint64 nextXid;
	pg_atomic_uint64 lastXidWhenUpdatedGlobalXmin;
	pg_atomic_uint64 runXmin;
	pg_atomic_uint64 globalXmin;

	pg_atomic_uint64 writeInProgressXmin;
	pg_atomic_uint64 writtenXmin;
	pg_atomic_uint64 checkpointRetainXmin;
	pg_atomic_uint64 checkpointRetainXmax;
	pg_atomic_uint64 cleanedXmin;
	pg_atomic_uint64 cleanedCheckpointXmin;
	pg_atomic_uint64 cleanedCheckpointXmax;

	slock_t		xminMutex;

	int			xidMapTrancheId;
	LWLock		xidMapWriteLock;
} XidMeta;

extern XidMeta *xid_meta;

extern Size oxid_shmem_needs(void);
extern void oxid_init_shmem(Pointer ptr, bool found);
extern bool wait_for_oxid(OXid oxid);
extern void oxid_notify(OXid oxid);
extern void oxid_notify_all(void);
extern void advance_oxids(OXid new_xid);
extern OXid get_current_oxid(void);
extern void set_oxid_csn(OXid oxid, CommitSeqNo csn);
extern void set_current_oxid(OXid oxid);
extern void parallel_worker_set_oxid(void);
extern void reset_current_oxid(void);
extern OXid get_current_oxid_if_any(void);
extern void current_oxid_precommit(void);
extern void current_oxid_commit(CommitSeqNo csn);
extern void current_oxid_abort(void);
extern CommitSeqNo oxid_get_csn(OXid oxid);
extern void fill_current_oxid_csn(OXid *oxid, CommitSeqNo *csn);
extern int	oxid_get_procnum(OXid oxid);
extern bool xid_is_finished(OXid xid);
extern bool xid_is_finished_for_everybody(OXid xid);
extern void fsync_xidmap_range(OXid xmin, OXid xmax, uint32 wait_event_info);

#endif							/* __OXID_H__ */
