/*-------------------------------------------------------------------------
 *
 * safe_recovery.h
 *		Per-OXID buffering recovery mode for OrioleDB.
 *
 * When orioledb.safe_recovery is on, redo defers application of every
 * modify-class WAL record per OXID until that OXID's fate (COMMIT,
 * JOINT_COMMIT + heap commit, or ROLLBACK) is observed.  Transactions
 * whose container stream was flushed but never reached COMMIT (typical
 * mid-DML crash signature) are reported at LOG level and discarded.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/safe_recovery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __SAFE_RECOVERY_H__
#define __SAFE_RECOVERY_H__

#include "orioledb.h"

#include "access/xlogdefs.h"

extern void safe_recovery_startup(void);
extern bool safe_recovery_replay_container(Pointer startPtr, Pointer endPtr,
										   XLogRecPtr xlogRecPtr,
										   XLogRecPtr xlogRecEndPtr);
extern void safe_recovery_xact_redo(TransactionId xid, XLogRecPtr lsn,
									bool commit);
extern void safe_recovery_finish(void);

#endif							/* __SAFE_RECOVERY_H__ */
