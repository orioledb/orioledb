/*-------------------------------------------------------------------------
 *
 * wal_container_iter.h
 * 		WAL parser declarations for OrioleDB.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal_container_iter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WAL_CONTAINER_ITER_H__
#define __WAL_CONTAINER_ITER_H__

#include "recovery/wal_reader.h"
#include "recovery/wal_event.h"

typedef WalParseStatus (*WalCheckVersionFn) (const WalReader *r);
typedef WalParseStatus (*WalOnFlagFn) (void *ctx, const WalEvent *ev);
typedef WalParseStatus (*WalOnEventFn) (void *ctx, WalEvent *ev);

typedef struct WalConsumer
{
	void	   *ctx;
	WalCheckVersionFn check_version;
	WalOnFlagFn on_flag;
	WalOnEventFn on_event;

} WalConsumer;

WalParseStatus parse_wal_container(WalReader *r, WalConsumer *consumer, bool allow_logging);

#endif							/* __WAL_CONTAINER_ITER_H__ */
