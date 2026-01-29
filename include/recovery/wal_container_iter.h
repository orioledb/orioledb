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

typedef WalParseStatus (*WalOnEventFn) (void *ctx, const WalEvent *ev);

typedef struct WalConsumer
{
	void	   *ctx;
	WalOnEventFn on_event;

} WalConsumer;

WalParseStatus wal_container_iterate(WalReader *r, WalConsumer *consumer, bool allow_logging);

/* @TODO rename after migration! */
WalParseStatus wr_wal_container_read_header(WalReader *r, bool allow_logging);

#endif							/* __WAL_CONTAINER_ITER_H__ */
