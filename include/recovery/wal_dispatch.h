/*-------------------------------------------------------------------------
 *
 * wal_dispatch.h
 * 		WAL parser declarations for OrioleDB.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal_dispatch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WAL_DISPATCH_H__
#define __WAL_DISPATCH_H__

#include "recovery/wal_reader.h"
#include "recovery/wal_event.h"

typedef WalParseResult (*WalParseFn) (WalReaderState *r, WalEvent *ev);

typedef struct WalRecordDesc
{
	wal_type_t	type;
	const char *name;
	WalParseFn	parse;

} WalRecordDesc;

const WalRecordDesc *wal_get_desc(wal_type_t type);
const char *wal_type_name(wal_type_t type);

const WalRecordDesc *wal_flag_get_desc(wal_type_t type);
const char *wal_flag_type_name(wal_type_t type);

#endif							/* __WAL_DISPATCH_H__ */
