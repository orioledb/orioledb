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

typedef WalParseStatus (*WalParseFn) (WalReader *r, WalEvent *ev);

typedef struct WalRecordDesc
{
	uint8		type;
	const char *name;
	WalParseFn	parse;

} WalRecordDesc;

const WalRecordDesc *wal_get_desc(uint8 type);
const char *wal_type_name(uint8 type);

#endif							/* __WAL_DISPATCH_H__ */
