/*-------------------------------------------------------------------------
 *
 * wal_dispatch.c
 *		Routines dealing with WAL parsing for OrioleDB.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/wal_dispatch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/page_contents.h"
#include "recovery/wal_dispatch.h"
#include "recovery/wal.h"

static const WalRecordDesc g_wal_descs[] =
{
#define X(sym, val, name, fn) { (uint8)(val), name, (fn) },
	ORIOLE_WAL_RECORDS(X)
#undef X
};

const WalRecordDesc *
wal_get_desc(uint8 type)
{
	for (size_t i = 0; i < lengthof(g_wal_descs); i++)
		if (g_wal_descs[i].type == type)
			return &g_wal_descs[i];
	return NULL;
}

const char *
wal_type_name(uint8 type)
{
	const WalRecordDesc *d = wal_get_desc(type);

	return d ? d->name : "UNKNOWN";
}
