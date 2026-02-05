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
#define X(sym, val, name, fn) { (wal_type_t)(val), name, (fn) },
	ORIOLE_WAL_RECORDS(X)
#undef X
};

static const WalRecordDesc g_wal_flag_descs[] =
{
#define X(sym, val, name, fn) { (wal_type_t)(val), name, (fn) },
	ORIOLE_WAL_FLAGS(X)
#undef X
};

const WalRecordDesc *
wal_get_desc(wal_type_t type)
{
	for (size_t i = 0; i < lengthof(g_wal_descs); i++)
		if (g_wal_descs[i].type == type)
			return &g_wal_descs[i];
	return NULL;
}

const char *
wal_type_name(wal_type_t type)
{
	const WalRecordDesc *d = wal_get_desc(type);

	return d ? d->name : "UNKNOWN";
}

const WalRecordDesc *
wal_flag_get_desc(wal_type_t type)
{
	for (size_t i = 0; i < lengthof(g_wal_flag_descs); i++)
		if (g_wal_flag_descs[i].type == type)
			return &g_wal_flag_descs[i];
	return NULL;
}

const char *
wal_flag_type_name(wal_type_t type)
{
	const WalRecordDesc *d = wal_flag_get_desc(type);

	return d ? d->name : "UNKNOWN";
}

void
build_fixed_tuple_from_tuple_view(const WalTupleView *view, OFixedTuple *tuple)
{
	Assert(view);
	Assert(tuple);

	tuple->tuple.formatFlags = view->formatFlags;
	Assert(tuple->fixedData);
	memcpy(tuple->fixedData, view->data, view->len);
	if (view->len != MAXALIGN(view->len))
		memset(&tuple->fixedData[view->len], 0, MAXALIGN(view->len) - view->len);
	tuple->tuple.data = tuple->fixedData;
}
