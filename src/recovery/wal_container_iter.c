/*-------------------------------------------------------------------------
 *
 * wal_container_iter.c
 *		Routines dealing with WAL parsing for OrioleDB.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/wal_container_iter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/page_contents.h"

#include "recovery/wal_container_iter.h"
#include "recovery/wal_dispatch.h"
#include "recovery/wal.h"

WalParseStatus
wr_wal_container_read_header(WalReader *r, bool allow_logging)
{
	uint16		wal_version = 0;
	uint8		wal_flags = 0;
	uint8		firstByte = 0;

	WR_PEEK(r, &firstByte);

	if (firstByte >= FIRST_ORIOLEDB_WAL_VERSION)
	{
		/*
		 * Container starts with a valid WAL version. First WAL record is just
		 * after it.
		 */
		WR_READ(r, &wal_version);
	}
	else
	{
		/*
		 * Container starts with rec_type of first WAL record (its maximum
		 * value was under FIRST_ORIOLEDB_WAL_VERSION at the time of
		 * introducing WAL versioning. Consider this as version 0 and don't
		 * increase pointer
		 */
		wal_version = 0;
	}

	if (wal_version > ORIOLEDB_WAL_VERSION)
	{
		if (allow_logging)
		{
#ifdef IS_DEV
			/* Always fail tests on difference */
			elog(FATAL, "Can't apply WAL container version %u that is newer than supported %u. Intentionally fail tests", wal_version, ORIOLEDB_WAL_VERSION);
#else
			elog(WARNING, "Can't apply WAL container version %u that is newer than supported %u", wal_version, ORIOLEDB_WAL_VERSION);
			/* Further fail and output is caller-specific */
#endif
		}
	}
	else if (wal_version < ORIOLEDB_WAL_VERSION)
	{
		if (allow_logging)
		{
#ifdef IS_DEV
			/* Always fail tests on difference */
			elog(FATAL, "WAL container version %u is older than current %u. Intentionally fail tests", wal_version, ORIOLEDB_WAL_VERSION);
#else
			elog(LOG, "WAL container version %u is older than current %u. Applying with conversion.", wal_version, ORIOLEDB_WAL_VERSION);
#endif
		}
	}

	if (wal_version >= ORIOLEDB_XACT_INFO_WAL_VERSION)
	{
		/*
		 * WAL container flags were added by ORIOLEDB_XACT_INFO_WAL_VERSION.
		 */
		WR_READ(r, &wal_flags);
	}

	r->wal_version = wal_version;
	r->wal_flags = wal_flags;

	return WALPARSE_OK;
}

WalParseStatus
wal_container_iterate(WalReader *r, WalConsumer *consumer, bool allow_logging)
{
	WalParseStatus st;
	WalEvent	ev;

	memset(&ev, 0, sizeof(ev));

	ev.relreplident = REPLICA_IDENTITY_DEFAULT;

	st = wr_wal_container_read_header(r, allow_logging);
	if (st)
		return st;

	if (r->wal_flags & WAL_CONTAINER_HAS_XACT_INFO)
	{
		WR_SKIP(r, sizeof(WALRecXactInfo));
	}

	while (r->ptr < r->end)
	{
		const WalRecordDesc *d = NULL;

		WR_READ(r, &ev.type);

		d = wal_get_desc(ev.type);
		if (!d)
			return WALPARSE_BAD_TYPE;

		if (!d->parse)
			return WALPARSE_INTERNAL;

		st = d->parse(r, &ev);
		if (st)
			return st;

		st = consumer->on_event(consumer->ctx, &ev);
		if (st)
			return st;
	}

	return WALPARSE_OK;
}
