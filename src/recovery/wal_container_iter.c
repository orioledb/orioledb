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

#include "replication/origin.h"

#include "recovery/wal_container_iter.h"
#include "recovery/wal_dispatch.h"
#include "recovery/wal.h"

/* @TODO rename after migration! */
static WalParseResult
wr_wal_container_read_header(WalReaderState *r, bool allow_logging)
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
#ifdef IS_DEV
		/* Always fail tests on difference */
		if (allow_logging)
			elog(FATAL, "Can't apply WAL container version %u that is newer than supported %u. Intentionally fail tests", wal_version, ORIOLEDB_WAL_VERSION);

		return WALPARSE_BAD_VERSION;
#else
		if (allow_logging)
			elog(WARNING, "Can't apply WAL container version %u that is newer than supported %u", wal_version, ORIOLEDB_WAL_VERSION);

		/* Further fail and output is caller-specific */
#endif
	}
	else if (wal_version < ORIOLEDB_WAL_VERSION)
	{
#ifdef IS_DEV
		/* Always fail tests on difference */
		if (allow_logging)
			elog(FATAL, "WAL container version %u is older than current %u. Intentionally fail tests", wal_version, ORIOLEDB_WAL_VERSION);

		return WALPARSE_BAD_VERSION;
#else
		if (allow_logging)
			elog(LOG, "WAL container version %u is older than current %u. Applying with conversion.", wal_version, ORIOLEDB_WAL_VERSION);
#endif
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

static WalParseResult
wal_container_flag_check(WalReaderState *r, WalConsumer *consumer, wal_type_t type)
{
	WalParseResult st = WALPARSE_OK;

	Assert(r);
	Assert(consumer);

	if (r->wal_flags & type)
	{
		const WalRecordDesc *d = wal_flag_get_desc(type);

		Assert(d && d->parse);
		if (d && d->parse)
		{
			WalEvent	ev;

			memset(&ev, 0, sizeof(ev));

			ev.type = type;
			st = d->parse(r, &ev);
			if (st == WALPARSE_OK && consumer->on_flag)
				st = consumer->on_flag(consumer->ctx, &ev);
		}
		else
			st = WALPARSE_BAD_TYPE;
	}

	return st;
}

static WalParseResult
wal_container_flags_iterate(WalReaderState *r, WalConsumer *consumer)
{
	WalParseResult st = WALPARSE_OK;

	Assert(r);
	Assert(consumer);

#define X(sym, val, name, fn) \
do { \
	st = wal_container_flag_check(r, consumer, sym); \
	if (st) \
		return st; \
} while(0);

	ORIOLE_WAL_FLAGS(X)

#undef X

		return st;
}

WalParseResult
parse_wal_container(WalReaderState *r, WalConsumer *consumer, bool allow_logging)
{
	WalParseResult st;
	WalEvent	ev;

	Assert(r);
	Assert(consumer);
	Assert(consumer->on_event); /* consumer must handle every record */

	memset(&ev, 0, sizeof(ev));

	ev.relreplident = REPLICA_IDENTITY_DEFAULT;
	ev.origin_id = InvalidRepOriginId;
	ev.origin_lsn = InvalidXLogRecPtr;

	/*
	 * Read and validate container header framing.
	 *
	 * The header establishes wal_version, wal_flags and positions r->ptr at
	 * the first byte after the header area.
	 *
	 * From this point on, parse_wal_container() is the sole authority that
	 * drives the reader forward: each iteration consumes a record tag byte
	 * and (optionally) a record payload according to the descriptor.
	 */
	st = wr_wal_container_read_header(r, allow_logging);
	if (st)
		return st;

	/*
	 * Version compatibility check.
	 *
	 * We keep parse_wal_container() largely version-agnostic and delegate
	 * version policy to the consumer (recovery / logical decoding / etc.).
	 * Parsing WAL from a newer OrioleDB version can be unsafe even if the
	 * container framing is understood, because record encodings may differ.
	 */
	if (consumer->check_version)
	{
		st = consumer->check_version(r);
		if (st)
			return st;
	}

	/*
	 * Process container-level flags (a header prefix).
	 *
	 * Flags describe metadata that applies to the whole container and may
	 * carry their own payload. These bytes logically belong to the header
	 * area and must be consumed before we start scanning record tags.
	 * Otherwise, r->ptr would be misaligned and record parsing would
	 * desynchronize.
	 *
	 * The flag handling also gives the consumer a chance to capture
	 * header-wide context (e.g. xact-info) before any records are delivered.
	 */
	st = wal_container_flags_iterate(r, consumer);
	if (st)
		return st;

	/*
	 * Main record scan.
	 *
	 * Container format is: [tag byte][payload...][tag byte][payload...]...
	 *
	 * For each record:
	 *
	 * - read the tag (rec_type = ev.type),
	 *
	 * - look up its descriptor,
	 *
	 * - if the record has a payload, parse it and advance r->ptr,
	 *
	 * - deliver the event to the consumer.
	 */
	while (r->ptr < r->end)
	{
		uint8		rec_type;
		const WalRecordDesc *d = NULL;

		/*
		 * Offset from container start at which this record tag was found.
		 * Useful for consumers that need stable relative addressing, debug
		 * logging, or for building LSN-relative positions.
		 */
		ev.delta = r->ptr - r->start;

		/*
		 * Read record tag byte. After this, r->ptr points to payload (if
		 * any).
		 */
		WR_READ(r, &rec_type);
		ev.type = rec_type;

		/*
		 * value_ptr points to the first byte after the tag, i.e. to the
		 * payload. For payload-less records, value_ptr points to the next
		 * record tag.
		 *
		 * Consumers must treat value_ptr as "record-local" pointer only; it
		 * is valid only while the reader buffer remains intact.
		 */
		ev.value_ptr = r->ptr;

		d = wal_get_desc(ev.type);
		if (!d)
		{
			/*
			 * Unknown record type.
			 *
			 * No descriptor registered for the record type we have just read.
			 *
			 * parse_wal_container() is the single authority for OrioleDB WAL
			 * container binary format: it reads a record type byte and then
			 * advances the reader by calling the corresponding parse routine.
			 *
			 * If wal_get_desc() returns NULL, we cannot safely continue
			 * because we don't know how many bytes belong to this record
			 * (i.e. we can't advance r->ptr without risking
			 * desynchronization). Treat it as a hard protocol error.
			 *
			 * This typically means one of:
			 *
			 * 1) WAL / binary version mismatch (WAL from the "future"): The
			 * WAL stream contains a record type introduced in a newer
			 * OrioleDB version than the current build understands. The
			 * version check above is expected to prevent this; hitting this
			 * path may indicate that the container version/flags are
			 * inconsistent, or that the version gate in
			 * consumer->check_version is incomplete.
			 *
			 * 2) Registration mistake for a new record type: A new WAL_REC_*
			 * constant was added, but the corresponding WalRecordDesc entry
			 * was not registered via ORIOLE_WAL_RECORDS (or the descriptor
			 * table generator), so wal_get_desc() can't find it. This is a
			 * build-time integration bug.
			 *
			 * 3) Stream corruption / desynchronization: The reader is not
			 * positioned at a real record boundary. This can be caused by:
			 *
			 * - corrupted WAL container payload,
			 *
			 * - an earlier parser advancing r->ptr incorrectly (e.g. wrong
			 * size calculation / missing bounds checks),
			 *
			 * - reordering/truncation bugs leading to partial containers.
			 *
			 * In these cases the "type byte" may just be random data.
			 *
			 * 4) Feature/flag mismatch within the same WAL version: A record
			 * type is conditionally present under a container flag (or
			 * extension feature), but the corresponding flag handling was not
			 * applied (or was parsed incorrectly), shifting r->ptr.
			 *
			 * We return WALPARSE_BAD_TYPE to make the failure explicit and to
			 * avoid cascading parse errors.
			 */
			if (allow_logging)
			{
				elog(LOG, "[%s] UNKNOWN WAL RECORD TYPE %u(`%s`): chunk/tail len %ld/%ld",
					 __func__, ev.type, wal_type_name(ev.type),
					 r->end - r->start,
					 r->end - r->ptr);
			}
			return WALPARSE_BAD_TYPE;
		}

		if (allow_logging)
		{
			elog(DEBUG4, "[%s] WAL RECORD TYPE %u(`%s`)", __func__, ev.type, wal_type_name(ev.type));
		}

		/*
		 * Parse record payload (if any).
		 *
		 * Some OrioleDB WAL records are intentionally zero-length markers:
		 * they consist only of the tag byte and carry no additional bytes in
		 * the container. Their descriptor must have d->parse == NULL.
		 *
		 * This is distinct from an unknown record type: unknown types are
		 * rejected above, while known types with d->parse == NULL are valid
		 * and must still be delivered to the consumer.
		 *
		 * Parser contract:
		 *
		 * - if d->parse is present, it must consume exactly this record's
		 * payload and leave r->ptr positioned at the next record tag;
		 *
		 * - if d->parse is NULL, r->ptr already points to the next record
		 * tag.
		 */
		if (d->parse)
		{
			st = d->parse(r, &ev);
			if (st)
				return st;
		}

		/*
		 * Deliver the (possibly parsed) event to the consumer.
		 *
		 * Consumers may update decoding/replay state, apply changes, or
		 * selectively ignore records. Any non-OK status is treated as fatal
		 * for this container iteration.
		 */
		st = consumer->on_event(consumer->ctx, &ev);
		if (st)
			return st;
	}

	return WALPARSE_OK;
}
