/*-------------------------------------------------------------------------
 *
 * wal_reader.c
 *		Routines dealing with WAL parsing for OrioleDB.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/wal_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/page_contents.h"

#include "replication/origin.h"

#include "recovery/wal_reader.h"
#include "recovery/wal.h"

/*
 * This file implements the generic OrioleDB WAL container parser.
 *
 * The parser is responsible only for:
 *   - container framing (header, flags prefix, record tag/payload scanning),
 *   - descriptor-driven payload decoding,
 *   - safe cursor advancement with bounds checking.
 *
 * Parsing is strictly single-pass and forward-only.
 *
 * The reader never rewinds and never performs lookahead beyond what is
 * required for the current record. This guarantees predictable cursor
 * movement and avoids quadratic behavior on large containers.
 *
 * All semantic decisions (version policy, applying changes, decoding, etc.)
 * are delegated to the WalReaderState's consumer callbacks.
 *
 * Descriptor tables are generated from ORIOLE_WAL_RECORDS / ORIOLE_WAL_FLAGS,
 * making the type->parser mapping a single source of truth and preventing
 * protocol-breaking omissions when new types/flags are introduced.
 */

/*
 * g_wal_descs / g_wal_flag_descs
 *
 * Static descriptor tables generated from the X-macro lists.
 *
 * Keeping these tables centralized ensures that adding a new WAL record or
 * container flag is a single-point change: the parser automatically learns
 * about new types via the descriptor list, and cannot "forget" to handle a
 * flag payload (which would desynchronize the stream).
 */

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

/*
 * wal_get_desc() / wal_flag_get_desc()
 *
 * Lookup descriptor for a record/flag type.
 *
 * Returns NULL if the type is not registered in the static descriptor
 * tables generated from ORIOLE_WAL_RECORDS / ORIOLE_WAL_FLAGS.
 *
 * Note: unknown type is a hard protocol error for parse_wal_container(),
 * because without a descriptor we cannot determine payload length and cannot
 * safely advance the cursor.
 */

const WalRecordDesc *
wal_get_desc(wal_type_t type)
{
	for (size_t i = 0; i < lengthof(g_wal_descs); i++)
		if (g_wal_descs[i].type == type)
			return &g_wal_descs[i];
	return NULL;
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
wal_type_name(wal_type_t type)
{
	const WalRecordDesc *d = wal_get_desc(type);

	return d ? d->name : "UNKNOWN";
}

const char *
wal_flag_type_name(wal_type_t type)
{
	const WalRecordDesc *d = wal_flag_get_desc(type);

	return d ? d->name : "UNKNOWN";
}

static void
build_fixed_tuple_from_tuple_view(const OTuple *view, const OffsetNumber len, OFixedTuple *tuple)
{
	Assert(view);
	Assert(tuple);

	tuple->tuple.formatFlags = view->formatFlags;
	Assert(tuple->fixedData);
	memcpy(tuple->fixedData, view->data, len);
	if (len != MAXALIGN(len))
		memset(&tuple->fixedData[len], 0, MAXALIGN(len) - len);
	tuple->tuple.data = tuple->fixedData;
}

/*
 * build_fixed_tuples()
 *
 * Helper for consumers that need stable tuple bytes for modify records.
 *
 * Modify records (INSERT/UPDATE/DELETE/REINSERT) expose tuple payload as
 * pointers into the WAL container buffer. This is intentional: the parser
 * does not allocate or copy data.
 *
 * This function exists to make the ownership boundary explicit:
 * the WAL parser never allocates.
 *
 * build_fixed_tuples() copies the tuple bytes into OFixedTuple buffers
 * provided by the caller (including MAXALIGN padding), producing OTuple
 * instances safe to use after the current callback returns.
 *
 * The function expects tuple->fixedData to be allocated by the caller and
 * sized to hold MAXALIGN(len).
 */
void
build_fixed_tuples(const WalRecord *rec, OFixedTuple *tuple1, OFixedTuple *tuple2)
{
	Assert(rec);
	Assert(tuple1);
	Assert(tuple2);
	Assert(rec->type == WAL_REC_INSERT || rec->type == WAL_REC_UPDATE || rec->type == WAL_REC_DELETE || rec->type == WAL_REC_REINSERT);

	if (!rec->u.modify.read_two_tuples)
	{
		build_fixed_tuple_from_tuple_view(&rec->u.modify.t1, rec->u.modify.len1, tuple1);
		O_TUPLE_SET_NULL(tuple2->tuple);
	}
	else
	{
		build_fixed_tuple_from_tuple_view(&rec->u.modify.t1, rec->u.modify.len1, tuple1);
		build_fixed_tuple_from_tuple_view(&rec->u.modify.t2, rec->u.modify.len2, tuple2);
	}
}

/*
 * On success, r->ptr is positioned at the first byte after the
 * container header.
 *
 * @TODO rename after migration!
 */
static WalParseResult
wr_wal_container_read_header(WalReaderState *r, bool allow_logging)
{
	uint16		wal_version = 0;
	uint8		wal_flags = 0;
	uint8		firstByte = 0;

	firstByte = *(uint8 *) r->ptr;

	if (firstByte >= FIRST_ORIOLEDB_WAL_VERSION)
	{
		/*
		 * Container starts with a valid WAL version. First WAL record is just
		 * after it.
		 */
		WR_PARSE(r, &wal_version);
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
		WR_PARSE(r, &wal_flags);
	}

	r->wal_version = wal_version;
	r->wal_flags = wal_flags;

	return WALPARSE_OK;
}

/*
 * wal_container_parse_flag()
 *
 * Consume a single container flag payload if the flag is present.
 *
 * Container flags are advertised as a bitmask (r->wal_flags) but some flags
 * carry additional bytes in the stream. Such payload is part of the container
 * header prefix and MUST be consumed before reading the first record tag.
 *
 * Semantics:
 *
 *   - If the flag bit is not set, the function is a no-op and does not advance
 *     r->ptr.
 *
 *   - If the flag bit is set, the flag descriptor is mandatory. We must know
 *     the exact payload format and length in order to advance r->ptr safely.
 *     A missing descriptor or parser is treated as WALPARSE_BAD_TYPE: without
 *     it, we cannot maintain record boundary alignment.
 *
 *   - If the flag has a payload parser, it must consume exactly this flag's
 *     payload bytes and leave r->ptr positioned at the next element
 *     (either another flag payload or the first record tag).
 *
 *   - After successful parsing, the flag is optionally delivered to the
 *     consumer via r->on_flag(). The callback may capture header-wide context
 *     (e.g. PG xid/origin) needed for interpreting subsequent records, but must
 *     not attempt to move r->ptr.
 *
 * Note: flags with payload are protocol-critical. Forgetting to consume a flag
 * payload would desynchronize the stream and cause the next record tag to be
 * misread.
 */
static inline WalParseResult
wal_container_parse_flag(WalReaderState *r, wal_type_t type)
{
	WalParseResult st = WALPARSE_OK;

	Assert(r);

	if (r->wal_flags & type)
	{
		/*
		 * Descriptor is required to know payload length and keep cursor
		 * aligned.
		 */
		const WalRecordDesc *d = wal_flag_get_desc(type);

		Assert(d && d->parse);
		if (d && d->parse)
		{
			WalRecord	rec;

			memset(&rec, 0, sizeof(rec));

			rec.type = type;
			st = d->parse(r, &rec);
			if (st == WALPARSE_OK && r->on_flag)
				st = r->on_flag(r->ctx, &rec);
		}
		else
			st = WALPARSE_BAD_TYPE;
	}

	return st;
}

/*
 * wal_container_parse_flags()
 *
 * Consume all known container flags (header prefix).
 *
 * Flags are parsed in a deterministic order defined by ORIOLE_WAL_FLAGS.
 *
 * Although flags are represented on-wire as a bitmask, their payloads (if any)
 * are serialized as a byte stream in the container header area. Therefore,
 * the parser must consume payloads in a deterministic, protocol-defined order.
 *
 * We define that order by expanding ORIOLE_WAL_FLAGS(X) and calling
 * wal_container_parse_flag() once per declared flag. This provides a single
 * source of truth: introducing a new container flag requires updating only
 * ORIOLE_WAL_FLAGS, and the parser automatically starts consuming its payload.
 *
 * This is intentionally not a runtime loop: the expansion is compile-time and
 * avoids maintaining a separate hand-written list of flags in the parser,
 * which is otherwise easy to forget and would silently break protocol framing.
 *
 * On success, r->ptr is positioned at the first record tag byte.
 */
static inline WalParseResult
wal_container_parse_flags(WalReaderState *r)
{
	WalParseResult st = WALPARSE_OK;

	Assert(r);

#define X(sym, val, name, fn) \
do { \
	st = wal_container_parse_flag(r, sym); \
	if (st) \
		return st; \
} while(0);

	ORIOLE_WAL_FLAGS(X)

#undef X

		return st;
}

/*
 * parse_wal_container()
 *
 * Parse a single OrioleDB WAL container payload and deliver records to a
 * consumer.
 *
 * The function is responsible for:
 *   - reading the container header (version + flags),
 *   - invoking consumer's check_version() (if provided),
 *   - consuming and delivering all container flags (header prefix),
 *   - scanning the record stream and invoking consumer's on_record() per record.
 *
 * It is intentionally minimal:
 *   - no allocations,
 *   - no buffering,
 *   - no policy beyond protocol safety.
 *
 * Error handling:
 *   - returns WALPARSE_EOF if the input buffer ends mid-element,
 *   - returns WALPARSE_BAD_TYPE for unknown types / missing descriptors,
 *   - returns consumer-provided status for version policy decisions.
 *
 * The caller owns the input buffer; record-local pointers are valid only
 * while the input buffer remains valid.
 */
WalParseResult
parse_wal_container(WalReaderState *r, bool allow_logging)
{
	WalParseResult st;
	WalRecord	rec;

	Assert(r);
	Assert(r->on_record);		/* consumer must handle every record */

	memset(&rec, 0, sizeof(rec));

	rec.relreplident = REPLICA_IDENTITY_DEFAULT;
	rec.origin_id = InvalidRepOriginId;
	rec.origin_lsn = InvalidXLogRecPtr;

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
	if (r->check_version)
	{
		st = r->check_version(r);
		if (st)
			return st;
	}

	rec.wal_version = r->wal_version;

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
	st = wal_container_parse_flags(r);
	if (st)
		return st;

	/*
	 * Main record scan.
	 *
	 * Container format is: [tag byte][payload...][tag byte][payload...]...
	 *
	 * For each record:
	 *
	 * - read the tag (rec_type = rec.type),
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
		rec.delta = r->ptr - r->start;

		/*
		 * Read record tag byte. After this, r->ptr points to payload (if
		 * any).
		 */
		WR_PARSE(r, &rec_type);
		rec.type = rec_type;

		/*
		 * value_ptr points to the first byte after the tag, i.e. to the
		 * payload. For payload-less records, value_ptr points to the next
		 * record tag.
		 *
		 * Consumers must treat value_ptr as "record-local" pointer only; it
		 * is valid only while the reader buffer remains intact.
		 */
		rec.value_ptr = r->ptr;

		d = wal_get_desc(rec.type);
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
			 * inconsistent, or that the version gate in r->check_version is
			 * incomplete.
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
					 __func__, rec.type, wal_type_name(rec.type),
					 r->end - r->start,
					 r->end - r->ptr);
			}
			return WALPARSE_BAD_TYPE;
		}

		if (allow_logging)
		{
			elog(DEBUG4, "[%s] WAL RECORD TYPE %u(`%s`)", __func__, rec.type, wal_type_name(rec.type));
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
			st = d->parse(r, &rec);
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
		st = r->on_record(r->ctx, &rec);
		if (st)
			return st;
	}

	return WALPARSE_OK;
}
