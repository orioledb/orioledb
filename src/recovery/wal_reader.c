/*-------------------------------------------------------------------------
 *
 * wal_reader.c
 *	  Routines to parse generic OrioleDB WAL containers.
 *
 * The parser is responsible for:
 * - container framing (header, flags prefix, record tag/payload scanning),
 * - descriptor-driven payload decoding,
 * - safe cursor advancement with bounds checking.
 *
 * Parsing is strictly single-pass and forward-only.
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

#include "btree/page_contents.h"
#include "catalog/sys_trees.h"
#include "recovery/wal_reader.h"
#include "recovery/wal.h"

/* Parsers */
static WalParseResult wal_parse_empty(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_xid(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_finish(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_relation(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_o_tables_meta_unlock(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_savepoint(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_rollback_to_savepoint(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_joint_commit(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_truncate(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_bridge_erase(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_switch_logical_xid(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_relreplident(WalReaderState *r, WalRecord *rec);
static WalParseResult wal_parse_rec_modify(WalReaderState *r, WalRecord *rec);

const char *
wal_type_name(WalRecordType type)
{
	switch (type)
	{
#define X(sym, val, name, fn) case sym: return name;
			ORIOLE_WAL_RECORDS(X)
#undef X
		default:
			return "UNKNOWN";
	}
}

/* Parse zero-length record */
static WalParseResult
wal_parse_empty(WalReaderState *r, WalRecord *rec)
{
	return WALPARSE_OK;
}

/* Parser for WAL_REC_XID */
static WalParseResult
wal_parse_rec_xid(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->oxid);
	WR_PARSE(r, &rec->logicalXid);
	if (r->container.version >= 17)
	{
		WR_PARSE(r, &rec->heapXid);
	}
	else
	{
		rec->heapXid = InvalidTransactionId;
	}
	return WALPARSE_OK;
}

/* Parser for WAL_REC_COMMIT and WAL_REC_ROLLBACK */
static WalParseResult
wal_parse_rec_finish(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.finish.xmin);
	WR_PARSE(r, &rec->u.finish.csn);

	return WALPARSE_OK;
}

/* Parser for WAL_REC_JOINT_COMMIT */
static WalParseResult
wal_parse_rec_joint_commit(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.joint_commit.xid);
	WR_PARSE(r, &rec->u.joint_commit.xmin);
	WR_PARSE(r, &rec->u.joint_commit.csn);

	return WALPARSE_OK;
}

/* Parser for WAL_REC_RELATION */
static WalParseResult
wal_parse_rec_relation(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.relation.treeType);
	WR_PARSE(r, &rec->oids.datoid);
	WR_PARSE(r, &rec->oids.reloid);
	WR_PARSE(r, &rec->oids.relnode);

	if (r->container.version >= 17)
	{
		OXid		xmin;

		WR_PARSE(r, &xmin);
		WR_PARSE(r, &rec->u.relation.snapshot.csn);
		WR_PARSE(r, &rec->u.relation.snapshot.cid);

		rec->u.relation.snapshot.xmin = xmin;

		WR_PARSE(r, &rec->u.relation.version);
		WR_PARSE(r, &rec->u.relation.base_version);
	}
	else
	{
		rec->u.relation.snapshot = o_non_deleted_snapshot;
		rec->u.relation.version = O_TABLE_INVALID_VERSION;
		rec->u.relation.base_version = O_TABLE_INVALID_VERSION;
	}

	return WALPARSE_OK;
}

/* Parser for WAL_REC_RELREPLIDENT */
static WalParseResult
wal_parse_rec_relreplident(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	/* Should be set only once and only from default */
	Assert(rec->relreplident == REPLICA_IDENTITY_DEFAULT);

	WR_PARSE(r, &rec->relreplident);

	/*
	 * relreplident_ix_oid is reserved in the WAL_REC_RELREPLIDENT for the
	 * future implementation of REPLICA IDENTITY USING INDEX and not used now
	 */
	WR_PARSE(r, &rec->u.relreplident.relreplident_ix_oid);
	Assert(rec->u.relreplident.relreplident_ix_oid == InvalidOid);

	return WALPARSE_OK;
}

/* Parser for WAL_REC_O_TABLES_META_UNLOCK */
static WalParseResult
wal_parse_rec_o_tables_meta_unlock(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.unlock.oids.datoid);
	WR_PARSE(r, &rec->u.unlock.oids.reloid);
	WR_PARSE(r, &rec->u.unlock.oldRelnode);
	WR_PARSE(r, &rec->u.unlock.oids.relnode);

	return WALPARSE_OK;
}

/* Parser for WAL_REC_SAVEPOINT */
static WalParseResult
wal_parse_rec_savepoint(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.savepoint.parentSubid);
	WR_PARSE(r, &rec->logicalXid);
	WR_PARSE(r, &rec->u.savepoint.parentLogicalXid);

	return WALPARSE_OK;
}

/* Parser for WAL_REC_ROLLBACK_TO_SAVEPOINT */
static WalParseResult
wal_parse_rec_rollback_to_savepoint(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.rb_to_sp.parentSubid);
	if (r->container.version >= 17)
	{
		WR_PARSE(r, &rec->u.rb_to_sp.xmin);
		WR_PARSE(r, &rec->u.rb_to_sp.csn);
	}
	else
	{
		rec->u.rb_to_sp.xmin = InvalidOXid;
		rec->u.rb_to_sp.csn = 0;
	}
	return WALPARSE_OK;
}

/* Parser for WAL_REC_TRUNCATE */
static WalParseResult
wal_parse_rec_truncate(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.truncate.oids.datoid);
	WR_PARSE(r, &rec->u.truncate.oids.reloid);
	WR_PARSE(r, &rec->u.truncate.oids.relnode);

	return WALPARSE_OK;
}

/* Parser for WAL_REC_BRIDGE_ERASE */
static WalParseResult
wal_parse_rec_bridge_erase(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.bridge_erase.iptr);

	return WALPARSE_OK;
}

/* Parser for WAL_REC_SWITCH_LOGICAL_XID */
static WalParseResult
wal_parse_rec_switch_logical_xid(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	WR_PARSE(r, &rec->u.swxid.topXid);
	WR_PARSE(r, &rec->u.swxid.subXid);

	return WALPARSE_OK;
}

/*
 * Read one or two tuples from modify WAL record.
 * Two tuples in certain cases: (1) WAL_REC_REINSERT, (2) WAL_REC_UPDATE with REPLICA_IDENTITY_FULL
 */
static WalParseResult
wal_parse_rec_modify(WalReaderState *r, WalRecord *rec)
{
	Assert(r);
	Assert(rec);

	rec->u.modify.read_two_tuples = (rec->type == WAL_REC_REINSERT || (rec->type == WAL_REC_UPDATE && rec->relreplident == REPLICA_IDENTITY_FULL));

	if (!rec->u.modify.read_two_tuples)
	{
		WR_PARSE(r, &rec->u.modify.t1.formatFlags);
		WR_PARSE(r, &rec->u.modify.len1);
		Assert(rec->u.modify.len1 > 0);

		rec->u.modify.t1.data = r->ptr;
		WR_SKIP(r, rec->u.modify.len1);

		O_TUPLE_SET_NULL(rec->u.modify.t2);
	}
	else
	{
		WR_PARSE(r, &rec->u.modify.t1.formatFlags);
		WR_PARSE(r, &rec->u.modify.t2.formatFlags);

		WR_PARSE(r, &rec->u.modify.len1);
		WR_PARSE(r, &rec->u.modify.len2);

		Assert(rec->u.modify.len1 > 0);
		Assert(rec->u.modify.len2 > 0);

		rec->u.modify.t1.data = r->ptr;
		WR_SKIP(r, rec->u.modify.len1);

		rec->u.modify.t2.data = r->ptr;
		WR_SKIP(r, rec->u.modify.len2);
	}

	return WALPARSE_OK;
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
 * wal_container_read_header()
 *
 * Reads and validates container-level framing:
 *   - extracts wal_version and wal_flags,
 *   - ensures the container format is structurally compatible with
 *     this build (i.e. we understand its framing rules).
 *
 * This function performs a format-level compatibility check only.
 *
 * It guarantees that:
 *   - we can safely interpret the container layout,
 *   - header fields are readable,
 *   - flag prefix handling is valid for this wal_version.
 *
 * It does NOT decide whether WAL from this version should be applied.
 * Higher-level version policy (e.g. cross-version replay rules) is
 * delegated to the consumer via r->check_version().
 *
 * On success, r->ptr is positioned at the first byte after the
 * container header.
 */
static WalParseResult
wal_container_read_header(WalReaderState *r, bool allow_logging)
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

	if (wal_version >= ORIOLEDB_CONTAINER_FLAGS_WAL_VERSION)
	{
		/*
		 * WAL container flags were added by
		 * ORIOLEDB_CONTAINER_FLAGS_WAL_VERSION.
		 */
		WR_PARSE(r, &wal_flags);
	}

	r->container.version = wal_version;
	r->container.flags = wal_flags;

	return WALPARSE_OK;
}

/*
 * wal_container_parse_flags()
 *
 * Consume all known container flags (header prefix).
 *
 * Although flags are represented on-wire as a bitmask, their payloads (if any)
 * are serialized as a byte stream in the container header area. Therefore,
 * the parser must consume payloads in a deterministic, protocol-defined order.
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
	if (r->container.flags & WAL_CONTAINER_HAS_XACT_INFO)
	{
		WR_PARSE(r, &r->container.xact_info.xactTime);
		WR_PARSE(r, &r->container.xact_info.xid);
	}

	if (r->container.flags & WAL_CONTAINER_HAS_ORIGIN_INFO)
	{
		WR_PARSE(r, &r->container.origin_info.id);
		WR_PARSE(r, &r->container.origin_info.lsn);
	}

	return WALPARSE_OK;
}

/*
 * wal_parse_container()
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
wal_parse_container(WalReaderState *r, bool allow_logging)
{
	WalParseResult st;
	WalRecord	rec;

	Assert(r);
	Assert(r->on_record);		/* consumer must handle every record */

	memset(&rec, 0, sizeof(rec));

	rec.relreplident = REPLICA_IDENTITY_DEFAULT;

	/*
	 * Read and validate container header framing and check structural
	 * compatibility via WAL version.
	 */
	st = wal_container_read_header(r, allow_logging);
	if (st != WALPARSE_OK)
		return st;

	/*
	 * Version policy check (consumer-level).
	 *
	 * wal_container_read_header() has already verified that the container
	 * framing is structurally compatible with this build.
	 *
	 * Here we delegate higher-level compatibility policy to the consumer.
	 * Even if the format is understood, the consumer may reject WAL based on
	 * semantic/version rules (e.g. replaying WAL from a different OrioleDB
	 * major version).
	 *
	 * wal_parse_container() itself remains version-agnostic and does not
	 * enforce cross-version replay policy.
	 */
	if (r->check_version)
	{
		st = r->check_version(r);
		if (st != WALPARSE_OK)
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
	 */
	st = wal_container_parse_flags(r);
	if (st != WALPARSE_OK)
		return st;

	if (r->on_container)
	{
		st = r->on_container(r);
		if (st != WALPARSE_OK)
			return st;
	}

	/*
	 * Main record scan.
	 *
	 * Container format is: [tag byte][payload...][tag byte][payload...]...
	 */
	while (r->ptr < r->end)
	{
		uint8		rec_type;

		/*
		 * Offset from container start at which this record tag was found.
		 */
		rec.offset = r->ptr - r->start;

		/*
		 * Read record tag byte. After this, r->ptr points to payload (if
		 * any).
		 */
		WR_PARSE(r, &rec_type);
		rec.type = rec_type;

		/*
		 * data points to the first byte after the tag, i.e. to the payload.
		 * For payload-less records, data points to the next record tag.
		 */
		rec.data = r->ptr;

		if (allow_logging)
			elog(DEBUG4, "[%s] WAL RECORD TYPE %u(`%s`)", __func__, rec.type, wal_type_name(rec.type));

		/*
		 * Parse record payload (if any).
		 *
		 * Some OrioleDB WAL records are intentionally zero-length markers:
		 * they consist only of the tag byte and carry no additional bytes in
		 * the container. Their descriptor must have d->parse ==
		 * wal_parse_empty.
		 */

		switch (rec.type)
		{
#define X(sym, val, name, fn) \
			case sym: \
				st = fn(r, &rec); \
				if (st != WALPARSE_OK) \
					return st; \
				break;
				ORIOLE_WAL_RECORDS(X)
#undef X
			default:
				/* Unknown record type. */
				if (allow_logging)
					elog(LOG, "[%s] UNKNOWN WAL RECORD TYPE %u: chunk/tail len %ld/%ld",
						 __func__, rec.type,
						 r->end - r->start, r->end - r->ptr);
				return WALPARSE_BAD_TYPE;
		}

		/*
		 * Deliver the (possibly parsed) event to the consumer.
		 *
		 * Consumers may update decoding/replay state, apply changes, or
		 * selectively ignore records. Any non-OK status is treated as fatal
		 * for this container iteration.
		 */
		st = r->on_record(r, &rec);
		if (st != WALPARSE_OK)
			return st;
	}

	return WALPARSE_OK;
}
