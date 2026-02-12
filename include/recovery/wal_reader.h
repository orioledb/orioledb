/*-------------------------------------------------------------------------
 *
 * wal_reader.h
 * 		WAL parser declarations for OrioleDB.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal_reader.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WAL_READER_H__
#define __WAL_READER_H__

typedef unsigned int wal_type_t;

/*
 * WalRecord instances are transient and reused across iterations.
 *
 * Callers must not retain pointers to the record itself.
 * Any data that must outlive the callback must be copied.
 */
typedef struct WalRecord
{
	wal_type_t	type;

	uint32		delta;
	Pointer		value_ptr;
	uint16		wal_version;

	ORelOids	oids;
	OXid		oxid;
	TransactionId logicalXid;
	TransactionId heapXid;
	char		relreplident;

	RepOriginId origin_id;
	XLogRecPtr	origin_lsn;

	union
	{
		struct
		{
			OXid		xmin;
			CommitSeqNo csn;
		}			finish;
		struct
		{
			TransactionId topXid;
			TransactionId subXid;
		}			swxid;
		struct
		{
			TransactionId xid;
			OXid		xmin;
			CommitSeqNo csn;
		}			joint_commit;
		struct
		{
			uint8		treeType;
			OSnapshot	snapshot;
			uint32		version;
			uint32		base_version;
		}			relation;
		struct
		{
			Oid			relreplident_ix_oid;
		}			relreplident;
		struct
		{
			ORelOids	oids;
			Oid			oldRelnode;
		}			unlock;
		struct
		{
			ORelOids	oids;
		}			truncate;
		struct
		{
			SubTransactionId parentSubid;
			TransactionId parentLogicalXid;
		}			savepoint;
		struct
		{
			SubTransactionId parentSubid;
			OXid		xmin;
			CommitSeqNo csn;
		}			rb_to_sp;
		struct
		{
			ItemPointerData iptr;
		}			bridge_erase;

		struct
		{
			OTuple		t1;
			OffsetNumber len1;

			OTuple		t2;
			OffsetNumber len2;

			bool		read_two_tuples;
		}			modify;

		/* Flags */
		struct
		{
			TimestampTz xactTime;
			TransactionId xid;
		}			xact_info;

	}			u;

} WalRecord;

/*
 * WalParseResult
 *
 * Status codes returned by the WAL container parser and callbacks.
 *
 * WALPARSE_OK
 *     Success.
 *
 * WALPARSE_EOF
 *     Not enough bytes in the input buffer to parse the requested element.
 *     This is a "need more data" / framing error depending on the caller.
 *
 * WALPARSE_BAD_TYPE
 *     Unknown record/flag type, missing descriptor, or otherwise
 *     unparseable tag. Treated as a hard protocol error.
 *
 * WALPARSE_BAD_VERSION
 *     Container version policy rejected by the consumer (typically WAL from
 *     a newer or unsupported OrioleDB version).
 *
 * WALPARSE_INTERNAL
 *     Internal invariant violation or unexpected condition.
 */
typedef enum WalParseResult
{
	WALPARSE_OK = 0,
	WALPARSE_EOF,				/* not enough bytes */
	WALPARSE_BAD_TYPE,
	WALPARSE_BAD_VERSION,
	WALPARSE_INTERNAL

} WalParseResult;

struct WalReaderState;

typedef WalParseResult (*WalCheckVersionFn) (const struct WalReaderState *r);
typedef WalParseResult (*WalOnFlagFn) (void *ctx, const WalRecord *rec);
typedef WalParseResult (*WalOnRecordFn) (void *ctx, WalRecord *rec);

/*
 * Cursor advancement invariant:
 *
 * r->ptr must only be advanced by:
 *
 *   - WR_PARSE / WR_SKIP,
 *   - record parse routines,
 *   - container flag parsers.
 *
 * Consumers must never modify ptr.
 */
typedef struct WalReaderState
{
	Pointer		start;
	Pointer		end;
	Pointer		ptr;
	uint16		wal_version;
	uint8		wal_flags;

	/* Consumer */
	void	   *ctx;
	WalCheckVersionFn check_version;
	WalOnFlagFn on_flag;
	WalOnRecordFn on_record;

} WalReaderState;

/*
 * WalParseFn
 *
 * Parser routine for a single record type.
 *
 * The parser must:
 *   - read exactly this record's payload from r->ptr,
 *   - populate rec->u.* fields as needed,
 *   - leave r->ptr positioned at the next record tag.
 *
 * It must not read beyond r->end; use WR_REQUIRE_SIZE / WR_PARSE / WR_SKIP.
 *
 * For payload-less records, descriptor->parse is NULL (record is tag-only).
 */
typedef WalParseResult (*WalParseFn) (WalReaderState *r, WalRecord *rec);

/*
 * WalRecordDesc
 *
 * Static descriptor for a record (or flag) type.
 *
 * The descriptor table is the single source of truth for:
 *   - mapping type -> name (debug),
 *   - mapping type -> payload parser.
 */
typedef struct WalRecordDesc
{
	wal_type_t	type;
	const char *name;
	WalParseFn	parse;

} WalRecordDesc;

/*
 * Reader helpers.
 *
 * WR_REQUIRE_SIZE()
 *     Ensures that at least nbytes remain in the input buffer.
 *
 * WR_PARSE()
 *     Copies sizeof(*out) bytes from r->ptr into *out and advances r->ptr.
 *
 * WR_SKIP()
 *     Advances r->ptr by sz bytes after bounds check.
 *
 * These macros are the preferred way to move the cursor. Direct arithmetic
 * on r->ptr should be avoided outside of low-level parsing code.
 */

#define WR_REQUIRE_SIZE(r, nbytes) \
do { \
	if (((size_t) ((r)->end - (r)->ptr)) < (size_t)(nbytes)) \
		return WALPARSE_EOF; \
} while (0)

#define WR_PARSE(r, out) \
{ \
	WR_REQUIRE_SIZE(r, sizeof(*out)); \
	memcpy(out, r->ptr, sizeof(*out)); \
    r->ptr += sizeof(*out); \
}

#define WR_SKIP(r, sz) \
{ \
	WR_REQUIRE_SIZE(r, sz); \
    r->ptr += sz; \
}

extern void build_fixed_tuples(const WalRecord *rec, OFixedTuple *tuple1, OFixedTuple *tuple2);

extern const WalRecordDesc *wal_get_desc(wal_type_t type);
extern const WalRecordDesc *wal_flag_get_desc(wal_type_t type);

extern const char *wal_type_name(wal_type_t type);
extern const char *wal_flag_type_name(wal_type_t type);

extern WalParseResult parse_wal_container(WalReaderState *r, bool allow_logging);

#endif							/* __WAL_READER_H__ */
