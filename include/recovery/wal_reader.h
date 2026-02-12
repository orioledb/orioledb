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

typedef struct WalRecord
{
	wal_type_t	type;

	uint32		delta;
	Pointer		value_ptr;

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

typedef struct WalReaderState
{
	Pointer		start;
	Pointer		end;

	Pointer		ptr;

	uint16		wal_version;
	uint8		wal_flags;

} WalReaderState;

typedef enum WalParseResult
{
	WALPARSE_OK = 0,
	WALPARSE_EOF,				/* not enough bytes */
	WALPARSE_BAD_TYPE,
	WALPARSE_BAD_VERSION,
	WALPARSE_INTERNAL

} WalParseResult;

typedef WalParseResult (*WalParseFn) (WalReaderState *r, WalRecord *rec);

typedef struct WalRecordDesc
{
	wal_type_t	type;
	const char *name;
	WalParseFn	parse;

} WalRecordDesc;

typedef WalParseResult (*WalCheckVersionFn) (const WalReaderState *r);
typedef WalParseResult (*WalOnFlagFn) (void *ctx, const WalRecord *rec);
typedef WalParseResult (*WalOnEventFn) (void *ctx, WalRecord *rec);

typedef struct WalConsumer
{
	void	   *ctx;
	WalCheckVersionFn check_version;
	WalOnFlagFn on_flag;
	WalOnEventFn on_event;

} WalConsumer;

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
extern const char *wal_type_name(wal_type_t type);

extern const WalRecordDesc *wal_flag_get_desc(wal_type_t type);
extern const char *wal_flag_type_name(wal_type_t type);

extern WalParseResult parse_wal_container(WalReaderState *r, WalConsumer *consumer, bool allow_logging);

#endif							/* __WAL_READER_H__ */
