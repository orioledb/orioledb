/*-------------------------------------------------------------------------
 *
 * wal_event.h
 * 		WAL parser declarations for OrioleDB.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal_event.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WAL_EVENT_H__
#define __WAL_EVENT_H__

typedef unsigned int wal_type_t;

/*
 * WalTupleView
 *
 * A lightweight, non-owning view over tuple data stored inside the WAL buffer.
 *
 * This structure does NOT own the memory it points to. Instead, "data" points
 * directly into the WalReaderState buffer and remains valid only while that buffer
 * is alive and unchanged.
 *
 * The parser intentionally avoids copying tuple payload in order to keep WAL
 * iteration zero-copy and cache-friendly. Consumers that need to keep the tuple
 * beyond the current parsing step MUST copy the data.
 *
 * In other words:
 *
 *   WalTupleView = borrowed pointer + length + format metadata
 *
 * Lifetime rules:
 *
 *   - Valid only during the current parse_wal_container() cycle.
 *   - Invalid once the reader advances to another WAL chunk or the buffer
 *     is reused.
 *
 * The name "View" is intentional: this is not a materialized tuple but merely
 * a view into the WAL stream.
 */
typedef struct WalTupleView
{
	Pointer		data;
	OffsetNumber len;
	uint8		formatFlags;

} WalTupleView;

#define WAL_TUPLE_VIEW_SET_NULL(tv) \
do { \
    tv.data = NULL; \
    tv.len = 0; \
    tv.formatFlags = 0; \
} while(0);

typedef struct WalEvent
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
			WalTupleView t1;
			WalTupleView t2;
			bool		read_two_tuples;
		}			modify;

		/* Flags */
		struct
		{
			TimestampTz xactTime;
			TransactionId xid;
		}			xact_info;

	}			u;

} WalEvent;

extern void build_fixed_tuples(const WalEvent *ev, OFixedTuple *tuple1, OFixedTuple *tuple2);

#endif							/* __WAL_EVENT_H__ */
