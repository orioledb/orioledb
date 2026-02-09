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

typedef struct WalTupleView
{
	Pointer		data;
	OffsetNumber len;
	uint8		formatFlags;

} WalTupleView;

extern void build_fixed_tuple_from_tuple_view(const WalTupleView *view, OFixedTuple *tuple);

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

		struct
		{
			TimestampTz xactTime;
			TransactionId xid;
		}			xact_info;

	}			u;

} WalEvent;

#endif							/* __WAL_EVENT_H__ */
