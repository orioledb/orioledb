#include "postgres.h"

#include "orioledb_types.h"

#include "recovery/wal.h"

#include "access/xlogrecord.h"

void
wal_iterate(Pointer startPtr, Pointer endPtr, XLogRecPtr xlogRecPtr,
			wal_iterate_callback_type record_callback, void *arg)
{
	Pointer		ptr = startPtr;

	while (ptr < endPtr)
	{
		uint8		rec_type;
		XLogRecPtr	xlogPtr = InvalidXLogRecPtr;

		if (!XLogRecPtrIsInvalid(xlogRecPtr))
			xlogPtr = xlogRecPtr + (ptr - startPtr);
		rec_type = *ptr;
		ptr++;

		record_callback(rec_type, ptr, xlogPtr, arg);

		if (rec_type == WAL_REC_XID)
		{
			ptr += sizeof(OXid) + sizeof(XLogRecPtr);
		}
		else if (rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK)
		{
			ptr += sizeof(OXid);
		}
		else if (rec_type == WAL_REC_JOINT_COMMIT)
		{
			ptr += sizeof(TransactionId) + sizeof(OXid);
		}
		else if (rec_type == WAL_REC_RELATION)
		{
			ptr += 1 + 3 * sizeof(Oid);
		}
		else if (rec_type == WAL_REC_O_TABLES_META_LOCK)
		{
		}
		else if (rec_type == WAL_REC_O_TABLES_META_UNLOCK)
		{
			ptr += 4 * sizeof(Oid);
		}
		else if (rec_type == WAL_REC_TRUNCATE)
		{
			ptr += 3 * sizeof(Oid);
		}
		else if (rec_type == WAL_REC_SAVEPOINT)
		{
			ptr += sizeof(SubTransactionId);
		}
		else if (rec_type == WAL_REC_ROLLBACK_TO_SAVEPOINT)
		{
			ptr += sizeof(SubTransactionId);
		}
		else if (rec_type == WAL_REC_BRIDGE_ERASE)
		{
			ptr += sizeof(ItemPointerData);
		}
		else
		{
			OffsetNumber length;

			ptr++;
			memcpy(&length, ptr, sizeof(OffsetNumber));
			ptr += sizeof(OffsetNumber) + length;
		}
	}
}
