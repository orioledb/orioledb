/*-------------------------------------------------------------------------
 *
 * o_indices.c
 * 		Routines for orioledb indices system tree.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_indices.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "catalog/indices.h"
#include "catalog/o_indices.h"
#include "catalog/o_sys_cache.h"
#include "catalog/o_tables.h"
#include "checkpoint/checkpoint.h"
#include "commands/defrem.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "tuple/slot.h"
#include "tuple/toast.h"

#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_opclass_d.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_FUNCTION_INFO_V1(orioledb_index_oids);
PG_FUNCTION_INFO_V1(orioledb_index_description);
PG_FUNCTION_INFO_V1(orioledb_index_rows);

static OIndex *make_ctid_o_index(OTable *table);

static BTreeDescr *
oIndicesGetBTreeDesc(void *arg)
{
	BTreeDescr *desc = (BTreeDescr *) arg;

	return desc;
}

static uint32
oIndicesGetKeySize(void *arg)
{
	return sizeof(OIndexChunkKey);
}

static uint32
oIndicesGetMaxChunkSize(void *key, void *arg)
{
	uint32		max_chunk_size;

	max_chunk_size = MAXALIGN_DOWN((O_BTREE_MAX_TUPLE_SIZE * 3 - MAXALIGN(sizeof(OIndexChunkKey))) / 3) - offsetof(OIndexChunk, data);

	return max_chunk_size;
}

static void
oIndicesUpdateKey(void *key, uint32 chunknum, void *arg)
{
	OIndexChunkKey *ckey = (OIndexChunkKey *) key;

	ckey->chunknum = chunknum;
}

static void *
oIndicesGetNextKey(void *key, void *arg)
{
	OIndexChunkKey *ckey = (OIndexChunkKey *) key;
	static OIndexChunkKey nextKey;

	elog(LOG, "[%s] GET NEXT FOR ckey [ %u %u %u ]; type %u; chunknum %u; version %u", __func__,
		 ckey->oids.datoid, ckey->oids.reloid, ckey->oids.relnode,
		 ckey->type,
		 ckey->chunknum,
		 ckey->version);

	nextKey = *ckey;
	nextKey.oids.relnode++;
	nextKey.chunknum = 0;

	elog(LOG, "[%s] NEXT IS [ %u %u %u ]; type %u; chunknum %u; version %u", __func__,
		 nextKey.oids.datoid, nextKey.oids.reloid, nextKey.oids.relnode,
		 nextKey.type,
		 nextKey.chunknum,
		 nextKey.version);

	return &nextKey;
}

static OTuple
oIndicesCreateTuple(void *key, Pointer data, uint32 offset, uint32 chunknum,
					int length, void *arg)
{
	OIndexChunkKey *ckey = (OIndexChunkKey *) key;
	OIndexChunk *chunk;
	OTuple		result;

	ckey->chunknum = chunknum;

	chunk = (OIndexChunk *) palloc(offsetof(OIndexChunk, data) + length);
	chunk->key = *ckey;
	chunk->dataLength = length;
	memcpy(chunk->data, data + offset, length);

	result.data = (Pointer) chunk;
	result.formatFlags = 0;
	return result;
}

static OTuple
oIndicesCreateKey(void *key, uint32 chunknum, void *arg)
{
	OIndexChunkKey *ckey = (OIndexChunkKey *) key;
	OIndexChunkKey *ckeyCopy;
	OTuple		result;

	ckeyCopy = (OIndexChunkKey *) palloc(sizeof(OIndexChunkKey));
	*ckeyCopy = *ckey;

	result.data = (Pointer) ckeyCopy;
	result.formatFlags = 0;

	return result;
}

static Pointer
oIndicesGetTupleData(OTuple tuple, void *arg)
{
	OIndexChunk *chunk = (OIndexChunk *) tuple.data;

	return chunk->data;
}

static uint32
oIndicesGetTupleChunknum(OTuple tuple, void *arg)
{
	OIndexChunk *chunk = (OIndexChunk *) tuple.data;

	return chunk->key.chunknum;
}

static uint32
oIndicesGetTupleDataSize(OTuple tuple, void *arg)
{
	OIndexChunk *chunk = (OIndexChunk *) tuple.data;

	return chunk->dataLength;
}

static TupleFetchCallbackResult
oIndicesFetchCallback(OTuple tuple, OXid tupOxid, OSnapshot *oSnapshot,
					  void *arg, TupleFetchCallbackCheckType check_type)
{
	OIndexChunkKey *tupleKey = (OIndexChunkKey *) tuple.data;
	OIndexChunkKey *boundKey = (OIndexChunkKey *) arg;

	/* Ignore reloid because it may changes */
	if (tupleKey->oids.datoid == boundKey->oids.datoid &&
		tupleKey->oids.relnode == boundKey->oids.relnode &&
		tupleKey->type && boundKey->type)
	{
		if (boundKey->version == O_TABLE_INVALID_VERSION)
			boundKey->version = tupleKey->version;

		if (tupleKey->version > boundKey->version)
			return OTupleFetchNext;
		else if (tupleKey->version == boundKey->version)
			return OTupleFetchMatch;
		else
			return OTupleFetchNotMatch;
	}
	return OTupleFetchNext;
}

ToastAPI	oIndicesToastAPI = {
	.getBTreeDesc = oIndicesGetBTreeDesc,
	.getBTreeVersion = NULL,
	.getBaseBTreeVersion = NULL,
	.getKeySize = oIndicesGetKeySize,
	.getMaxChunkSize = oIndicesGetMaxChunkSize,
	.updateKey = oIndicesUpdateKey,
	.getNextKey = oIndicesGetNextKey,
	.createTuple = oIndicesCreateTuple,
	.createKey = oIndicesCreateKey,
	.getTupleData = oIndicesGetTupleData,
	.getTupleChunknum = oIndicesGetTupleChunknum,
	.getTupleDataSize = oIndicesGetTupleDataSize,
	.deleteLogFullTuple = true,
	.fetchCallback = oIndicesFetchCallback
};

static void
make_builtin_field(OTableField *leafField, OTableIndexField *internalField,
				   Oid type, const char *name, int attnum, Oid opclass)
{
	if (leafField)
	{
		*leafField = *o_tables_get_builtin_field(type);
		namestrcpy(&leafField->name, name);
	}
	if (internalField)
	{
		internalField->attnum = attnum;
		internalField->collation = InvalidOid;
		internalField->opclass = opclass;
		internalField->ordering = SORTBY_ASC;
	}
}

static OIndex *
make_ctid_o_index(OTable *table)
{
	OIndex	   *result = (OIndex *) palloc0(sizeof(OIndex));
	int			i;
	int			nadded = 0;

	Assert(!table->has_primary);
	result->indexOids = table->oids;
	result->indexType = oIndexPrimary;
	namestrcpy(&result->name, "ctid_primary");
	result->tableOids = table->oids;
	result->table_persistence = table->persistence;
	result->primaryIsCtid = true;
	result->compress = table->primary_compress;
	result->fillfactor = table->fillfactor;
	result->tablespace = table->tablespace;
	result->nLeafFields = table->nfields + 1;
	if (table->index_bridging)
		result->nLeafFields++;
	result->nNonLeafFields = 1;
	result->nPrimaryFields = 0;
	result->nKeyFields = 1;
	result->nUniqueFields = 1;
	result->indexVersion = ++table->primary_ixversion;

	elog(LOG, "[%s] oids [ %u %u %u ] version %u", __func__,
		 table->oids.datoid, table->oids.reloid, table->oids.relnode,
		 table->primary_ixversion);

	result->immediate = true;
	result->leafTableFields = (OTableField *) palloc0(sizeof(OTableField) * result->nLeafFields);
	result->leafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) * result->nLeafFields);

	make_builtin_field(&result->leafTableFields[nadded++], &result->leafFields[0],
					   TIDOID, "ctid", SelfItemPointerAttributeNumber,
					   table->tid_btree_ops_oid);

	result->bridging = table->index_bridging;
	if (table->index_bridging)
		make_builtin_field(&result->leafTableFields[nadded++], NULL,
						   TIDOID, "index_bridging_ctid", FirstLowInvalidHeapAttributeNumber,
						   table->tid_btree_ops_oid);

	for (i = 0; i < table->nfields; i++)
		result->leafTableFields[nadded++] = table->fields[i];

	return result;
}

static int
find_existing_field(OIndex *index, int maxIndex, OTableIndexField *field)
{
	int			i;

	for (i = 0; i < maxIndex; i++)
	{
		if (field->attnum != EXPR_ATTNUM &&
			field->attnum == index->leafFields[i].attnum &&
			field->opclass == index->leafFields[i].opclass)
		{
			return i;
		}
	}
	return -1;
}

static OIndex *
make_primary_o_index(OTable *table)
{
	OTableIndex *tableIndex;
	OIndex	   *result = (OIndex *) palloc0(sizeof(OIndex));
	int			i;
	int			saved_nLeafFields;
	int			nadded = 0;
	MemoryContext mcxt;
	MemoryContext old_mcxt;

	Assert(table->has_primary && table->nindices >= 1);
	tableIndex = &table->indices[0];
	result->indexOids = tableIndex->oids;
	result->indexType = oIndexPrimary;
	namestrcpy(&result->name, tableIndex->name.data);
	Assert(tableIndex->type == oIndexPrimary);
	result->tableOids = table->oids;
	result->table_persistence = table->persistence;
	result->primaryIsCtid = false;
	if (OCompressIsValid(tableIndex->compress))
		result->compress = tableIndex->compress;
	else
		result->compress = table->primary_compress;
	result->fillfactor = table->fillfactor;
	result->tablespace = tableIndex->tablespace;
	saved_nLeafFields = table->nfields;
	result->nLeafFields = table->nfields;
	if (table->index_bridging)
		result->nLeafFields++;
	result->nNonLeafFields = tableIndex->nfields;
	result->nIncludedFields = tableIndex->nfields - tableIndex->nkeyfields;
	result->nPrimaryFields = 0;
	result->nKeyFields = tableIndex->nkeyfields;
	result->indexVersion = ++table->primary_ixversion;
	tableIndex->version = result->indexVersion;

	elog(LOG, "[%s] oids [ %u %u %u ] version %u", __func__,
		 table->oids.datoid, table->oids.reloid, table->oids.relnode,
		 table->primary_ixversion);

	result->immediate = tableIndex->immediate;
	result->leafTableFields = (OTableField *) palloc0(sizeof(OTableField) * result->nLeafFields);
	result->leafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) * result->nLeafFields);

	result->bridging = table->index_bridging;
	if (table->index_bridging)
		make_builtin_field(&result->leafTableFields[nadded++], NULL,
						   TIDOID, "index_bridging_ctid", FirstLowInvalidHeapAttributeNumber,
						   table->tid_btree_ops_oid);

	/*
	 * TODO: We should probably use add_index_fields to not duplicate code,
	 * but now it should be rewritten
	 */
	for (i = 0; i < saved_nLeafFields; i++)
		result->leafTableFields[nadded++] = table->fields[i];

	/* Switching context to store duplicates */
	mcxt = OGetIndexContext(result);
	old_mcxt = MemoryContextSwitchTo(mcxt);

	nadded = 0;
	for (i = 0; i < result->nNonLeafFields; i++)
	{
		int			found_attnum;

		found_attnum = find_existing_field(result, nadded, &tableIndex->fields[i]);
		if (found_attnum >= 0)
		{
			List	   *duplicate;

			if (i < result->nKeyFields)
				result->nKeyFields--;
			else
				result->nIncludedFields--;

			/* (fieldnum, original fieldnum) */
			/* cppcheck-suppress unknownEvaluationOrder */
			duplicate = list_make2_int(nadded, found_attnum);
			result->duplicates = lappend(result->duplicates, duplicate);
			continue;
		}

		result->leafFields[nadded++] = tableIndex->fields[i];
	}
	MemoryContextSwitchTo(old_mcxt);
	Assert(nadded <= result->nNonLeafFields);
	result->nUniqueFields = result->nKeyFields;
	result->nNonLeafFields = nadded;

	return result;
}

static void
add_index_fields(OIndex *index, OTable *table, OTableIndex *tableIndex, int *nadded, bool fillPrimary)
{
	int			i;
	int			expr_field = 0;
	int			init_nKeyFields = index->nKeyFields;

	if (tableIndex)
	{
		int			nFields = fillPrimary ? tableIndex->nkeyfields : tableIndex->nfields;

		for (i = 0; i < nFields; i++)
		{
			int			attnum = tableIndex->fields[i].attnum;
			int			found_attnum;

			found_attnum = find_existing_field(index, *nadded, &tableIndex->fields[i]);
			if (found_attnum >= 0)
			{
				if (fillPrimary)
					index->primaryFieldsAttnums[index->nPrimaryFields++] = found_attnum + 1;
				else
				{
					List	   *duplicate;

					if (i < init_nKeyFields)
						index->nKeyFields--;
					else
						index->nIncludedFields--;

					Assert(CurrentMemoryContext == index->index_mctx);
					/* (fieldnum, original fieldnum) */
					/* cppcheck-suppress unknownEvaluationOrder */
					duplicate = list_make2_int(*nadded, found_attnum);
					index->duplicates = lappend(index->duplicates, duplicate);
				}

				continue;
			}

			if (attnum != EXPR_ATTNUM)
				index->leafTableFields[*nadded] = table->fields[attnum];
			else
				index->leafTableFields[*nadded] = tableIndex->exprfields[expr_field++];
			Assert(index->leafTableFields[*nadded].typid);
			index->leafFields[*nadded] = tableIndex->fields[i];
			if (fillPrimary)
				index->primaryFieldsAttnums[index->nPrimaryFields++] = *nadded + 1;
			(*nadded)++;
		}
	}
	else
	{
		make_builtin_field(&index->leafTableFields[*nadded], &index->leafFields[*nadded],
						   TIDOID, "ctid", SelfItemPointerAttributeNumber,
						   table->tid_btree_ops_oid);
		if (fillPrimary)
			index->primaryFieldsAttnums[index->nPrimaryFields++] = *nadded + 1;
		(*nadded)++;
	}
}

static OIndex *
make_secondary_o_index(OTable *table, OTableIndex *tableIndex)
{
	OTableIndex *primary = NULL;
	OIndex	   *result = (OIndex *) palloc0(sizeof(OIndex));
	int			nadded;
	MemoryContext mcxt;
	MemoryContext old_mcxt;

	if (table->has_primary)
	{
		primary = &table->indices[0];
		Assert(primary->type == oIndexPrimary);
	}

	result->indexOids = tableIndex->oids;
	result->indexType = tableIndex->type;
	result->indexVersion = tableIndex->version;
	elog(LOG, "[%s] indexVersion %u", __func__, result->indexVersion);
	namestrcpy(&result->name, tableIndex->name.data);
	result->tableOids = table->oids;
	result->table_persistence = table->persistence;
	result->primaryIsCtid = !table->has_primary;
	result->bridging = table->index_bridging;
	result->compress = tableIndex->compress;
	result->fillfactor = tableIndex->fillfactor;
	result->tablespace = tableIndex->tablespace;
	result->nulls_not_distinct = tableIndex->nulls_not_distinct;
	result->nIncludedFields = tableIndex->nfields - tableIndex->nkeyfields;
	result->nLeafFields = tableIndex->nfields;
	if (table->has_primary)
		result->nLeafFields += primary->nfields;
	else
		result->nLeafFields++;
	result->nNonLeafFields = result->nLeafFields;
	result->leafTableFields = (OTableField *) palloc0(sizeof(OTableField) * result->nLeafFields);
	result->leafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) * result->nLeafFields);
	result->nKeyFields = tableIndex->nkeyfields;
	result->immediate = tableIndex->immediate;

	nadded = 0;
	/* Switching context to store duplicates */
	mcxt = OGetIndexContext(result);
	old_mcxt = MemoryContextSwitchTo(mcxt);
	result->predicate = list_copy_deep(tableIndex->predicate);
	if (result->predicate)
		result->predicate_str = pstrdup(tableIndex->predicate_str);
	result->expressions = list_copy_deep(tableIndex->expressions);
	if (tableIndex->type == oIndexExclusion)
	{
		result->exclops = palloc0(tableIndex->nkeyfields * sizeof(Oid));
		memcpy(result->exclops, tableIndex->exclops, tableIndex->nkeyfields * sizeof(Oid));
	}
	add_index_fields(result, table, tableIndex, &nadded, false);
	if (tableIndex->nfields == tableIndex->nkeyfields)
		result->nKeyFields = nadded;
	Assert(nadded <= tableIndex->nfields);
	add_index_fields(result, table, primary, &nadded, true);
	Assert(nadded <= result->nLeafFields);
	MemoryContextSwitchTo(old_mcxt);
	result->nLeafFields = nadded;
	result->nNonLeafFields = nadded;

	if (tableIndex->type == oIndexUnique)
		result->nUniqueFields = result->nKeyFields;
	else
		result->nUniqueFields = result->nNonLeafFields;

	return result;
}

static OIndex *
make_toast_o_index(OTable *table)
{
	OTableIndex *primary = NULL;
	OIndex	   *result = (OIndex *) palloc0(sizeof(OIndex));
	int			nadded;

	if (table->has_primary)
	{
		primary = &table->indices[0];
		Assert(primary->type == oIndexPrimary);
	}

	result->indexVersion = ++table->toast_ixversion;
	result->indexOids = table->toast_oids;
	result->indexType = oIndexToast;
	namestrcpy(&result->name, "toast");
	result->tableOids = table->oids;
	result->table_persistence = table->persistence;
	result->primaryIsCtid = !table->has_primary;
	result->compress = table->toast_compress;
	result->fillfactor = HEAP_DEFAULT_FILLFACTOR;
	result->tablespace = table->tablespace;
	if (table->has_primary)
	{
		result->nLeafFields = primary->nfields;
		result->nNonLeafFields = primary->nkeyfields;
		result->nKeyFields = primary->nkeyfields;
	}
	else
	{
		/* ctid_primary case */
		result->nLeafFields = 1;
		result->nNonLeafFields = 1;
		result->nKeyFields = 1;
	}
	result->nLeafFields += TOAST_LEAF_FIELDS_NUM;
	result->nNonLeafFields += TOAST_NON_LEAF_FIELDS_NUM;
	result->immediate = true;

	elog(LOG, "[%s] oids [ %u %u %u ] version %u", __func__,
		 table->oids.datoid, table->oids.reloid, table->oids.relnode,
		 table->toast_ixversion);

	result->leafTableFields = (OTableField *) palloc0(sizeof(OTableField) * result->nLeafFields);
	result->leafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) * result->nLeafFields);

	nadded = 0;
	add_index_fields(result, table, primary, &nadded, true);
	make_builtin_field(&result->leafTableFields[nadded], &result->leafFields[nadded],
					   INT2OID, "attnum", FirstLowInvalidHeapAttributeNumber,
					   INT2_BTREE_OPS_OID);
	nadded++;
	make_builtin_field(&result->leafTableFields[nadded], &result->leafFields[nadded],
					   INT4OID, "chunknum", FirstLowInvalidHeapAttributeNumber,
					   INT4_BTREE_OPS_OID);
	nadded++;
	Assert(nadded <= result->nNonLeafFields);
	result->nUniqueFields = nadded;
	result->nNonLeafFields = nadded;
	make_builtin_field(&result->leafTableFields[nadded], NULL,
					   BYTEAOID, "data", FirstLowInvalidHeapAttributeNumber,
					   InvalidOid);
	nadded++;
	Assert(nadded <= result->nLeafFields);
	result->nLeafFields = nadded;

	return result;
}

static OIndex *
make_bridge_o_index(OTable *table)
{
	OTableIndex *primary = NULL;
	OIndex	   *result = (OIndex *) palloc0(sizeof(OIndex));
	int			nadded;

	if (table->has_primary)
	{
		primary = &table->indices[0];
		Assert(primary->type == oIndexPrimary);
	}

	result->indexVersion = ++table->bridge_ixversion;
	result->indexOids = table->bridge_oids;
	result->indexType = oIndexBridge;
	namestrcpy(&result->name, "index_bridge");
	result->tableOids = table->oids;
	result->bridging = true;
	result->table_persistence = table->persistence;
	result->primaryIsCtid = !table->has_primary;
	result->compress = table->primary_compress;
	result->nLeafFields = 1;
	if (table->has_primary)
		result->nLeafFields += primary->nfields;
	else
		result->nLeafFields++;
	result->nNonLeafFields = 1;
	result->nKeyFields = 1;
	result->nUniqueFields = 1;
	result->leafTableFields = (OTableField *) palloc0(sizeof(OTableField) * result->nLeafFields);
	result->leafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) * result->nLeafFields);

	nadded = 0;
	make_builtin_field(&result->leafTableFields[0], &result->leafFields[0],
					   TIDOID, "index_bridging_ctid", FirstLowInvalidHeapAttributeNumber,
					   table->tid_btree_ops_oid);
	nadded++;
	add_index_fields(result, table, primary, &nadded, true);
	Assert(nadded == result->nLeafFields);
	result->nLeafFields = nadded;

	return result;
}

void
free_o_index(OIndex *o_index)
{
	Assert(o_index != NULL);

	pfree(o_index->leafTableFields);
	pfree(o_index->leafFields);
	if (o_index->index_mctx)
	{
		MemoryContextDelete(o_index->index_mctx);
	}
	pfree(o_index);
}

void
o_serialize_string(char *serialized, StringInfo str)
{
	size_t		str_len = serialized ? strlen(serialized) + 1 : 0;

	appendBinaryStringInfo(str, (Pointer) &str_len, sizeof(size_t));
	if (serialized)
		appendBinaryStringInfo(str, serialized, str_len);
}

char *
o_deserialize_string(Pointer *ptr)
{
	char	   *result = NULL;
	size_t		str_len;
	int			len;

	len = sizeof(size_t);
	memcpy(&str_len, *ptr, len);
	*ptr += len;

	if (str_len != 0)
	{
		len = str_len;
		result = (char *) palloc(len);
		memcpy(result, *ptr, len);
		*ptr += len;
	}

	return result;
}

void
o_serialize_node(Node *node, StringInfo str)
{
	char	   *node_str;
	size_t		node_str_len;

	node_str = nodeToString(node);
	node_str_len = strlen(node_str) + 1;
	appendBinaryStringInfo(str, (Pointer) &node_str_len, sizeof(size_t));
	appendBinaryStringInfo(str, node_str, node_str_len);
	pfree(node_str);
}

Node *
o_deserialize_node(Pointer *ptr)
{
	Node	   *result;
	size_t		node_str_len;
	int			len;

	len = sizeof(size_t);
	memcpy(&node_str_len, *ptr, len);
	*ptr += len;

	len = node_str_len;
	result = stringToNode(*ptr);
	*ptr += len;
	return result;
}

static Pointer
serialize_o_index(OIndex *o_index, int *size)
{
	StringInfoData str;

	initStringInfo(&str);

	Assert(o_index->data_version == ORIOLEDB_DATA_VERSION);
	appendBinaryStringInfo(&str,
						   (Pointer) o_index + offsetof(OIndex, tableOids),
						   offsetof(OIndex, leafTableFields) - offsetof(OIndex, tableOids));
	appendBinaryStringInfo(&str, (Pointer) o_index->leafTableFields,
						   o_index->nLeafFields * sizeof(o_index->leafTableFields[0]));
	appendBinaryStringInfo(&str, (Pointer) o_index->leafFields,
						   o_index->nLeafFields * sizeof(o_index->leafFields[0]));
	o_serialize_node((Node *) o_index->predicate, &str);
	if (o_index->predicate)
		o_serialize_string(o_index->predicate_str, &str);
	o_serialize_node((Node *) o_index->expressions, &str);
	o_serialize_node((Node *) o_index->duplicates, &str);

	appendBinaryStringInfo(&str, (Pointer) &o_index->tablespace, sizeof(Oid));
	if (o_index->indexType == oIndexExclusion)
		appendBinaryStringInfo(&str, (Pointer) o_index->exclops, sizeof(Oid) * o_index->nKeyFields);
	appendBinaryStringInfo(&str, (Pointer) &o_index->immediate, sizeof(bool));

	*size = str.len;
	return str.data;
}

static OIndex *
deserialize_o_index(OIndexChunkKey *key, Pointer data, Size length)
{
	Pointer		ptr = data;
	OIndex	   *oIndex;
	int			len;
	MemoryContext mcxt,
				old_mcxt;

	oIndex = (OIndex *) palloc0(sizeof(OIndex));
	oIndex->indexOids = key->oids;
	oIndex->indexType = key->type;
	oIndex->indexVersion = key->version;

	len = offsetof(OIndex, leafTableFields) - offsetof(OIndex, tableOids);
	Assert((ptr - data) + len <= length);
	memcpy((Pointer) oIndex + offsetof(OIndex, tableOids), ptr, len);
	ptr += len;
	Assert(oIndex->data_version == ORIOLEDB_DATA_VERSION);

	len = oIndex->nLeafFields * sizeof(OTableField);
	oIndex->leafTableFields = (OTableField *) palloc(len);
	Assert((ptr - data) + len <= length);
	memcpy(oIndex->leafTableFields, ptr, len);
	ptr += len;

	len = oIndex->nLeafFields * sizeof(OTableIndexField);
	oIndex->leafFields = (OTableIndexField *) palloc(len);
	Assert((ptr - data) + len <= length);
	memcpy(oIndex->leafFields, ptr, len);
	ptr += len;

	mcxt = OGetIndexContext(oIndex);
	old_mcxt = MemoryContextSwitchTo(mcxt);
	oIndex->predicate = (List *) o_deserialize_node(&ptr);
	if (oIndex->predicate)
		oIndex->predicate_str = o_deserialize_string(&ptr);
	oIndex->expressions = (List *) o_deserialize_node(&ptr);
	oIndex->duplicates = (List *) o_deserialize_node(&ptr);
	MemoryContextSwitchTo(old_mcxt);

	if (oIndex->data_version >= 2)
	{
		len = sizeof(Oid);
		Assert((ptr - data) + len <= length);
		memcpy(&oIndex->tablespace, ptr, len);
		ptr += len;
	}
	else
		oIndex->tablespace = DEFAULTTABLESPACE_OID;

	if (oIndex->data_version >= 3 && oIndex->indexType == oIndexExclusion)
	{
		mcxt = OGetIndexContext(oIndex);
		old_mcxt = MemoryContextSwitchTo(mcxt);
		len = sizeof(Oid) * oIndex->nKeyFields;
		Assert((ptr - data) + len <= length);
		oIndex->exclops = (Oid *) palloc0(len);
		memcpy(oIndex->exclops, ptr, len);
		ptr += len;
		MemoryContextSwitchTo(old_mcxt);
	}
	else
		oIndex->exclops = NULL;
	if (oIndex->data_version >= 3)
	{
		len = sizeof(bool);
		Assert((ptr - data) + len <= length);
		memcpy(&oIndex->immediate, ptr, len);
		ptr += len;

	}
	else
		oIndex->immediate = true;
	Assert((ptr - data) == length);

	return oIndex;
}

static uint32 *
get_version(OTable *table, OIndexNumber ixNum)
{
	bool		primaryIsCtid = table->nindices == 0 || table->indices[0].type != oIndexPrimary;

	if (ixNum == PrimaryIndexNumber)
	{
		return primaryIsCtid ? &table->primary_ixversion : &table->indices[0].version;
	}
	else if (ixNum == TOASTIndexNumber)
	{
		return &table->toast_ixversion;
	}
	else if (ixNum == BridgeIndexNumber)
	{
		return &table->bridge_ixversion;
	}
	return NULL;
	/* @TODO ! !! */
}

OIndex *
make_o_index(OTable *table, OIndexNumber ixNum)
{
	bool		primaryIsCtid = table->nindices == 0 || table->indices[0].type != oIndexPrimary;
	OIndex	   *index;

	if (ixNum == PrimaryIndexNumber)
	{
		if (primaryIsCtid)
			index = make_ctid_o_index(table);
		else
			index = make_primary_o_index(table);
	}
	else if (ixNum == TOASTIndexNumber)
	{
		index = make_toast_o_index(table);
	}
	else if (ixNum == BridgeIndexNumber)
	{
		index = make_bridge_o_index(table);
	}
	else
	{
		OTableIndex *tableIndex;
		int			ctid_idx_off = 0;

		if (primaryIsCtid)
			ctid_idx_off++;

		Assert(ixNum - ctid_idx_off >= 0);
		tableIndex = &table->indices[ixNum - ctid_idx_off];

		index = make_secondary_o_index(table, tableIndex);
	}

	index->data_version = ORIOLEDB_DATA_VERSION;
	return index;
}

static void
fillFixedFormatSpec(TupleDesc tupdesc, OTupleFixedFormatSpec *spec,
					bool mayHaveToast, uint16 *primary_init_nfields)
{
	int			i,
				len = 0;
	int			natts;

	if (primary_init_nfields)
		natts = *primary_init_nfields;
	else
		natts = tupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attlen <= 0)
			break;

		len = att_align_nominal(len, attr->attalign);
		len += attr->attlen;
	}
	spec->natts = i;
	spec->len = len;
}

static int
attrnumber_cmp(const void *p1, const void *p2)
{
	AttrNumberMap n1 = *(const AttrNumberMap *) p1;
	AttrNumberMap n2 = *(const AttrNumberMap *) p2;

	if (n1.key != n2.key)
		return n1.key > n2.key ? 1 : -1;
	return 0;
}

static void
cache_scan_tupdesc_and_slot(OIndexDescr *index_descr, OIndex *oIndex)
{
	ListCell   *lc = NULL;
	List	   *duplicate = NIL;
	int			nfields;
	int			pk_nfields;
	int			i;
	int			pk_from = oIndex->nKeyFields + oIndex->nIncludedFields;
	int			nduplicates = list_length(oIndex->duplicates);
	int			cur_attr;

	/*
	 * TODO: Check why this called multiple times for ctid_primary during
	 * single CREATE INDEX
	 */

	if (!index_descr->primaryIsCtid)
	{

		pk_nfields = oIndex->nPrimaryFields;
		for (i = 0; i < oIndex->nPrimaryFields; i++)
		{
			AttrNumber	pk_attnum = oIndex->primaryFieldsAttnums[i] - 1;

			if (pk_attnum < pk_from)
				pk_nfields--;
		}
	}
	else
	{
		pk_nfields = 0;
	}

	nfields = pk_from + nduplicates + pk_nfields;
	index_descr->itupdesc = CreateTemplateTupleDesc(nfields);

	if (oIndex->duplicates != NIL)
		lc = list_head(oIndex->duplicates);
	if (lc != NULL)
		duplicate = (List *) lfirst(lc);

	cur_attr = 0;
	for (i = 0; i < nfields; i++)
	{
		if (duplicate != NIL && linitial_int(duplicate) == cur_attr)
		{
			int			src_attnum = lsecond_int(duplicate);

			lc = lnext(oIndex->duplicates, lc);
			if (lc != NULL)
				duplicate = (List *) lfirst(lc);
			else
				duplicate = NIL;

			TupleDescCopyEntry(index_descr->itupdesc, i + 1, index_descr->itupdesc, src_attnum + 1);
		}
		else
		{
			TupleDescCopyEntry(index_descr->itupdesc, i + 1, index_descr->nonLeafTupdesc, cur_attr + 1);
			cur_attr++;
		}
	}

	index_descr->index_slot = MakeSingleTupleTableSlot(index_descr->itupdesc, &TTSOpsOrioleDB);
}

/*
 * o_index_fill_descr()
 *
 * Initialize *descr from the catalog OIndex entry.
 *
 * The function resets the descriptor, copies identity fields (OIDs/name/version),
 * builds leaf/non-leaf tuple descriptors, fills per-field metadata (collation,
 * ordering, opclasses, comparators), and initializes predicate/expressions state.
 *
 * Base table dependency:
 * - For oIndexPrimary, additional data is taken from OTable (constraints and
 *   primary_init_nfields). The table is obtained using o_table_source/source).
 * - For other index types, o_table_source/source are currently unused.
 *
 * Memory/ownership:
 * - Descriptor-owned allocations are made in OGetIndexContext(descr).
 * - If OTable loaded from OTableFetchContext (when source == oTableSourceContext)
 *   it is freed before return.
 *
 * Requirements:
 * - oIndex != NULL, descr points to writable memory.
 */
void
o_index_fill_descr(OIndexDescr *descr, OIndex *oIndex, void *o_table_source, OTableSource source)
{
	int			i;
	int			maxTableAttnum = 0;
	uint16	   *primary_init_nfields = NULL;
	ListCell   *lc;
	MemoryContext mcxt,
				old_mcxt;

	Assert(oIndex != NULL);

	memset(descr, 0, sizeof(*descr));
	descr->oids = oIndex->indexOids;
	descr->tableOids = oIndex->tableOids;
	descr->version = oIndex->indexVersion;
	descr->refcnt = 0;
	descr->valid = true;
	namestrcpy(&descr->name, oIndex->name.data);
	descr->leafTupdesc = o_table_fields_make_tupdesc(oIndex->leafTableFields,
													 oIndex->nLeafFields);

	if (oIndex->indexType == oIndexPrimary)
	{
		bool		free_oTable = false;
		OTable	   *oTable = NULL;

		Assert(o_table_source);

		if (OTableSourceLoad(source))
		{
			oTable = o_tables_get_extended(descr->tableOids, *((OTableFetchContext *) o_table_source));
			free_oTable = (oTable != NULL);
		}
		else
		{
			oTable = (OTable *) o_table_source;
			Assert(oTable);
			free_oTable = false;
		}

		if (oTable)
		{
			o_tupdesc_load_constr(descr->leafTupdesc, oTable, descr);
			primary_init_nfields = palloc(sizeof(*primary_init_nfields));
			*primary_init_nfields = oTable->primary_init_nfields;
			if (free_oTable)
				o_table_free(oTable);
		}
	}

	if (oIndex->indexType == oIndexPrimary)
	{
		descr->nonLeafTupdesc = CreateTemplateTupleDesc(oIndex->nNonLeafFields);
		if (oIndex->primaryIsCtid)
		{
			Assert(oIndex->nNonLeafFields == 1);
			Assert(oIndex->leafFields[0].attnum == SelfItemPointerAttributeNumber);
			TupleDescCopyEntry(descr->nonLeafTupdesc, 1,
							   descr->leafTupdesc, 1);
		}
		else
		{
			for (i = 0; i < oIndex->nNonLeafFields; i++)
			{
				int			ctid_off = oIndex->bridging ? 1 : 0;
				int			attnum = oIndex->leafFields[i].attnum + ctid_off;

				Assert(attnum >= 0 && attnum < oIndex->nLeafFields);
				TupleDescCopyEntry(descr->nonLeafTupdesc,
								   i + 1,
								   descr->leafTupdesc,
								   attnum + 1);
			}
		}
	}
	else if (oIndex->indexType == oIndexRegular ||
			 oIndex->indexType == oIndexUnique ||
			 oIndex->indexType == oIndexBridge ||
			 oIndex->indexType == oIndexExclusion)
	{
		if (oIndex->nNonLeafFields == oIndex->nLeafFields)
			descr->nonLeafTupdesc = CreateTupleDescCopy(descr->leafTupdesc);
		else
		{
			Assert(strncmp(oIndex->name.data, "index_bridge", NAMEDATALEN) == 0);
			Assert(oIndex->nNonLeafFields == 1);
			descr->nonLeafTupdesc = CreateTemplateTupleDesc(oIndex->nNonLeafFields);
			TupleDescCopyEntry(descr->nonLeafTupdesc, 1,
							   descr->leafTupdesc, 1);
		}
	}
	else if (oIndex->indexType == oIndexToast)
	{
		Assert(oIndex->nLeafFields - TOAST_LEAF_FIELDS_NUM == oIndex->nNonLeafFields - TOAST_NON_LEAF_FIELDS_NUM);
		descr->nonLeafTupdesc = CreateTemplateTupleDesc(oIndex->nNonLeafFields);
		for (i = 0; i < oIndex->nNonLeafFields; i++)
			TupleDescCopyEntry(descr->nonLeafTupdesc, i + 1,
							   descr->leafTupdesc, i + 1);
	}
	descr->primaryIsCtid = oIndex->primaryIsCtid;
	descr->bridging = oIndex->bridging;
	descr->unique = (oIndex->indexType == oIndexUnique ||
					 oIndex->indexType == oIndexPrimary);
	descr->immediate = oIndex->immediate;
	descr->nulls_not_distinct = oIndex->nulls_not_distinct;
	descr->nUniqueFields = oIndex->nUniqueFields;
	descr->nFields = oIndex->nNonLeafFields;

	mcxt = OGetIndexContext(descr);
	descr->fields = (OIndexField *) MemoryContextAllocZero(mcxt,
														   sizeof(OIndexField) * descr->nFields);
	descr->tableAttnums = (AttrNumber *) MemoryContextAllocZero(mcxt, sizeof(AttrNumber) * oIndex->nLeafFields);
	descr->pk_comparators = (OComparator **) MemoryContextAllocZero(mcxt, sizeof(OComparator *) * oIndex->nPrimaryFields);

	descr->nKeyFields = oIndex->nKeyFields;
	descr->nIncludedFields = oIndex->nIncludedFields;
	for (i = 0; i < oIndex->nLeafFields; i++)
	{
		OTableIndexField *iField = &oIndex->leafFields[i];
		int			attnum = iField->attnum;

		if (attnum == SelfItemPointerAttributeNumber)
		{
			Assert(oIndex->primaryIsCtid);
			attnum = 1;
		}
		else if (attnum == FirstLowInvalidHeapAttributeNumber)
		{
			attnum = -1;
		}
		else if (attnum != EXPR_ATTNUM)
		{
			Assert(attnum >= 0);
			attnum += oIndex->primaryIsCtid ? 2 : 1;
		}

		descr->tableAttnums[i] = attnum;
		maxTableAttnum = Max(maxTableAttnum, attnum);
	}

	for (i = 0; i < oIndex->nNonLeafFields; i++)
	{
		OIndexField *field = &descr->fields[i];
		OTableIndexField *iField = &oIndex->leafFields[i];
		bool		add_opclass = false;
		bool		needs_exclop = false;

		field->collation = TupleDescAttr(descr->nonLeafTupdesc, i)->attcollation;
		if (OidIsValid(iField->collation))
			field->collation = iField->collation;
		field->ascending = iField->ordering != SORTBY_DESC;

		if (iField->nullsOrdering == SORTBY_NULLS_DEFAULT)
		{
			/* default null ordering is LAST for ASC, FIRST for DESC */
			field->nullfirst = !field->ascending;
		}
		else
		{
			field->nullfirst = (iField->nullsOrdering == SORTBY_NULLS_FIRST);
		}

		add_opclass = !OIgnoreColumn(descr, i);
		needs_exclop = oIndex->exclops && i < oIndex->nKeyFields;
		if (add_opclass)
			oFillFieldOpClassAndComparator(field, oIndex->tableOids.datoid,
										   iField->opclass,
										   needs_exclop ? oIndex->exclops[i] : InvalidOid);
	}

	for (i = 0; i < oIndex->nPrimaryFields; i++)
	{
		OIndexField temp_field = {0};
		AttrNumber	attnum = oIndex->primaryFieldsAttnums[i] - 1;
		OTableIndexField *iField = &oIndex->leafFields[attnum];

		temp_field.collation = TupleDescAttr(descr->leafTupdesc, i)->attcollation;
		if (OidIsValid(iField->collation))
			temp_field.collation = iField->collation;

		oFillFieldOpClassAndComparator(&temp_field, oIndex->tableOids.datoid,
									   iField->opclass, iField->exclop);
		descr->pk_comparators[i] = temp_field.comparator;
	}

	old_mcxt = MemoryContextSwitchTo(mcxt);
	descr->predicate = list_copy_deep(oIndex->predicate);
	descr->duplicates = list_copy_deep(oIndex->duplicates);
	if (descr->predicate)
		descr->predicate_str = pstrdup(oIndex->predicate_str);
	descr->expressions = list_copy_deep(oIndex->expressions);
	if (!(oIndex->indexType == oIndexToast || (oIndex->indexType == oIndexPrimary && oIndex->primaryIsCtid)))
	{
		descr->old_leaf_slot = MakeSingleTupleTableSlot(descr->leafTupdesc, &TTSOpsOrioleDB);
		descr->new_leaf_slot = MakeSingleTupleTableSlot(descr->leafTupdesc, &TTSOpsOrioleDB);
		if (oIndex->indexType != oIndexBridge)
			cache_scan_tupdesc_and_slot(descr, oIndex);
	}

	o_set_syscache_hooks();
	descr->predicate_state = ExecInitQual(descr->predicate, NULL);
	descr->expressions_state = NIL;
	foreach(lc, descr->expressions)
	{
		Expr	   *node = (Expr *) lfirst(lc);
		ExprState  *expr_state;

		expr_state = ExecInitExpr(node, NULL);

		descr->expressions_state = lappend(descr->expressions_state,
										   expr_state);
	}
	o_unset_syscache_hooks();
	if (oIndex->indexType == oIndexPrimary)
	{
		descr->pk_tbl_field_map = palloc0(sizeof(AttrNumberMap) * descr->nFields);
		for (i = 0; i < descr->nFields; i++)
		{
			descr->pk_tbl_field_map[i].key = descr->tableAttnums[i] - 1;
			descr->pk_tbl_field_map[i].value = i;
		}
		pg_qsort(descr->pk_tbl_field_map, descr->nFields, sizeof(AttrNumberMap), attrnumber_cmp);
	}
	descr->econtext = CreateStandaloneExprContext();
	MemoryContextSwitchTo(old_mcxt);

	descr->maxTableAttnum = maxTableAttnum;

	descr->nPrimaryFields = oIndex->nPrimaryFields;
	memcpy(descr->primaryFieldsAttnums,
		   oIndex->primaryFieldsAttnums,
		   descr->nPrimaryFields * sizeof(descr->primaryFieldsAttnums[0]));
	descr->compress = oIndex->compress;
	if (oIndex->fillfactor > 0 && oIndex->fillfactor < 100)
		descr->fillfactor = oIndex->fillfactor;
	else if (oIndex->indexType == oIndexToast)
		descr->fillfactor = HEAP_DEFAULT_FILLFACTOR;
	else
		descr->fillfactor = BTREE_DEFAULT_FILLFACTOR;

	fillFixedFormatSpec(descr->leafTupdesc, &descr->leafSpec,
						(oIndex->indexType == oIndexPrimary),
						primary_init_nfields);
	fillFixedFormatSpec(descr->nonLeafTupdesc, &descr->nonLeafSpec,
						false, NULL);
	if (primary_init_nfields)
		pfree(primary_init_nfields);
}

bool
o_indices_add(OTable *table, OIndexNumber ixNum, OXid oxid, CommitSeqNo csn)
{
	OIndexChunkKey key;
	bool		result;
	OIndex	   *oIndex;
	Pointer		data;
	int			len;
	BTreeDescr *sys_tree;
	uint32	   *version_ptr;

	oIndex = make_o_index(table, ixNum);
	version_ptr = get_version(table, ixNum);
	if (version_ptr)
		*version_ptr = 0;

	oIndex->createOxid = oxid;
	key.oids = oIndex->indexOids;
	key.type = oIndex->indexType;
	key.chunknum = 0;
	key.version = 0;
	/* oIndex->indexVersion; */

	elog(LOG, "[%s] key oids [ %u %u %u ]; ixNum %u type %u; chunknum %u; version %u", __func__,
		 key.oids.datoid, key.oids.reloid, key.oids.relnode,
		 ixNum,
		 key.type,
		 key.chunknum,
		 key.version);

	data = serialize_o_index(oIndex, &len);
	free_o_index(oIndex);

	sys_tree = get_sys_tree(SYS_TREES_O_INDICES);
	result = generic_toast_insert_optional_wal(&oIndicesToastAPI,
											   (Pointer) &key, data, len, oxid,
											   csn, sys_tree, table->persistence != RELPERSISTENCE_TEMP);
	pfree(data);
	return result;
}

bool
o_indices_del(OTable *table, OIndexNumber ixNum, OXid oxid, CommitSeqNo csn)
{
	OIndexChunkKey key;
	bool		result;
	OIndex	   *oIndex;
	BTreeDescr *sys_tree;

	oIndex = make_o_index(table, ixNum);
	key.oids = oIndex->indexOids;
	key.type = oIndex->indexType;
	key.chunknum = 0;
	key.version = oIndex->indexVersion;

	elog(LOG, "[%s] key oids [ %u %u %u ]; type %u; chunknum %u; version %u", __func__,
		 key.oids.datoid, key.oids.reloid, key.oids.relnode,
		 key.type,
		 key.chunknum,
		 key.version);

	free_o_index(oIndex);

	sys_tree = get_sys_tree(SYS_TREES_O_INDICES);
	result = generic_toast_delete_optional_wal(&oIndicesToastAPI,
											   (Pointer) &key, oxid, csn,
											   sys_tree, table->persistence != RELPERSISTENCE_TEMP);
	return result;
}

OIndex *
o_indices_get(ORelOids oids, OIndexType type)
{
	return o_indices_get_extended(oids, type, default_table_fetch_context);
}

OIndex *
o_indices_get_extended(ORelOids oids, OIndexType type, OTableFetchContext ctx)
{
	OIndexChunkKey key,
			   *found_key = NULL;
	Size		dataLength;
	Pointer		result;
	OIndex	   *oIndex;

	key.type = type;
	key.oids = oids;
	key.chunknum = 0;
	key.version = ctx.version;

	elog(LOG, "[%s] key oids [ %u %u %u ]; type %u; chunknum %u; version %u", __func__,
		 key.oids.datoid, key.oids.reloid, key.oids.relnode,
		 key.type,
		 key.chunknum,
		 key.version);

	found_key = &key;
	result = generic_toast_get_any_with_key(&oIndicesToastAPI, (Pointer) &key,
											&dataLength,
											ctx.snapshot,
											get_sys_tree(SYS_TREES_O_INDICES),
											(Pointer *) &found_key);

	if (result == NULL)
		return NULL;

	oIndex = deserialize_o_index(&key, result, dataLength);
	pfree(result);
	pfree(found_key);

	return oIndex;
}

bool
o_indices_update(OTable *table, OIndexNumber ixNum, OXid oxid, CommitSeqNo csn)
{
	OIndex	   *oIndex;
	OIndex	   *oIndexOld;
	OIndexChunkKey key;
	bool		result;
	Pointer		data;
	int			len;
	BTreeDescr *sys_tree;

	oIndex = make_o_index(table, ixNum);
	oIndexOld = o_indices_get_extended(oIndex->indexOids, oIndex->indexType, default_table_fetch_context);
	if (oIndexOld)
	{
		oIndex->createOxid = oIndexOld->createOxid;
		free_o_index(oIndexOld);
	}
	data = serialize_o_index(oIndex, &len);
	key.oids = oIndex->indexOids;
	key.type = oIndex->indexType;
	key.chunknum = 0;
	key.version = oIndex->indexVersion;

	elog(LOG, "[%s] key oids [ %u %u %u ]; type %u; chunknum %u; version %u", __func__,
		 key.oids.datoid, key.oids.reloid, key.oids.relnode,
		 key.type,
		 key.chunknum,
		 key.version);

	free_o_index(oIndex);
	systrees_modify_start();
	sys_tree = get_sys_tree(SYS_TREES_O_INDICES);
	result = generic_toast_update_optional_wal(&oIndicesToastAPI,
											   (Pointer) &key, data, len, oxid,
											   csn, sys_tree, table->persistence != RELPERSISTENCE_TEMP);
	systrees_modify_end(table->persistence != RELPERSISTENCE_TEMP);

	pfree(data);

	return result;
}

/*
 * This method is used by o_tables_get_by_tree only, which is unused
 */
bool
o_indices_find_table_oids(ORelOids indexOids, OIndexType type,
						  OSnapshot *oSnapshot, ORelOids *tableOids)
{
	OIndexChunkKey key;
	Pointer		data;
	Size		dataSize;

	key.oids = indexOids;
	key.type = type;
	key.chunknum = 0;
	key.version = O_TABLE_INVALID_VERSION;

	data = generic_toast_get_any(&oIndicesToastAPI, (Pointer) &key, &dataSize,
								 oSnapshot, get_sys_tree(SYS_TREES_O_INDICES));
	if (data)
	{
		memcpy(tableOids, data, sizeof(ORelOids));
		pfree(data);
		return true;
	}
	return false;
}

void
o_indices_foreach_oids(OIndexOidsCallback callback, void *arg)
{
	OIndexChunkKey chunkKey;
	ORelOids	oids = {0, 0, 0},
				old_oids PG_USED_FOR_ASSERTS_ONLY;
	OIndexType	type = oIndexInvalid;
	BTreeIterator *it;
	OTuple		tuple;
	BTreeDescr *desc = get_sys_tree(SYS_TREES_O_INDICES);

	chunkKey.type = type;
	chunkKey.oids = oids;
	chunkKey.chunknum = 0;
	chunkKey.version = O_TABLE_INVALID_VERSION;

	/*
	 * Only actual versions are needed here, so it's fine to use
	 * o_non_deleted_snapshot and O_TABLE_INVALID_VERSION
	 */

	it = o_btree_iterator_create(desc, (Pointer) &chunkKey, BTreeKeyBound,
								 &o_non_deleted_snapshot, ForwardScanDirection);

	tuple = o_btree_iterator_fetch(it, NULL, NULL,
								   BTreeKeyNone, false, NULL);
	old_oids = oids;
	while (!O_TUPLE_IS_NULL(tuple))
	{
		OIndexChunk *chunk = (OIndexChunk *) tuple.data;
		ORelOids	tableOids;
		bool		temp_table;

		type = chunk->key.type;
		oids = chunk->key.oids;
		Assert(chunk->dataLength >= sizeof(tableOids));
		memcpy(&tableOids, chunk->data, sizeof(tableOids));
		memcpy(&temp_table, chunk->data + sizeof(tableOids), sizeof(bool));
		Assert(chunk->key.chunknum == 0);
		Assert(ORelOidsIsValid(oids));
		Assert(!ORelOidsIsEqual(old_oids, oids));
		old_oids = oids;

		callback(type, oids, tableOids, arg);

		pfree(tuple.data);
		btree_iterator_free(it);

		oids.relnode += 1;		/* go to the next oid */
		chunkKey.oids = oids;
		chunkKey.type = type;
		chunkKey.chunknum = 0;

		it = o_btree_iterator_create(desc, (Pointer) &chunkKey, BTreeKeyBound,
									 &o_non_deleted_snapshot, ForwardScanDirection);
		tuple = o_btree_iterator_fetch(it, NULL, NULL,
									   BTreeKeyNone, false, NULL);
	}
}

static const char *
index_type_to_str(OIndexType type)
{
	switch (type)
	{
		case oIndexToast:
			return "toast";
		case oIndexPrimary:
			return "primary";
		case oIndexUnique:
			return "unique";
		case oIndexRegular:
			return "regular";
		case oIndexBridge:
			return "bridge";
		case oIndexExclusion:
			return "exclusion";
		default:
			return "invalid";
	}
}

static OIndexType
index_type_from_str(const char *s, int len)
{
	if (!strncmp(s, "toast", len))
		return oIndexToast;
	else if (!strncmp(s, "primary", len))
		return oIndexPrimary;
	else if (!strncmp(s, "unique", len))
		return oIndexUnique;
	else if (!strncmp(s, "regular", len))
		return oIndexRegular;
	else if (!strncmp(s, "bridge", len))
		return oIndexBridge;
	else if (!strncmp(s, "exclusion", len))
		return oIndexExclusion;
	else
		return oIndexInvalid;
}

static void
o_index_oids_array_callback(OIndexType type, ORelOids treeOids,
							ORelOids tableOids, void *arg)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) arg;
	Datum		values[6];
	bool		nulls[6] = {false};

	Assert(tableOids.datoid == treeOids.datoid);
	values[0] = tableOids.datoid;
	values[1] = tableOids.reloid;
	values[2] = tableOids.relnode;
	values[3] = treeOids.reloid;
	values[4] = treeOids.relnode;
	values[5] = PointerGetDatum(cstring_to_text(index_type_to_str(type)));
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

Datum
orioledb_index_oids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	o_indices_foreach_oids(o_index_oids_array_callback, rsinfo);

	return (Datum) 0;
}

static HeapTuple
describe_index(TupleDesc tupdesc, ORelOids oids, OIndexType type)
{
	OIndex	   *index;
	StringInfoData buf,
				format,
				title;
	char	   *column_str = "Column",
			   *type_str = "Type",
			   *collation_str = "Collation";
	int			i,
				max_column_str,
				max_type_str,
				max_collation_str;
	Datum		values[2];
	bool		isnull[2] = {false};

	/* Only actual versions are needed here */
	index = o_indices_get(oids, type);
	if (index == NULL)
		elog(ERROR, "unable to find orioledb index description.");

	max_column_str = strlen(column_str);
	max_type_str = strlen(type_str);
	max_collation_str = strlen(collation_str);
	for (i = 0; i < index->nLeafFields; i++)
	{
		OTableField *field = &index->leafTableFields[i];
		char	   *typename = o_get_type_name(field->typid, field->typmod);
		char	   *colname = get_collation_name(field->collation);

		if (max_column_str < strlen(NameStr(field->name)))
			max_column_str = strlen(NameStr(field->name));
		if (max_type_str < strlen(typename))
			max_type_str = strlen(typename);
		if (colname != NULL)
		{
			if (max_collation_str < strlen(colname))
				max_collation_str = strlen(colname);
		}
	}

	initStringInfo(&title);
	appendStringInfo(&title, " %%%ds | %%%ds | %%%ds | Nullable | Droped \n",
					 max_column_str,
					 max_type_str,
					 max_collation_str);
	initStringInfo(&format);
	appendStringInfo(&format, " %%%ds | %%%ds | %%%ds | %%8s | %%6s \n",
					 max_column_str,
					 max_type_str,
					 max_collation_str);
	initStringInfo(&buf);
	appendStringInfo(&buf, title.data, column_str, type_str, collation_str);

	for (i = 0; i < index->nLeafFields; i++)
	{
		OTableField *field = &index->leafTableFields[i];
		char	   *typename = o_get_type_name(field->typid, field->typmod);
		char	   *colname = get_collation_name(field->collation);

		appendStringInfo(&buf, format.data,
						 NameStr(field->name),
						 typename,
						 colname ? colname : "(null)",
						 field->notnull ? "false" : "true",
						 field->droped ? "true" : "false");
	}

	appendStringInfo(&buf, "\nKey fields: (");
	for (i = 0; i < index->nNonLeafFields; i++)
	{
		OTableIndexField *nonLeafField = &index->leafFields[i];
		OTableField *leafField;

		if (type == oIndexPrimary)
		{
			if (nonLeafField->attnum == SelfItemPointerAttributeNumber)
			{
				Assert(index->primaryIsCtid);
				leafField = &index->leafTableFields[0];
			}
			if (index->primaryIsCtid)
				leafField = &index->leafTableFields[nonLeafField->attnum + 1];
			else
				leafField = &index->leafTableFields[nonLeafField->attnum];
		}
		else
		{
			leafField = &index->leafTableFields[i];
		}
		if (i != 0)
			appendStringInfo(&buf, ", ");
		appendStringInfo(&buf, "%s", NameStr(leafField->name));
		if (i + 1 == index->nUniqueFields)
			appendStringInfo(&buf, ")");
	}
	appendStringInfo(&buf, "\n");

	values[0] = PointerGetDatum(cstring_to_text(NameStr(index->name)));
	values[1] = PointerGetDatum(cstring_to_text(buf.data));

	return heap_form_tuple(tupdesc, values, isnull);
}

Datum
orioledb_index_description(PG_FUNCTION_ARGS)
{
	ORelOids	oids;
	text	   *indexTypeText = PG_GETARG_TEXT_PP(3);
	OIndexType	indexType;
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	indexType = index_type_from_str(VARDATA_ANY(indexTypeText),
									VARSIZE_ANY_EXHDR(indexTypeText));
	oids.datoid = PG_GETARG_OID(0);
	oids.reloid = PG_GETARG_OID(1);
	oids.relnode = PG_GETARG_OID(2);

	PG_RETURN_DATUM(HeapTupleGetDatum(describe_index(tupdesc, oids, indexType)));
}

/*
 * Returns amount of all rows and dead rows
 */
Datum
orioledb_index_rows(PG_FUNCTION_ARGS)
{
	Oid			ix_reloid = PG_GETARG_OID(0);
	Relation	idx,
				tbl;
	OTableDescr *descr;
	OIndexNumber ix_num;
	int64		total = 0,
				dead = 0;
	BTreeIterator *it;
	BTreeDescr *td;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	Datum		values[2];
	bool		nulls[2];

	idx = index_open(ix_reloid, AccessShareLock);
	tbl = table_open(idx->rd_index->indrelid, AccessShareLock);
	descr = relation_get_descr(tbl);
	ix_num = o_find_ix_num_by_name(descr, idx->rd_rel->relname.data);
	relation_close(tbl, AccessShareLock);
	relation_close(idx, AccessShareLock);

	if (ix_num == InvalidIndexNumber)
		elog(ERROR, "Invalid index");

	td = &descr->indices[ix_num]->desc;
	o_btree_load_shmem(td);

	/*
	 * Build a tuple descriptor for our result type
	 */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 &o_in_progress_snapshot, ForwardScanDirection);

	do
	{
		bool		end;
		OTuple		tup = btree_iterate_raw(it, NULL, BTreeKeyNone,
											false, &end, NULL);

		if (end)
			break;
		if (O_TUPLE_IS_NULL(tup))
			dead += 1;
		total += 1;
	} while (true);

	btree_iterator_free(it);

	tupleDesc = BlessTupleDesc(tupleDesc);

	/*
	 * Build and return the tuple
	 */
	MemSet(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(total);
	values[1] = Int64GetDatum(dead);
	tuple = heap_form_tuple(tupleDesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
