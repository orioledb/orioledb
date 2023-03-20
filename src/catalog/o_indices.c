/*-------------------------------------------------------------------------
 *
 * o_indices.c
 * 		Routines for orioledb indices system tree.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
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
#include "tuple/toast.h"

#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_opclass_d.h"
#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"

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
oIndicesGetMaxChunkSize(void *key, void *arg)
{
	uint32		max_chunk_size;

	max_chunk_size = MAXALIGN_DOWN((O_BTREE_MAX_TUPLE_SIZE * 3 - MAXALIGN(sizeof(OIndexChunkKey))) / 3) - offsetof(OIndexChunk, data);

	return max_chunk_size;
}

static void
oIndicesUpdateKey(void *key, uint32 offset, void *arg)
{
	OIndexChunkKey *ckey = (OIndexChunkKey *) key;

	ckey->offset = offset;
}

static void *
oIndicesGetNextKey(void *key, void *arg)
{
	OIndexChunkKey *ckey = (OIndexChunkKey *) key;
	static OIndexChunkKey nextKey;

	nextKey = *ckey;
	nextKey.oids.relnode++;
	nextKey.offset = 0;

	return &nextKey;
}

static OTuple
oIndicesCreateTuple(void *key, Pointer data, uint32 offset,
					int length, void *arg)
{
	OIndexChunkKey *ckey = (OIndexChunkKey *) key;
	OIndexChunk *chunk;
	OTuple		result;

	ckey->offset = offset;

	chunk = (OIndexChunk *) palloc(offsetof(OIndexChunk, data) + length);
	chunk->key = *ckey;
	chunk->dataLength = length;
	memcpy(chunk->data, data + offset, length);

	result.data = (Pointer) chunk;
	result.formatFlags = 0;
	return result;
}

static OTuple
oIndicesCreateKey(void *key, uint32 offset, void *arg)
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
oIndicesGetTupleOffset(OTuple tuple, void *arg)
{
	OIndexChunk *chunk = (OIndexChunk *) tuple.data;

	return chunk->key.offset;
}

static uint32
oIndicesGetTupleDataSize(OTuple tuple, void *arg)
{
	OIndexChunk *chunk = (OIndexChunk *) tuple.data;

	return chunk->dataLength;
}

ToastAPI	oIndicesToastAPI = {
	.getBTreeDesc = oIndicesGetBTreeDesc,
	.getMaxChunkSize = oIndicesGetMaxChunkSize,
	.updateKey = oIndicesUpdateKey,
	.getNextKey = oIndicesGetNextKey,
	.createTuple = oIndicesCreateTuple,
	.createKey = oIndicesCreateKey,
	.getTupleData = oIndicesGetTupleData,
	.getTupleOffset = oIndicesGetTupleOffset,
	.getTupleDataSize = oIndicesGetTupleDataSize,
	.deleteLogFullTuple = true,
	.versionCallback = NULL
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

	Assert(!table->has_primary);
	result->indexOids = table->oids;
	result->indexType = oIndexPrimary;
	namestrcpy(&result->name, "ctid_primary");
	result->tableOids = table->oids;
	result->primaryIsCtid = true;
	result->compress = table->primary_compress;
	result->nLeafFields = table->nfields + 1;
	result->nNonLeafFields = 1;
	result->nPrimaryFields = 0;
	result->nKeyFields = 1;
	result->nUniqueFields = 1;

	result->leafFields = (OTableField *) palloc0(sizeof(OTableField) *
												 result->nLeafFields);
	result->nonLeafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) *
														 result->nNonLeafFields);

	make_builtin_field(&result->leafFields[0], &result->nonLeafFields[0],
					   TIDOID, "ctid", SelfItemPointerAttributeNumber,
					   table->tid_btree_ops_oid);

	for (i = 0; i < table->nfields; i++)
		result->leafFields[i + 1] = table->fields[i];

	return result;
}

static int
find_existing_field(OIndex *index, int maxIndex, OTableIndexField *field)
{
	int			i;

	for (i = 0; i < maxIndex; i++)
	{
		if (field->attnum != EXPR_ATTNUM &&
			field->attnum == index->nonLeafFields[i].attnum &&
			field->opclass == index->nonLeafFields[i].opclass)
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
	int			i,
				j;

	Assert(table->has_primary && table->nindices >= 1);
	tableIndex = &table->indices[0];
	result->indexOids = tableIndex->oids;
	result->indexType = oIndexPrimary;
	namestrcpy(&result->name, tableIndex->name.data);
	Assert(tableIndex->type == oIndexPrimary);
	result->tableOids = table->oids;
	result->primaryIsCtid = false;
	if (OCompressIsValid(tableIndex->compress))
		result->compress = tableIndex->compress;
	else
		result->compress = table->primary_compress;
	result->nLeafFields = table->nfields;
	result->nNonLeafFields = tableIndex->nfields;
	result->nIncludedFields = tableIndex->nfields - tableIndex->nkeyfields;
	result->nPrimaryFields = 0;
	result->nKeyFields = tableIndex->nkeyfields;

	result->leafFields = (OTableField *) palloc0(sizeof(OTableField) *
												 result->nLeafFields);
	result->nonLeafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) *
														 result->nNonLeafFields);

	for (i = 0; i < result->nLeafFields; i++)
		result->leafFields[i] = table->fields[i];

	j = 0;
	for (i = 0; i < result->nNonLeafFields; i++)
	{
		if (find_existing_field(result, j, &tableIndex->fields[i]) >= 0)
		{
			if (i < result->nKeyFields)
				result->nKeyFields--;
			else
				result->nIncludedFields--;
			continue;
		}

		result->nonLeafFields[j++] = tableIndex->fields[i];
	}
	Assert(j <= result->nNonLeafFields);
	result->nUniqueFields = result->nKeyFields;
	result->nNonLeafFields = j;

	return result;
}

static void
add_index_fields(OIndex *index, OTable *table, OTableIndex *tableIndex, int *j,
				 uint16 *nKeyFields, bool fillPrimary)
{
	int			i;
	int			expr_field = 0;

	if (tableIndex)
	{
		int			nFields = fillPrimary ? tableIndex->nkeyfields : tableIndex->nfields;

		for (i = 0; i < nFields; i++)
		{
			int			attnum = tableIndex->fields[i].attnum;
			int			k;

			if (i == tableIndex->nkeyfields && nKeyFields)
				(*nKeyFields) = *j;

			k = find_existing_field(index, *j, &tableIndex->fields[i]);
			if (k >= 0)
			{
				if (fillPrimary)
					index->primaryFieldsAttnums[index->nPrimaryFields++] = k + 1;
				else if (i < index->nKeyFields)
					index->nKeyFields--;
				else
					index->nIncludedFields--;
				continue;
			}

			if (attnum != EXPR_ATTNUM)
				index->leafFields[*j] = table->fields[attnum];
			else
				index->leafFields[*j] = tableIndex->exprfields[expr_field++];
			Assert(index->leafFields[*j].typid);
			index->nonLeafFields[*j] = tableIndex->fields[i];
			if (fillPrimary)
				index->primaryFieldsAttnums[index->nPrimaryFields++] = *j + 1;
			(*j)++;
		}

		if (i == tableIndex->nkeyfields && nKeyFields)
			(*nKeyFields) = *j;
	}
	else
	{
		make_builtin_field(&index->leafFields[*j], &index->nonLeafFields[*j],
						   TIDOID, "ctid", SelfItemPointerAttributeNumber,
						   table->tid_btree_ops_oid);
		if (fillPrimary)
			index->primaryFieldsAttnums[index->nPrimaryFields++] = *j + 1;
		(*j)++;
		if (nKeyFields)
			(*nKeyFields) = 1;
	}
}

static OIndex *
make_secondary_o_index(OTable *table, OTableIndex *tableIndex)
{
	OTableIndex *primary = NULL;
	OIndex	   *result = (OIndex *) palloc0(sizeof(OIndex));
	int			j;
	MemoryContext mcxt;
	MemoryContext old_mcxt;

	if (table->has_primary)
	{
		primary = &table->indices[0];
		Assert(primary->type == oIndexPrimary);
	}

	result->indexOids = tableIndex->oids;
	result->indexType = tableIndex->type;
	namestrcpy(&result->name, tableIndex->name.data);
	result->tableOids = table->oids;
	result->primaryIsCtid = !table->has_primary;
	result->compress = tableIndex->compress;
	result->nIncludedFields = tableIndex->nfields - tableIndex->nkeyfields;
	result->nLeafFields = tableIndex->nfields;
	if (table->has_primary)
		result->nLeafFields += primary->nfields;
	else
		result->nLeafFields++;
	result->nNonLeafFields = result->nLeafFields;
	result->leafFields = (OTableField *) palloc0(sizeof(OTableField) *
												 result->nLeafFields);
	result->nonLeafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) *
														 result->nNonLeafFields);
	result->nKeyFields = tableIndex->nkeyfields;

	j = 0;
	mcxt = OGetIndexContext(result);
	old_mcxt = MemoryContextSwitchTo(mcxt);
	result->predicate = list_copy_deep(tableIndex->predicate);
	if (result->predicate)
		result->predicate_str = pstrdup(tableIndex->predicate_str);
	result->expressions = list_copy_deep(tableIndex->expressions);
	MemoryContextSwitchTo(old_mcxt);
	add_index_fields(result, table, tableIndex, &j, &result->nKeyFields, false);
	Assert(j <= tableIndex->nfields);
	add_index_fields(result, table, primary, &j, NULL, true);
	Assert(j <= result->nLeafFields);
	result->nLeafFields = j;
	result->nNonLeafFields = j;

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
	int			j;

	if (table->has_primary)
	{
		primary = &table->indices[0];
		Assert(primary->type == oIndexPrimary);
	}

	result->indexOids = table->toast_oids;
	result->indexType = oIndexToast;
	namestrcpy(&result->name, "toast");
	result->tableOids = table->oids;
	result->primaryIsCtid = !table->has_primary;
	result->compress = table->toast_compress;
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

	result->leafFields = (OTableField *) palloc0(sizeof(OTableField) *
												 result->nLeafFields);
	result->nonLeafFields = (OTableIndexField *) palloc0(sizeof(OTableIndexField) *
														 result->nNonLeafFields);

	j = 0;
	add_index_fields(result, table, primary, &j, NULL, true);
	make_builtin_field(&result->leafFields[j], &result->nonLeafFields[j],
					   INT2OID, "attnum", FirstLowInvalidHeapAttributeNumber,
					   INT2_BTREE_OPS_OID);
	j++;
	make_builtin_field(&result->leafFields[j], &result->nonLeafFields[j],
					   INT4OID, "offset", FirstLowInvalidHeapAttributeNumber,
					   INT4_BTREE_OPS_OID);
	j++;
	Assert(j <= result->nNonLeafFields);
	result->nUniqueFields = j;
	result->nNonLeafFields = j;
	make_builtin_field(&result->leafFields[j], NULL,
					   BYTEAOID, "data", FirstLowInvalidHeapAttributeNumber,
					   InvalidOid);
	j++;
	Assert(j <= result->nLeafFields);
	result->nLeafFields = j;

	return result;
}

void
free_o_index(OIndex *o_index)
{
	pfree(o_index->leafFields);
	pfree(o_index->nonLeafFields);
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
	appendBinaryStringInfo(&str,
						   (Pointer) o_index + offsetof(OIndex, tableOids),
						   offsetof(OIndex, leafFields) - offsetof(OIndex, tableOids));
	appendBinaryStringInfo(&str, (Pointer) o_index->leafFields,
						   o_index->nLeafFields * sizeof(o_index->leafFields[0]));
	appendBinaryStringInfo(&str, (Pointer) o_index->nonLeafFields,
						   o_index->nNonLeafFields * sizeof(o_index->nonLeafFields[0]));
	o_serialize_node((Node *) o_index->predicate, &str);
	if (o_index->predicate)
		o_serialize_string(o_index->predicate_str, &str);
	o_serialize_node((Node *) o_index->expressions, &str);

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

	len = offsetof(OIndex, leafFields) - offsetof(OIndex, tableOids);
	Assert((ptr - data) + len <= length);
	memcpy((Pointer) oIndex + offsetof(OIndex, tableOids), ptr, len);
	ptr += len;

	len = oIndex->nLeafFields * sizeof(OTableField);
	oIndex->leafFields = (OTableField *) palloc(len);
	Assert((ptr - data) + len <= length);
	memcpy(oIndex->leafFields, ptr, len);
	ptr += len;

	len = oIndex->nNonLeafFields * sizeof(OTableIndexField);
	oIndex->nonLeafFields = (OTableIndexField *) palloc(len);
	Assert((ptr - data) + len <= length);
	memcpy(oIndex->nonLeafFields, ptr, len);
	ptr += len;

	mcxt = OGetIndexContext(oIndex);
	old_mcxt = MemoryContextSwitchTo(mcxt);
	oIndex->predicate = (List *) o_deserialize_node(&ptr);
	if (oIndex->predicate)
		oIndex->predicate_str = o_deserialize_string(&ptr);
	oIndex->expressions = (List *) o_deserialize_node(&ptr);
	MemoryContextSwitchTo(old_mcxt);

	Assert((ptr - data) == length);

	return oIndex;
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
	else
	{
		OTableIndex *tableIndex = &table->indices[ixNum - (primaryIsCtid ? 1 : 0)];

		index = make_secondary_o_index(table, tableIndex);
	}
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

void
o_index_fill_descr(OIndexDescr *descr, OIndex *oIndex)
{
	int			i;
	int			maxTableAttnum = 0;
	uint16	   *primary_init_nfields = NULL;
	ListCell   *lc;
	MemoryContext mcxt,
				old_mcxt;

	memset(descr, 0, sizeof(*descr));
	descr->oids = oIndex->indexOids;
	descr->tableOids = oIndex->tableOids;
	descr->refcnt = 0;
	descr->valid = true;
	namestrcpy(&descr->name, oIndex->name.data);
	descr->leafTupdesc = o_table_fields_make_tupdesc(oIndex->leafFields,
													 oIndex->nLeafFields);
	if (oIndex->indexType == oIndexPrimary)
	{
		OTable	   *o_table = o_tables_get(descr->tableOids);

		if (o_table)
		{
			o_tupdesc_load_constr(descr->leafTupdesc, o_table, descr);
			primary_init_nfields = palloc(sizeof(*primary_init_nfields));
			*primary_init_nfields = o_table->primary_init_nfields;
			o_table_free(o_table);
		}
	}

	if (oIndex->indexType == oIndexPrimary)
	{
		descr->nonLeafTupdesc = CreateTemplateTupleDesc(oIndex->nNonLeafFields);
		if (oIndex->primaryIsCtid)
		{
			Assert(oIndex->nNonLeafFields == 1);
			Assert(oIndex->nonLeafFields[0].attnum == SelfItemPointerAttributeNumber);
			TupleDescCopyEntry(descr->nonLeafTupdesc, 1,
							   descr->leafTupdesc, 1);
		}
		else
		{
			for (i = 0; i < oIndex->nNonLeafFields; i++)
			{
				int			attnum = oIndex->nonLeafFields[i].attnum;

				Assert(attnum >= 0 && attnum < oIndex->nLeafFields);
				TupleDescCopyEntry(descr->nonLeafTupdesc,
								   i + 1,
								   descr->leafTupdesc,
								   attnum + 1);
			}
		}
	}
	else if (oIndex->indexType == oIndexRegular ||
			 oIndex->indexType == oIndexUnique)
	{
		Assert(oIndex->nNonLeafFields == oIndex->nLeafFields);
		descr->nonLeafTupdesc = CreateTupleDescCopy(descr->leafTupdesc);
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
	descr->unique = (oIndex->indexType == oIndexUnique ||
					 oIndex->indexType == oIndexPrimary);
	descr->nUniqueFields = oIndex->nUniqueFields;
	descr->nFields = oIndex->nNonLeafFields;

	descr->nKeyFields = oIndex->nKeyFields;
	descr->nIncludedFields = oIndex->nIncludedFields;
	for (i = 0; i < oIndex->nNonLeafFields; i++)
	{
		OIndexField *field = &descr->fields[i];
		OTableIndexField *iField = &oIndex->nonLeafFields[i];
		int			attnum = iField->attnum;
		bool		add_opclass = false;

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

		field->tableAttnum = attnum;
		maxTableAttnum = Max(maxTableAttnum, attnum);
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
		if (add_opclass)
			oFillFieldOpClassAndComparator(field, oIndex->tableOids.datoid,
										   iField->opclass);
	}

	mcxt = OGetIndexContext(descr);
	old_mcxt = MemoryContextSwitchTo(mcxt);
	descr->predicate = list_copy_deep(oIndex->predicate);
	if (descr->predicate)
		descr->predicate_str = pstrdup(oIndex->predicate_str);
	descr->expressions = list_copy_deep(oIndex->expressions);

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
	if (oIndex->indexType == oIndexPrimary)
	{
		descr->tbl_attnums = palloc0(sizeof(AttrNumberMap) * descr->nFields);
		for (i = 0; i < descr->nFields; i++)
		{
			descr->tbl_attnums[i].key = descr->fields[i].tableAttnum - 1;
			descr->tbl_attnums[i].value = i;
		}
		pg_qsort(descr->tbl_attnums, descr->nFields, sizeof(AttrNumberMap), attrnumber_cmp);
	}
	MemoryContextSwitchTo(old_mcxt);
	descr->econtext = CreateStandaloneExprContext();

	descr->maxTableAttnum = maxTableAttnum;

	descr->nPrimaryFields = oIndex->nPrimaryFields;
	memcpy(descr->primaryFieldsAttnums,
		   oIndex->primaryFieldsAttnums,
		   descr->nPrimaryFields * sizeof(descr->primaryFieldsAttnums[0]));
	descr->compress = oIndex->compress;

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

	oIndex = make_o_index(table, ixNum);
	oIndex->createOxid = oxid;
	key.oids = oIndex->indexOids;
	key.type = oIndex->indexType;
	key.offset = 0;
	data = serialize_o_index(oIndex, &len);
	free_o_index(oIndex);

	result = generic_toast_insert(&oIndicesToastAPI, (Pointer) &key, data, len,
								  oxid, csn, get_sys_tree(SYS_TREES_O_INDICES));
	pfree(data);
	return result;
}

bool
o_indices_del(OTable *table, OIndexNumber ixNum, OXid oxid, CommitSeqNo csn)
{
	OIndexChunkKey key;
	bool		result;
	OIndex	   *oIndex;

	oIndex = make_o_index(table, ixNum);
	key.oids = oIndex->indexOids;
	key.type = oIndex->indexType;
	key.offset = 0;
	free_o_index(oIndex);

	result = generic_toast_delete(&oIndicesToastAPI, (Pointer) &key,
								  oxid, csn, get_sys_tree(SYS_TREES_O_INDICES));
	return result;
}

OIndex *
o_indices_get(ORelOids oids, OIndexType type)
{
	OIndexChunkKey key;
	Size		dataLength;
	Pointer		result;
	OIndex	   *oIndex;

	key.type = type;
	key.oids = oids;
	key.offset = 0;

	result = generic_toast_get_any(&oIndicesToastAPI, (Pointer) &key,
								   &dataLength, COMMITSEQNO_NON_DELETED,
								   get_sys_tree(SYS_TREES_O_INDICES));

	if (result == NULL)
		return NULL;

	oIndex = deserialize_o_index(&key, result, dataLength);
	pfree(result);

	return oIndex;
}

bool
o_indices_update(OTable *table, OIndexNumber ixNum, OXid oxid, CommitSeqNo csn)
{
	OIndex	   *oIndex;
	OIndexChunkKey key;
	bool		result;
	Pointer		data;
	int			len;

	oIndex = make_o_index(table, ixNum);
	data = serialize_o_index(oIndex, &len);
	key.oids = oIndex->indexOids;
	key.type = oIndex->indexType;
	key.offset = 0;
	free_o_index(oIndex);

	systrees_modify_start();
	result = generic_toast_update(&oIndicesToastAPI, (Pointer) &key, data, len,
								  oxid, csn, get_sys_tree(SYS_TREES_O_INDICES));
	systrees_modify_end();

	pfree(data);

	return result;
}

bool
o_indices_find_table_oids(ORelOids indexOids, OIndexType type, CommitSeqNo csn,
						  ORelOids *tableOids)
{
	OIndexChunkKey key;
	Pointer		data;
	Size		dataSize;

	key.oids = indexOids;
	key.type = type;
	key.offset = 0;

	data = generic_toast_get_any(&oIndicesToastAPI, (Pointer) &key, &dataSize,
								 csn, get_sys_tree(SYS_TREES_O_INDICES));
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
	chunkKey.offset = 0;

	it = o_btree_iterator_create(desc, (Pointer) &chunkKey, BTreeKeyBound,
								 COMMITSEQNO_NON_DELETED, ForwardScanDirection);

	tuple = o_btree_iterator_fetch(it, NULL, NULL,
								   BTreeKeyNone, false, NULL);
	old_oids = oids;
	while (!O_TUPLE_IS_NULL(tuple))
	{
		OIndexChunk *chunk = (OIndexChunk *) tuple.data;
		ORelOids	tableOids;

		type = chunk->key.type;
		oids = chunk->key.oids;
		Assert(chunk->dataLength >= sizeof(tableOids));
		memcpy(&tableOids, chunk->data, sizeof(tableOids));
		Assert(chunk->key.offset == 0);
		Assert(ORelOidsIsValid(oids));
		Assert(!ORelOidsIsEqual(old_oids, oids));
		old_oids = oids;

		callback(type, oids, tableOids, arg);

		pfree(tuple.data);
		btree_iterator_free(it);

		oids.relnode += 1;		/* go to the next oid */
		chunkKey.oids = oids;
		chunkKey.type = type;
		chunkKey.offset = 0;

		it = o_btree_iterator_create(desc, (Pointer) &chunkKey, BTreeKeyBound,
									 COMMITSEQNO_NON_DELETED, ForwardScanDirection);
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
			return "regural";
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
	else if (!strncmp(s, "regural", len))
		return oIndexRegular;
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

	tuplestore_donestoring(tupstore);

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

	index = o_indices_get(oids, type);
	if (index == NULL)
		elog(ERROR, "unable to find orioledb index description.");

	max_column_str = strlen(column_str);
	max_type_str = strlen(type_str);
	max_collation_str = strlen(collation_str);
	for (i = 0; i < index->nLeafFields; i++)
	{
		OTableField *field = &index->leafFields[i];
		char	   *typename = o_get_type_name(field->typid, field->typmod);
		char	   *colname = o_get_collation_name(field->collation);

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
		OTableField *field = &index->leafFields[i];
		char	   *typename = o_get_type_name(field->typid, field->typmod);
		char	   *colname = o_get_collation_name(field->collation);

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
		OTableIndexField *nonLeafField = &index->nonLeafFields[i];
		OTableField *leafField;

		if (type == oIndexPrimary)
		{
			if (nonLeafField->attnum == SelfItemPointerAttributeNumber)
			{
				Assert(index->primaryIsCtid);
				leafField = &index->leafFields[0];
			}
			if (index->primaryIsCtid)
				leafField = &index->leafFields[nonLeafField->attnum + 1];
			else
				leafField = &index->leafFields[nonLeafField->attnum];
		}
		else
		{
			leafField = &index->leafFields[i];
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
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

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
