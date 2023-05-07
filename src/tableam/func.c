/*-------------------------------------------------------------------------
 *
 * func.c
 *		SQL functions implementation for orioledb module.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/func.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/check.h"
#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/page_chunks.h"
#include "catalog/indices.h"
#include "tableam/toast.h"
#include "tuple/format.h"
#include "utils/compress.h"

#include "pgstat.h"
#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_type_d.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_FUNCTION_INFO_V1(orioledb_tbl_structure);
PG_FUNCTION_INFO_V1(orioledb_idx_structure);
PG_FUNCTION_INFO_V1(orioledb_tbl_check);
PG_FUNCTION_INFO_V1(orioledb_compression_max_level);
PG_FUNCTION_INFO_V1(orioledb_tbl_compression_check);
PG_FUNCTION_INFO_V1(orioledb_tbl_indices);
PG_FUNCTION_INFO_V1(orioledb_relation_size);
PG_FUNCTION_INFO_V1(orioledb_tbl_are_indices_equal);
PG_FUNCTION_INFO_V1(orioledb_table_pages);

extern void log_btree(BTreeDescr *desc);

static void
o_tuple_print(TupleDesc tupDesc, OTupleFixedFormatSpec *spec,
			  FmgrInfo *outputFns, StringInfo buf, OTuple tup,
			  Datum *values, bool *nulls, bool printVersion)
{
	Form_pg_attribute atti;
	int			attnum,
				i;

	appendStringInfo(buf, "(");

	if (printVersion)
		appendStringInfo(buf, "(%u) ", o_tuple_get_version(tup));

	for (i = 0; i < tupDesc->natts; i++)
	{
		if (i > 0)
			appendStringInfo(buf, ", ");
		attnum = i + 1;
		values[i] = o_fastgetattr(tup, attnum, tupDesc, spec, &nulls[i]);
		if (nulls[i])
		{
			appendStringInfo(buf, "null");
		}
		else
		{
			atti = TupleDescAttr(tupDesc, i);
			if (!atti->attbyval && atti->attlen && !nulls[i])
			{
				Pointer		p = DatumGetPointer(values[i]);

				if (IS_TOAST_POINTER(p))
				{
					appendStringInfo(buf, "TOASTed");
					continue;
				}
			}
			appendStringInfo(buf, "'%s'",
							 OutputFunctionCall(&outputFns[i], values[i]));
		}
	}

	appendStringInfo(buf, ")");
}

static void
idx_key_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	TuplePrintOpaque *opaque = (TuplePrintOpaque *) arg;

	o_tuple_print(opaque->keyDesc, opaque->keySpec, opaque->keyOutputFns, buf,
				  tup, opaque->values, opaque->nulls, false);
}

static void
idx_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	TuplePrintOpaque *opaque = (TuplePrintOpaque *) arg;

	o_tuple_print(opaque->desc, opaque->spec, opaque->outputFns, buf,
				  tup, opaque->values, opaque->nulls, opaque->printRowVersion);
}

void
init_print_options(BTreePrintOptions *printOptions, VarChar *optionsArg)
{
	int			i;
	int			optionsSize = VARSIZE(optionsArg) - VARHDRSZ;
	char	   *options = (char *) VARDATA(optionsArg);

	/* parse options argument and update options */
	for (i = 0; i < optionsSize; i++)
	{
		switch (options[i])
		{
			case 'n':
				printOptions->pagePrintType = BTreePrintRelative;
				break;
			case 'C':
				printOptions->csnPrintType = BTreePrintAbsolute;
				break;
			case 'c':
				printOptions->csnPrintType = BTreePrintRelative;
				break;
			case 'b':
				printOptions->backendIdPrintType = BTreePrintAbsolute;
				break;
			case 'U':
				printOptions->undoLogLocationPrintType = BTreePrintAbsolute;
				break;
			case 'u':
				printOptions->undoLogLocationPrintType = BTreePrintRelative;
				break;
			case 'e':
				printOptions->idsPrintType = BTreePrintRelative;
				break;
			case 'i':
				printOptions->changeCountPrintType = BTreePrintAbsolute;
				break;
			case 'K':
				printOptions->checkpointNumPrintType = BTreePrintAbsolute;
				break;
			case 'k':
				printOptions->checkpointNumPrintType = BTreePrintRelative;
				break;
			case 'S':
				printOptions->printStateValue = true;
				break;
			case 'v':
				printOptions->printRowVersion = true;
				break;
			case 'o':
				printOptions->printFileOffset = true;
				break;
			default:
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								(errmsg("invalid option '%c'", options[i]))));
				break;
		}
	}
}

static void
print_unloaded_tree(StringInfoData *buf, BTreeDescr *td, const char *treeName,
					BTreePrintOptions *printOptions)
{
	char	   *prev_chkp_fname;
	File		prev_chkp_file;
	CheckpointFileHeader file_header = {0};
	SeqBufTag	prev_chkp_tag;
	EvictedTreeData *evicted_data;

	memset(&prev_chkp_tag, 0, sizeof(prev_chkp_tag));
	prev_chkp_tag.type = td->type;
	prev_chkp_tag.datoid = td->oids.datoid;
	prev_chkp_tag.relnode = td->oids.relnode;
	prev_chkp_tag.num = checkpoint_state->lastCheckpointNumber;
	prev_chkp_tag.type = 'm';

	evicted_data = read_evicted_data(td->oids,
									 checkpoint_state->lastCheckpointNumber + 1);

	/*
	 * If found in eviction hash then use cached file_header to initialize
	 * tree
	 */
	if (evicted_data != NULL)
	{
		file_header = evicted_data->file_header;
		pfree(evicted_data);
	}
	else
	{
		prev_chkp_fname = get_seq_buf_filename(&prev_chkp_tag);
		prev_chkp_file = PathNameOpenFile(prev_chkp_fname, O_RDONLY | PG_BINARY);
		if (prev_chkp_file > 0)
		{
			OFileRead(prev_chkp_file, (Pointer) &file_header, sizeof(file_header), 0, WAIT_EVENT_SLRU_READ);
			FileClose(prev_chkp_file);
		}
		else
		{
			file_header.rootDownlink = InvalidDiskDownlink;
		}
	}

	appendStringInfo(buf, "Index %s: not loaded", treeName);
	if (printOptions->idsPrintType == BTreePrintAbsolute)
	{
		appendStringInfo(buf, ", ");
		appendStringInfo(buf, "datoid = %d, relnode = %d, ",
						 td->oids.datoid, td->oids.relnode);
		if (DiskDownlinkIsValid(file_header.rootDownlink))
			appendStringInfo(buf, "rootOffset = " UINT64_FORMAT ", %u",
							 DOWNLINK_GET_DISK_OFF(file_header.rootDownlink),
							 DOWNLINK_GET_DISK_LEN(file_header.rootDownlink));
		else
			appendStringInfo(buf, "rootOffset is invalid");
	}
	appendStringInfo(buf, "\n");
}

static void
tree_structure(StringInfo buf,
			   OIndexDescr *id,
			   BTreePrintOptions printOptions,
			   int depth)
{
	int			i;
	TuplePrintOpaque opaque;
	SharedRootInfoKey key = {0};
	SharedRootInfo *sharedRootInfo = NULL;
	BTreeDescr *td;
	const char *treeName;

	opaque.desc = id->leafTupdesc;
	opaque.spec = &id->leafSpec;
	opaque.keyDesc = id->nonLeafTupdesc;
	opaque.keySpec = &id->nonLeafSpec;
	opaque.values = (Datum *) palloc(sizeof(Datum) * opaque.desc->natts);
	opaque.nulls = (bool *) palloc(sizeof(bool) * opaque.desc->natts);
	opaque.outputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.desc->natts);
	opaque.keyOutputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.keyDesc->natts);
	opaque.printRowVersion = printOptions.printRowVersion;

	for (i = 0; i < opaque.desc->natts; i++)
	{
		Oid			output;
		bool		varlena;

		getTypeOutputInfo(opaque.desc->attrs[i].atttypid, &output, &varlena);
		fmgr_info(output, &opaque.outputFns[i]);
	}

	for (i = 0; i < opaque.keyDesc->natts; i++)
	{
		Oid			output;
		bool		varlena;

		getTypeOutputInfo(opaque.keyDesc->attrs[i].atttypid, &output, &varlena);
		fmgr_info(output, &opaque.keyOutputFns[i]);
	}

	td = &id->desc;
	treeName = id->name.data;

	key.datoid = td->oids.datoid;
	key.relnode = td->oids.relnode;
	sharedRootInfo = o_find_shared_root_info(&key);

	if (sharedRootInfo != NULL && !sharedRootInfo->placeholder)
	{
		o_btree_load_shmem(td);

		appendStringInfo(buf, "Index %s contents\n", treeName);
		if (td->type != oIndexToast)
			o_print_btree_pages(td, buf, idx_key_print, idx_tup_print,
								(Pointer) &opaque, &printOptions, depth);
		else
			o_print_btree_pages(td, buf, o_toast_key_print, o_toast_tup_print,
								(Pointer) &opaque, &printOptions, depth);
	}
	else
	{
		print_unloaded_tree(buf, td, treeName, &printOptions);
	}
}

Datum
orioledb_tbl_structure(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	VarChar    *optionsArg = (VarChar *) PG_GETARG_VARCHAR_P(1);
	int			depth = PG_GETARG_INT32(2);
	OTableDescr *descr;
	Relation	rel;
	text	   *result;
	int			treen;
	StringInfoData buf;
	BTreePrintOptions printOptions = {0};

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);

	if (!rel)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u does not exists", relid)));

	descr = relation_get_descr(rel);

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	initStringInfo(&buf);

	init_print_options(&printOptions, optionsArg);

	/* index trees + toast tree */
	for (treen = 0; treen < descr->nIndices; treen++)
		tree_structure(&buf, descr->indices[treen], printOptions, depth);
	tree_structure(&buf, descr->toast, printOptions, depth);

	result = cstring_to_text(buf.data);
	relation_close(rel, AccessShareLock);

	PG_RETURN_POINTER(result);
}

Datum
orioledb_idx_structure(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	const char *treeName = text_to_cstring(PG_GETARG_TEXT_PP(1));
	VarChar    *optionsArg = (VarChar *) PG_GETARG_VARCHAR_P(2);
	int			depth = PG_GETARG_INT32(3);
	OTableDescr *descr;
	Relation	rel;
	text	   *result;
	int			treen;
	StringInfoData buf;
	BTreePrintOptions printOptions = {0};

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);

	descr = relation_get_descr(rel);
	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	initStringInfo(&buf);

	init_print_options(&printOptions, optionsArg);

	/* index trees + toast tree */
	for (treen = 0; treen < descr->nIndices; treen++)
	{
		if (!strcmp(treeName, NameStr(descr->indices[treen]->name)))
			tree_structure(&buf, descr->indices[treen], printOptions, depth);
	}
	if (!strcmp(treeName, NameStr(descr->toast->name)))
		tree_structure(&buf, descr->toast, printOptions, depth);

	result = cstring_to_text(buf.data);
	relation_close(rel, AccessShareLock);

	PG_RETURN_POINTER(result);
}

void
log_btree(BTreeDescr *desc)
{
	BTreePrintOptions printOptions = {
		.pagePrintType = BTreePrintAbsolute,
		.csnPrintType = BTreePrintAbsolute,
		.undoLogLocationPrintType = BTreePrintAbsolute,
		.idsPrintType = BTreePrintAbsolute,
		.changeCountPrintType = BTreePrintAbsolute,
		.checkpointNumPrintType = BTreePrintAbsolute,
		.printRowVersion = true,
		.printStateValue = true,
		.printFileOffset = true
	};
	static Oid	typeoids[] = {TIDOID, TEXTOID, INT4OID, INT2OID, BYTEAOID};
	static Oid	outoids[] = {F_TIDOUT, F_TEXTOUT, F_INT4OUT, F_INT2OUT, F_BYTEAOUT};
	StringInfoData buf;

	initStringInfo(&buf);
	if (!IS_SYS_TREE_OIDS(desc->oids))
	{
		OIndexDescr *id = (OIndexDescr *) desc->arg;
		TuplePrintOpaque opaque;
		int			i,
					j;

		opaque.desc = id->leafTupdesc;
		opaque.spec = &id->leafSpec;
		opaque.keyDesc = id->nonLeafTupdesc;
		opaque.keySpec = &id->nonLeafSpec;
		opaque.values = (Datum *) palloc(sizeof(Datum) * opaque.desc->natts);
		opaque.nulls = (bool *) palloc(sizeof(bool) * opaque.desc->natts);
		opaque.outputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.desc->natts);
		opaque.keyOutputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.keyDesc->natts);
		opaque.printRowVersion = printOptions.printRowVersion;
		for (i = 0; i < opaque.desc->natts; i++)
		{
			Oid			output = InvalidOid;
			bool		varlena;

			for (j = 0; j < sizeof(typeoids) / sizeof(typeoids[0]); j++)
				if (typeoids[j] == opaque.desc->attrs[i].atttypid)
					output = outoids[j];

			if (output == InvalidOid)
				getTypeOutputInfo(opaque.desc->attrs[i].atttypid, &output, &varlena);

			fmgr_info(output, &opaque.outputFns[i]);
		}

		for (i = 0; i < opaque.keyDesc->natts; i++)
		{
			Oid			output;
			bool		varlena;

			for (j = 0; j < sizeof(typeoids) / sizeof(typeoids[0]); j++)
				if (typeoids[j] == opaque.keyDesc->attrs[i].atttypid)
					output = outoids[j];

			if (output == InvalidOid)
				getTypeOutputInfo(opaque.keyDesc->attrs[i].atttypid, &output, &varlena);

			fmgr_info(output, &opaque.keyOutputFns[i]);
		}
		o_print_btree_pages(desc, &buf, idx_key_print, idx_tup_print,
							(Pointer) &opaque, &printOptions, ORIOLEDB_MAX_DEPTH);
	}
	else
	{
		int			num = desc->oids.relnode;

		o_print_btree_pages(get_sys_tree(num), &buf,
							sys_tree_key_print(get_sys_tree(num)),
							sys_tree_tup_print(get_sys_tree(num)),
							NULL, &printOptions,
							ORIOLEDB_MAX_DEPTH);
	}

	elog(LOG, "%s", buf.data);
}

static void
table_pages_walk_page(BTreeDescr *desc, BlockNumber blkno,
					  TupleDesc tupdesc, Tuplestorestate *tupstore)
{
	Datum		values[4];
	bool		nulls[4];
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *pageHdr = (BTreePageHeader *) p;
	int			j = 0;
	BTreePageItemLocator loc;

	values[j] = Int64GetDatum(blkno);
	nulls[j] = false;
	j++;
	values[j] = Int32GetDatum(PAGE_GET_LEVEL(p));
	nulls[j] = false;
	j++;
	if (OInMemoryBlknoIsValid(pageHdr->rightLink))
	{
		values[j] = Int64GetDatum(pageHdr->rightLink);
		nulls[j] = false;
	}
	else
	{
		nulls[j] = true;
	}
	j++;
	if (!O_PAGE_IS(p, RIGHTMOST))
	{
		JsonbParseState *state = NULL;
		JsonbValue *jsval;
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, p);
		jsval = o_btree_key_to_jsonb(desc, hikey, &state);
		values[j] = PointerGetDatum(JsonbValueToJsonb(jsval));
		nulls[j] = false;
	}
	else
	{
		nulls[j] = true;
	}
	j++;
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	if (O_PAGE_IS(p, LEAF))
		return;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		BTreeNonLeafTuphdr *hdr;

		hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
		if (DOWNLINK_IS_IN_MEMORY(hdr->downlink))
			table_pages_walk_page(desc, DOWNLINK_GET_IN_MEMORY_BLKNO(hdr->downlink),
								  tupdesc, tupstore);
	}

}

Datum
orioledb_table_pages(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid			relid = PG_GETARG_OID(0);
	bool		randomAccess;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext oldcontext;
	Relation	rel;
	OTableDescr *descr;
	int			treen;
	AttrNumber	attnum;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupdesc = CreateTemplateTupleDesc(4);
	attnum = (AttrNumber) 1;
	TupleDescInitEntry(tupdesc, attnum, "blkno", INT8OID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "level", INT4OID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "rightlink", INT8OID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "hikey", JSONBOID, -1, 0);
	attnum++;

	randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
	tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);;

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	for (treen = 0; treen < descr->nIndices + 1; treen++)
	{
		BTreeDescr *td;
		SharedRootInfoKey key = {0};
		SharedRootInfo *sharedRootInfo = NULL;

		if (treen < descr->nIndices)
			td = &descr->indices[treen]->desc;
		else
			td = &descr->toast->desc;

		key.datoid = td->oids.datoid;
		key.relnode = td->oids.relnode;
		sharedRootInfo = o_find_shared_root_info(&key);

		if (sharedRootInfo == NULL || sharedRootInfo->placeholder)
			continue;
		o_btree_load_shmem(td);

		table_pages_walk_page(td, td->rootInfo.rootPageBlkno, tupdesc, tupstore);
	}

	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}

Datum
orioledb_tbl_are_indices_equal(PG_FUNCTION_ARGS)
{
	Oid			idx_oid1 = PG_GETARG_OID(0),
				idx_oid2 = PG_GETARG_OID(1);
	OTableDescr *descr1,
			   *descr2;
	Relation	idx1,
				idx2,
				tbl1,
				tbl2;
	bool		are_equal = true;
	int			i;
	OIndexDescr *td1,
			   *td2;
	BTreeIterator *iter1,
			   *iter2;
	OIndexNumber ix_num1,
				ix_num2;

	orioledb_check_shmem();

	idx1 = index_open(idx_oid1, AccessShareLock);
	idx2 = index_open(idx_oid2, AccessShareLock);
	tbl1 = table_open(idx1->rd_index->indrelid, AccessShareLock);
	tbl2 = table_open(idx2->rd_index->indrelid, AccessShareLock);
	descr1 = relation_get_descr(tbl1);
	descr2 = relation_get_descr(tbl2);;

	ix_num1 = o_find_ix_num_by_name(descr1, idx1->rd_rel->relname.data);
	ix_num2 = o_find_ix_num_by_name(descr2, idx2->rd_rel->relname.data);

	relation_close(tbl1, AccessShareLock);
	relation_close(tbl2, AccessShareLock);
	relation_close(idx1, AccessShareLock);
	relation_close(idx2, AccessShareLock);

	if (ix_num1 == InvalidIndexNumber || ix_num2 == InvalidIndexNumber)
		elog(ERROR, "Invalid indexes");

	td1 = descr1->indices[ix_num1];
	td2 = descr2->indices[ix_num2];
	o_btree_load_shmem(&td1->desc);
	o_btree_load_shmem(&td2->desc);

	are_equal = td1->leafTupdesc->natts == td2->leafTupdesc->natts;

	for (i = 0; are_equal && i < td1->leafTupdesc->natts; i++)
	{
		are_equal = td1->leafTupdesc->attrs[i].atttypid ==
			td2->leafTupdesc->attrs[i].atttypid;
	}

	if (are_equal)
	{
		iter1 = o_btree_iterator_create(&td1->desc, NULL, BTreeKeyNone,
										COMMITSEQNO_INPROGRESS, ForwardScanDirection);
		iter2 = o_btree_iterator_create(&td2->desc, NULL, BTreeKeyNone,
										COMMITSEQNO_INPROGRESS, ForwardScanDirection);
		while (are_equal)
		{
			OTuple		tuple1,
						tuple2;

			tuple1 = o_btree_iterator_fetch(iter1, NULL, NULL, BTreeKeyNone, true, NULL);
			tuple2 = o_btree_iterator_fetch(iter2, NULL, NULL, BTreeKeyNone, true, NULL);

			if (O_TUPLE_IS_NULL(tuple1) && O_TUPLE_IS_NULL(tuple2))
				break;

			are_equal = !O_TUPLE_IS_NULL(tuple1) && !O_TUPLE_IS_NULL(tuple2) &&
				o_btree_cmp(&td1->desc,
							&tuple1, BTreeKeyLeafTuple,
							&tuple2, BTreeKeyLeafTuple) == 0;

			if (!O_TUPLE_IS_NULL(tuple1))
				pfree(tuple1.data);

			if (!O_TUPLE_IS_NULL(tuple2))
				pfree(tuple2.data);
		}

		btree_iterator_free(iter1);
		btree_iterator_free(iter2);
	}

	PG_RETURN_BOOL(are_equal);
}

Datum
orioledb_tbl_check(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	bool		force_map_check = PG_GETARG_OID(1);
	Relation	rel;
	OTableDescr *descr;
	bool		result = true;
	int			i;

	orioledb_check_shmem();

	/*
	 * ExclusiveLock helps to avoid changes in map/tmp files and concurrent
	 * eviction by bgwriter
	 */
	rel = relation_open(relid, AccessExclusiveLock);
	descr = relation_get_descr(rel);

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	for (i = 0; i < descr->nIndices; i++)
	{
		o_btree_load_shmem(&descr->indices[i]->desc);
		result = check_btree(&descr->indices[i]->desc, force_map_check);

		if (result == false)
			break;
	}
	relation_close(rel, AccessExclusiveLock);

	PG_RETURN_BOOL(result);
}

Datum
orioledb_compression_max_level(PG_FUNCTION_ARGS)
{
	int			max_lvl = o_compress_max_lvl();

	PG_RETURN_INT16(max_lvl);
}

Datum
orioledb_tbl_compression_check(PG_FUNCTION_ARGS)
{
	BTreeCompressStats stats;
	OTableDescr *descr;
	StringInfoData result;
	ArrayType  *array;
	int32	   *values;
	int			compression_lvl = PG_GETARG_INT16(0),
				i,
				j,
				narray,
				next_from;
	Oid			relid = PG_GETARG_OID(1);
	Relation	rel;

	Assert(PG_NARGS() == 3);

	/* checks compression lvl arg */
	if (compression_lvl < 0 || compression_lvl > o_compress_max_lvl())
		elog(ERROR, "Compression level must be between 0 and %d", o_compress_max_lvl());

	/* checks relation arg */
	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);
	relation_close(rel, AccessShareLock);

	if (descr == NULL)
		elog(ERROR, "orioledb relation not found.");

	/* checks range array arg */
	if (PG_ARGISNULL(2))
		elog(ERROR, "ranges array must be not NULL");

	array = PG_GETARG_ARRAYTYPE_P(2);
	if (array_contains_nulls(array))
		elog(ERROR, "ranges array must not contain nulls");

	narray = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	values = (int32 *) ARR_DATA_PTR(array);

	/* fills stats */
	stats.errors = 0;
	stats.oversize = 0;
	stats.totalSize = 0;
	stats.totalCompressedSize = 0;
	stats.nranges = narray + 1;
	stats.ranges = palloc(sizeof(BTreeCompressRange) * stats.nranges);

	/* fills ranges */
	next_from = 0;
	for (i = 0; i < narray; i++)
	{
		if (*values <= 0 || *values >= ORIOLEDB_BLCKSZ)
			elog(ERROR, "range value must be between %d and %d", 0, ORIOLEDB_BLCKSZ);

		if (*values <= next_from)
			elog(ERROR, "range array must be sorted ascending");

		stats.ranges[i].from = next_from;
		next_from = *values++;
		stats.ranges[i].to = next_from - 1;
		stats.ranges[i].leaf_count = 0;
		stats.ranges[i].node_count = 0;
	}
	stats.ranges[narray].from = next_from;
	stats.ranges[narray].to = ORIOLEDB_BLCKSZ;
	stats.ranges[narray].leaf_count = 0;
	stats.ranges[narray].node_count = 0;

	/* collect stats for each BTree loop */
	initStringInfo(&result);
	for (i = 0; i <= descr->nIndices; i++)
	{
		BTreeDescr *td;
		const char *treeName;

		if (i < descr->nIndices)
		{
			td = &descr->indices[i]->desc;
			treeName = descr->indices[i]->name.data;
		}
		else
		{
			td = &descr->toast->desc;
			treeName = "toast";
		}

		check_btree_compression(td, &stats, compression_lvl);

		if (i > 0)
			appendStringInfo(&result, "\n\n");
		appendStringInfo(&result, "Compression check for index %s\n", treeName);
		appendStringInfo(&result, "Errors %d, oversize %d\n", stats.errors, stats.oversize);
		appendStringInfo(&result, "Total size = " INT64_FORMAT "\n", stats.totalSize);
		appendStringInfo(&result, "Total compressed size = " INT64_FORMAT "\n", stats.totalCompressedSize);
		appendStringInfo(&result, "Ratio = %lf\n", (double) stats.totalCompressedSize / (double) stats.totalSize);

		/* nodes */
		appendStringInfo(&result, "\nCompressed pages size for nodes:\n");
		for (j = 0; j < stats.nranges; j++)
		{
			appendStringInfo(&result, "%4d - %4d = %d nodes\n",
							 stats.ranges[j].from,
							 stats.ranges[j].to,
							 stats.ranges[j].node_count);
		}

		/* leafs */
		appendStringInfo(&result, "\nCompressed pages size for leafs:\n");
		for (j = 0; j < stats.nranges; j++)
		{
			appendStringInfo(&result, "%4d - %4d = %d leafs\n",
							 stats.ranges[j].from,
							 stats.ranges[j].to,
							 stats.ranges[j].leaf_count);
		}

		/* summary */
		appendStringInfo(&result, "\nCompressed pages size summary:\n");
		for (j = 0; j < stats.nranges; j++)
		{
			appendStringInfo(&result, "%4d - %4d = %d pages\n",
							 stats.ranges[j].from,
							 stats.ranges[j].to,
							 stats.ranges[j].node_count + stats.ranges[j].leaf_count);
		}

		/* reset stats before next BTree */
		for (j = 0; j < stats.nranges; j++)
		{
			stats.ranges[j].leaf_count = 0;
			stats.ranges[j].node_count = 0;
		}
		stats.oversize = 0;
		stats.errors = 0;
		stats.totalSize = 0;
		stats.totalCompressedSize = 0;
	}

	pfree(stats.ranges);
	PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

Datum
orioledb_tbl_indices(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	OTableDescr *descr;
	StringInfoData buf;
	text	   *result;
	int			i,
				j;

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);
	if (descr == NULL)
	{
		relation_close(rel, AccessShareLock);
		elog(ERROR, "orioledb relation not found");
	}

	initStringInfo(&buf);

	for (i = 0; i < descr->nIndices; i++)
	{
		OIndexDescr *ct = descr->indices[i];
		int			nonLeafSize = ct->nonLeafTupdesc->natts;
		int			leafSize = ct->leafTupdesc->natts;
		bool		primary = i == PrimaryIndexNumber;

		appendStringInfo(&buf, "Index %s\n", ct->name.data);
		appendStringInfo(&buf, "    Index type: %s", primary ? "primary" : "secondary");
		appendStringInfo(&buf, "%s", ct->unique ? ", unique" : "");
		if (OCompressIsValid(ct->compress))
			appendStringInfo(&buf, ", compression = %d", ct->compress);
		appendStringInfo(&buf, "%s\n", primary && ct->primaryIsCtid ? ", ctid" : "");
		if (ct->predicate)
			appendStringInfo(&buf, "    Predicate: %s\n", ct->predicate_str);
		appendStringInfo(&buf, "    Leaf tuple size: %d, non-leaf tuple size: %d\n",
						 leafSize, nonLeafSize);
		appendStringInfo(&buf, "    Non-leaf tuple fields: ");
		for (j = 0; j < nonLeafSize; j++)
		{
			appendStringInfo(&buf, "%s", TupleDescAttr(ct->nonLeafTupdesc, j)->attname.data);
			if (j + 1 != nonLeafSize)
				appendStringInfo(&buf, ", ");
		}
		appendStringInfo(&buf, "\n");
		if (ct->desc.type != oIndexPrimary)
		{
			appendStringInfo(&buf, "    Leaf tuple fields: ");
			for (j = 0; j < leafSize; j++)
			{
				appendStringInfo(&buf, "%s", TupleDescAttr(ct->leafTupdesc, j)->attname.data);
				if (j + 1 != nonLeafSize)
					appendStringInfo(&buf, ", ");
			}
			appendStringInfo(&buf, "\n");
		}
	}

	relation_close(rel, AccessShareLock);

	result = cstring_to_text(buf.data);
	PG_RETURN_POINTER(result);
}

Datum
orioledb_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	int			i;
	int64		result = 0;
	BTreeDescr *td;
	OTableDescr *descr;

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);;

	for (i = 0; i < descr->nIndices + 1; i++)
	{
		td = i != descr->nIndices ? &descr->indices[i]->desc : &descr->toast->desc;
		o_btree_load_shmem(td);

		result += (int64) TREE_NUM_LEAF_PAGES(td) * (int64) ORIOLEDB_BLCKSZ;
	}

	relation_close(rel, AccessShareLock);
	PG_RETURN_INT64(result);
}
