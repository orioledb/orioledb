/*-------------------------------------------------------------------------
 *
 * compress.c
 *		Compression functions for BTree pages. Wrapper for libzstd.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/compress.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "utils/compress.h"

#include "utils/elog.h"
#include "utils/memdebug.h"

#include <zstd.h>

static ZSTD_CCtx *zstd_cctx = NULL;
static ZSTD_DCtx *zstd_dctx = NULL;
static size_t zstd_dst_size;
static Pointer zstd_dst = NULL;

/*
 * Initializes compression context.
 */
void
o_compress_init(void)
{
	zstd_cctx = ZSTD_createCCtx();
	zstd_dctx = ZSTD_createDCtx();
	zstd_dst_size = ZSTD_compressBound(ORIOLEDB_BLCKSZ);
	zstd_dst = malloc(zstd_dst_size);

	/*
	 * It helps to avoid Valgrind uninitialized bytes error inside
	 * OFileWrite().
	 *
	 * We write compressed pages to a file with size % ORIOLEDB_COMP_BLCKSZ ==
	 * 0, where size >= compressed page size. So it's normal to write
	 * uninitialized bytes.
	 */
	VALGRIND_MAKE_MEM_DEFINED(zstd_dst, ORIOLEDB_BLCKSZ);
}

/*
 * Compresses a BTree page.
 */
Pointer
o_compress_page(Pointer page, size_t *size, OCompress lvl)
{
	VALGRIND_CHECK_MEM_IS_DEFINED(page, ORIOLEDB_BLCKSZ);
	*size = ZSTD_compressCCtx(zstd_cctx,
							  zstd_dst, zstd_dst_size,
							  page, ORIOLEDB_BLCKSZ,
							  lvl);
	VALGRIND_MAKE_MEM_DEFINED(zstd_dst, *size);
	if (ZSTD_isError(*size))
	{
		elog(PANIC,
			 "Unable to compress page, reason: %s", ZSTD_getErrorName(*size));
	}

	return zstd_dst;
}

/*
 * Decompresses a BTree page.
 */
void
o_decompress_page(Pointer src, size_t size, Pointer page)
{
	size_t		result;

	result = ZSTD_decompressDCtx(zstd_dctx,
								 page, ORIOLEDB_BLCKSZ,
								 src, size);
	if (ZSTD_isError(result))
	{
		elog(PANIC,
			 "Unable to decompress page, reason: %s", ZSTD_getErrorName(result));
	}

	Assert(result == ORIOLEDB_BLCKSZ);
}

/*
 * Returns max orioledb compression level.
 */
OCompress
o_compress_max_lvl()
{
	return ZSTD_maxCLevel();
}
