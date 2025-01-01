/*-------------------------------------------------------------------------
 *
 * compress.h
 *		Compression functions for BTree pages.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/compress.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __COMPRESS_H__
#define __COMPRESS_H__

extern void o_compress_init(void);
extern Pointer o_compress_page(Pointer page, size_t *size, OCompress lvl);
extern void o_decompress_page(Pointer src, size_t size, Pointer page);
extern OCompress o_compress_max_lvl(void);
extern void validate_compress(OCompress compress, char *prefix);

#endif							/* __COMPRESS_H__ */
