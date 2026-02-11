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
#define WR_PEEK(r, out) \
{ \
	WR_REQUIRE_SIZE(r, sizeof(*out)); \
	memcpy(out, r->ptr, sizeof(*out)); \
}
#define WR_SKIP(r, sz) \
{ \
	WR_REQUIRE_SIZE(r, sz); \
    r->ptr += sz; \
}

#endif							/* __WAL_READER_H__ */
