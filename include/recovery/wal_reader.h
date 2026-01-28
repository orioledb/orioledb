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

typedef struct WalReader
{
	Pointer		ptr;
	Pointer		end;
	uint16		wal_version;
	uint8		wal_flags;

} WalReader;

typedef enum WalParseStatus
{
	WALPARSE_OK = 0,
	WALPARSE_EOF,				/* not enough bytes */
	WALPARSE_BAD_TYPE,
	WALPARSE_BAD_VERSION,
	WALPARSE_INTERNAL

} WalParseStatus;

static inline size_t
wr_remaining(const WalReader *r)
{
	return (size_t) (r->end - r->ptr);
}

#define WR_REQUIRE(r, nbytes) \
	do { if (wr_remaining((r)) < (size_t)(nbytes)) return WALPARSE_EOF; } while (0)

#define WR_READ(r, out) \
{ \
	WR_REQUIRE(r, sizeof(*out)); \
	if (out) \
        memcpy(out, r->ptr, sizeof(*out)); \
    r->ptr += sizeof(*out); \
}
#define WR_PEEK(r, out) \
{ \
	WR_REQUIRE(r, sizeof(*out)); \
	if (out) \
        memcpy(out, r->ptr, sizeof(*out)); \
}
#define WR_SKIP(r, sz) \
{ \
	WR_REQUIRE(r, sz); \
    r->ptr += sz; \
}

#endif							/* __WAL_READER_H__ */
