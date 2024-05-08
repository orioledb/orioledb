/*-------------------------------------------------------------------------
 *
 * logical.h
 *		External declarations for logical decoding of OrioleDB tables.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/logical.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __LOGICAL_H__
#define __LOGICAL_H__

#include "btree/btree.h"
#include "recovery/internal.h"

#include "replication/decode.h"
#include "replication/logical.h"

extern void orioledb_decode(LogicalDecodingContext *ctx,
							XLogRecordBuffer *buf);

#endif							/* __LOGICAL_H__ */
