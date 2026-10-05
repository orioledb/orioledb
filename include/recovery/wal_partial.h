/*-------------------------------------------------------------------------
 *
 * wal_partial.h
 *		Declarations for WAL update records carrying the changed fields only.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal_partial.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WAL_PARTIAL_H__
#define __WAL_PARTIAL_H__

#include "orioledb.h"

#include "btree/btree.h"
#include "lib/stringinfo.h"

extern bool o_wal_partial_update_payload(OIndexDescr *primary,
										 uint32 version,
										 OTuple oldTuple, OTuple newTuple,
										 StringInfo buf);
extern uint32 o_wal_partial_update_get_version(Pointer payload);
extern OTuple o_wal_partial_update_get_key(Pointer payload);
extern bool o_wal_partial_update_build(OIndexDescr *primary, Pointer payload,
									   OTuple *newTuple);

#endif							/* __WAL_PARTIAL_H__ */
