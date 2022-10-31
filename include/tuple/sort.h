/*-------------------------------------------------------------------------
 *
 * sort.h
 * 		Declarations for implementation of orioledb tuple sorting
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tuple/sort.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TUPLE_SORT_H
#define __TUPLE_SORT_H

#include "tableam/descr.h"

extern Tuplesortstate *tuplesort_begin_orioledb_index(OIndexDescr *idx,
													  int workMem,
													  bool randomAccess,
													  SortCoordinate coordinate);
extern Tuplesortstate *tuplesort_begin_orioledb_toast(OIndexDescr *toast,
													  OIndexDescr *primary,
													  int workMem,
													  bool randomAccess,
													  SortCoordinate coordinate);
extern OTuple tuplesort_getotuple(Tuplesortstate *state, bool forward);
extern void tuplesort_putotuple(Tuplesortstate *state, OTuple tup);

#endif							/* __TUPLE_SORT_H */
