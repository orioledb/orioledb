/*-------------------------------------------------------------------------
 *
 * checkpoint.h
 *		Declarations for S3 checkpointing.
 *
 * Copyright (c) 2023, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/checkpoint.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_CHECKPOINT_H__
#define __S3_CHECKPOINT_H__

extern void s3_perform_backup(S3TaskLocation location);

#endif							/* __S3_CHECKPOINT_H__ */
