/*-------------------------------------------------------------------------
 *
 * checksum_block.inc.c
 *		Core algorithm for orioledb page checksums, private to io.c.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/checksum_block.inc.c
 *
 *-------------------------------------------------------------------------
 */

/* there is deliberately not an #ifndef CHECKSUM_BLOCK_INC_C here */

uint32		sums[N_SUMS];
uint32		result = 0;
uint32		i,
			j;

/* initialize partial checksums to their corresponding offsets */
memcpy(sums, checksumBaseOffsets, sizeof(checksumBaseOffsets));

/* main checksum calculation */
for (i = 0; i < len; i++)
	for (j = 0; j < N_SUMS; j++)
		CHECKSUM_COMP(sums[j], page->data[i][j]);

/* finally add in two rounds of zeroes for additional mixing */
for (i = 0; i < 2; i++)
	for (j = 0; j < N_SUMS; j++)
		CHECKSUM_COMP(sums[j], 0);

/* xor fold partial checksums together */
for (i = 0; i < N_SUMS; i++)
	result ^= sums[i];

return result;
