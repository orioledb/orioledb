/*-------------------------------------------------------------------------
 *
 * key_bitmap.c
 *		Routines for bitmap scan of orioledb table
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/key_bitmap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/page_chunks.h"
#include "tableam/bitmap_scan.h"
#include "tableam/index_scan.h"
#include "tuple/slot.h"

#include "lib/rbtree.h"

static int	bm_rbt_comparator(const RBTNode *a, const RBTNode *b, void *arg);
static void bm_rbt_combiner(RBTNode *existing, const RBTNode *newdata, void *arg);
static RBTNode *bm_rbt_allocfunc(void *arg);
static void bm_rbt_freefunc(RBTNode *x, void *arg);

#define HIGH_PART_MASK	(0xFFFFFFFFFFFFFC00)
#define LOW_PART_MASK	(0x00000000000003FF)
#define BITMAP_SIZE		0x80

typedef struct
{
	RBTNode		rbtnode;
	uint64		key;
	uint8	   *bitmap;
} OKeyBitmapRBTNode;

RBTree *
o_keybitmap_create(void)
{
	return rbt_create(sizeof(OKeyBitmapRBTNode),
					  bm_rbt_comparator,
					  bm_rbt_combiner,
					  bm_rbt_allocfunc,
					  bm_rbt_freefunc,
					  NULL);
}

void
o_keybitmap_insert(RBTree *rbtree, uint64 value)
{
	OKeyBitmapRBTNode node;
	bool		is_new;

	node.key = value;
	node.bitmap = NULL;
	(void) rbt_insert(rbtree, &node.rbtnode, &is_new);
}

bool
o_keybitmap_test(RBTree *rbtree, uint64 value)
{
	OKeyBitmapRBTNode node;
	OKeyBitmapRBTNode *found;

	node.key = value;
	node.bitmap = NULL;
	found = (OKeyBitmapRBTNode *) rbt_find(rbtree, &node.rbtnode);
	if (!found)
		return false;

	if (found->bitmap)
	{
		int			offset = (value & LOW_PART_MASK);

		if (found->bitmap[offset >> 3] & (1 << (offset & 7)))
			return true;
		else
			return false;
	}
	else
	{
		return (found->key == value);
	}
}

bool
o_keybitmap_range_is_valid(RBTree *rbtree, uint64 low, uint64 high)
{
	OKeyBitmapRBTNode lowNode;
	OKeyBitmapRBTNode *node;
	int			i,
				iStart,
				iEnd;
	uint8		startMask,
				endMask;
	bool		valid = false;
	bool		first = true;

	while (!valid && ((low & HIGH_PART_MASK) <= (high & HIGH_PART_MASK)))
	{
		bool		skip_step = false;

		lowNode.key = low;
		lowNode.bitmap = NULL;

		node = (OKeyBitmapRBTNode *) rbt_find_great(rbtree,
													&lowNode.rbtnode,
													true);
		if (!node)
			break;

		if (!node->bitmap)
		{
			if (node->key >= low && node->key < high)
				valid = true;
			skip_step = true;
		}

		if (!skip_step)
		{
			if ((low & HIGH_PART_MASK) == (node->key & HIGH_PART_MASK))
			{
				iStart = (low & LOW_PART_MASK) >> 3;
				startMask = 0xFF << ((low & LOW_PART_MASK) & 7);
			}
			else
			{
				iStart = 0;
				startMask = 0xFF;
			}

			if ((high & HIGH_PART_MASK) == (node->key & HIGH_PART_MASK))
			{
				iEnd = ((high - 1) & LOW_PART_MASK) >> 3;
				endMask = 0xFF >> (7 - (((high - 1) & LOW_PART_MASK) & 7));
			}
			else
			{
				iEnd = BITMAP_SIZE - 1;
				endMask = 0xFF;
			}
			for (i = iStart; i <= iEnd; i++)
			{
				uint8		mask;

				mask = (i == iStart) ? startMask : 0xFF;
				if (i == iEnd)
					mask &= endMask;

				if (node->bitmap[i] & mask)
					valid = true;
			}
		}
		if (!valid)
		{
			low = node->key;
			if (!first)
				low += (1L << 10);
		}
		first = false;
	}

	return valid;
}

static int
find_next_offset(uint8 *bitmap, int minOffset)
{
	int			i;
	uint8		mask;

	i = minOffset >> 3;
	mask = 0xFF << (minOffset & 7);
	while (i < BITMAP_SIZE)
	{
		mask &= bitmap[i];
		if (mask)
		{
			int			result;

			result = i << 3;
			while (!(mask & 1))
			{
				result++;
				mask >>= 1;
			}
			return result;
		}
		mask = 0xFF;
		i++;
	}
	return -1;
}


uint64
o_keybitmap_get_next(RBTree *rbtree, uint64 prev, bool *found)
{
	OKeyBitmapRBTNode lowNode;
	OKeyBitmapRBTNode *node;
	RBTreeIterator iter;

	lowNode.key = prev;
	lowNode.bitmap = NULL;
	node = (OKeyBitmapRBTNode *) rbt_find_great(rbtree,
												&lowNode.rbtnode,
												true);
	if (!node)
	{
		*found = false;
		return 0;
	}

	if (!node->bitmap)
	{
		if (node->key >= prev)
		{
			*found = true;
			return node->key;
		}
	}
	else if ((prev & HIGH_PART_MASK) == (node->key & HIGH_PART_MASK))
	{
		int			nextOffset;

		nextOffset = find_next_offset(node->bitmap, prev & LOW_PART_MASK);

		if (nextOffset >= 0)
		{
			*found = true;
			return node->key + nextOffset;
		}
	}

	if ((prev & HIGH_PART_MASK) == (node->key & HIGH_PART_MASK))
	{
		rbt_begin_iterate(rbtree, LeftRightWalk, &iter);
		iter.last_visited = (RBTNode *) node;
		node = (OKeyBitmapRBTNode *) rbt_iterate(&iter);
	}

	if (!node)
	{
		*found = false;
		return 0;
	}

	if (!node->bitmap)
	{
		*found = true;
		return node->key;
	}
	else
	{
		int			nextOffset = find_next_offset(node->bitmap, 0);

		Assert(nextOffset >= 0);
		*found = true;
		return node->key + nextOffset;
	}
}

static void
free_tree_node(RBTNode *node)
{
	OKeyBitmapRBTNode *keyNode = (OKeyBitmapRBTNode *) node;

	if (node->left == node)
	{
		Assert(node->right == node);
		return;
	}
	if (keyNode->bitmap)
		pfree(keyNode->bitmap);
	free_tree_node(node->left);
	free_tree_node(node->right);
	pfree(node);
}

void
o_keybitmap_free(RBTree *tree)
{
	free_tree_node(*((RBTNode **) tree));
	pfree(tree);
}

bool
o_keybitmap_is_empty(RBTree *rbtree)
{
	return rbt_leftmost(rbtree) == NULL;
}

void
o_keybitmap_intersect(RBTree *a, RBTree *b)
{
	RBTreeIterator iterA;
	RBTreeIterator iterB;
	OKeyBitmapRBTNode *nodeA;
	OKeyBitmapRBTNode *nodeB;
	List	   *removing = NIL;
	ListCell   *lc;

	rbt_begin_iterate(a, LeftRightWalk, &iterA);
	rbt_begin_iterate(b, LeftRightWalk, &iterB);

	nodeB = (OKeyBitmapRBTNode *) rbt_iterate(&iterB);
	while ((nodeA = (OKeyBitmapRBTNode *) rbt_iterate(&iterA)) != NULL)
	{
		while (nodeB &&
			   (nodeB->key & HIGH_PART_MASK) < (nodeA->key & HIGH_PART_MASK))
		{
			nodeB = (OKeyBitmapRBTNode *) rbt_iterate(&iterB);
		}

		if (!nodeB ||
			(nodeB->key & HIGH_PART_MASK) > (nodeA->key & HIGH_PART_MASK))
		{
			OKeyBitmapRBTNode *removed_node;

			removed_node = palloc0(sizeof(OKeyBitmapRBTNode));
			memcpy(removed_node, nodeA, sizeof(OKeyBitmapRBTNode));
			removing = lappend(removing, removed_node);
			continue;
		}

		Assert((nodeA->key & HIGH_PART_MASK) == (nodeB->key & HIGH_PART_MASK));

		if (!nodeA->bitmap)
		{
			if (!nodeB->bitmap)
			{
				if (nodeA->key != nodeB->key)
				{
					OKeyBitmapRBTNode *removed_node;

					removed_node = palloc0(sizeof(OKeyBitmapRBTNode));
					memcpy(removed_node, nodeA, sizeof(OKeyBitmapRBTNode));
					removing = lappend(removing, removed_node);
					continue;
				}
			}
			else
			{
				int			offset = (nodeA->key & LOW_PART_MASK);

				if (!(nodeB->bitmap[offset >> 3] & (1 << (offset & 7))))
				{
					OKeyBitmapRBTNode *removed_node;

					removed_node = palloc0(sizeof(OKeyBitmapRBTNode));
					memcpy(removed_node, nodeA, sizeof(OKeyBitmapRBTNode));
					removing = lappend(removing, removed_node);
					continue;
				}
			}
		}
		else
		{
			if (!nodeB->bitmap)
			{
				int			offset = (nodeB->key & LOW_PART_MASK);

				if (!(nodeA->bitmap[offset >> 3] & (1 << (offset & 7))))
				{
					OKeyBitmapRBTNode *removed_node;

					removed_node = palloc0(sizeof(OKeyBitmapRBTNode));
					memcpy(removed_node, nodeA, sizeof(OKeyBitmapRBTNode));
					removing = lappend(removing, removed_node);
					continue;
				}
				pfree(nodeA->bitmap);
				nodeA->bitmap = NULL;
				nodeA->key = nodeB->key;
			}
			else
			{
				int			i;
				bool		empty = true;

				for (i = 0; i < BITMAP_SIZE; i++)
				{
					nodeA->bitmap[i] &= nodeB->bitmap[i];
					if (nodeA->bitmap[i] != 0)
						empty = false;
				}

				if (empty)
				{
					OKeyBitmapRBTNode *removed_node;

					removed_node = palloc0(sizeof(OKeyBitmapRBTNode));
					memcpy(removed_node, nodeA, sizeof(OKeyBitmapRBTNode));
					removing = lappend(removing, removed_node);
					continue;
				}
			}
		}
	}

	foreach(lc, removing)
	{
		OKeyBitmapRBTNode *search_node;
		OKeyBitmapRBTNode *removing_node;

		search_node = (OKeyBitmapRBTNode *) lfirst(lc);
		if (search_node->bitmap)
			pfree(search_node->bitmap);
		removing_node = (OKeyBitmapRBTNode *) rbt_find(a,
													   (RBTNode *) search_node);
		rbt_delete(a, (RBTNode *) removing_node);
		pfree(search_node);
	}
	list_free(removing);
}

void
o_keybitmap_union(RBTree *a, RBTree *b)
{
	RBTreeIterator iterB;
	OKeyBitmapRBTNode *nodeB;

	rbt_begin_iterate(b, LeftRightWalk, &iterB);
	while ((nodeB = (OKeyBitmapRBTNode *) rbt_iterate(&iterB)) != NULL)
	{
		bool		is_new;

		rbt_insert(a, &nodeB->rbtnode, &is_new);
	}
}

static int
bm_rbt_comparator(const RBTNode *a, const RBTNode *b, void *arg)
{
	const OKeyBitmapRBTNode *keyA = (OKeyBitmapRBTNode *) a;
	const OKeyBitmapRBTNode *keyB = (OKeyBitmapRBTNode *) b;
	uint64		va = keyA->key & HIGH_PART_MASK;
	uint64		vb = keyB->key & HIGH_PART_MASK;

	return va > vb ? 1 : va < vb ? -1 : 0;
}

static void
node_make_bitmap(OKeyBitmapRBTNode *node)
{
	int			offset;

	node->bitmap = palloc0(BITMAP_SIZE);
	offset = node->key & LOW_PART_MASK;

	node->bitmap[offset >> 3] |= 1 << (offset & 7);
	node->key &= HIGH_PART_MASK;
}

static void
bm_rbt_combiner(RBTNode *existing,
				const RBTNode *newdata,
				void *arg)
{
	OKeyBitmapRBTNode *old = (OKeyBitmapRBTNode *) existing;
	OKeyBitmapRBTNode *new = (OKeyBitmapRBTNode *) newdata;

	if (!old->bitmap)
	{
		if (!new->bitmap && new->key == old->key)
			return;
		node_make_bitmap(old);
	}

	if (!new->bitmap)
	{
		int			offset = new->key & LOW_PART_MASK;

		old->bitmap[offset >> 3] |= 1 << (offset & 7);
	}
	else
	{
		int			i;

		for (i = 0; i < BITMAP_SIZE; i++)
			old->bitmap[i] |= new->bitmap[i];
	}
}

static RBTNode *
bm_rbt_allocfunc(void *arg)
{
	RBTNode    *result = palloc0(sizeof(OKeyBitmapRBTNode));

	return result;
}

static void
bm_rbt_freefunc(RBTNode *x, void *arg)
{
	pfree(x);
}
