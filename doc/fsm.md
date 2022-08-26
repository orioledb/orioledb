Free space management in OrioleDB
=================================

Regular trees
-------------

Each OrioleDB has an associated data file.  The length of that datafile is kept in the `BTreeMetaPage.datafileLength`.  It does not necessarily strictly matches the actual file length.  The data file might be shorter than `BTreeMetaPage.datafileLength`: it means that some pages have their offsets virtually allocated but have not been written there yet.  Also, the data file might be longer than `BTreeMetaPage.datafileLength`: some pages have been written beyond during previous database run, which ended up with a crash, but now that places should be considered as free space.

Each tree checkpoint has associated two files for free space management.

 * `*.tmp` file contains block numbers, which have been freed from previous checkpoint completion to the current checkpoint completion.  This file is optional and might not exist if no blocks were freed in that period.
 * `*.map` file, which contains:
     * link to the tree root,
     * The data file length during the checkpoint completion,
     * the array of free block numbers in this checkpoint.

Therefore, root and all the blocks directly or indirectly referenced by root are considered busy in this checkpoint.  All other blocks within the datafile length are considered free and should be listed in the array of free blocks.

When OrioleDB needs a block to write the page, it gets it in the following order:

 * from the array of free block numbers in the `*.map` file of the checkpoint where the database instance was started,
 * from the `*.tmp` files of further completed checkpoints, if any,
 * by increasing the data file length (atomic increment of `BTreeMetaPage.datafileLength`).

The picture below illustrates the page writing of the tree when there is no concurrent checkpointing.

![Free space management 1](images/fsm_1.svg)

If the page to be written already has an associated block in the data file, that block number is written to the `*.tmp` and `*.map` files of the next checkpoint.  On the picture, previous block number is written to the `*.tmp` and `*.map` files of checkpoint 2.

This page's new block number is acquired according to the rules described above.  On the picture, we get it from the `*.tmp` file of checkpoint 1.

The picture below illustrates the page writing of the tree when there is no concurrent checkpointing.

![Free space management 1](images/fsm_2.svg)

In the picture above, checkpoint 1 is completed, while checkpoint 2 is in-progress.  Checkpointer processes trees in some deterministic order.

For the trees passed by checkpointer, the in-progress checkpoint is considered completed.  In the picture, tree 1 is passed by checkpointer.  Therefore, checkpoint 2 is considered completed and checkpoint 3 as future.

For the trees not yet reached by checkpointer, the in-progress checkpoint is the same as the non-started.  In the picture, tree 3 is not yet reached by checkpointer.  Therefore, checkpoint 1 is considered completed and checkpoint 2 as future.

The tree under checkpointer is the most complicated case.  The checkpointer has already written some parts of the tree.  When we need to write that part of a tree, we cannot write it in-place because it would violate the copy-on-write principle.  Instead, we write the pages to the new place.  That place is the checkpoint next to the in-progress one.  Thus, when the checkpointer walks the tree from left to right, the particular logic depends on whether the checkpointer already passed the particular page.

  * When the page to be written is already passed by checkpointer has an associated block in the data file, that block is written to the `*.tmp` and `*.map` files of the checkpoint next to the in-progress one.  On the picture, we write the previous block number page 5 of tree 2 to the `*.tmp` and `*.map` files of checkpoint 3.

  * When the page to be written is already passed by checkpointer, the new block number is acquired according to the rules described above.  The in-progress checkpoint  `*.tmp` file still cannot be the source for new blocks because it is not completed for this tree.  However, when we get the new block number, we have to also write it to the `*.map` file of in progress checkpoint because that block belongs to the next checkpoint and is free for in progress checkpoint.   On the picture, we get a new block number for page 5 of tree 2 from the `*.tmp` file of checkpoint 1 and also write it to the `*.map` file of checkpoint 2.

  * When the page to be written is not yet passed by the checkpointer, it works similarly to the trees not yet reached by the checkpointer.  The same rules apply to the checkpointer when it writes the pages itself.  If there is an associated block in the data file, that block number is written to the `*.tmp` and `*.map` files of the in progress.  On the picture, the previous block number of page 6 of tree 2 is written to the `*.tmp` and `*.map` files of checkpoint 2.  The page's new block number is acquired according to the rules described above.  On the picture, we get a new block number for page 6 of tree 2 from the `*.tmp` file of checkpoint 1.

When checkpointer finishes checkpointing the particular tree, it needs to finish `*.map` file.  In particular, it adds all the free blocks available in the `*.map` and `*.tmp` files (according to the rules) to the `*.map` file and writes the current value of `BTreeMetaPage.datafileLength` as the file length.

Compressed trees
----------------

OrioleDB implements page-level compression.  Pages have fixed size in-memory but variable size in the data file.  Therefore, the free space management described above needs some advancements.

At first, `*.tmp` and `*.map` files contains not free block numbers but free extents.  Extent comprises offset and length.

Since we are dealing with extents, we can just read `*.tmp` and `*.map` files sequentially because we may meet extents that don't match the required length.  In order to deal with free extents, we also have system trees `SYS_TREES_EXTENTS_OFF_LEN` and `SYS_TREES_EXTENTS_LEN_OFF`.  The `SYS_TREES_EXTENTS_LEN_OFF` is ordered by the extent length, and we use it to find the extent which is the best fit for our needs.  The `SYS_TREES_EXTENTS_OFF_LEN` is ordered by the extent offset, and we use it to join the conjuncted extents.

`SYS_TREES_EXTENTS_OFF_LEN` and `SYS_TREES_EXTENTS_LEN_OFF` trees are temporary.  Their content does not survive server restart: we always start with these trees empty.  Once we load some tree, we also load the content of its `*.map` file to `SYS_TREES_EXTENTS_OFF_LEN` and `SYS_TREES_EXTENTS_LEN_OFF`.  When the checkpoint of a tree is completed, we read the content of `*.tmp` file of the previous checkpoint to these trees.  When the checkpoint is completed, we add extents from those trees to the `*.map` file.

Multiple processes could be concurrently looking for the free extent, and one process inserting a new free extent.  Correct handling the concurrency problem is a challenge.  The algorithms are considered below.

The algorithm for getting a free extent for writing the page is given below.

 1.  Find the shortest fitting extent in the `SYS_TREES_EXTENTS_LEN_OFF` tree and delete it.  If not found, increase `BTreeMetaPage.datafileLength` by the required length and return the corresponding extent.
 2.  If there is a remaining part of the selected extent, insert it into the `SYS_TREES_EXTENTS_OFF_LEN` tree.
 3.  Delete the selected extent from the `SYS_TREES_EXTENTS_OFF_LEN` tree.
 4.  If there is a remaining part of the selected extent, insert it into the `SYS_TREES_EXTENTS_LEN_OFF` tree.

Please, note that if the concurrent process needs to merge the remaining part, it will always find some extent for merge.  In `SYS_TREES_EXTENTS_OFF_LEN`, we insert first (step 2) and only then delete (step 3).  So, there is no intermediate state with no extent to merge.

The algorithm for insertion of a new free extent is given below.

 1.  In the `SYS_TREES_EXTENTS_OFF_LEN` tree, find the left and right siblings of the new extent.  Check if they are adjacent to a new extent; thus, we need to merge them.
 2.  If we need to merge the left sibling, delete it from the `SYS_TREES_EXTENTS_LEN_OFF` tree.  On failure, re-try from step 1.
 3.  If we need to merge the right sibling, delete it from the `SYS_TREES_EXTENTS_LEN_OFF` tree.  On failure, re-insert the left sibling if it was deleted, and re-try from step 1.
 4.  At this point concurrent process cannot use either left or right siblings because they were deleted from the `SYS_TREES_EXTENTS_LEN_OFF` tree.
 5.  Delete siblings to be merged from the `SYS_TREES_EXTENTS_OFF_LEN` tree.
 6.  Insert new extent (with siblings merged) into the `SYS_TREES_EXTENTS_OFF_LEN` tree, then the `SYS_TREES_EXTENTS_LEN_OFF` tree.
