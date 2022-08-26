Checkpoints in OrioleDB
=======================

Tree walk order
---------------

Checkpointer walks OrioleDB trees in LNR-order.  The walk is divided into steps.  The result of each step is a message, which determines the next step.  The possible messages are given below.

 1.  `WalkDownwards` – the last step found an in-memory downlink within an internal page.  The next step should be to visit a page on the lower level and start processing it.
 2.  `WalkUpwards` – the last step finished processing the page.  The next step should continue processing the parent.
 3.  `WalkContinue` – continue working with the current page after releasing a lock.  That happens when the checkpointer has to wait for the concurrent operation.

The picture below is the example of OrioleDB B-tree walking by the checkpointer.

![Checkpoint walk the tree](images/checkpoint_walk.svg)

Checkpointer comes to the root page `n1` with the `WalkDownwards` message (step 1), then checks the first downlink `l1`.  Since `l1` is the in-memory downlink, the checkpointer moves to the leaf page `n2` with the `WalkDownwards` message (step 2).  After flushing `n2`, checkpointer comes back to `n1` with the `WalkUpwards` message (step 3) and continues iteration over `n1` downlinks.  Similarly, it walks down to the `n3` and back via the `l2` downlink (steps 4 and 5).  `l3` appears to be IO in-progress downlink, and the checkpointer has to unlock the `n1` page, and wait till IO is completed and continue with `WalkContinue` message (step 6).  After relocking `n1`, checkpointer finds `l3` to be an on-disk downlink and copies it "as is".   Finally, checkpointer walks down to the `n4` and back via the `l4` downlink (steps 7 and 8).  Then `n1` is done, the checkpointer finishes the walk with the `WalkUpwards` message.

Checkpoint state
----------------

While the checkpointer is writing children of non-leaf page, concurrent splits and merges could happen.  Therefore, the checkpoint state contains images of non-leaf pages under checkpointing as its reconstructed as the checkpointer visits the downlinks.  If there are no concurrent changes to the non-leaf page, the reconstructed state finally matches the page state.  Otherwise, the reconstructed page state could not even match to page state at any moment of time but always match the history of the checkpointer tree walk.

Note that the reconstructed state does not contain in-memory downlinks.  In-memory downlinks are replaced with on-disk downlinks as we wrote the children's pages.

The picture below represents an example of a checkpoint state.

![Checkpoint state 1](images/checkpoint_state_1.svg)

The root page `n1`, internal page `n3`, and leaf page `n8` are currently under checkpointing.  The downlink `l2` is written to the reconstructed state.  The downlink `l3` is the `next downlink` in the reconstructed state.  Its key is known, but the link is not because the corresponding children were not written yet.  Similarly, downlink `l6` is written, downlink `l7` is the `next downlink`, and downlink `l8` is not processed yet.

Let us imagine that the following event happened:

 1.  Page `n7` was written by checkpointer.
 2.  Page `n8` was split into `n8` and `n9`.
 3.  Pages `n6` and `n7` were merged.  The result is marked as `n67`.
 4.  Checkpointer has written page `n8` and started processing page `n9`.

The resulting state is given in the picture below.  Note that the reconstructed page image contains links `l6` and `l7` (as we visited them before the merge) but contains `l8` and the `next downlink` corresponding to `l9` (as we visited those downlinks after the split).

![Checkpoint state 1](images/checkpoint_state_2.svg)

Autonomous non-leaf pages
-------------------------

If the non-leaf page under checkpointing gets modified concurrently, it becomes an "autonomous" non-leaf page.  Autonomous pages work with the rules below.

 1.  If the page is marked as "autonomous", all its parents to the root are also marked as "autonomous".
 2.  If the page has an associated on-disk location, this association is cleared.  The corresponding location is marked as free space at the current checkpoint.
 3.  The autonomous page will be processed until its hikey is met, disregarding how many pages will be visited to meet this target (due to concurrent insertion, it could be many pages).
 4.  Even if the initial page corresponding to the autonomous page has been split.  The page holding the initial hikey is tracked.  The merge, which would remove that hikey, is prevented.
 5.  If the autonomous page is full, but the corresponding hikey is not yet met, current contents are flushed to the disk (and parent got the corresponding downlink with `WalkUpwards`), but processing of the autonomous page continues till the hikey is met.
 6.  When flushing autonomous, the corresponding "on-disk" location is marked as free for the future checkpoint.

Checkpointer messages
---------------------

Consider more details regarding the checkpointer messages we enumerated above.

### WalkDownwards

This message has the parameters below.

 * The number and change count of the in-memory page to be visited.
 * Low key ("lokey").  The lokey is from the parent page downlink, or it is the parent page lokey if the downlink is the first on the page.

The checkpointer has to process the referenced page.  After the page is processed, the `WalkUpwards` message must be returned.  If the referenced page is non-leaf, more messages will be issued during its processing, but finally, there must be `WalkUpwards` of the referenced page.

There might be a failure due to concurrent operations: the in-memory page might have a different change count.  In this case, the corresponding `WalkUpwards` should return the invalid downlink.  Also, in a failure case, `WalkUpwards` message should go just after `WalkDownwards`: once we start processing the non-leaf page, we must finish it.

### WalkUpwards

This message has the parameters below.

 * On-disk downlink.  This link might be invalid, as described above.
 * Next key.  That is actually a hikey of the page written.  It might not match the subsequent downlink of the parent page due to concurrent splits and merges.  On mismatch, the parent page must be marked "autonomous".
 * Flag indicating that parent page must be marked as "dirty".  This flag is set when the page has been written to the new place after the previous checkpoint.  This flag is not set if the page and its children will not be modified then.  The parent must be marked as "dirty" to be written and reflect the new on-disk downlink.
 * The flag indicates that we must save the existing `next downlink` on the parent page.  That happens to the autonomous page when the current reconstructed image is finished: we have the page written and need to insert a new downlink to the parent, but we still need to visit the same `next downlink`.

This message indicates that the child page has been processed, and the parent needs to add the downlink.  If there is no parent, we have processed the root and now have a pointer to the new root on-disk location.

### WalkContinue

This message has no parameters.  It just indicates that checkpointer must continue processing the same page with the same `next downlink`.  That happens when the checkpointer has to wait for the concurrent operation.  Such as meeting IO in-process downlink and having to release the log and wait till the IO is finished.
