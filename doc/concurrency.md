Concurrency algorithms in OrioleDB B-tree
=========================================

Page change count
-----------------

In OrioleDB B-tree, we do not lock pages unless we need to modify them.  Therefore, after traversing the downlink, we need to check if we reach the target page (because it could be concurrently evicted, merged, etc.).  In order to cope with that, OrioleDB each in-memory page has `OrioleDBPageHeader.pageChangeCount` field, which got incremented every time the in-memory page changes its identity.  The in-memory page identity means a particular tree, level, and lokey (left bound of the page key range).  Despite the page lokey is not stored at the page, it is structurally determined.  Therefore left page of split and the left page of merge save identities.  Thanks to the change count mechanism, the process traversing the downlink can detect a concurrent change of the page identity.

Rightlinks
----------

In OrioleDB, in-memory downlinks can be changed to on-disk downlinks and vice versa on page eviction and load correspondingly.  Therefore, OrioleDB pages do not normally have rightlinks or leftlinks.  Rightlinks temporarily exist during split to prevent blocking tree navigation.  Consider the page split process in the pictures below.

Step 1 depicts the initial state of part of the tree comprising parent page 1 and child page 2.

![Split step 1](split_step_1.svg)

Step 2 depicts the split of page 2, creating the new page 3.  At this point, page 2 has a rightlink to page 3.  At this point, if a concurrent process is looking for the key located on page 3, it will check the hikey of page 2 and traverse to page 3 via rightlink.  The keys located on page 2 could be found as usual.

![Split step 2](split_step_2.svg)

Step 3 depicts the insertion of the new downlink to page 1.  The rightlink between pages 2 and 3 still persists.  At this point, the concurrent process can locate page 3 via downlink.  If the concurrent process came to page 2 before downlink insertion, it still could use the rightlink.

![Split step 3](split_step_3.svg)

Step 4 depicts the removal of the rightlink from page 2 to page 3.  If a concurrent process, which came to page 2 before downlink insertion, is looking for a key located on page 3, then it has to check the rightlink and restart from page 1.

![Split step 4](split_step_4.svg)

Page eviction
-------------

Page eviction is forbidden for the page with a rightlink or without a downlink from the parent.  Generally, both source and target of rightlink are forbidden.  Therefore page eviction does not have to deal with rightlinks.  Righlinks can connect only in-memory pages.  Consider the page eviction process in the pictures below.

Step 1 depicts the initial state of part of the tree comprising parent page 1 and child page 2.  Page 2 is locked and should be evicted.  At this point evicting process should find and lock the parent page 1 using the page 2 hikey to find it.

![Evict step 1](evict_step_1.svg)

Step 2 depicts both page 1 and page 2 locked.  At this point evicting process repaces in-memory downlink with IO downlink and increases page change count.  IO downlink prevents a concurrent process from using it, making them wait till IO is completed.  Increased change count prevents all the concurrent processes, which managed to use in-memory downlink, from using page 2.

![Evict step 2](evict_step_2.svg)

Step 3 depicts page 1 disconnected from its child with IO downlink.

![Evict step 3](evict_step_3.svg)

Finally evicting process writes the on-disk downlink to page 1 (step 4).

![Evict step 4](evict_step_4.svg)
