
## BEGIN TX FLOW
[exec_simple_query](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017) -- main entry point of query execution. A tx creation goes through it.
|
[start_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051) -- starts every tx. Assigns some IDs and set up proper env for tx.
		[StartTransactionCommand](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L2774) -- perform initial tx setup
		|
		[AtStart_Cache](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2180) -- process all invalidation msgs at the start of tx for valid cache behavior. 
|
[drop_unnamed_stmt](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1059) -- drops artifacts of former exec plan
|
switch mem ctx + parsing trees 
|
for 
	|
	some preparing stuff 
	[analyze](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195) & [plan](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198) query trees
	[CreatePortal](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220) -- creating a portal for query execution
	[PortalStart](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239) -- initial start of a portal without actual query execution. Need to be done for portal configuration like set-up of output format etc
	[PortalRun](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284) -- actual execution of a query
		|
		some set-up stuff
		PG_TRY() -- on exception we do some memory ctx management, mark Portal as failed and simply rethrow 
			|
			PortalRun(Multi|Select) -- for most cases it calls [Multi](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L789-L790) version, begin/commit tx block is not an exception.
				| 
				[loops](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1215) over every query with [PortalRunUtility](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1309-L1321) execution, PortalRunUtility itself create a snapshot if required and call [ProcessUtility](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1156-L1163) that calls Oriole's hook [orioledb_utility_command](https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L872-L879) . Oriole apply any modification if provided and [forward execution to standard_process_utility](https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L1521-L1524) 
					|
					Falls into TRANS_STMT_BEGIN/TRANS_STMT_START block.
					[begin tx and set necessary env vars that required by isolation level of tx](https://github.com/orioledb/postgres/blob/e2fb3dfa817fbe89494a62c100e9cb442f4d6b15/src/backend/tcop/utility.c#L609-L626) standard_ProcessUtility and [ProcessUtilitySlow](https://github.com/orioledb/postgres/blob/e2fb3dfa817fbe89494a62c100e9cb442f4d6b15/src/backend/tcop/utility.c#L1089) are the main processing functions for utility stmts in pg. The difference between them is that Slow version dedicated for processing trigger-based events. 
				|
				PortalRunMulti drops all snapshots and release memory 
		
		memory releasing phase for almost every func in callstack ....
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303) fell down to [simply toggling tx state to IN_PROGRESS](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3185-L3187) 
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354) here is literally no-op


## COMMIT TX FLOW

Two-stage. The TRANS_STMT_COMMIT case **only flips state**; the actual commit happens later in `finish_xact_command` → `CommitTransactionCommand` → `CommitTransaction`


[exec_simple_query](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017) -- main entry point of query execution. A tx creation goes through it.
|
[start_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051) -- starts every tx. Assigns some IDs and set up proper env for tx.
		[StartTransactionCommand](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L2774) -- perform initial tx setup
		|
		[AtStart_Cache](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2180) -- process all invalidation msgs at the start of tx for valid cache behavior. 
|
[drop_unnamed_stmt](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1059) -- drops artifacts of former exec plan
|
switch mem ctx + parsing trees 
|
for 
	|
	some preparing stuff 
	[analyze](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195) & [plan](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198) query trees
	[CreatePortal](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220) -- creating a portal for query execution
	[PortalStart](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239) -- initial start of a portal without actual query execution. Need to be done for portal configuration like set-up of output format etc
	[PortalRun](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284) -- actual execution of a query
		|
		some set-up stuff
		PG_TRY() -- on exception we do some memory ctx management, mark Portal as failed and simply rethrow 
			|
			PortalRun(Multi|Select) -- for most cases it calls [Multi](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L789-L790) version, begin/commit tx block is not an exception.
				|
				[loops](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1215) over every query with [PortalRunUtility](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1309-L1321) execution, PortalRunUtility itself create a snapshot if required and call [ProcessUtility](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1156-L1163) that calls Oriole's hook [orioledb_utility_command](https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L872-L879) . Oriole apply any modification if provided and [forward execution to standard_process_utility](https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L1521-L1524) 
					|
					Falls into TRANS_STMT_COMMIT block.
					Simply no-op in case of commit as a [EndTransactionBlock](https://github.com/orioledb/postgres/blob/e2fb3dfa817fbe89494a62c100e9cb442f4d6b15/src/backend/tcop/utility.c#L631) returns true for 'true' commit commands.
				|
				PortalRunMulti drops all snapshots and release memory 
			
			memory releasing phase for almost every func in callstack ....
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303) 
	|
	[CommitTransactionCommand](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L2805) -> [CommitTransactionCommandInternal](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3136) -> [CommitTransaction](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3175)
		|
		[CallXactCallbacks](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2255-L2256) -- triggers custom storage engines (e.g. OrioleDB) callbacks that were registered with [RegisterXactCallback](https://github.com/orioledb/orioledb/blob/bbd7c1254e4cbd23bc4cafda02289c91609111e6/src/orioledb.c#L1210).
			|
			[undo_xact_callback](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2110) with event = XACT_EVENT_PRE_COMMIT
				|
				in general out tx is considered as an independent oriole tx.
				|
				[precommit_undo_stack](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2232) 
					|
					[walk_undo_range_with_buf](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1426-L1429) 
						|
						[walk_undo_range](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1290-L1291) -- Walk through in the undo stack in a foor loop calling the callbacks for each item. **good point for injection** between different iterations, for making partial change visible. **notice, that loop make != 0 iterations only in rollback/abort scenario** 
				| 
				other if branches are not taken
		|
		[loops over the triggers](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2232-L2246) that should be called before commits and close all portals 
		After this point any user-defined code can't be executed, but errors still may appear.
		|
		[PreCommit_on_commit_actions](https://github.com/orioledb/postgres/blob/1d7f7407002d8f7ffb2f5c815cef418fa10f3777/src/backend/commands/tablecmds.c#L17638) **good point for injection** due commets before function explicitly says that error may be ecountered. Function itself truncate and delete some relations/rows that should be removed after tx commit. Function perform a lot modifications, so it seems good to inject faults in multiple places within that func. (e.g. [before/after of pushing snapshot](https://github.com/orioledb/postgres/blob/1d7f7407002d8f7ffb2f5c815cef418fa10f3777/src/backend/commands/tablecmds.c#L17706))
		| 
		[smgrDoPendingSync](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2293) -- Synchronize files that are created and not WAL-logged during this transaction. **good point for injection**. Synchronization can performed by emitting extra wal records for smaller relations. It worth testing whether test bank_account relations is considered as a small one. **ALSO SEEMS UNREACHABLE FOR ORIOLE**
		|
		[PreCommit_Notify](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2304) -- irrelevant for us, only relevant for LISTEN/UNLISTEN, NOTIFY, but looks good to abort tx right after the notification.
		|
		[RecordTransactionCommit](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2343) -- actual machinery for marking tx as commited. **good point for injection** 
			|
			[XLogFlush](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L1505) -- flushes *ALL* wal records (as oriole flushed into common buffer before in callback)  (for SYNC MODE)
			[XLogSetAsynCXactLSN](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L1526) -- reports the latest LSN for wal writer (for ASYNC MODE)
		|
		[ProcArrayEndTransaction](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2371) -- marks tx as invalid. Some manipulations are performed under the lock in crit section, so it seems unintended behavior to throw an errors here.
		|
		after that point we have a comment in the code:
		"This is all post-commit cleanup.  Note that if an error is raised here, it's too late to abort the transaction.  This should be just noncritical resource releasing."
		So it seems that fault-injection here is irrelevant
		| 
		[CallXactCallbacks](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2393-L2394) 
			|
			[undo_xact_callback](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2110) with event = XACT_EVENT_COMMIT 
				|
				[seq_scan_cleanup](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2136) -- inside pg defined CRIT_SECTION clean-up resources dedicated to seq scans
				|
				in general out tx is considered as an independent oriole tx.
				|
				[assign_xidless_commit_lsn](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2262) 
					|
					[current_oxid_xlog_precommit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L219) 
						|
						[set_oxid_xlog_ptr](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1410-L1412) 
							|
							[set_oxid_xlog_ptr_internal](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L649) -- changes a commit ptr to one dedicated to curr tx inside xidBuffer in lockfree manner **maybe good place for fault-injection**, let's put maybe fault or barrier right before [CAS](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L614) 
					| 
					[wal_commit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L220) 
						|
						[flush_local_wal_if_needed](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L290) -- may or may not cause flush of wal according to the size threshold, so it looks **good place for fault-injection**. (???) Maybe need further investigation 
						|
						[add_xid_wal_record](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L294) -- no lock-free, single threaded code
						|
						[add_finish_wal_record](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L296) -- **good point** It worths testing to inject-fault during wal building right before finish record
						|
						[flush_local_wal](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L297) -- flushes local oriole wal to single wal stream
					|
					[set_oxid_xlog_ptr](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L221) -- covered earlier
				| 
				[current_oxid_precommit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2290) 
					|
					[set_oxid_csn](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1397-L1399) -- same as set_oxid_xlog_ptr_internal, but with different fields. So **also a good place for fault-injection** 
				| 
				[increments csn ](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2291-L2293)
				|
				**good place for injection** 
				|
				[current_oxid_commit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2295) 
					|
					[set_oxid_csn](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1475-L1476) -- again some lock-free stuff
					|
					[advance_run_xmin](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1480) -- CAS loop 
					| 
					[release_assigned_logical_xids](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1482) -- some atomics manipulations
				| 
				[on_commit_undo_stack](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2307-L2310) -- **good place for injection**, especially between iterations if any
				| 
				some clean-up stuff
				| 
				[also clean-up](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2378-L2392)  
		|
		[ResourceOwnerRelease](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2418-L2423) Release Locks and AfterLocks
		| 
		AtCommitNotify and other per-backend clean up stuff
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354) for the second time is no-op



oxid space:   ▒▒▒▒▒▒▒▒│░░░░░░░░│████████████→  growing oxids
              └──A──┘  └──B──┘  └─────C─────┘
                       ↑        ↑
                       │        │
                writtenXmin   writeInProgressXmin

| Zone  | Range                                      | State of `xidBuffer[oxid % size]`                                 | Status of the data                |
| ----- | ------------------------------------------ | ----------------------------------------------------------------- | --------------------------------- |
| **A** | `oxid < writtenXmin`                       | Already overwritten or recyclable for newer oxids                 | Authoritative copy is **on disk** |
| **B** | `writtenXmin ≤ oxid < writeInProgressXmin` | Being torn down right now; entries are FROZEN sentinels mid-write | Disk write in progress; transient |
| **C** | `oxid ≥ writeInProgressXmin`               | Authoritative ring slot, owned by the regular CAS protocol        | In memory, lock-free reads/writes |