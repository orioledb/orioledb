
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

**Note on recursive (PANIC-prone) injection points in this section.** Several commit-side helpers are *also* called from the abort handler (`undo_xact_callback` at `XACT_EVENT_ABORT`). Attaching an `error`-mode injection at the function entry would fire on the commit path, raise ereport, drive abort, and re-fire inside the abort handler -- ereport during `XACT_EVENT_ABORT` escalates to **PANIC**. Such points are tagged `**RECURSIVE -- raises PANIC unless gated by isCommit**` in the flow below. The standard fix is to wrap the `INJECTION_POINT` with an `isCommit` (or `!IsAbortPath`) check at the call site, the same pattern already used at [wal.c#L684](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L684); alternatively, branch on a discriminator parameter (e.g. `rec_type == WAL_REC_COMMIT`, `csn != COMMITSEQNO_ABORTED`) inside the function, or define separate point names for commit-side and abort-side. Affected helpers in this flow: `set_oxid_xlog_ptr_internal`, `flush_local_wal_if_needed`, `add_finish_wal_record`, both `set_oxid_csn` calls.

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
							[set_oxid_xlog_ptr_internal](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L649) -- changes a commit ptr to one dedicated to curr tx inside xidBuffer in lockfree manner **maybe good place for fault-injection**, let's put maybe fault or barrier right before [CAS](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L614). **RECURSIVE -- raises PANIC if injected without an `isCommit` guard.** `set_oxid_xlog_ptr` is also called on the abort path at [undo.c#L2348](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2348) (`set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr)` inside the abort case of `undo_xact_callback`). An `error`-mode injection attached during commit raises ereport, ereport drives abort, abort re-enters the same function and re-fires the injection during `XACT_EVENT_ABORT` -> PANIC. Mitigation: gate the `INJECTION_POINT` with an `isCommit`-style flag at the call site (the [wal.c#L684](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L684) pattern), or use distinct point names for commit-side and abort-side. 
					| 
					[wal_commit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L220) 
						|
						[flush_local_wal_if_needed](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L290) -- may or may not cause flush of wal according to the size threshold, so it looks **good place for fault-injection**. (???) Maybe need further investigation. **RECURSIVE -- raises PANIC unless gated by `isCommit`.** `flush_local_wal_if_needed` is also called on the abort path from `wal_rollback` at [wal.c#L352](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L352) when reserving space for the `WAL_REC_ROLLBACK` finish record. The current `INJECTION_POINT("orioledb-wal-flush")` at [wal.c#L721](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L721) is **NOT** gated and CAN re-fire during abort if the local WAL buffer is near-full when `wal_rollback` runs. Compare with the correctly-gated sibling at [wal.c#L684](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L684) (`if (isCommit) INJECTION_POINT(...)`). Mitigation: wrap the L721 injection in an `isCommit` (or `!IsAbortPath`) guard at the call site, same as the L684 commit-flush variant. 
						|
						[add_xid_wal_record](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L294) -- no lock-free, single threaded code
						|
						[add_finish_wal_record](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L296) -- **good point** It worths testing to inject-fault during wal building right before finish record. **RECURSIVE -- raises PANIC if injected at the function level without an `isCommit` guard.** `add_finish_wal_record` is also called on the abort path from `wal_rollback` at [wal.c#L358](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L358) (`add_finish_wal_record(WAL_REC_ROLLBACK, ...)`). An `error`-mode injection inside the function entry would fire on both `WAL_REC_COMMIT` (commit) and `WAL_REC_ROLLBACK` (abort); the second hit happens during `XACT_EVENT_ABORT` -> PANIC. Mitigation: place the `INJECTION_POINT` either at the call site in `wal_commit` only ([wal.c#L296](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L296)), or branch on the `rec_type` parameter inside the function (`if (rec_type == WAL_REC_COMMIT) INJECTION_POINT(...)`), or use distinct point names for commit vs rollback finishes.
						|
						[flush_local_wal](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L297) -- flushes local oriole wal to single wal stream
					|
					[set_oxid_xlog_ptr](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L221) -- covered earlier
				| 
				[current_oxid_precommit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2290) 
					|
					[set_oxid_csn](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1397-L1399) -- same as set_oxid_xlog_ptr_internal, but with different fields. So **also a good place for fault-injection**. **RECURSIVE -- raises PANIC unless guarded.** `set_oxid_csn` is called both on commit (`current_oxid_precommit` at [oxid.c#L1397](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1397) and `current_oxid_commit` at [oxid.c#L1475](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1475)) and on abort, inside `current_oxid_abort` at [oxid.c#L1493](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1493) (`set_oxid_csn(curOxid, COMMITSEQNO_ABORTED)`), reached from [undo.c#L2347](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2347). An `error`-mode injection inside the function fires on the commit-side CSN flip first; ereport drives abort, abort calls `current_oxid_abort` which re-enters `set_oxid_csn` and re-fires the injection during `XACT_EVENT_ABORT` -> PANIC. Mitigation: place the `INJECTION_POINT` at the commit call sites only (in `current_oxid_precommit` / `current_oxid_commit`), not at the function entry; alternatively branch on the incoming `csn` value (`csn != COMMITSEQNO_ABORTED`), or use distinct point names. 
				| 
				[increments csn ](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2291-L2293)
				|
				**good place for injection** 
				|
				[current_oxid_commit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2295) 
					|
					[set_oxid_csn](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1475-L1476) -- again some lock-free stuff. **RECURSIVE with the abort-side `current_oxid_abort` -- same caveat as the precommit `set_oxid_csn` above; raise PANIC unless gated.** Re-uses the same function on the abort path; either gate the `INJECTION_POINT` at this commit call site only, or branch on `csn != COMMITSEQNO_ABORTED`.
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

## SELECT TX FLOW

The bank-test reader issues e.g. `SELECT balance, token FROM o_bank_account WHERE id = X` inside an already-open RR transaction. Read-only path: no undo push, no WAL emission, no page mutation; only the snapshot-retain-undo bookkeeping is touched on the Oriole side. Note: OrioleDB's `set_rel_pathlist_hook` rewrites every PG-native scan path (`Path` / `IndexPath` / `BitmapHeapPath`) into a `CustomScan` plan node ([scan.c:343](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L343)), so the executor never reaches `ExecIndexScan` / `IndexNext` / `index_fetch_heap` / `orioledb_index_fetch_tuple`. The tuple comes via Oriole's own custom-scan exec methods.

[exec_simple_query](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017) -- main entry point of query execution.
|
[start_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051) -- inside an open block this is just a state toggle, no new tx is started.
|
parse + [analyze](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195) + [plan](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198) -- yields a plan tree with a `CustomScan` node (Oriole's `o_scan_methods`) wrapping the index lookup.
|
[CreatePortal](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220) + [PortalStart](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239) + [PortalRun](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284) -- portal machinery wraps query execution.
	|
	[PortalRunSelect](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L766) -- SELECT-only branch; bypasses the multi-utility loop since there's no DDL or transaction-control to dispatch.
		|
		[ExecutorRun](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L922) -- generic executor entry; immediately delegates to the standard variant.
			|
			[standard_ExecutorRun](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L306) -- driver of the executor.
				|
				[ExecutePlan](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L360) -- pulls tuples from the plan tree's root node.
					|
					[ExecProcNode](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L1697) -- function-pointer dispatch via `planstate->ExecProcNode(planstate)`. For our `CustomScan` plan node the pointer was set to `ExecCustomScan` at plan init.
						|
						[ExecCustomScan](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeCustom.c#L114) -- PG's CustomScan executor; reached via function-pointer dispatch, so the link points to the function definition.
							|
							[methods->ExecCustomScan](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeCustom.c#L122) -- second function-pointer dispatch into the `CustomExecMethods` registered for this scan; for OrioleDB this resolves to `o_exec_custom_scan` via `o_scan_exec_methods`.
								|
								[o_exec_custom_scan](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L678) -- Oriole's `ExecCustomScan` callback; reached via function-pointer dispatch, so the link points to the function definition.
									|
									[o_exec_fetch](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L746) -- index-scan branch of `o_exec_custom_scan` (the `O_IndexPlan` case). Loops fetching tuples until one passes the qual.
										|
										[o_index_scan_getnext](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L552) -- per-call driver; advances the scan key range and pulls the next tuple.
											|
											[o_iterate_index](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L489) -- per-key-range driver. For an `exact` (point) lookup like `WHERE id = X` the `ostate->exact` branch is taken below.
												|
												[o_btree_find_tuple_by_key](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L377) -- exact-match B-tree lookup-by-key entry. Calls into the find machinery on the primary index.
													|
													[o_btree_find_tuple_by_key_cb](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L274) -- inner workhorse. Decides whether the visible version sits on the data page or has to be reconstructed by combining the data page with the undo log image (when our snapshot is in the past *and* this backend has its own pending changes on this tree).
														|
														[init_page_find_context](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L147-L149) -- snapshot-aware find context; the CSN is what drives "see this version, ignore newer-CSN tuples".
														|
														[find_page](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L155) -- root-to-leaf descent of the primary index. Each level acquires a page lock, evaluates the downlink for our key, releases the parent. STOPEVENT `step_down` fires per level, `page_read` fires after each leaf is read. **good point for injection** -- can be approached via the existing `page_read` stopevent (freeze) or a fresh ereport injection (abort), both safe outside critical sections.
														|
														[combinedResult branch](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L170) -- when our snapshot is older than the page CSN, combine on-page tuple with undo-log replay. Pure read path, no shared-state mutation.
													|
													visibility callback walk -- per-tuple `xactInfo.oxid` is consulted against the CSN buffer; if INPROGRESS or above-snapshot, follow the undo chain backward until visible version is found. Lock-free reads only.
										|
										[tts_orioledb_store_tuple](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L560) -- copies the visible tuple into the executor's scan slot. After this point the row is "delivered" to the SQL layer.
								|
								[o_exec_project](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L770) -- per-tuple projection (run only if the plan needs it). Returns the projected slot back up the dispatch chain.
					|
					slot returned to [ExecutePlan's loop](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L1697) which sends the row to `dest->receiveSlot`; on the next iteration `ExecProcNode` is called again until a NULL slot ends the loop.
		|
		`PortalRun` returns the rendered tuples to the client; portal teardown drops snapshots and releases per-statement memory.
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303) -- inside an open transaction block this just toggles state back to `IN_PROGRESS` (no commit happens here).
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354) -- second call at the end of `exec_simple_query` is a no-op (still in the same tx block).

Notes on injection candidates that are *unsuitable* for SELECT:

* Inside `find_page`'s page-lock acquisition -- wrapped in critical sections at the lowest level; ereport(ERROR) from there escalates to PANIC.
* During visibility callback while a tuple's `xactInfo` is being followed -- can be paused via stopevents but must not raise; raising mid-walk leaves no broken state on this backend (read-only) but pollutes the page-find context's image buffer.

The reader path doesn't touch undo or WAL on the producer side, so the bank-test's reader threads will not exercise the `wal_chaos` injection point at all -- only the `stopevent_chaos` ones (`page_read`, `step_down`).


## UPDATE TX FLOW

The bank-test writer issues e.g. `UPDATE o_bank_account SET balance = ?, token = ? WHERE id = X`, which mutates the PK row (balance/token columns) *and* requires updating the unique secondary index on `token`. PostgreSQL drives the two as separate operations: TableAM updates the primary index first, then the executor calls IndexAM `amupdate` for each affected secondary. The row to update is sourced from `ExecModifyTable`'s inner subplan, which in OrioleDB's case is a `CustomScan` (same path as SELECT, see [scan.c:343](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L343) -- `set_rel_pathlist_hook` rewrites all PG-native scan paths into Oriole's `CustomScan`). What follows traces only the modification path, not the row-fetch.

[exec_simple_query](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017) -- main entry point of query execution.
|
[start_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051) -- inside an open block this is just a state toggle.
|
parse + [analyze](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195) + [plan](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198) -- yields a plan tree with a ModifyTable node wrapping an index scan that locates the row to update.
|
[CreatePortal](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220) + [PortalStart](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239) + [PortalRun](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284) -- portal machinery wraps query execution.
	|
	[PortalRunMulti](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L789) -- UPDATE goes through Multi (its plan tree contains a non-SELECT mutation node).
		|
		[ProcessQuery](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1275) -- per-non-utility statement handler invoked by PortalRunMulti.
			|
			[ExecutorRun](https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L160) -- generic executor entry; immediately delegates to the standard variant.
				|
				[standard_ExecutorRun](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L306) -- driver of the executor.
					|
					[ExecutePlan](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L360) -- pulls tuples from the plan tree's root node.
						|
						[ExecModifyTable](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L3654) -- driver of the ModifyTable plan node. Reached via function-pointer dispatch through `ExecProcNode`, so the link points to the function definition. Per-row loop body below.
							|
							**Phase 0 -- fetch the candidate row from the inner subplan**
							|
							[ExecProcNode(subplanstate)](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L3760) -- per-iteration call inside ExecModifyTable that pulls the next candidate row from the inner subplan into `context.planSlot`. The slot also carries a junk attribute holding the rowid that TableAM will use to locate the on-disk tuple.
								|
								the inner subplan is OrioleDB's `CustomScan` (because `set_rel_pathlist_hook` rewrote the IndexScan path -- see `## SELECT TX FLOW`). The full fetch chain mirrors SELECT: `ExecCustomScan` → `o_exec_custom_scan` → [o_exec_fetch](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L746) → `o_index_scan_getnext` → `o_iterate_index` → `o_btree_find_tuple_by_key` → `find_page` → visibility walk → `tts_orioledb_store_tuple`. The returned slot becomes `context.planSlot`.
							|
							junk-attribute extraction -- `tableoid` / rowid / wholetuple junk fields are pulled out of `context.planSlot` to identify the target row and old tuple image. On NULL slot the per-row loop exits.
							|
							[ExecUpdate](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2479) -- per-row UPDATE handler invoked once the candidate row has been fetched. Splits into **two further phases**: PK side via TableAM, then SK side via IndexAM.
								|
								**Phase 1 -- PK update via TableAM**
								|
								[ExecUpdateAct](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2266) -- runs `BEFORE UPDATE` triggers, evaluates the new row, then dispatches to TableAM.
									|
									[table_tuple_update](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2024) -- TableAM dispatch via `relation->rd_tableam->tuple_update`. Falls into Oriole's hook below.
									|
									[orioledb_tuple_update](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L629) -- TableAM update hook. Receives the rowid of the row to mutate, the new tuple in `slot`, and the CommandId.
										|
										[get_current_oxid](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L657) -- ensures this backend has an oxid; allocates one lazily on first DML.
										|
										[get_keys_from_rowid](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L659-L660) -- decode old PK from rowid (same primitive as in SELECT).
										|
										[o_tbl_update](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L684-L685) -- main table-level update body.
											|
											[CheckCmdReplicaIdentity](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1103) -- pre-check for replication identity validity. Read-only at this point.
											|
											[tts_orioledb_form_tuple](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1237) -- materializes the new in-memory row image.
											|
											pkey-equality check -- if old/new PK keys are equal, take the in-place `overwrite` branch; otherwise the more invasive `reinsert` branch (delete old + insert new). The bank workload's UPDATE never changes `id`, so it's always overwrite.
											|
											[o_tbl_indices_overwrite](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1244-L1245) -- PK-only update entry for the in-place case. Despite the name, this only modifies the primary index; secondary indexes are updated later by IndexAM dispatch.
												|
												[o_btree_modify](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1578-L1582) with action = `BTreeOperationUpdate` -- public B-tree modify entry. Just dispatches to `o_btree_normal_modify`.
													|
													[o_btree_normal_modify](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L987) -- reserves undo space, reserves page-pool capacity, then walks the tree. STOPEVENT `modify_start` fires at the entry. Calls [find_page](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L155) (or `refind_page` when the caller passed a hint) to locate the target leaf and acquire the page write-lock; on success dispatches into `o_btree_modify_internal` with the populated page-find context.
														|
														[o_btree_modify_internal](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L110) -- operates on the already-found, write-locked leaf page. Reads the existing tuple via `BTREE_PAGE_READ_LEAF_ITEM`, runs the modify-callback (`o_update_callback`) for visibility / row-lock / self-modification decisions, and dispatches by action: `o_btree_modify_delete` / `o_btree_modify_lock` for those, or `o_btree_modify_insert_update` for the insert/update branch.
															|
															[o_btree_modify_insert_update](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L370) -- the insert/update sub-routine. Reserves an undo entry, then drives the page mutation.
																|
																[o_btree_modify_add_undo_record](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L701) -- pushes a `ModifyUndoItemType` undo entry capturing the pre-image and stores its location into `leafTuphdr->undoLocation`. Page is still write-locked but no mutation yet.
																|
																[o_btree_insert_tuple_to_leaf](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L725) -- enters the insert machinery in btree/insert.c (function definition at [insert.c#L1350](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1350)).
																	|
																	[o_btree_insert_item](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1374) -- dispatcher; contains **no** critical section. Walks the insert stack, handles split fix-ups, then dispatches to one of two leaf-mutation helpers depending on whether other backends are waiting to insert at the same leaf.
																		|
																		[o_btree_insert_item_no_waiters](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1337) -- typical UPDATE path (uncontended). The `BTreeItemPageFitAsIs` branch handles in-place tuple replacement. (The contended sibling [o_btree_insert_item_with_waiters](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1332) has the same crit-section structure at [insert.c#L1002-L1012](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1002), same lock-release-before-return property.)
																			|
																			[START_CRIT_SECTION](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1064) -- critical section begins. **NOT suitable for injection** -- ereport(ERROR) inside a critical section escalates to PANIC.
																			|
																			page mutation: `page_block_reads` + `memcpy` of new tuple header + body + `MARK_DIRTY`.
																			|
																			[unlock_page(blkno)](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1142) -- called **inside** the crit-section, one statement before `END_CRIT_SECTION`. **The leaf write-lock is dropped here**, before control returns up the callback chain.
																			|
																			[END_CRIT_SECTION](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1144) -- critical section ends.
															|
															control returns up: `o_btree_modify_insert_update` -> `o_btree_modify_internal`, which then calls [unlock_release(&context, false)](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L371). Note the `false` -- on this path [unlock_release](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L376) does **not** call `unlock_page` (the leaf was already unlocked by the insert machinery above); it only releases the reserved undo size and the page-pool reservation. The early-return / pre-mutation branches of `o_btree_modify_internal` ([L242](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L242), [L317](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L317), [L355](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L355)) call `unlock_release(..., true)` instead -- those paths bail before reaching the insert machinery and so still hold the leaf lock at exit.
											|
											**good point for injection (`orioledb-pk-mutated-pre-wal`)** -- at this instant the PK leaf has been mutated in shared buffers and *already write-unlocked* (the unlock happened deep inside the insert machinery, see above), the `ModifyUndoItemType` undo entry is pushed and `leafTuphdr->undoLocation` is wired up, but **no `WAL_REC_UPDATE` has been packed into the local WAL buffer yet** -- that is what the `o_wal_update` call below does. Concurrent readers can already hit this leaf (page is unlocked) and rely entirely on the lock-free undo chain + INPROGRESS CSN to resolve to the pre-image. Two interesting failure modes here: (a) `kill -9` of the postmaster -- shared mem holds the new tuple, undo holds the pre-image, but Postgres XLog has no record of the update for this oxid; recovery just sees this oxid never committed and there is nothing to replay or roll back from WAL. Exercises the "page-mutated-but-no-WAL" recovery case. (b) `ereport(ERROR)` -- drives `apply_undo_stack` to revert the leaf via `modify_undo_callback`, then `wal_rollback` emits `WAL_REC_ROLLBACK` with no preceding modify-record for this row in the local buffer -- "rollback of changes that never made it to WAL", which the recovery side must handle gracefully.
												|
												[o_wal_update](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1294) -- emits `WAL_REC_UPDATE` into the per-backend local WAL buffer.
												|
												[add_modify_wal_record](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/recovery/wal.c#L834) -- per-row WAL helper. Just packs the record into the local buffer; no flush yet. The "page-mutated-but-no-WAL" race is captured by the `orioledb-pk-mutated-pre-wal` injection one frame up (right before `o_wal_update`); a separate injection at the function level here would also fire on system-tree updates and is therefore not used.
							|
							control returns to ExecUpdate. **good point for injection (`orioledb-update-pk-done-pre-sk`)** -- PK leaf is mutated and unlocked in shared buffers, the `ModifyUndoItemType` undo is pushed, and `WAL_REC_UPDATE` is packed in the per-backend *local* WAL buffer (not yet flushed -- that happens at COMMIT via `flush_local_wal`). Secondary indexes still hold the old token entry. A reader scanning by `token` at this instant would see the row at the *old* token key. This is precisely the inconsistency window the bank-test's `reader_sk` cross-check probes for.
							|
							**Phase 2 -- SK update via IndexAM** (driven by PG executor for each affected secondary index)
							|
							[ExecUpdateIndexTuples](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2053) -- post-table-update entry; loops over the relation's indexes. For an unchanged index it skips, for a changed one it issues an update. The bank-test's UPDATE always changes `token`, so the unique SK is processed every time.
								|
								[index_update](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execIndexing.c#L707) call site -- per-index loop body inside the `ExecUpdateIndexTuples`.
									|
									[index_update](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/index/indexam.c#L266) -- IndexAM dispatch via `indexRelation->rd_indam->amupdate`. Falls into Oriole's per-index hook.
										|
										[orioledb_amupdate](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/indexam/handler.c#L666) -- IndexAM `amupdate` hook. Called once per affected secondary index. For non-orioledb indexes (e.g. via index_bridging) it forwards to standard `index_insert`; for native orioledb indexes it goes the path below.
											|
											key bound construction for old + new SK keys.
											|
											[o_update_secondary_index](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/indexam/handler.c#L759-L763) -- the actual SK B-tree work. Two B-tree modifies in sequence: delete-by-old-key, then insert-by-new-key.
												|
												old/new key equality short-circuit -- if SK key values didn't change, return immediately. The bank workload always changes `token`, so this branch is never taken.
												|
												[o_btree_modify(BTreeOperationDelete)](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1513-L1517) -- delete the old SK entry.
													|
													page lock + undo push (`ModifyUndoItemType` for the SK B-tree) + `START_CRIT_SECTION` + page mutation + `END_CRIT_SECTION`.
													|
													**No WAL emitted for the SK delete.** The regular `o_btree_modify` path used for user-table SK updates does *not* call `add_modify_wal_record` -- the `o_wal_insert/delete` calls at [modify.c:1526](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L1526) / [modify.c:1577](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L1577) live inside `o_btree_autonomous_insert/delete`, which assert `IS_SYS_TREE_OIDS(...)` and only fire for system-catalog trees.
												|
												**good point for injection** between SK-delete and SK-insert -- table state has the new PK row, the SK is *missing* the entry entirely (neither old key nor new key resolves to this row). A reader doing `SELECT ... ORDER BY token` would skip the row at this instant. The bank-test's `reader_sk` invariant catches this directly: SK-scan total != PK-scan total or `len(tokens) != n_accounts`.
												|
												[o_btree_modify(BTreeOperationInsert)](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1532-L1536) for non-unique SK / [o_btree_insert_unique](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1538-L1542) for unique SK -- insert the new SK entry. The bank workload's token-uniqueness index uses the unique variant, which performs the deferred-uniqueness check via PG's constraint trigger queue rather than inline.
													|
													page lock + undo push + `START_CRIT_SECTION` + page mutation + `END_CRIT_SECTION`.
													|
													**No WAL emitted for the SK insert either** (same reason as the SK delete above). The SK page mutation and undo entry are sufficient for the live transaction; recovery / replication will re-derive the SK changes from the PK's `WAL_REC_UPDATE` record by replaying the new+old PK rows through `o_tbl_indices_overwrite` on the redo side.
							|
							ExecUpdateIndexTuples loop continues for any remaining secondary index, then returns. ExecUpdate returns to ExecModifyTable.
						|
						ExecModifyTable iterates the inner subplan for the next row (none for our PK-equality `WHERE id = X`); on NULL slot it ends.
				|
				ExecutorRun / ProcessQuery return to PortalRunMulti, which loops to the next PlannedStmt or completes.
		|
		`PortalRun` finishes; portal teardown drops snapshots and releases per-statement memory.
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303) -- inside an open transaction block this just toggles state back to `IN_PROGRESS` (no commit happens here -- the writer's `con.commit()` later issues a separate `COMMIT` statement that drives the COMMIT TX FLOW).
|
[finish_xact_command](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354) -- second call at the end of `exec_simple_query` is a no-op.

Per writer tx the bank workload pushes ~6 `ModifyUndoItemType` items (PK update + SK delete + SK insert per UPDATE, ×2 because the tx has two UPDATEs) but emits only ~2 `WAL_REC_UPDATE` records (one per UPDATE, both from `o_wal_update` on the primary descriptor). SK index changes are *not* WAL-logged separately -- only the PK's update is, and recovery re-derives the SK page mutations by replaying the PK update through `o_tbl_indices_overwrite`. Everything is flushed at COMMIT.


## ABORT TX FLOW

Triggered by any `ereport(ERROR)` raised during the tx -- native serialization conflict, deferred-uniqueness violation at COMMIT, `wal_chaos` injection at flush boundary, `pg_terminate_backend`, statement timeout, etc. The ereport longjmps out of whatever call stack raised it; control lands in `PostgresMain`'s top-level error handler, which drives the abort path.

[ereport(ERROR, ...)](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/utils/error/elog.c#L346) -- error raised somewhere in Oriole or PG. `errstart` allocates an error data slot.
	|
	[errfinish](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/utils/error/elog.c#L477) -- finalizes the error record, then `siglongjmp`s out of the current call stack.
|
[PostgresMain sigsetjmp catch](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4426) -- the top-level catch in the backend's main loop receives the longjmp.
	|
	[EmitErrorReport](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4465) -- ships the error message to the client / log.
	|
	[AbortCurrentTransaction](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4482) -- the wrapper that drives the abort regardless of which TBLOCK state we were in.
		|
		[AbortCurrentTransactionInternal](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3430) -- state-machine dispatch; for the in-block-tx case (`TBLOCK_INPROGRESS`) unconditionally calls `AbortTransaction`.
			|
			[AbortTransaction](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3464) -- the actual rollback machinery. Fires xact-event callbacks, releases resources, transitions oxid state.
				|
				(For an explicit `ROLLBACK` SQL statement the entry path is different: `exec_simple_query` → `PortalRun` → `PortalRunMulti` → `PortalRunUtility` → `standard_ProcessUtility` → `TRANS_STMT_ROLLBACK` → `UserAbortTransactionBlock`, which only flips state to `TBLOCK_ABORT_PENDING`. The real abort runs at the next `finish_xact_command` via `CommitTransactionCommand` → `AbortTransaction`. Rarely hit in the bank-test because writers abort via `ereport(ERROR)`, not explicit ROLLBACK -- psycopg2's `con.rollback()` only fires after the server-side abort already happened.)
				|
				[CallXactCallbacks](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2393-L2394) with event = XACT_EVENT_ABORT -- triggers OrioleDB's registered handler (same registration as commit, see [RegisterXactCallback](https://github.com/orioledb/orioledb/blob/bbd7c1254e4cbd23bc4cafda02289c91609111e6/src/orioledb.c#L1210)).
					|
					[undo_xact_callback](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2110) with event = XACT_EVENT_ABORT -- the function's body runs the steps below in order. The XACT_EVENT_ABORT switch case at [undo.c:2327](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2327) is reached only after the pre-switch setup completes.
						|
						**Pre-switch setup (common to COMMIT and ABORT)**
						|
						`oxid = get_current_oxid_if_any()` and `isParallelWorker = ...` are evaluated at the top -- determines whether this tx ever allocated an oxid and whether we're running in a parallel worker (parallel workers take a fast path).
						|
						`ea_counters = NULL` -- clears EXPLAIN ANALYZE counters that the executor may have left dangling if the abort happened mid-node.
						|
						[seq_scans_cleanup](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2136) -- releases per-backend sequential-scan state (read locks on internal pages, scan iterators). Common to COMMIT and ABORT; runs unconditionally here, before any undo / WAL work.
						|
						[no-oxid / parallel-worker fast path](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2144-L2154) -- if `!OXidIsValid(oxid) || isParallelWorker` (read-only tx that never allocated an oxid, or a parallel worker leader), only run trivial state resets (`reset_cur_undo_locations`, `orioledb_reset_xmin_hook`, `reset_command_undo_locations`, clear `oxid_needs_wal_flush` / `xidless_commit_lsn` / `minParentSubId`) and exit. **No undo walk, no `wal_rollback`, no CSN flip** -- there's nothing to roll back. This is the path the bank-test's reader threads take on abort.
						|
						**Else branch -- a real oxid abort:**
						|
						`heapXid = GetTopTransactionIdIfAny()` and `get_current_logical_xid_ctx(&logicalXidContext)` -- captures the heap xid (if any) and logical-xid bookkeeping. Used to decide whether this is a pure-Oriole tx, a heap-only tx, or a `SWITCH_LOGICAL_XID` cross-engine tx. For the bank-test writer (Oriole-only) `heapXid == InvalidTransactionId`.
						|
						[Assert(!RecoveryInProgress())](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2215) -- abort callbacks must not fire on the recovery side; recovery has its own abort handling via `recovery_finish_current_oxid(COMMITSEQNO_ABORTED, ...)` driven by `WAL_REC_ROLLBACK` records.
						|
						**XACT_EVENT_ABORT switch case** -- the actual abort work begins below.
						|
						[wal_rollback](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2334) -- emits `WAL_REC_ROLLBACK` if the tx had material changes. **No-op for read-only or no-change txs.** The early bail at `local_wal_has_material_changes == false` skips WAL emission entirely.
							|
							[flush_local_wal_if_needed](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L351) -- ensure room for the rollback record.
							|
							[add_finish_wal_record(WAL_REC_ROLLBACK)](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L357-L358) -- pushes the rollback marker.
							|
							[flush_local_wal](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L359) -- flushes the local buffer to global XLog. **NOT suitable for re-injection here** -- we are already in the abort path; raising again would re-enter abort and PANIC. The `wal_chaos` injection point is gated by `isCommit` to skip exactly this call.
						|
						[apply_undo_stack(undoType)](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2336-L2337) per UndoLogType -- walks the per-backend undo chain backward from `sharedLocations->location` to InvalidUndoLocation, calling each item's abort callback.
							|
							[walk_undo_stack(abortTrx=true)](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1416) -- the iteration framework. Sets up the buffer for reading undo items.
								|
								[walk_undo_range_with_buf](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1375-L1378) -- read a contiguous range of undo items from the per-undo-type log.
									|
									[walk_undo_range loop](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1241) -- for-each-undo-item loop. Per item: read the undo record, dispatch to the type-specific callback (`modify_undo_callback` for `ModifyUndoItemType`), advance to `item->prev`. **good instrumentation point between iterations** -- partial undo applied, leaf pages reverted for the items processed so far but not the rest. Exactly the "torn rollback" race that the bank invariant catches if state is observably inconsistent at this moment. Two complementary modes here: (a) **ereport-style injection** -- raises mid-abort and re-enters the abort handler, which Postgres escalates to **PANIC**. Useful to test that recovery (and the postmaster's restart-after-crash) correctly finishes the partial rollback by replaying `WAL_REC_ROLLBACK` and re-applying undo. (b) **stopevent / `injection_points_attach('...', 'wait')` -style freeze** -- pauses the aborting backend on a condvar without raising; concurrent readers race against the partial state, then the test releases. Use this mode to expose visibility races without crashing the cluster.
									|
									per-item callback runs INSIDE the loop body -- for `ModifyUndoItemType` this restores the pre-image on the SK/PK leaf page (in-place, in shared buffers, under page lock). Each callback that mutates a page enters its own `START_CRIT_SECTION`/`END_CRIT_SECTION`, so injection MUST be *between* callbacks, never inside.
							|
							`branchLocation` / `onCommitLocation` cleanup -- after the iteration the function clears or repositions the undo head pointers under `undoStackLocationsFlushLock`.
						|
						**good instrumentation point between `apply_undo_stack` and `current_oxid_abort`** -- in-memory pages are reverted but the oxid's CSN is still INPROGRESS in `xidBuffer`. Concurrent readers consulting the CSN see "tx is still running" while the underlying data has already disappeared. Same two modes apply as inside the loop: (a) **ereport-style injection here triggers PANIC** because we are still in the abort handler (re-entrant ereport during XACT_EVENT_ABORT). Useful for stressing the crash-restart path -- recovery sees `WAL_REC_ROLLBACK`, replays it, re-flips CSN to ABORTED. (b) **stopevent / `injection_wait`-style freeze** is the safe option for live cluster races -- pause here, let other backends consult the inconsistent CSN+page state, then release. The bank-test's invariants catch any observable anomaly.
						|
						[wal_after_commit](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2343) -- clears `commitInProgressXlogLocation`. Same routine used by the commit path. NOT a write to WAL.
						|
						[reset_cur_undo_locations + reset_command_undo_locations](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2345-L2346) -- forget local pointers into the undo log.
						|
						[current_oxid_abort](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2347) -- the visibility flip.
							|
							[set_oxid_csn(curOxid, COMMITSEQNO_ABORTED)](https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1493) -- single atomic write that makes the tx invisible to all snapshots. Same lock-free CAS shape as the commit-side `set_oxid_csn`, with the same fast-path / on-disk-fallback split.
						|
						[set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr)](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2348) -- clears the xlog-ptr buffer slot. Pairs with the CSN write so the oxid's two-tuple state is `{ABORTED, InvalidXLogRecPtr}`.
						|
						registered-snapshot teardown loop -- frees `retainUndoLocHeaps` so subsequent `runXmin` advances aren't blocked by this tx's snapshot.
						|
						`xidless_commit_lsn` / `oxid_needs_wal_flush` / `in_nontransactional_truncate` resets.
					|
					**End of XACT_EVENT_ABORT switch case; common COMMIT/ABORT post-switch cleanup follows.**
					|
					[release_undo_size per UndoLogType](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2380-L2381) -- returns this backend's reserved undo-location quota to the global pool so other txs can use it.
					|
					[ppool_release_reserved per OPagePoolType](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2383-L2387) -- releases reserved page-pool slots that this tx had set aside for its modifications.
					|
					[free_retained_undo_location per UndoLogType](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2390-L2391) -- final teardown of any per-tx retained undo locations not yet released.
				|
				`undo_xact_callback` returns to PG's `CallXactCallbacks` loop, which then proceeds to subsequent registered xact callbacks (if any).
			|
			`AbortTransaction` returns; transaction state is now `TBLOCK_ABORT` (or `TBLOCK_DEFAULT` for non-block aborts). [CleanupTransaction](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3465) is called next to free the TransactionState memory.
		|
		`AbortCurrentTransactionInternal` returns true (state machine is done); the [while loop in AbortCurrentTransaction](https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3430) terminates.
	|
	control returns from `AbortCurrentTransaction` back into the `PostgresMain` `sigsetjmp` block.
|
[PostgresMain main loop](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4486) resumes -- emits `ReadyForQuery` to the client (with status `'E'` for "in failed transaction"), then waits in [ReadCommand](https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4500) for the next client message. The client is expected to issue `ROLLBACK` (or `COMMIT`, which is also treated as ROLLBACK in TBLOCK_ABORT) to exit the failed-transaction state. psycopg2 issues `ROLLBACK` automatically on the next `con.rollback()` call from the bank-test's `except Exception:` branch.

Two-mode reminder for the whole ABORT body:

* **ereport-style injection inside the abort flow always escalates to PANIC** (because we are already inside the XACT_EVENT_ABORT handler). That is *not* useless -- it deliberately drives the postmaster's "crash of another server process" path, forcing crash recovery to replay `WAL_REC_ROLLBACK` and finish the partial undo on next startup. Use this mode when the goal is to stress crash-recovery correctness or the postmaster's restart loop.
* **stopevent / `injection_points_attach('...', 'wait')`-style freezes are the live-cluster-safe alternative.** They pause the aborting backend on a condvar without raising, so concurrent readers/writers race against the partial state while the backend stays alive. Use this mode when the goal is to expose visibility / consistency races without taking the cluster down. The bank-test's existing `stopevent_chaos` worker is the right harness for this.

Notes on places that are *unsuitable for either mode*:

* Inside `walk_undo_range_with_buf`'s per-item callback when the callback is mid-`START_CRIT_SECTION` -- both modes break here. `ereport(ERROR)` inside a critical section escalates to PANIC even outside of abort context. Stopevents pause execution, but pausing while holding LWLocks / page-pin reservations inside a critical section starves other backends; the cluster won't deadlock outright but will hang until the freeze is released.
* After `current_oxid_abort` -- Postgres comment in `CommitTransaction` (which mirrors the abort cleanup style) explicitly says "if an error is raised here, it's too late to abort the transaction. This should be just noncritical resource releasing." For the abort tail the same caveat applies: ereport here turns into PANIC, AND there is nothing left to observe (the CSN flip is done), so freeze-style injection only delays cleanup without exposing anything new. Skip this region.

