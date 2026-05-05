
## BEGIN TX FLOW
<pre>
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017">exec_simple_query</a> -- main entry point of query execution. A tx creation goes through it.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051">start_xact_command</a> -- starts every tx. Assigns some IDs and set up proper env for tx.
		<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L2774">StartTransactionCommand</a> -- perform initial tx setup
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2180">AtStart_Cache</a> -- process all invalidation msgs at the start of tx for valid cache behavior. 
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1059">drop_unnamed_stmt</a> -- drops artifacts of former exec plan
|
switch mem ctx + parsing trees 
|
for 
	|
	some preparing stuff 
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195">analyze</a> &amp; <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198">plan</a> query trees
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220">CreatePortal</a> -- creating a portal for query execution
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239">PortalStart</a> -- initial start of a portal without actual query execution. Need to be done for portal configuration like set-up of output format etc
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284">PortalRun</a> -- actual execution of a query
		|
		some set-up stuff
		PG_TRY() -- on exception we do some memory ctx management, mark Portal as failed and simply rethrow 
			|
			PortalRun(Multi|Select) -- for most cases it calls <a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L789-L790">Multi</a> version, begin/commit tx block is not an exception.
				| 
				<a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1215">loops</a> over every query with <a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1309-L1321">PortalRunUtility</a> execution, PortalRunUtility itself create a snapshot if required and call <a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1156-L1163">ProcessUtility</a> that calls Oriole's hook <a href="https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L872-L879">orioledb_utility_command</a> . Oriole apply any modification if provided and <a href="https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L1521-L1524">forward execution to standard_process_utility</a> 
					|
					Falls into TRANS_STMT_BEGIN/TRANS_STMT_START block.
					<a href="https://github.com/orioledb/postgres/blob/e2fb3dfa817fbe89494a62c100e9cb442f4d6b15/src/backend/tcop/utility.c#L609-L626">begin tx and set necessary env vars that required by isolation level of tx</a> standard_ProcessUtility and <a href="https://github.com/orioledb/postgres/blob/e2fb3dfa817fbe89494a62c100e9cb442f4d6b15/src/backend/tcop/utility.c#L1089">ProcessUtilitySlow</a> are the main processing functions for utility stmts in pg. The difference between them is that Slow version dedicated for processing trigger-based events. 
				|
				PortalRunMulti drops all snapshots and release memory 
		
		memory releasing phase for almost every func in callstack ....
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303">finish_xact_command</a> fell down to <a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3185-L3187">simply toggling tx state to IN_PROGRESS</a> 
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354">finish_xact_command</a> here is literally no-op
</pre>
## COMMIT TX FLOW

**Note on recursive (PANIC-prone) injection points in this section.** Several commit-side helpers are *also* called from the abort handler (`undo_xact_callback` at `XACT_EVENT_ABORT`). Attaching an `error`-mode injection at the function entry would fire on the commit path, raise ereport, drive abort, and re-fire inside the abort handler -- ereport during `XACT_EVENT_ABORT` escalates to **PANIC**. Such points are tagged `**RECURSIVE -- raises PANIC unless gated by isCommit**` in the flow below. The standard fix is to wrap the `INJECTION_POINT` with an `isCommit` (or `!IsAbortPath`) check at the call site, the same pattern already used at [wal.c#L684](https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L684); alternatively, branch on a discriminator parameter (e.g. `rec_type == WAL_REC_COMMIT`, `csn != COMMITSEQNO_ABORTED`) inside the function, or define separate point names for commit-side and abort-side. Affected helpers in this flow: `set_oxid_xlog_ptr_internal`, `flush_local_wal_if_needed`, `add_finish_wal_record`, both `set_oxid_csn` calls.

Two-stage. The TRANS_STMT_COMMIT case **only flips state**; the actual commit happens later in `finish_xact_command` → `CommitTransactionCommand` → `CommitTransaction`


<pre>
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017">exec_simple_query</a> -- main entry point of query execution. A tx creation goes through it.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051">start_xact_command</a> -- starts every tx. Assigns some IDs and set up proper env for tx.
		<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L2774">StartTransactionCommand</a> -- perform initial tx setup
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2180">AtStart_Cache</a> -- process all invalidation msgs at the start of tx for valid cache behavior. 
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1059">drop_unnamed_stmt</a> -- drops artifacts of former exec plan
|
switch mem ctx + parsing trees 
|
for 
	|
	some preparing stuff 
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195">analyze</a> &amp; <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198">plan</a> query trees
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220">CreatePortal</a> -- creating a portal for query execution
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239">PortalStart</a> -- initial start of a portal without actual query execution. Need to be done for portal configuration like set-up of output format etc
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284">PortalRun</a> -- actual execution of a query
		|
		some set-up stuff
		PG_TRY() -- on exception we do some memory ctx management, mark Portal as failed and simply rethrow 
			|
			PortalRun(Multi|Select) -- for most cases it calls <a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L789-L790">Multi</a> version, begin/commit tx block is not an exception.
				|
				<a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1215">loops</a> over every query with <a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1309-L1321">PortalRunUtility</a> execution, PortalRunUtility itself create a snapshot if required and call <a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1156-L1163">ProcessUtility</a> that calls Oriole's hook <a href="https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L872-L879">orioledb_utility_command</a> . Oriole apply any modification if provided and <a href="https://github.com/orioledb/orioledb/blob/d94c141098441d89b0e403d69ff3fc67586731c2/src/catalog/ddl.c#L1521-L1524">forward execution to standard_process_utility</a> 
					|
					Falls into TRANS_STMT_COMMIT block.
					Simply no-op in case of commit as a <a href="https://github.com/orioledb/postgres/blob/e2fb3dfa817fbe89494a62c100e9cb442f4d6b15/src/backend/tcop/utility.c#L631">EndTransactionBlock</a> returns true for 'true' commit commands.
				|
				PortalRunMulti drops all snapshots and release memory 
			
			memory releasing phase for almost every func in callstack ....
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303">finish_xact_command</a> 
	|
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L2805">CommitTransactionCommand</a> -&gt; <a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3136">CommitTransactionCommandInternal</a> -&gt; <a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3175">CommitTransaction</a>
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2255-L2256">CallXactCallbacks</a> -- triggers custom storage engines (e.g. OrioleDB) callbacks that were registered with <a href="https://github.com/orioledb/orioledb/blob/bbd7c1254e4cbd23bc4cafda02289c91609111e6/src/orioledb.c#L1210">RegisterXactCallback</a>.
			|
			<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2110">undo_xact_callback</a> with event = XACT_EVENT_PRE_COMMIT
				|
				in general out tx is considered as an independent oriole tx.
				|
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2232">precommit_undo_stack</a> 
					|
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1426-L1429">walk_undo_range_with_buf</a> 
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1290-L1291">walk_undo_range</a> -- Walk through in the undo stack in a foor loop calling the callbacks for each item. **good point for injection** between different iterations, for making partial change visible. **notice, that loop make != 0 iterations only in rollback/abort scenario** 
				| 
				other if branches are not taken
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2232-L2246">loops over the triggers</a> that should be called before commits and close all portals 
		After this point any user-defined code can't be executed, but errors still may appear.
		|
		<a href="https://github.com/orioledb/postgres/blob/1d7f7407002d8f7ffb2f5c815cef418fa10f3777/src/backend/commands/tablecmds.c#L17638">PreCommit_on_commit_actions</a> **good point for injection (<code>postgres-precommit-on-commit-actions</code>)** due commets before function explicitly says that error may be ecountered. Function itself truncate and delete some relations/rows that should be removed after tx commit. Function perform a lot modifications, so it seems good to inject faults in multiple places within that func. (e.g. <a href="https://github.com/orioledb/postgres/blob/1d7f7407002d8f7ffb2f5c815cef418fa10f3777/src/backend/commands/tablecmds.c#L17706">before/after of pushing snapshot</a>)
		| 
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2293">smgrDoPendingSync</a> -- Synchronize files that are created and not WAL-logged during this transaction. **good point for injection**. Synchronization can performed by emitting extra wal records for smaller relations. It worth testing whether test bank_account relations is considered as a small one. **ALSO SEEMS UNREACHABLE FOR ORIOLE**
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2304">PreCommit_Notify</a> -- irrelevant for us, only relevant for LISTEN/UNLISTEN, NOTIFY, but looks good to abort tx right after the notification.
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2343">RecordTransactionCommit</a> -- actual machinery for marking tx as commited. **good point for injection** 
			|
			<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L1505">XLogFlush</a> -- flushes *ALL* wal records (as oriole flushed into common buffer before in callback)  (for SYNC MODE)
			<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L1526">XLogSetAsynCXactLSN</a> -- reports the latest LSN for wal writer (for ASYNC MODE)
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2371">ProcArrayEndTransaction</a> -- marks tx as invalid. Some manipulations are performed under the lock in crit section, so it seems unintended behavior to throw an errors here.
		|
		after that point we have a comment in the code:
		"This is all post-commit cleanup.  Note that if an error is raised here, it's too late to abort the transaction.  This should be just noncritical resource releasing."
		So it seems that fault-injection here is irrelevant
		| 
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2393-L2394">CallXactCallbacks</a> 
			|
			<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2110">undo_xact_callback</a> with event = XACT_EVENT_COMMIT 
				|
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2136">seq_scan_cleanup</a> -- inside pg defined CRIT_SECTION clean-up resources dedicated to seq scans
				|
				in general out tx is considered as an independent oriole tx.
				|
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2262">assign_xidless_commit_lsn</a> 
					|
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L219">current_oxid_xlog_precommit</a> 
						|
						<a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1410-L1412">set_oxid_xlog_ptr</a> 
							|
							<a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L649">set_oxid_xlog_ptr_internal</a> -- changes a commit ptr to one dedicated to curr tx inside xidBuffer in lockfree manner **maybe good place for fault-injection (<code>orioledb-set-xlog-ptr</code> / <code>orioledb-set-xlog-ptr-guarded</code>)**, let's put maybe fault or barrier right before <a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L614">CAS</a>. **RECURSIVE -- raises PANIC if injected without an <code>isCommit</code> guard.** <code>set_oxid_xlog_ptr</code> is also called on the abort path at <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2348">undo.c#L2348</a> (<code>set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr)</code> inside the abort case of <code>undo_xact_callback</code>). An <code>error</code>-mode injection attached during commit raises ereport, ereport drives abort, abort re-enters the same function and re-fires the injection during <code>XACT_EVENT_ABORT</code> -&gt; PANIC. Mitigation: gate the <code>INJECTION_POINT</code> with an <code>isCommit</code>-style flag at the call site (the <a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L684">wal.c#L684</a> pattern), or use distinct point names for commit-side and abort-side. 
					| 
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L220">wal_commit</a> 
						|
						<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L290">flush_local_wal_if_needed</a> -- may or may not cause flush of wal according to the size threshold, so it looks **good place for fault-injection (<code>orioledb-wal-flush</code> / <code>orioledb-wal-flush-guarded</code>)**. (???) Maybe need further investigation. **RECURSIVE -- raises PANIC unless gated by <code>isCommit</code>.** <code>flush_local_wal_if_needed</code> is also called on the abort path from <code>wal_rollback</code> at <a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L352">wal.c#L352</a> when reserving space for the <code>WAL_REC_ROLLBACK</code> finish record. The current <code>INJECTION_POINT("orioledb-wal-flush")</code> at <a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L721">wal.c#L721</a> is **NOT** gated and CAN re-fire during abort if the local WAL buffer is near-full when <code>wal_rollback</code> runs. Compare with the correctly-gated sibling at <a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L684">wal.c#L684</a> (<code>if (isCommit) INJECTION_POINT(...)</code>). Mitigation: wrap the L721 injection in an <code>isCommit</code> (or <code>!IsAbortPath</code>) guard at the call site, same as the L684 commit-flush variant. 
						|
						<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L294">add_xid_wal_record</a> -- no lock-free, single threaded code
						|
						<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L296">add_finish_wal_record</a> -- **good point (<code>orioledb-add-finish-wal</code> / <code>orioledb-add-finish-wal-guarded</code>)** It worths testing to inject-fault during wal building right before finish record. **RECURSIVE -- raises PANIC if injected at the function level without an <code>isCommit</code> guard.** <code>add_finish_wal_record</code> is also called on the abort path from <code>wal_rollback</code> at <a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L358">wal.c#L358</a> (<code>add_finish_wal_record(WAL_REC_ROLLBACK, ...)</code>). An <code>error</code>-mode injection inside the function entry would fire on both <code>WAL_REC_COMMIT</code> (commit) and <code>WAL_REC_ROLLBACK</code> (abort); the second hit happens during <code>XACT_EVENT_ABORT</code> -&gt; PANIC. Mitigation: place the <code>INJECTION_POINT</code> either at the call site in <code>wal_commit</code> only (<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L296">wal.c#L296</a>), or branch on the <code>rec_type</code> parameter inside the function (<code>if (rec_type == WAL_REC_COMMIT) INJECTION_POINT(...)</code>), or use distinct point names for commit vs rollback finishes.
						|
						<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L297">flush_local_wal</a> -- flushes local oriole wal to single wal stream
					|
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L221">set_oxid_xlog_ptr</a> -- covered earlier
				| 
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2290">current_oxid_precommit</a> 
					|
					<a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1397-L1399">set_oxid_csn</a> -- same as set_oxid_xlog_ptr_internal, but with different fields. So **also a good place for fault-injection (<code>orioledb-set-csn</code> / <code>orioledb-set-csn-guarded</code>)**. **RECURSIVE -- raises PANIC unless guarded.** <code>set_oxid_csn</code> is called both on commit (<code>current_oxid_precommit</code> at <a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1397">oxid.c#L1397</a> and <code>current_oxid_commit</code> at <a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1475">oxid.c#L1475</a>) and on abort, inside <code>current_oxid_abort</code> at <a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1493">oxid.c#L1493</a> (<code>set_oxid_csn(curOxid, COMMITSEQNO_ABORTED)</code>), reached from <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2347">undo.c#L2347</a>. An <code>error</code>-mode injection inside the function fires on the commit-side CSN flip first; ereport drives abort, abort calls <code>current_oxid_abort</code> which re-enters <code>set_oxid_csn</code> and re-fires the injection during <code>XACT_EVENT_ABORT</code> -&gt; PANIC. Mitigation: place the <code>INJECTION_POINT</code> at the commit call sites only (in <code>current_oxid_precommit</code> / <code>current_oxid_commit</code>), not at the function entry; alternatively branch on the incoming <code>csn</code> value (<code>csn != COMMITSEQNO_ABORTED</code>), or use distinct point names. 
				| 
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2291-L2293">increments csn </a>
				|
				**good place for injection (<code>orioledb-csn-incremented</code>)**  Different atomics are modified sequentially in a lock-free manner, inject fault between such modifications. <code>nextCommitSeqNo</code> has already advanced and is visible to every backend acquiring a fresh CSN, but our oxid still publishes <code>COMMITSEQNO_COMMITTING</code> in <code>xidBuffer</code> (set by <code>current_oxid_precommit</code> above) -- snapshot acquirers see a CSN past ours while our slot still says "committing". Commit-side only -- not reached on the abort path, no <code>-guarded</code> companion needed.
				|
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2295">current_oxid_commit</a> 
					|
					<a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1475-L1476">set_oxid_csn</a> -- again some lock-free stuff. **RECURSIVE with the abort-side <code>current_oxid_abort</code> -- same caveat as the precommit <code>set_oxid_csn</code> above; raise PANIC unless gated.** Re-uses the same function on the abort path; either gate the <code>INJECTION_POINT</code> at this commit call site only, or branch on <code>csn != COMMITSEQNO_ABORTED</code>.
					|
					<a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1480">advance_run_xmin</a> -- CAS loop (**may be good injection point, need further investigation into min xid behavior**)
					| 
					<a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1482">release_assigned_logical_xids</a> -- some atomics manipulations
				| 
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2307-L2310">on_commit_undo_stack</a> -- good place for injection, especially between iterations if any (no implementation -- function-entry placement was tried and removed; correct placement would require instrumenting the loop interior).
				| 
				some clean-up stuff
				| 
				<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2378-L2392">also clean-up</a>  
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2418-L2423">ResourceOwnerRelease</a> Release Locks and AfterLocks
		| 
		AtCommitNotify and other per-backend clean up stuff
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354">finish_xact_command</a> for the second time is no-op
</pre>

<pre>
oxid space:   ▒▒▒▒▒▒▒▒│░░░░░░░░│████████████→  growing oxids
              └──A──┘  └──B──┘  └─────C─────┘
                       ↑        ↑
                       │        │
                writtenXmin   writeInProgressXmin
</pre>

| Zone  | Range                                      | State of `xidBuffer[oxid % size]`                                 | Status of the data                |
| ----- | ------------------------------------------ | ----------------------------------------------------------------- | --------------------------------- |
| **A** | `oxid < writtenXmin`                       | Already overwritten or recyclable for newer oxids                 | Authoritative copy is **on disk** |
| **B** | `writtenXmin ≤ oxid < writeInProgressXmin` | Being torn down right now; entries are FROZEN sentinels mid-write | Disk write in progress; transient |
| **C** | `oxid ≥ writeInProgressXmin`               | Authoritative ring slot, owned by the regular CAS protocol        | In memory, lock-free reads/writes |

## SELECT TX FLOW

The bank-test reader issues e.g. `SELECT balance, token FROM o_bank_account WHERE id = X` inside an already-open RR transaction. Read-only path: no undo push, no WAL emission, no page mutation; only the snapshot-retain-undo bookkeeping is touched on the Oriole side. Note: OrioleDB's `set_rel_pathlist_hook` rewrites every PG-native scan path (`Path` / `IndexPath` / `BitmapHeapPath`) into a `CustomScan` plan node ([scan.c:343](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L343)), so the executor never reaches `ExecIndexScan` / `IndexNext` / `index_fetch_heap` / `orioledb_index_fetch_tuple`. The tuple comes via Oriole's own custom-scan exec methods.

<pre>
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017">exec_simple_query</a> -- main entry point of query execution.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051">start_xact_command</a> -- inside an open block this is just a state toggle, no new tx is started.
|
parse + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195">analyze</a> + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198">plan</a> -- yields a plan tree with a <code>CustomScan</code> node (Oriole's <code>o_scan_methods</code>) wrapping the index lookup.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220">CreatePortal</a> + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239">PortalStart</a> + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284">PortalRun</a> -- portal machinery wraps query execution.
	|
	<a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L766">PortalRunSelect</a> -- SELECT-only branch; bypasses the multi-utility loop since there's no DDL or transaction-control to dispatch.
		|
		<a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L922">ExecutorRun</a> -- generic executor entry; immediately delegates to the standard variant.
			|
			<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L306">standard_ExecutorRun</a> -- driver of the executor.
				|
				<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L360">ExecutePlan</a> -- pulls tuples from the plan tree's root node.
					|
					<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L1697">ExecProcNode</a> -- function-pointer dispatch via <code>planstate-&gt;ExecProcNode(planstate)</code>. For our <code>CustomScan</code> plan node the pointer was set to <code>ExecCustomScan</code> at plan init.
						|
						<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeCustom.c#L114">ExecCustomScan</a> -- PG's CustomScan executor; reached via function-pointer dispatch, so the link points to the function definition.
							|
							<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeCustom.c#L122">methods->ExecCustomScan</a> -- second function-pointer dispatch into the <code>CustomExecMethods</code> registered for this scan; for OrioleDB this resolves to <code>o_exec_custom_scan</code> via <code>o_scan_exec_methods</code>.
								|
								<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L678">o_exec_custom_scan</a> -- Oriole's <code>ExecCustomScan</code> callback; reached via function-pointer dispatch, so the link points to the function definition.
									|
									<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L746">o_exec_fetch</a> -- index-scan branch of <code>o_exec_custom_scan</code> (the <code>O_IndexPlan</code> case). Loops fetching tuples until one passes the qual.
										|
										<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L552">o_index_scan_getnext</a> -- per-call driver; advances the scan key range and pulls the next tuple.
											|
											<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L489">o_iterate_index</a> -- per-key-range driver. For an <code>exact</code> (point) lookup like <code>WHERE id = X</code> the <code>ostate-&gt;exact</code> branch is taken below.
												|
												<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L377">o_btree_find_tuple_by_key</a> -- exact-match B-tree lookup-by-key entry. Calls into the find machinery on the primary index.
													|
													<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L274">o_btree_find_tuple_by_key_cb</a> -- inner workhorse. Decides whether the visible version sits on the data page or has to be reconstructed by combining the data page with the undo log image (when our snapshot is in the past *and* this backend has its own pending changes on this tree).
														|
														<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L147-L149">init_page_find_context</a> -- snapshot-aware find context; the CSN is what drives "see this version, ignore newer-CSN tuples".
														|
														<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L155">find_page</a> -- root-to-leaf descent of the primary index. Each level acquires a page lock, evaluates the downlink for our key, releases the parent. STOPEVENT <code>step_down</code> fires per level, <code>page_read</code> fires after each leaf is read. **good point for injection** -- can be approached via the existing <code>page_read</code> stopevent (freeze) or a fresh ereport injection (abort), both safe outside critical sections.
														|
														<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L170">combinedResult branch</a> -- when our snapshot is older than the page CSN, combine on-page tuple with undo-log replay. Pure read path, no shared-state mutation.
													|
													visibility callback walk -- per-tuple <code>xactInfo.oxid</code> is consulted against the CSN buffer; if INPROGRESS or above-snapshot, follow the undo chain backward until visible version is found. Lock-free reads only.
										|
										<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/index_scan.c#L560">tts_orioledb_store_tuple</a> -- copies the visible tuple into the executor's scan slot. After this point the row is "delivered" to the SQL layer.
								|
								<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L770">o_exec_project</a> -- per-tuple projection (run only if the plan needs it). Returns the projected slot back up the dispatch chain.
					|
					slot returned to <a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L1697">ExecutePlan's loop</a> which sends the row to <code>dest-&gt;receiveSlot</code>; on the next iteration <code>ExecProcNode</code> is called again until a NULL slot ends the loop.
		|
		<code>PortalRun</code> returns the rendered tuples to the client; portal teardown drops snapshots and releases per-statement memory.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303">finish_xact_command</a> -- inside an open transaction block this just toggles state back to <code>IN_PROGRESS</code> (no commit happens here).
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354">finish_xact_command</a> -- second call at the end of <code>exec_simple_query</code> is a no-op (still in the same tx block).
</pre>
Notes on injection candidates that are *unsuitable* for SELECT:

* Inside `find_page`'s page-lock acquisition -- wrapped in critical sections at the lowest level; ereport(ERROR) from there escalates to PANIC.
* During visibility callback while a tuple's `xactInfo` is being followed -- can be paused via stopevents but must not raise; raising mid-walk leaves no broken state on this backend (read-only) but pollutes the page-find context's image buffer.

The reader path doesn't touch undo or WAL on the producer side, so the bank-test's reader threads will not exercise the `wal_chaos` injection point at all -- only the `stopevent_chaos` ones (`page_read`, `step_down`).


## UPDATE TX FLOW

The bank-test writer issues e.g. `UPDATE o_bank_account SET balance = ?, token = ? WHERE id = X`, which mutates the PK row (balance/token columns) *and* requires updating the unique secondary index on `token`. PostgreSQL drives the two as separate operations: TableAM updates the primary index first, then the executor calls IndexAM `amupdate` for each affected secondary. The row to update is sourced from `ExecModifyTable`'s inner subplan, which in OrioleDB's case is a `CustomScan` (same path as SELECT, see [scan.c:343](https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L343) -- `set_rel_pathlist_hook` rewrites all PG-native scan paths into Oriole's `CustomScan`). What follows traces only the modification path, not the row-fetch.

<pre>
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1017">exec_simple_query</a> -- main entry point of query execution.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1051">start_xact_command</a> -- inside an open block this is just a state toggle.
|
parse + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1194-L1195">analyze</a> + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1197-L1198">plan</a> -- yields a plan tree with a ModifyTable node wrapping an index scan that locates the row to update.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1220">CreatePortal</a> + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1239">PortalStart</a> + <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1278-L1284">PortalRun</a> -- portal machinery wraps query execution.
	|
	<a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L789">PortalRunMulti</a> -- UPDATE goes through Multi (its plan tree contains a non-SELECT mutation node).
		|
		<a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L1275">ProcessQuery</a> -- per-non-utility statement handler invoked by PortalRunMulti.
			|
			<a href="https://github.com/orioledb/postgres/blob/0c466f5e0b34bb9ddc53a422e9872b726f5f9620/src/backend/tcop/pquery.c#L160">ExecutorRun</a> -- generic executor entry; immediately delegates to the standard variant.
				|
				<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L306">standard_ExecutorRun</a> -- driver of the executor.
					|
					<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execMain.c#L360">ExecutePlan</a> -- pulls tuples from the plan tree's root node.
						|
						<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L3654">ExecModifyTable</a> -- driver of the ModifyTable plan node. Reached via function-pointer dispatch through <code>ExecProcNode</code>, so the link points to the function definition. Per-row loop body below.
							|
							**Phase 0 -- fetch the candidate row from the inner subplan**
							|
							<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L3760">ExecProcNode(subplanstate)</a> -- per-iteration call inside ExecModifyTable that pulls the next candidate row from the inner subplan into <code>context.planSlot</code>. The slot also carries a junk attribute holding the rowid that TableAM will use to locate the on-disk tuple.
								|
								the inner subplan is OrioleDB's <code>CustomScan</code> (because <code>set_rel_pathlist_hook</code> rewrote the IndexScan path -- see <code>## SELECT TX FLOW</code>). The full fetch chain mirrors SELECT: <code>ExecCustomScan</code> → <code>o_exec_custom_scan</code> → <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/scan.c#L746">o_exec_fetch</a> → <code>o_index_scan_getnext</code> → <code>o_iterate_index</code> → <code>o_btree_find_tuple_by_key</code> → <code>find_page</code> → visibility walk → <code>tts_orioledb_store_tuple</code>. The returned slot becomes <code>context.planSlot</code>.
							|
							junk-attribute extraction -- <code>tableoid</code> / rowid / wholetuple junk fields are pulled out of <code>context.planSlot</code> to identify the target row and old tuple image. On NULL slot the per-row loop exits.
							|
							<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2479">ExecUpdate</a> -- per-row UPDATE handler invoked once the candidate row has been fetched. Splits into **two further phases**: PK side via TableAM, then SK side via IndexAM.
								|
								**Phase 1 -- PK update via TableAM**
								|
								<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2266">ExecUpdateAct</a> -- runs <code>BEFORE UPDATE</code> triggers, evaluates the new row, then dispatches to TableAM.
									|
									<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2024">table_tuple_update</a> -- TableAM dispatch via <code>relation-&gt;rd_tableam-&gt;tuple_update</code>. Falls into Oriole's hook below.
									|
									<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L629">orioledb_tuple_update</a> -- TableAM update hook. Receives the rowid of the row to mutate, the new tuple in <code>slot</code>, and the CommandId.
										|
										<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L657">get_current_oxid</a> -- ensures this backend has an oxid; allocates one lazily on first DML.
										|
										<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L659-L660">get_keys_from_rowid</a> -- decode old PK from rowid (same primitive as in SELECT).
										|
										<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/handler.c#L684-L685">o_tbl_update</a> -- main table-level update body.
											|
											<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1103">CheckCmdReplicaIdentity</a> -- pre-check for replication identity validity. Read-only at this point.
											|
											<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1237">tts_orioledb_form_tuple</a> -- materializes the new in-memory row image.
											|
											pkey-equality check -- if old/new PK keys are equal, take the in-place <code>overwrite</code> branch; otherwise the more invasive <code>reinsert</code> branch (delete old + insert new). The bank workload's UPDATE never changes <code>id</code>, so it's always overwrite.
											|
											<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1244-L1245">o_tbl_indices_overwrite</a> -- PK-only update entry for the in-place case. Despite the name, this only modifies the primary index; secondary indexes are updated later by IndexAM dispatch.
												|
												<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1578-L1582">o_btree_modify</a> with action = <code>BTreeOperationUpdate</code> -- public B-tree modify entry. Just dispatches to <code>o_btree_normal_modify</code>.
													|
													<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L987">o_btree_normal_modify</a> -- reserves undo space, reserves page-pool capacity, then walks the tree. STOPEVENT <code>modify_start</code> fires at the entry. Calls <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/iterator.c#L155">find_page</a> (or <code>refind_page</code> when the caller passed a hint) to locate the target leaf and acquire the page write-lock; on success dispatches into <code>o_btree_modify_internal</code> with the populated page-find context.
														|
														<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L110">o_btree_modify_internal</a> -- operates on the already-found, write-locked leaf page. Reads the existing tuple via <code>BTREE_PAGE_READ_LEAF_ITEM</code>, runs the modify-callback (<code>o_update_callback</code>) for visibility / row-lock / self-modification decisions, and dispatches by action: <code>o_btree_modify_delete</code> / <code>o_btree_modify_lock</code> for those, or <code>o_btree_modify_insert_update</code> for the insert/update branch.
															|
															<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L370">o_btree_modify_insert_update</a> -- the insert/update sub-routine. Reserves an undo entry, then drives the page mutation.
																|
																<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L701">o_btree_modify_add_undo_record</a> -- pushes a <code>ModifyUndoItemType</code> undo entry capturing the pre-image and stores its location into <code>leafTuphdr-&gt;undoLocation</code>. Page is still write-locked but no mutation yet.
																|
																<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L725">o_btree_insert_tuple_to_leaf</a> -- enters the insert machinery in btree/insert.c (function definition at <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1350">insert.c#L1350</a>).
																	|
																	<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1374">o_btree_insert_item</a> -- dispatcher; contains **no** critical section. Walks the insert stack, handles split fix-ups, then dispatches to one of two leaf-mutation helpers depending on whether other backends are waiting to insert at the same leaf.
																		|
																		<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1337">o_btree_insert_item_no_waiters</a> -- typical UPDATE path (uncontended). The <code>BTreeItemPageFitAsIs</code> branch handles in-place tuple replacement. (The contended sibling <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1332">o_btree_insert_item_with_waiters</a> has the same crit-section structure at <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1002">insert.c#L1002-L1012</a>, same lock-release-before-return property.)
																			|
																			<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1064">START_CRIT_SECTION</a> -- critical section begins. **NOT suitable for injection** -- ereport(ERROR) inside a critical section escalates to PANIC.
																			|
																			page mutation: <code>page_block_reads</code> + <code>memcpy</code> of new tuple header + body + <code>MARK_DIRTY</code>.
																			|
																			<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1142">unlock_page(blkno)</a> -- called **inside** the crit-section, one statement before <code>END_CRIT_SECTION</code>. **The leaf write-lock is dropped here**, before control returns up the callback chain.
																			|
																			<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/insert.c#L1144">END_CRIT_SECTION</a> -- critical section ends.
															|
															control returns up: <code>o_btree_modify_insert_update</code> -&gt; <code>o_btree_modify_internal</code>, which then calls <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L371">unlock_release(&context, false)</a>. Note the <code>false</code> -- on this path <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L376">unlock_release</a> does **not** call <code>unlock_page</code> (the leaf was already unlocked by the insert machinery above); it only releases the reserved undo size and the page-pool reservation. The early-return / pre-mutation branches of <code>o_btree_modify_internal</code> (<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L242">L242</a>, <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L317">L317</a>, <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L355">L355</a>) call <code>unlock_release(..., true)</code> instead -- those paths bail before reaching the insert machinery and so still hold the leaf lock at exit.
											|
											**good point for injection (<code>orioledb-pk-mutated-pre-wal</code>)** -- at this instant the PK leaf has been mutated in shared buffers and *already write-unlocked* (the unlock happened deep inside the insert machinery, see above), the <code>ModifyUndoItemType</code> undo entry is pushed and <code>leafTuphdr-&gt;undoLocation</code> is wired up, but **no <code>WAL_REC_UPDATE</code> has been packed into the local WAL buffer yet** -- that is what the <code>o_wal_update</code> call below does. Concurrent readers can already hit this leaf (page is unlocked) and rely entirely on the lock-free undo chain + INPROGRESS CSN to resolve to the pre-image. Two interesting failure modes here: (a) <code>kill -9</code> of the postmaster -- shared mem holds the new tuple, undo holds the pre-image, but Postgres XLog has no record of the update for this oxid; recovery just sees this oxid never committed and there is nothing to replay or roll back from WAL. Exercises the "page-mutated-but-no-WAL" recovery case. (b) <code>ereport(ERROR)</code> -- drives <code>apply_undo_stack</code> to revert the leaf via <code>modify_undo_callback</code>, then <code>wal_rollback</code> emits <code>WAL_REC_ROLLBACK</code> with no preceding modify-record for this row in the local buffer -- "rollback of changes that never made it to WAL", which the recovery side must handle gracefully.
												|
												<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1294">o_wal_update</a> -- emits <code>WAL_REC_UPDATE</code> into the per-backend local WAL buffer.
												|
												<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/recovery/wal.c#L834">add_modify_wal_record</a> -- per-row WAL helper. Just packs the record into the local buffer; no flush yet. The "page-mutated-but-no-WAL" race is captured by the <code>orioledb-pk-mutated-pre-wal</code> injection one frame up (right before <code>o_wal_update</code>); a separate injection at the function level here would also fire on system-tree updates and is therefore not used.
							|
							control returns to ExecUpdate. **good point for injection (<code>orioledb-update-pk-done-pre-sk</code>)** -- PK leaf is mutated and unlocked in shared buffers, the <code>ModifyUndoItemType</code> undo is pushed, and <code>WAL_REC_UPDATE</code> is packed in the per-backend *local* WAL buffer (not yet flushed -- that happens at COMMIT via <code>flush_local_wal</code>). Secondary indexes still hold the old token entry. A reader scanning by <code>token</code> at this instant would see the row at the *old* token key. This is precisely the inconsistency window the bank-test's <code>reader_sk</code> cross-check probes for.
							|
							**Phase 2 -- SK update via IndexAM** (driven by PG executor for each affected secondary index)
							|
							<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/nodeModifyTable.c#L2053">ExecUpdateIndexTuples</a> -- post-table-update entry; loops over the relation's indexes. For an unchanged index it skips, for a changed one it issues an update. The bank-test's UPDATE always changes <code>token</code>, so the unique SK is processed every time.
								|
								<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/executor/execIndexing.c#L707">index_update</a> call site -- per-index loop body inside the <code>ExecUpdateIndexTuples</code>.
									|
									<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/index/indexam.c#L266">index_update</a> -- IndexAM dispatch via <code>indexRelation-&gt;rd_indam-&gt;amupdate</code>. Falls into Oriole's per-index hook.
										|
										<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/indexam/handler.c#L666">orioledb_amupdate</a> -- IndexAM <code>amupdate</code> hook. Called once per affected secondary index. For non-orioledb indexes (e.g. via index_bridging) it forwards to standard <code>index_insert</code>; for native orioledb indexes it goes the path below.
											|
											key bound construction for old + new SK keys.
											|
											<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/indexam/handler.c#L759-L763">o_update_secondary_index</a> -- the actual SK B-tree work. Two B-tree modifies in sequence: delete-by-old-key, then insert-by-new-key.
												|
												old/new key equality short-circuit -- if SK key values didn't change, return immediately. The bank workload always changes <code>token</code>, so this branch is never taken.
												|
												<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1513-L1517">o_btree_modify(BTreeOperationDelete)</a> -- delete the old SK entry.
													|
													page lock + undo push (<code>ModifyUndoItemType</code> for the SK B-tree) + <code>START_CRIT_SECTION</code> + page mutation + <code>END_CRIT_SECTION</code>.
													|
													**No WAL emitted for the SK delete.** The regular <code>o_btree_modify</code> path used for user-table SK updates does *not* call <code>add_modify_wal_record</code> -- the <code>o_wal_insert/delete</code> calls at <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L1526">modify.c:1526</a> / <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/btree/modify.c#L1577">modify.c:1577</a> live inside <code>o_btree_autonomous_insert/delete</code>, which assert <code>IS_SYS_TREE_OIDS(...)</code> and only fire for system-catalog trees.
												|
												**good point for injection (<code>orioledb-sk-mid-update</code>)** between SK-delete and SK-insert -- table state has the new PK row, the SK is *missing* the entry entirely (neither old key nor new key resolves to this row). A reader doing <code>SELECT ... ORDER BY token</code> would skip the row at this instant. The bank-test's <code>reader_sk</code> invariant catches this directly: SK-scan total != PK-scan total or <code>len(tokens) != n_accounts</code>.
												|
												<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1532-L1536">o_btree_modify(BTreeOperationInsert)</a> for non-unique SK / <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/tableam/operations.c#L1538-L1542">o_btree_insert_unique</a> for unique SK -- insert the new SK entry. The bank workload's token-uniqueness index uses the unique variant, which performs the deferred-uniqueness check via PG's constraint trigger queue rather than inline.
													|
													page lock + undo push + <code>START_CRIT_SECTION</code> + page mutation + <code>END_CRIT_SECTION</code>.
													|
													**No WAL emitted for the SK insert either** (same reason as the SK delete above). The SK page mutation and undo entry are sufficient for the live transaction; recovery / replication will re-derive the SK changes from the PK's <code>WAL_REC_UPDATE</code> record by replaying the new+old PK rows through <code>o_tbl_indices_overwrite</code> on the redo side.
							|
							ExecUpdateIndexTuples loop continues for any remaining secondary index, then returns. ExecUpdate returns to ExecModifyTable.
						|
						ExecModifyTable iterates the inner subplan for the next row (none for our PK-equality <code>WHERE id = X</code>); on NULL slot it ends.
				|
				ExecutorRun / ProcessQuery return to PortalRunMulti, which loops to the next PlannedStmt or completes.
		|
		<code>PortalRun</code> finishes; portal teardown drops snapshots and releases per-statement memory.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1303">finish_xact_command</a> -- inside an open transaction block this just toggles state back to <code>IN_PROGRESS</code> (no commit happens here -- the writer's <code>con.commit()</code> later issues a separate <code>COMMIT</code> statement that drives the COMMIT TX FLOW).
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L1354">finish_xact_command</a> -- second call at the end of <code>exec_simple_query</code> is a no-op.
</pre>
Per writer tx the bank workload pushes ~6 `ModifyUndoItemType` items (PK update + SK delete + SK insert per UPDATE, ×2 because the tx has two UPDATEs) but emits only ~2 `WAL_REC_UPDATE` records (one per UPDATE, both from `o_wal_update` on the primary descriptor). SK index changes are *not* WAL-logged separately -- only the PK's update is, and recovery re-derives the SK page mutations by replaying the PK update through `o_tbl_indices_overwrite`. Everything is flushed at COMMIT.


## ABORT TX FLOW

Triggered by any `ereport(ERROR)` raised during the tx -- native serialization conflict, deferred-uniqueness violation at COMMIT, `wal_chaos` injection at flush boundary, `pg_terminate_backend`, statement timeout, etc. The ereport longjmps out of whatever call stack raised it; control lands in `PostgresMain`'s top-level error handler, which drives the abort path.

<pre>
<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/utils/error/elog.c#L346">ereport(ERROR, ...)</a> -- error raised somewhere in Oriole or PG. <code>errstart</code> allocates an error data slot.
	|
	<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/utils/error/elog.c#L477">errfinish</a> -- finalizes the error record, then <code>siglongjmp</code>s out of the current call stack.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4426">PostgresMain sigsetjmp catch</a> -- the top-level catch in the backend's main loop receives the longjmp.
	|
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4465">EmitErrorReport</a> -- ships the error message to the client / log.
	|
	<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4482">AbortCurrentTransaction</a> -- the wrapper that drives the abort regardless of which TBLOCK state we were in.
		|
		<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3430">AbortCurrentTransactionInternal</a> -- state-machine dispatch; for the in-block-tx case (<code>TBLOCK_INPROGRESS</code>) unconditionally calls <code>AbortTransaction</code>.
			|
			<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3464">AbortTransaction</a> -- the actual rollback machinery. Fires xact-event callbacks, releases resources, transitions oxid state.
				|
				(For an explicit <code>ROLLBACK</code> SQL statement the entry path is different: <code>exec_simple_query</code> → <code>PortalRun</code> → <code>PortalRunMulti</code> → <code>PortalRunUtility</code> → <code>standard_ProcessUtility</code> → <code>TRANS_STMT_ROLLBACK</code> → <code>UserAbortTransactionBlock</code>, which only flips state to <code>TBLOCK_ABORT_PENDING</code>. The real abort runs at the next <code>finish_xact_command</code> via <code>CommitTransactionCommand</code> → <code>AbortTransaction</code>. Rarely hit in the bank-test because writers abort via <code>ereport(ERROR)</code>, not explicit ROLLBACK -- psycopg2's <code>con.rollback()</code> only fires after the server-side abort already happened.)
				|
				<a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L2393-L2394">CallXactCallbacks</a> with event = XACT_EVENT_ABORT -- triggers OrioleDB's registered handler (same registration as commit, see <a href="https://github.com/orioledb/orioledb/blob/bbd7c1254e4cbd23bc4cafda02289c91609111e6/src/orioledb.c#L1210">RegisterXactCallback</a>).
					|
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2110">undo_xact_callback</a> with event = XACT_EVENT_ABORT -- the function's body runs the steps below in order. The XACT_EVENT_ABORT switch case at <a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2327">undo.c:2327</a> is reached only after the pre-switch setup completes.
						|
						**Pre-switch setup (common to COMMIT and ABORT)**
						|
						<code>oxid = get_current_oxid_if_any()</code> and <code>isParallelWorker = ...</code> are evaluated at the top -- determines whether this tx ever allocated an oxid and whether we're running in a parallel worker (parallel workers take a fast path).
						|
						<code>ea_counters = NULL</code> -- clears EXPLAIN ANALYZE counters that the executor may have left dangling if the abort happened mid-node.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2136">seq_scans_cleanup</a> -- releases per-backend sequential-scan state (read locks on internal pages, scan iterators). Common to COMMIT and ABORT; runs unconditionally here, before any undo / WAL work.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2144-L2154">no-oxid / parallel-worker fast path</a> -- if <code>!OXidIsValid(oxid) || isParallelWorker</code> (read-only tx that never allocated an oxid, or a parallel worker leader), only run trivial state resets (<code>reset_cur_undo_locations</code>, <code>orioledb_reset_xmin_hook</code>, <code>reset_command_undo_locations</code>, clear <code>oxid_needs_wal_flush</code> / <code>xidless_commit_lsn</code> / <code>minParentSubId</code>) and exit. **No undo walk, no <code>wal_rollback</code>, no CSN flip** -- there's nothing to roll back. This is the path the bank-test's reader threads take on abort.
						|
						**Else branch -- a real oxid abort:**
						|
						<code>heapXid = GetTopTransactionIdIfAny()</code> and <code>get_current_logical_xid_ctx(&amp;logicalXidContext)</code> -- captures the heap xid (if any) and logical-xid bookkeeping. Used to decide whether this is a pure-Oriole tx, a heap-only tx, or a <code>SWITCH_LOGICAL_XID</code> cross-engine tx. For the bank-test writer (Oriole-only) <code>heapXid == InvalidTransactionId</code>.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2215">Assert(!RecoveryInProgress())</a> -- abort callbacks must not fire on the recovery side; recovery has its own abort handling via <code>recovery_finish_current_oxid(COMMITSEQNO_ABORTED, ...)</code> driven by <code>WAL_REC_ROLLBACK</code> records.
						|
						**XACT_EVENT_ABORT switch case** -- the actual abort work begins below.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2334">wal_rollback</a> -- emits <code>WAL_REC_ROLLBACK</code> if the tx had material changes. **No-op for read-only or no-change txs.** The early bail at <code>local_wal_has_material_changes == false</code> skips WAL emission entirely.
							|
							<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L351">flush_local_wal_if_needed</a> -- ensure room for the rollback record.
							|
							<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L357-L358">add_finish_wal_record(WAL_REC_ROLLBACK)</a> -- pushes the rollback marker.
							|
							<a href="https://github.com/orioledb/orioledb/blob/6f900ca6cf4b8eee3eec0634254ec21ee8c8918d/src/recovery/wal.c#L359">flush_local_wal</a> -- flushes the local buffer to global XLog. **NOT suitable for re-injection here** -- we are already in the abort path; raising again would re-enter abort and PANIC. The <code>wal_chaos</code> injection point is gated by <code>isCommit</code> to skip exactly this call.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2336-L2337">apply_undo_stack(undoType)</a> per UndoLogType -- walks the per-backend undo chain backward from <code>sharedLocations-&gt;location</code> to InvalidUndoLocation, calling each item's abort callback.
							|
							<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1416">walk_undo_stack(abortTrx=true)</a> -- the iteration framework. Sets up the buffer for reading undo items.
								|
								<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1375-L1378">walk_undo_range_with_buf</a> -- read a contiguous range of undo items from the per-undo-type log.
									|
									<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L1241">walk_undo_range loop</a> -- for-each-undo-item loop. Per item: read the undo record, dispatch to the type-specific callback (<code>modify_undo_callback</code> for <code>ModifyUndoItemType</code>), advance to <code>item-&gt;prev</code>. **good instrumentation point between iterations** -- partial undo applied, leaf pages reverted for the items processed so far but not the rest. Exactly the "torn rollback" race that the bank invariant catches if state is observably inconsistent at this moment. Two complementary modes here: (a) **ereport-style injection** -- raises mid-abort and re-enters the abort handler, which Postgres escalates to **PANIC**. Useful to test that recovery (and the postmaster's restart-after-crash) correctly finishes the partial rollback by replaying <code>WAL_REC_ROLLBACK</code> and re-applying undo. (b) **stopevent / <code>injection_points_attach('...', 'wait')</code> -style freeze** -- pauses the aborting backend on a condvar without raising; concurrent readers race against the partial state, then the test releases. Use this mode to expose visibility races without crashing the cluster.
									|
									per-item callback runs INSIDE the loop body -- for <code>ModifyUndoItemType</code> this restores the pre-image on the SK/PK leaf page (in-place, in shared buffers, under page lock). Each callback that mutates a page enters its own <code>START_CRIT_SECTION</code>/<code>END_CRIT_SECTION</code>, so injection MUST be *between* callbacks, never inside.
							|
							<code>branchLocation</code> / <code>onCommitLocation</code> cleanup -- after the iteration the function clears or repositions the undo head pointers under <code>undoStackLocationsFlushLock</code>.
						|
						**good instrumentation point between <code>apply_undo_stack</code> and <code>current_oxid_abort</code>** -- in-memory pages are reverted but the oxid's CSN is still INPROGRESS in <code>xidBuffer</code>. Concurrent readers consulting the CSN see "tx is still running" while the underlying data has already disappeared. Same two modes apply as inside the loop: (a) **ereport-style injection here triggers PANIC** because we are still in the abort handler (re-entrant ereport during XACT_EVENT_ABORT). Useful for stressing the crash-restart path -- recovery sees <code>WAL_REC_ROLLBACK</code>, replays it, re-flips CSN to ABORTED. (b) **stopevent / <code>injection_wait</code>-style freeze** is the safe option for live cluster races -- pause here, let other backends consult the inconsistent CSN+page state, then release. The bank-test's invariants catch any observable anomaly.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2343">wal_after_commit</a> -- clears <code>commitInProgressXlogLocation</code>. Same routine used by the commit path. NOT a write to WAL.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2345-L2346">reset_cur_undo_locations + reset_command_undo_locations</a> -- forget local pointers into the undo log.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2347">current_oxid_abort</a> -- the visibility flip.
							|
							<a href="https://github.com/orioledb/orioledb/blob/4ca6f69408f76c58bfc37ed601000aae247538d8/src/transam/oxid.c#L1493">set_oxid_csn(curOxid, COMMITSEQNO_ABORTED)</a> -- single atomic write that makes the tx invisible to all snapshots. Same lock-free CAS shape as the commit-side <code>set_oxid_csn</code>, with the same fast-path / on-disk-fallback split.
						|
						<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2348">set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr)</a> -- clears the xlog-ptr buffer slot. Pairs with the CSN write so the oxid's two-tuple state is <code>{ABORTED, InvalidXLogRecPtr}</code>.
						|
						registered-snapshot teardown loop -- frees <code>retainUndoLocHeaps</code> so subsequent <code>runXmin</code> advances aren't blocked by this tx's snapshot.
						|
						<code>xidless_commit_lsn</code> / <code>oxid_needs_wal_flush</code> / <code>in_nontransactional_truncate</code> resets.
					|
					**End of XACT_EVENT_ABORT switch case; common COMMIT/ABORT post-switch cleanup follows.**
					|
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2380-L2381">release_undo_size per UndoLogType</a> -- returns this backend's reserved undo-location quota to the global pool so other txs can use it.
					|
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2383-L2387">ppool_release_reserved per OPagePoolType</a> -- releases reserved page-pool slots that this tx had set aside for its modifications.
					|
					<a href="https://github.com/orioledb/orioledb/blob/187f850a6ec182ce80f31c9c1594ac0dc26fe0b8/src/transam/undo.c#L2390-L2391">free_retained_undo_location per UndoLogType</a> -- final teardown of any per-tx retained undo locations not yet released.
				|
				<code>undo_xact_callback</code> returns to PG's <code>CallXactCallbacks</code> loop, which then proceeds to subsequent registered xact callbacks (if any).
			|
			<code>AbortTransaction</code> returns; transaction state is now <code>TBLOCK_ABORT</code> (or <code>TBLOCK_DEFAULT</code> for non-block aborts). <a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3465">CleanupTransaction</a> is called next to free the TransactionState memory.
		|
		<code>AbortCurrentTransactionInternal</code> returns true (state machine is done); the <a href="https://github.com/orioledb/postgres/blob/ac88d9a17c6eadfca2e55fc2f18b915283587271/src/backend/access/transam/xact.c#L3430">while loop in AbortCurrentTransaction</a> terminates.
	|
	control returns from <code>AbortCurrentTransaction</code> back into the <code>PostgresMain</code> <code>sigsetjmp</code> block.
|
<a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4486">PostgresMain main loop</a> resumes -- emits <code>ReadyForQuery</code> to the client (with status <code>'E'</code> for "in failed transaction"), then waits in <a href="https://github.com/orioledb/postgres/blob/e43537cdc36146f1becc0084b1acc24a46074ae6/src/backend/tcop/postgres.c#L4500">ReadCommand</a> for the next client message. The client is expected to issue <code>ROLLBACK</code> (or <code>COMMIT</code>, which is also treated as ROLLBACK in TBLOCK_ABORT) to exit the failed-transaction state. psycopg2 issues <code>ROLLBACK</code> automatically on the next <code>con.rollback()</code> call from the bank-test's <code>except Exception:</code> branch.
</pre>
Two-mode reminder for the whole ABORT body:

* **ereport-style injection inside the abort flow always escalates to PANIC** (because we are already inside the XACT_EVENT_ABORT handler). That is *not* useless -- it deliberately drives the postmaster's "crash of another server process" path, forcing crash recovery to replay `WAL_REC_ROLLBACK` and finish the partial undo on next startup. Use this mode when the goal is to stress crash-recovery correctness or the postmaster's restart loop.
* **stopevent / `injection_points_attach('...', 'wait')`-style freezes are the live-cluster-safe alternative.** They pause the aborting backend on a condvar without raising, so concurrent readers/writers race against the partial state while the backend stays alive. Use this mode when the goal is to expose visibility / consistency races without taking the cluster down. The bank-test's existing `stopevent_chaos` worker is the right harness for this.

Notes on places that are *unsuitable for either mode*:

* Inside `walk_undo_range_with_buf`'s per-item callback when the callback is mid-`START_CRIT_SECTION` -- both modes break here. `ereport(ERROR)` inside a critical section escalates to PANIC even outside of abort context. Stopevents pause execution, but pausing while holding LWLocks / page-pin reservations inside a critical section starves other backends; the cluster won't deadlock outright but will hang until the freeze is released.
* After `current_oxid_abort` -- Postgres comment in `CommitTransaction` (which mirrors the abort cleanup style) explicitly says "if an error is raised here, it's too late to abort the transaction. This should be just noncritical resource releasing." For the abort tail the same caveat applies: ereport here turns into PANIC, AND there is nothing left to observe (the CSN flip is done), so freeze-style injection only delays cleanup without exposing anything new. Skip this region.

