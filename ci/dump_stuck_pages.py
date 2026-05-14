"""
gdb helper: when a backtrace contains one of the orioledb page-locking
functions, dump the BTreePageHeader of the page being waited on plus
the chain of waiters queued on it.  Helps diagnose recovery-time
hangs where pages stay in locked / read-disabled state with no
backend recording the lock locally.

Loaded by ci/list_stuck.sh via `gdb -ex "source dump_stuck_pages.py"`.
"""

import gdb

LOCK_FUNCS = {
    "lock_page": "blkno",
    "lock_page_with_tuple": "*blkno",
    "relock_page": "blkno",
    "page_wait_for_read_enable": "blkno",
}

# Mirror of include/btree/page_state.h.
PAGE_STATE_LIST_TAIL_MASK = 0x3FFFF
PAGE_STATE_INVALID_PROCNO = PAGE_STATE_LIST_TAIL_MASK
ORIOLEDB_BLCKSZ = 8192
O_BLKNO_MASK = 0x7FFFFFFF


def safe_eval(expr):
	try:
		return gdb.parse_and_eval(expr)
	except gdb.error as e:
		return f"<eval error: {e}>"


def page_address(blkno):
	"""Return shared-buffers address for blkno, or None for local pages."""
	if (blkno >> 31) & 1:
		# Local page — its content is in this process's local pool only.
		return None
	sb = safe_eval("o_shared_buffers")
	if isinstance(sb, str):
		return None
	return int(sb) + (blkno & O_BLKNO_MASK) * ORIOLEDB_BLCKSZ


def get_blkno_from_frame(frame, arg_expr):
	"""Read the blkno argument from a frame.  arg_expr is "blkno" for
	a direct OInMemoryBlkno argument or "*blkno" when the arg is a
	pointer (lock_page_with_tuple).  Falls back to None when the
	argument was optimised out."""
	try:
		block = frame.block()
	except RuntimeError:
		return None
	want = arg_expr.lstrip("*")
	for sym in block:
		if not sym.is_argument or sym.name != want:
			continue
		try:
			val = sym.value(frame)
			if arg_expr.startswith("*"):
				val = val.dereference()
			return int(val)
		except (gdb.error, gdb.MemoryError):
			return None
	return None


def dump_page(blkno, seen):
	"""Print BTreePageHeader and waiter chain for blkno (once)."""
	if blkno in seen:
		return
	seen.add(blkno)

	if not (0 <= blkno <= 0xFFFFFFFE):
		print(f"    blkno={blkno!r}: invalid")
		return

	addr = page_address(blkno)
	if addr is None:
		print(f"    blkno={blkno}: local page — no shared dump")
		return

	print(f"    blkno={blkno} @ 0x{addr:x}")
	hdr = safe_eval(f"*(BTreePageHeader *) {addr}")
	print(f"    BTreePageHeader = {hdr}")

	state_expr = (
	    f"((BTreePageHeader *) {addr})->o_header.state.value")
	state = safe_eval(state_expr)
	if isinstance(state, str):
		print(f"    state read failed: {state}")
		return
	state_val = int(state)
	print(f"    state = 0x{state_val:016x}")

	proc = state_val & PAGE_STATE_LIST_TAIL_MASK
	if proc == PAGE_STATE_INVALID_PROCNO:
		print("    waiter chain: <empty>")
		return

	max_procs_val = safe_eval("max_procs")
	try:
		max_procs_int = int(max_procs_val)
	except (TypeError, gdb.error):
		max_procs_int = None
	print(f"    waiter chain (head → tail), max_procs={max_procs_int}:")
	steps = 0
	while proc != PAGE_STATE_INVALID_PROCNO:
		if steps > 1024:
			print("      … chain too long, aborting")
			break
		if max_procs_int is not None and proc >= max_procs_int:
			print(f"      [proc {proc}] out of range "
			      f"(>= max_procs {max_procs_int}); chain "
			      f"likely corrupt, aborting")
			break
		ws = safe_eval(f"lockerStates[{proc}]")
		print(f"      [proc {proc}] = {ws}")
		nxt = safe_eval(f"lockerStates[{proc}].next")
		try:
			proc = int(nxt) & PAGE_STATE_LIST_TAIL_MASK
		except (TypeError, gdb.error):
			print(f"      next read failed: {nxt}")
			break
		steps += 1


def main():
	seen = set()
	inferior = gdb.selected_inferior()
	threads = inferior.threads()
	if not threads:
		return
	saved = gdb.selected_thread()
	for thread in threads:
		try:
			thread.switch()
		except gdb.error:
			continue
		frame = gdb.newest_frame()
		while frame is not None:
			try:
				fname = frame.name() or ""
			except gdb.error:
				fname = ""
			if fname in LOCK_FUNCS:
				blkno = get_blkno_from_frame(frame, LOCK_FUNCS[fname])
				print(f"--- {fname}(blkno={blkno}) in thread "
				      f"{thread.num} ---")
				if blkno is not None:
					dump_page(blkno, seen)
				else:
					print("    (blkno optimised out)")
			try:
				frame = frame.older()
			except gdb.error:
				break
	if saved is not None:
		try:
			saved.switch()
		except gdb.error:
			pass


main()
