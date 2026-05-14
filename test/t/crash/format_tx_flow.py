#!/usr/bin/env python3
# Convert tx_flow.md (markdown source) to tx_flow_formatted.md (HTML-wrapped
# version that renders the indented tree diagrams inside <pre> blocks while
# keeping prose paragraphs and markdown tables as plain markdown).
#
# Conventions in the source we rely on:
#   * Sections are delimited by `## HEADER` lines.
#   * "Tree-like" content uses tabs/indentation, bare `|` connectors,
#     `[text](url)` markdown links and ASCII box-drawing.  Such content is
#     wrapped in <pre>.  Inside a <pre>, markdown links become <a href=...>
#     anchors and bare `&` become `&amp;`.
#   * Prose paragraphs (no leading whitespace, no tree markers) and
#     markdown tables stay as plain markdown.
#
# Usage:
#   python3 format_tx_flow.py [--check]
#       (no args)  rewrite tx_flow_formatted.md from tx_flow.md
#       --check    diff against the existing formatted file, exit non-zero
#                  if they differ
#
# The script lives next to the .md files and uses paths relative to itself.

from __future__ import annotations

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "tx_flow.md")
DST = os.path.join(HERE, "tx_flow_formatted.md")

LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
AMP_RE = re.compile(r"&(?![a-zA-Z]+;|amp;|#\d+;)")
BACKTICK_RE = re.compile(r"`([^`]+)`")
BOX_CHARS = set("▒░█→│└┘↑─├┤┬┴┼━╡╞┐┌▶◀")


def is_section_header(line: str) -> bool:
	return line.startswith("## ")


def is_bullet(line: str) -> bool:
	# Markdown bullet at column 0 (no leading whitespace).  Indented bullets
	# inside a tree are NOT counted -- they stay inside <pre>.
	return line[:2] in ("* ", "- ", "+ ")


def is_horizontal_rule(line: str) -> bool:
	return line.strip() in ("---", "***", "___")


def is_prose_paragraph_start(lines: list[str], i: int) -> bool:
	"""Heuristic: line i looks like the start of a standalone prose
	paragraph that should end the surrounding <pre> block.  We say so when
	a non-tree, non-indented, non-blank line is followed (within ~3
	non-blank lines) by a structural prose marker (bullet, HR, header,
	table).  This catches transitions like:
	    [last tree line]

	    Two-mode reminder for the whole ABORT body:

	    * bullet 1
	    * bullet 2
	"""
	if i >= len(lines):
		return False
	cur = lines[i]
	if not cur.strip():
		return False
	# Anything indented or already tree-like stays inside <pre>.
	if cur.startswith("\t") or cur.startswith("    "):
		return False
	if is_tree_like(cur):
		return False
	# Look ahead for a structural marker.
	probed = 0
	for j in range(i + 1, min(len(lines), i + 12)):
		ln = lines[j]
		if not ln.strip():
			continue
		probed += 1
		if (is_bullet(ln) or is_horizontal_rule(ln)
		    or is_section_header(ln) or is_table_row(ln)):
			return True
		# Hit another non-blank, non-marker line within probe window?
		# Could still be more prose -- continue probing.
		if probed >= 4:
			break
	return False


def is_edge_label_connector(line: str) -> bool:
	"""`|` followed by whitespace and `-- text` is a labeled tree edge,
	not a markdown table row.  Distinguish from a separator like `| ---`
	by requiring exactly two dashes (`--`) followed by a space."""
	stripped = line.lstrip()
	if not stripped.startswith("|"):
		return False
	rest = stripped[1:].lstrip()
	if not rest.startswith("-- "):
		return False
	return True


def is_table_row(line: str) -> bool:
	stripped = line.lstrip()
	if not stripped.startswith("|"):
		return False
	if stripped.rstrip() == "|":
		return False  # bare connector
	if is_edge_label_connector(line):
		return False
	if stripped.count("|") < 3:
		return False
	return True


def is_tree_like(line: str) -> bool:
	"""Return True if the line belongs inside a <pre> tree block."""
	if not line.strip():
		return False  # empty line decided by surrounding context
	if is_section_header(line):
		return False
	if is_table_row(line):
		return False
	# Indented (tab-prefixed) -- tree continuation.
	if line.startswith("\t") or line.startswith("    "):
		return True
	stripped = line.lstrip()
	# Bare connector or labeled-edge connector.
	if stripped == "|" or stripped.startswith("|  ") or stripped.startswith("| "):
		return True
	# Markdown link as the first token (top-level tree node).
	if stripped.startswith("["):
		return True
	# ASCII box-drawing diagram.
	if any(c in stripped for c in BOX_CHARS):
		return True
	# Top-level tree node with no link -- short identifier followed by " -- ".
	# Conservative: only count if the line is short or contains "--" early.
	if " -- " in stripped[:60]:
		return True
	# A "prose-ish" tree continuation line such as "control returns to ..."
	# after a tree -- handled by the state machine, not here.
	return False


def convert_inline(text: str) -> str:
	"""Convert markdown links and escape &; runs ONLY inside a <pre>."""
	# Escape ampersands first so we don't double-escape ones we introduce.
	text = AMP_RE.sub("&amp;", text)
	# Markdown links -> HTML anchors.
	text = LINK_RE.sub(r'<a href="\2">\1</a>', text)
	# Backtick code spans -> <code>...</code> (matches existing formatted).
	text = BACKTICK_RE.sub(r"<code>\1</code>", text)
	return text


def format_md(src_text: str) -> str:
	"""Run the state machine line-by-line, producing the formatted text."""
	lines = src_text.splitlines(keepends=False)
	out: list[str] = []
	in_pre = False
	pending_blanks: list[str] = []

	def flush_blanks():
		out.extend(pending_blanks)
		pending_blanks.clear()

	def open_pre():
		nonlocal in_pre
		assert not in_pre
		out.append("<pre>")
		in_pre = True

	def close_pre():
		nonlocal in_pre
		assert in_pre
		out.append("</pre>")
		in_pre = False

	for i, line in enumerate(lines):
		if line.strip() == "":
			pending_blanks.append(line)
			continue

		# A line is "outside-pre" when it is a section header, a markdown
		# table row, a column-0 bullet, a horizontal rule, or a line that
		# starts a standalone prose paragraph (look-ahead based).
		outside = (
		    is_section_header(line)
		    or is_table_row(line)
		    or is_bullet(line)
		    or is_horizontal_rule(line)
		    or is_prose_paragraph_start(lines, i)
		)

		if outside:
			if in_pre:
				close_pre()
			flush_blanks()
			out.append(line)
			continue

		# Tree-like or ambiguous-but-staying-inside-pre.
		if not in_pre:
			# Only open <pre> for explicitly tree-like lines.  An ambiguous
			# line that comes BEFORE any tree opening (e.g. plain prose
			# right after a section header) stays outside.
			if is_tree_like(line):
				flush_blanks()
				open_pre()
				out.append(convert_inline(line))
			else:
				flush_blanks()
				out.append(line)
		else:
			flush_blanks()
			out.append(convert_inline(line))

	# End of file.
	if in_pre:
		close_pre()
	out.extend(pending_blanks)
	return "\n".join(out) + ("\n" if src_text.endswith("\n") else "")


def main(argv: list[str]) -> int:
	check = "--check" in argv
	with open(SRC, "r") as f:
		src = f.read()
	formatted = format_md(src)
	if check:
		try:
			with open(DST, "r") as f:
				existing = f.read()
		except FileNotFoundError:
			existing = ""
		if existing == formatted:
			print(f"OK: {DST} matches generator output")
			return 0
		print(f"DIFF: {DST} differs from generator output", file=sys.stderr)
		return 1
	with open(DST, "w") as f:
		f.write(formatted)
	print(f"wrote {DST} ({len(formatted)} bytes, {formatted.count(chr(10))+1} lines)")
	return 0


if __name__ == "__main__":
	sys.exit(main(sys.argv[1:]))
