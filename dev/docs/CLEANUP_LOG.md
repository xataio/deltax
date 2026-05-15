# Cleanup Log

Append a row per cleanup session. Newest first. See
[`CLEANUP_PLAN.md`](./CLEANUP_PLAN.md) for the methodology and the per-file
checklist.

## Format

For each session, add a section like:

```
### YYYY-MM-DD — `path/to/file.rs` — <commit-sha>

**Scope:** which checklist steps ran (simplify / unsafe / tests / verify).
**LOC:** <before> → <after>   **`unsafe`:** <before> → <after>   **Tests:** <before> → <after>

- One line per notable change.
- Note any deferred work explicitly: "deferred: unsafe audit, will revisit
  in a follow-up session."
- **Benchmarks** (required when scan/exec path was touched): "clickbench
  local: no regression vs main", "rtabench: Q17 -8%, Q23 +3% (within
  noise)", "jsonbench: not run, doesn't apply".
- **Correctness:** "ran existing harness, all pass" or "added case for X".
- **Perf opportunities surfaced** (if any): one line per item — what,
  where, expected gain, deferred or done inline.
```

Keep entries terse. The log is for orientation across sessions, not for
narration.

## Sessions

_(none yet — first session starts here)_
