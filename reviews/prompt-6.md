Round 6 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 5:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v6"). Section 14 contains the review log and the decision list (now 25 decisions).
- Every round-5 item (the carried rows and all 15 new findings) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-5.md. All were adopted. The central changes: one work rule enforced identically on encode and decode (writer block flushing is driven by it, so default writer output is default-readable by construction); a transient-memory semaphore with reserve-before-allocate and constructor-validated per-block worst case; codec memory defined as a window cap with the bzip2 floor and integer windowLogMax; contextual logical-attribute handling with only fixed.size as syntax; Int decimal scale; the canonical-encoding comparison contract; a resolution work budget; parameter-aware Julia type naming with collision errors; Symbol admission for typed values; filter-aware Scan offset/limit; the scoped oracle-readability promise.
- Scratchpad evidence since round 5: fixtures/roots now has all 13 root kinds; fixtures/sortorder/verdicts.tsv has 47 normalised cases incl. every logical type (case32: decimal ff vs 00 -> encoded comparator 1, object comparator -1); fixtures/blocking/crossform has positive/sized array pairs; fixtures/highwindow has the xz 1 GiB-dictionary and zstd window-log-30 OCF blocks (Python's zstd module rejects the latter: "Frame requires too much memory").

Your task this round:
1. For each round-5 item (carried rows and new findings 1-15), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v6 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 6
## Disposition check (round-5 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
