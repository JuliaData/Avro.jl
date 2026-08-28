Round 3 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as rounds 1 and 2 (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 2:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v3"). Section 14 contains the review log.
- Every round-2 item (the 16 partially/not-resolved round-1 items, all 16 new findings, and the remaining amendment objections) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-2.md. All were adopted; the three contested amendments (findings 5, 6, 22 of round 1) and the fixed-value-identity amendment were withdrawn in your favour.
- The pinned Tables.Scan revision df4e68c15c874079521d9d4ce4be67dce4345a31 was cloned read-only to /private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/tables-scan (src/scan.jl) and section 6 was rewritten against it (Tables.resolve / BoundScan / All() identity / residual with retained overrides / Tables.scan conversion rules).
- Oracle capabilities were re-measured in the pinned venv (avro-py KNOWN_CODECS = null/deflate/bzip2; fastavro readers = all six + lz4; cramjam 2.11.0) and the Java in-JVM decode timing harness (TimeRead.java) was added to the scratchpad harness.

Your task this round:
1. For each round-2 item (the partial/not-resolved round-1 items and new findings 1-16), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v3 as a whole with the same mandate (spec coverage, audit accuracy, architecture/API, interop gates, tests/benchmarks, scope, milestones). Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly and list them as non-blocking follow-ups that can be handled during implementation, then AGREE.

Output format (markdown):
# Codex review round 3
## Disposition check (round-2 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
