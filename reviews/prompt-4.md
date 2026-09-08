Round 4 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 3:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v4"). Section 14 contains the review log.
- Every round-3 item (the 7 partially resolved round-1 items including the two flagged blocker-level, and all 19 new findings) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-3.md. All were adopted.
- New evidence gathered since round 3 in the scratchpad (/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad): fixtures/roots (9 non-record root schemas x all codecs), fastavro-produced files for all six codecs (fixtures/data, fixtures/roots), real Avro.jl 1.1.2 files (fixtures/legacy1x), Java TimeConversions vectors (fixtures/timevectors.tsv, 49 lines incl. the Helsinki example and the out-of-range nanos error), sort-order verdicts from both Java comparators (fixtures/sortorder/verdicts.tsv: case17 shows GenericData.compare ordering bytes signed while BinaryData.compare is unsigned), Java decimal edge behaviour (javah/DecEdge.java: empty payload -> NumberFormatException, absent scale -> 0, no decode-side precision check, encode overflow rejected).

Your task this round:
1. For each round-3 item (the 7 partially resolved round-1 items and new findings 1-19), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v4 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 4
## Disposition check (round-3 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
