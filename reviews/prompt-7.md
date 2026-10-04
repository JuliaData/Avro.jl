Round 7 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 6:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v7"). Section 14 contains the review log and the decision list (now 27 decisions).
- Every round-6 item (the carried rows and all 13 new findings, plus the non-blocking follow-ups) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-6.md. All were adopted. The resource model was simplified rather than extended: fixed portable defaults (1 GiB ceiling, no RAM probing); one per-operation ceiling shared by committed output and in-flight reservations, with coordinator-issued permits in block-index order, full worst-case reservation before decompression, streaming ordered assembly, and a written liveness argument; the writer enforces every cumulative reader limit (incl. metadata and schema limits) so the writer/reader invariant holds under identical limits; no compressed-size work pre-check; streamed mmap=false; self-alias idempotence; names= on fromjson; a complete Julia-derived name policy with avroname/avrosymbol hooks; liblzma/zstd thresholds; a latency gate that fixes the provisional work constants in Phase 2; a common max_json_depth.
- Scratchpad evidence since round 6: fixtures/sortorder now has 51 normalised cases incl. non-minimal decimal and mixed-case UUID.

Your task this round:
1. For each round-6 item (carried rows and new findings 1-13), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v7 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 7
## Disposition check (round-6 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
