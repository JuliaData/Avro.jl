Round 22 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 21:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v22"). Section 14 contains the review log and the decision list (now 58 decisions).
- Every round-21 item (3 majors, 17 minors, 2 nits) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-21.md. All were adopted. Changes: the instants= option is removed entirely (DateTime via explicit conversion or a typed T, which is always authoritative); the failure-work bound is inflight for head failures and inflight-1 for higher-block failures with both schedules forced; and all minors and nits (guard wording and the pending-reservation counter as a third global-state exception with finally restoration and no double counting; Rows synopsis by mode; direct-head role and max_inflight_blocks semantics; committed_payload/committed_bytes; one max_block_output_bytes formula; Row oracle exclusions; Phase 4a W measurement; comparison multiplier; security wording; public surface for Limits, SchemaCache, errors and schema constructors; structural props collisions; non-copying UnionValue; ZonedDateTime schema and float overflow rules; avro.codec UTF-8 and compressed-size checks; copied metadata values; dense-ID wording; recursive-root projection gates; PR-ready pins; ntasks=8 wording; Appendix B fixes).

Your task this round:
1. For each round-21 item (majors, minors and nits), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v22 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Convergence request: twenty-one rounds have each surfaced a further layer of the resource model. This round, please be exhaustive in a single pass — enumerate every remaining objection you can identify anywhere in the plan (resource model, parser, codecs, API, gates), so that the next revision can close the loop rather than discover one more layer. Apply the agreed severity bar strictly: a detail an implementer can settle within the recorded contracts (formula constants, scratch sizes, exact wording) is [minor]/[nit], not [major].
4. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 22
## Disposition check (round-21 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
