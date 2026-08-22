Round 18 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 17:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v18"). Section 14 contains the review log and the decision list (now 57 decisions).
- Every round-17 item (8 majors, 5 minors, 1 nit) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-17.md. All were adopted. Changes: contextual attribute grammar (metadata elsewhere, logical annotations never reject); correctly rounded linear-time Float32/Float64 parsers with no token cap and the Java-verified double-rounding vector; an Avro-owned Scan resolver under max_scan_nodes replacing the Tables.resolve exception; the invariant scoped to generic default consumption with conservative cross-version maxima and cross-read gates; sequential fallback whenever parallel-only memory blocks progress, assembly excluded from the 2x ratio with its own exact bound, one counter list everywhere; Phase 2/4a calibration split; public constructor limits=, DatumWriter precharge, single-object scope entries; npairs capacity and exact streamed peak everywhere; raw-number lexical equality; API/Scan synchronisation (write signature, rename validation, widening transformation); admission affordability deferred to the Phase 2 gate; stale wording and review-log order fixed.

Your task this round:
1. For each round-17 item (majors, minors and the nit), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v18 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Convergence request: seventeen rounds have each surfaced a further layer of the resource model. This round, please be exhaustive in a single pass — enumerate every remaining objection you can identify anywhere in the plan (resource model, parser, codecs, API, gates), so that the next revision can close the loop rather than discover one more layer. Apply the agreed severity bar strictly: a detail an implementer can settle within the recorded contracts (formula constants, scratch sizes, exact wording) is [minor]/[nit], not [major].
4. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 18
## Disposition check (round-17 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
