Round 17 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 16:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v17"). Section 14 contains the review log and the decision list (now 54 decisions).
- Every round-16 item (the blocker, 7 majors, 5 minors and 10 follow-ups) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-16.md. All were adopted. Changes: Avro owns its JSON reader entirely (pre-scan with number grammar, checked integers, bounded floats, raw metadata number tokens, WTF-8 strings, recursive descent) and JSON.jl is used only for the JSON.json(schema) printing overload; consumers narrowed (Reader/Rows for all roots, Table for records) with the exact streamed chunk-shell preflight and the complete gate moved to Phase 4b; denied non-lowest blocks self-evict and requeue, cancellable permit waits, in-datum eviction checks; an Avro-owned allocation-aware Scan filter evaluator, residual after ownership transfer, derived effective schema with provenance clearing; repaired-name schemas rejected by PCF-dependent and JSON operations; and all minors and follow-ups (see response-16.md).

Your task this round:
1. For each round-16 item (blocker, majors, minors and follow-ups), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v17 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Convergence request: sixteen rounds have each surfaced a further layer of the resource model. This round, please be exhaustive in a single pass — enumerate every remaining objection you can identify anywhere in the plan (resource model, parser, codecs, API, gates), so that the next revision can close the loop rather than discover one more layer. Apply the agreed severity bar strictly: a detail an implementer can settle within the recorded contracts (formula constants, scratch sizes, exact wording) is [minor]/[nit], not [major].
4. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 17
## Disposition check (round-16 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
