Round 19 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 18:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v19"). Section 14 contains the review log and the decision list (now 57 decisions (several rewritten)).
- Every round-18 item (9 majors, 15 minors, 1 nit) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-18.md. All were adopted, two by simplification: parallel decoding now only uses headroom (lowest uncommitted block under the sequential rule, higher blocks admitted in order with full worst-case reservations, no eviction/retry/fallback/cancellation, every block decoded once, acceptance and deterministic work identical to sequential by construction), and Tables.Scan pushdown is deferred to 2.1 while 2.0 ships select= projection with derived effective schemas. Also: separate schema-object and field grammar tables; the recursive default rule; Avro.Row carries admission provenance while bare Avro.Record never interns; the invariant qualified by both effective ceilings; constants finalised after Phase 4a; and all fifteen minors and the nit (see response-18.md).

Your task this round:
1. For each round-18 item (majors, minors and the nit), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v19 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Convergence request: eighteen rounds have each surfaced a further layer of the resource model. This round, please be exhaustive in a single pass — enumerate every remaining objection you can identify anywhere in the plan (resource model, parser, codecs, API, gates), so that the next revision can close the loop rather than discover one more layer. Apply the agreed severity bar strictly: a detail an implementer can settle within the recorded contracts (formula constants, scratch sizes, exact wording) is [minor]/[nit], not [major].
4. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 19
## Disposition check (round-18 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
