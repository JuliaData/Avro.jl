Round 20 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 19:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v20"). Section 14 contains the review log and the decision list (now 58 decisions).
- Every round-19 item (4 majors, 15 minors, 1 nit) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-19.md. All were adopted. Changes: schema-object type restricted to primitive names and the six complex-type keywords (named references only as strings, unions only as arrays); the lowest parallel block writes directly into its final-column slice exactly like the ntasks=1 path, W made complete, workers created only from headroom; work equality stated for successful operations with a bounded failure statement and pending stage-1 failures; identity-first union branch recovery; and all minors (signatures incl. register!/lookup limits, fromjson unknown=, Rows select=, Avro.Row and public value constructors; projection rules; Row in the storage oracle; map wording; Scan pin/bootstrap/precompile/offset-window text removed; FixedSchema.doc removed; empty enums; missing avro.schema; strict eachblock; decimal matching wording; GMP exceptions; StructUtils default errors and the TimeZones contract; fullname-first alias matching; certified portability matrix; log order).

Your task this round:
1. For each round-19 item (majors, minors and the nit), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v20 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Convergence request: nineteen rounds have each surfaced a further layer of the resource model. This round, please be exhaustive in a single pass — enumerate every remaining objection you can identify anywhere in the plan (resource model, parser, codecs, API, gates), so that the next revision can close the loop rather than discover one more layer. Apply the agreed severity bar strictly: a detail an implementer can settle within the recorded contracts (formula constants, scratch sizes, exact wording) is [minor]/[nit], not [major].
4. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 20
## Disposition check (round-19 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
