Round 12 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 11:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v12"). Section 14 contains the review log and the decision list (now 40 decisions).
- Every round-11 item (carried rows, new findings 1-9, and all follow-ups) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-11.md. All were adopted. Changes: the Avro.Map shell is measured at __init__ and actual capacities are charged (npairs/nunique/capacity terms) with a 64-probe cap and re-seeded rebuilds giving deterministic map work; JSON.jl duplicate-key tracking is never enabled (Avro sorts key spans; no parser/frozen/plan structure uses Base.Dict or Set); the typed conversion boundary (fast route = approved exactly charged representations; semantic route converts after ownership transfer in caller space); limits= on every schema operation with one shared budget; the writer preflights the reader complete peak including category (e) with combined tests; stale summaries corrected; the write-once layout constants recorded as the second global-state exception; measured oracle rows for xz padding (Java never verifies payload exhaustion) and zstandard skippable/empty frames.

Your task this round:
1. For each round-11 item (carried rows, new findings 1-9, and the follow-ups), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v12 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 12
## Disposition check (round-11 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
