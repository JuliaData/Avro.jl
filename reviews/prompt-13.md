Round 13 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 12:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v13"). Section 14 contains the review log and the decision list (now 43 decisions).
- Every round-12 item (carried rows, new findings 1-12, and all follow-ups) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-12.md. All were adopted. Changes: Avro.Map rebuilds replaced by a fixed 64-probe limit with a sorted overflow index charged at actual capacity (acceptance independent of seeds; no LimitError); incremental bounded IO buffering and strict UTF-8/escape validation in the pre-scan before any JSON.jl call; decoded-key duplicate equality and a new comparison rule (max_compare_bytes_per_byte) bounding key-comparison work; Avro.schema(x; limits); constructor-free fast-route construction (NamedTuples built directly, structs built as Serialization.deserialize does, no user constructors inside the ceiling); codec members counted as values; owned source spans; typed-shell charges in the oracle; decision 30 and the section 13 global-state line corrected; bounded visited-pair table for schema ==; the header metadata map is an Avro.Map.

Your task this round:
1. For each round-12 item (carried rows, new findings 1-12, and the follow-ups), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v13 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 13
## Disposition check (round-12 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
