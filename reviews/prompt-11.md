Round 11 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 10:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v11"). Section 14 contains the review log and the decision list (now 37 decisions).
- Every round-10 item (carried rows, new findings 1-9, and all follow-ups) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-10.md. All were adopted. Oracle behaviour for concatenated members was measured in the authoring session (avro-tools 1.12.2 and fastavro 1.12.2 on hand-built OCF blocks) and recorded in section 8.4. Changes: generic maps are now Avro.Map (package-owned, per-instance seeded hashing, deterministic never-rehashed capacity) and the symbol-admission table uses the same machinery, so no charge depends on Base.Dict; schema and plan graphs are accounting category (e) with new budget scopes; the storage oracle excludes Avro.Schema and the storage constants are measured at __init__; xz stream padding and zstandard skippable/empty frames are accepted with ZSTD_findFrameCompressedSize as a required symbol; stale length+32 / xz-estimate / "twice per block" clauses replaced by the authoritative rules; output buffers and yield-time ownership transfer specified; the RSS primitive supplemented by OS and allocator high-water marks with the reservation hook as the primary gate.

Your task this round:
1. For each round-10 item (carried rows, new findings 1-9, and the follow-ups), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v11 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 11
## Disposition check (round-10 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
