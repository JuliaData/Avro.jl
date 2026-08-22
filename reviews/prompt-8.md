Round 8 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 7:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v8"). Section 14 contains the review log and the decision list (now 29 decisions).
- Every round-7 item (carried rows, new findings 1-10, and the sanitisation follow-up) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-7.md. All were adopted. The parallel resource model was redesigned: exact final-column preallocation from the pre-scan (charged capacity, no reallocation); actual-size reservations before every allocation (compressed buffer, codec workspace from the frame's own requirement, decompressed buffer as it grows, decoded output as produced); lowest-block priority with eviction of higher in-flight blocks, so acceptance is identical to sequential decoding for every ntasks; a fixed peak-RSS method; explicit limits stated for the thread gate. Defaults were lowered (256 MiB ceiling, 16 MiB blocks, 64 MiB block output cap, 32 MiB codec cap with a 16 MiB floor) and an available-memory guard added. The writer now charges compressor workspace and checks the decoder requirement of emitted frames (liblzma memusage and libzstd estimate functions were verified exported in the pinned environment: xz preset 6 decoder 8.06 MiB, preset 9 encoder 673 MiB; zstd level 22 CStream 834 MiB). Constructor relations were restated without a worst-case W. Empty unions, alias normalisation, Nothing -> null, and the exact sanitisation algorithm were specified.

Your task this round:
1. For each round-7 item (carried rows and new findings 1-10 plus the follow-up), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v8 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 8
## Disposition check (round-7 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
