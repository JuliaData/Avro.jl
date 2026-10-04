Round 9 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 8:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v9"). Section 14 contains the review log and the decision list (now 32 decisions).
- Every round-8 item (carried rows, new findings 1-8, and all follow-ups) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-8.md. All were adopted, and your probe results were re-verified in the authoring environment (summarysize 9,000,040 for 10^6 Union{Missing,Float64}; ZSTD_estimateDStreamSize(2^25) = 34,043,696 with a 489,264-byte overhead from 2^20 to 2^31; fromFrame on the first 18 bytes returns the same; default windowLog 19/21/23/27 at levels 1/3/19/22; fastavro UnicodeDecodeError on metadata ff ff). Changes: exact Julia column storage incl. isbits-union tag bytes with storage/payload separation; max_codec_memory given one meaning (library-reported complete decoder requirement) with ZSTD_estimateDStreamSize_fromFrame on read and on every emitted frame, writer windowLog selection by the estimate, in-house xz header estimates cross-checked against liblzma; cooperative acknowledged eviction with at most two attempts per block, rolled-back counters, and a <= 2x work bound; the peak-RSS gate from a caller-owned faulted byte buffer with an in-flight high-water assertion; the non-UTF-8 metadata fastavro exception and fixture; the global filter pass for filtered parallel scans; fixed(0) decimal handling; Zstd_jll direct / XZ_jll weak dependencies with symbol checks; the task cap and streamed column growth stated; stale 8 MiB / 64 MiB wordings fixed; CPython 3.14 pinned.

Your task this round:
1. For each round-8 item (carried rows, new findings 1-8, and the follow-ups), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v9 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 9
## Disposition check (round-8 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
