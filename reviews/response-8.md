# Response to codex-review-8 (round 8) — disposition of all items

The plan was revised in place to DRAFT v9. Every finding and every follow-up was adopted. Section
references are to v9. Claims were re-verified in the authoring environment before adoption: Julia 1.12.6
`Base.summarysize` of `Vector{Union{Missing,Float64}}(undef, 10^6)` = 9,000,040 (9 bytes/row; `Bool`
2; `Int32` 5); zstd 1.5.7 `ZSTD_estimateDStreamSize(2^25)` = 34,043,696 = window + 489,264 (the same
overhead from 2^20 to 2^31; 99,120 at 2^10) and `ZSTD_estimateDStreamSize_fromFrame` on the first 18
bytes of a level-20/windowLog-25 frame returns the same number; `ZSTD_getCParams` default `windowLog`
19/21/23/27 at levels 1/3/19/22; fastavro 1.12.2 on CPython 3.14.2 raises `UnicodeDecodeError` for a
metadata value `ff ff`.

## Carried rows

* **R1-1 / R4-1 / R4-2 / R5-1 / R5-2 / R5-3 / R5-4 and the round-6/7 resource rows — Adopted.** Each
  remaining defect is one of findings 1–4 below; nothing else in those rows was contested.
* All rows marked RESOLVED are unchanged.

## New findings 1–8

1. **[major] Nullable column storage — Adopted.** §4.9 stage 1 charges the exact Julia storage of
   `Vector{e}(undef, rows)`: `rows × Base.elsize` plus one tag byte per element for isbits-`Union`
   element types plus the 40-byte header, with checked arithmetic and `Base.summarysize` as the test
   oracle; reference types charge 8 bytes per slot, with referenced payload charged separately as
   committed output (no double charging — decision 30). Tests: near-ceiling nullable `Int64`, `Float64`,
   `Date`, `UUID` columns (§9.11); Phases 4b/4c.
2. **[major] zstd decoder requirement — Adopted (one meaning).** `max_codec_memory` is now exactly the cap
   on the complete, library-reported decoder requirement of one frame (§4.4 field comment, §4.9 codec
   contract, decision 6 rewritten to match decision 28). Read side: `ZSTD_estimateDStreamSize_fromFrame`
   on the block's frame header before any decompressor exists, plus `windowLogMax = L` with `L` the
   largest log whose `ZSTD_estimateDStreamSize(2^L)` fits (belt and braces). Write side: the writer
   selects the largest `L ≤` the level's default `windowLog` whose estimate fits — no hard-coded
   overhead — and verifies every emitted frame with `fromFrame` before writing it. Gated one byte
   below/at/above every power-of-two estimate (§4.9, §9.10). The constructor-floor text cites the
   library numbers (8,877,872 bytes for an 8 MiB window; 489,264-byte overhead) and the high-window
   fixture threshold is the frame's reported estimate (1,074,231,088 bytes) (§4.4, §8.2).
3. **[major] Eviction vs work guarantee — Adopted.** §4.9 rule 2: eviction is cooperative and acknowledged
   (flag observed between ≤ 64 KiB decompression steps and between datums; a reservation counts as
   released only after the acknowledgement); **each block is attempted at most twice** — the first
   speculative, the second only as the lowest uncommitted block, where it can no longer be evicted;
   discarded attempts roll back their value/work counters (cumulative rules are checked only at commit),
   so acceptance is identical to sequential and total CPU work is bounded by 2 × the sequential bound;
   gated under forced eviction schedules (attempts ≤ 2, CPU ≤ 2 × sequential) (§9.11, §12 4c, decision
   22, risk list).
4. **[major] Peak-RSS gate on the sequential path — Adopted.** §4.9: the input is read into a caller-owned
   byte buffer and faulted before the baseline RSS is recorded, decoding runs with `ntasks = 8`, and a
   test-only `@atomic` high-water counter asserts ≥ 2 blocks in flight; §9.11 and §12 now say 16 MiB
   blocks (the stale 64 MiB wording is gone).
5. **[major] fastavro and non-UTF-8 metadata — Adopted.** Byte-valued metadata stays (spec). §7 records
   the third oracle-readability exception (fastavro decodes every metadata value as UTF-8), §8.4 gains
   the capability row, §8.2 the fixture (`x => ff ff`; Julia and Java accept, fastavro expected to
   reject), §9.10 the round-trip test, §12 4a the gate; decision 32.
6. **[major] Filtered Scan vs exact preallocation — Adopted.** §6: in the parallel path a row-dependent
   filter runs a global filter pass first (filter columns decoded for every block in parallel under the
   reservation rules, masks and qualifying counts retained and charged; strict mode completes validation
   in this pass), then `offset`/`limit` on the qualifying prefix sums, exact preallocation, and a
   selection pass over qualifying rows only (blocks without qualifying rows in the window skipped in both
   modes; ≤ 2 decompressions per block, inside the ≤ 2 × work statement). Sequential paths keep the
   per-block two-pass rule with reserved geometric growth. Gate: identical results for `ntasks ∈ {1,2,8}`
   incl. low-selectivity filters, offset/limit across block boundaries, near-ceiling files (§6, §12 4d,
   decision 31).
7. **[minor] `fixed(0)` decimal — Adopted.** §4.2: no positive precision is valid for `fixed(0)`; the
   annotation is dropped, raw props retained, the formula never evaluated at n = 0, and a test asserts no
   `DomainError` leaks (§9.10).
8. **[minor] Estimator APIs — Adopted.** §11: `Zstd_jll` (1.5) becomes a direct dependency and `XZ_jll`
   (5.8) a weak one (the xz extension triggers on `[CodecXz, XZ_jll]`); the four zstd estimators and the
   two liblzma memusage functions are reached by `ccall` against those pinned JLLs, with `Libdl.dlsym`
   symbol checks at `__init__`/extension load that remove `zstandard` from `Avro.codecs()` or raise
   `UnsupportedCodecError` naming the missing symbol; Phase 0 establishes this (§12); the risk list
   records the version-drift risk with the threshold tests as the detector.

## Non-blocking follow-ups (all adopted)

* bzip2 wording now "within the 16 MiB constructor floor" (§4.9).
* Peak-RSS workload is 16 MiB everywhere (§9.11, §12).
* Cooperative, acknowledged codec cancellation defined before "released" is claimed (§4.9 rule 2).
* Task cap stated: actual tasks `min(ntasks, nblocks)`; in-flight blocks `max_inflight_blocks == 0 ?
  tasks : min(max_inflight_blocks, tasks)`, further bounded by the ceiling (§4.9).
* Preallocated storage vs referenced payload distinguished (§4.9, decision 30).
* CPython 3.14 pinned for the interop job (§8.5, §11).
* Deferrals unchanged.

No material objection to any round-8 finding remains on Claude's side; DRAFT v9 is submitted for round 9.
