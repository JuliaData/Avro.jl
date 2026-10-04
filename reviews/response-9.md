# Response to codex-review-9 (round 9) — disposition of all items

The plan was revised in place to DRAFT v10. Every finding and every follow-up was adopted. Section
references are to v10. Before adoption the storage claims were re-measured on Julia 1.10.11 and 1.12.6
(`Base.summarysize`): `String` 8 + n, `Vector{UInt8}` 40 + n, boxed `Int64`/`Float64` in an `Any` slot
16, `BigInt` 48 + 8 limbs, `Fixed`-like struct 56 + n, `Dict{String,Int64}` after `sizehint!(d, n)`
16/32/256/2048/262,144 slots for n = 10/13/100/1000/100,000 at ≈ 17 bytes per slot plus a ≤ 256-byte
header, `Vector{Union{Missing,Float64}}` 9 bytes per element; the planned dependency set with the
planned `[compat]` bounds resolves on Julia 1.12 (JSON 1.7.1, StructUtils 2.8.5, CodecZstd 0.8.7,
Zstd_jll 1.5.7, XZ_jll 5.8.3, CodecXz 0.7.4, CodecBzip2 0.8.5, TimeZones 1.22.2).

## Carried rows

* **R1-1 / R4-1 / R4-2 / R5-2 / R5-3 and the round-6/7/8 partial rows — Adopted.** Each remaining defect
  is one of findings 1–5 below; the mechanisms marked resolved are unchanged.
* **Round-8 follow-ups "safe task cap" and "storage vs payload" — Adopted.** See findings 2 and 7.

## New findings 1–8

1. **[major] XZ reservation — Adopted (reserve the cap).** §4.4 category (c) and §4.9: liblzma does not
   report a block's requirement before allocating, so the xz workspace reservation is the configured
   `max_codec_memory` itself, with liblzma's `memlimit` enforcement as the hard check for every block of
   every stream; this only reduces xz parallelism under lowest-block priority and never acceptance
   (sequential reserves the same). The in-house formula and the `lzma_raw_decoder_memusage` route are
   gone; §11 states that the reader needs no per-block estimator (decision 6).
2. **[major] Generic output estimator — Adopted (representation formulas, oracle-asserted).** §4.4 now
   carries the authoritative list of four disjoint accounting categories: (a) storage shells (exact
   capacity incl. isbits-union tag bytes and headers, never per value), (b) referenced payload by
   representation-specific formulas in `src/limits.jl` (`storagebytes`: `String` 16 + n, `Vector{UInt8}`
   40 + n, `Fixed` 56 + n, `WideDecimal` 64 + n, boxed isbits 16 + `sizeof`, `Record` 56 + 8k + fields,
   `UnionValue` 16 + boxed value, `Vector{x}` 40 + n × (elsize + tag), `Dict{String,x}` 256 +
   `tablesz(n)` × (9 + slot bytes)) asserted `≥ Base.summarysize` for generated values of every member
   of `E` on every supported Julia version; maps are decoded as key/value vectors and materialised into
   one `Dict` sized once by `sizehint!` so the table capacity is a function of the known count; isbits
   cells in typed columns/chunks charge nothing in (b); payload ownership transfers from in-flight to
   committed at commit without re-charging (§4.9 rules 1 and 4 reworded accordingly); (c) input/codec
   buffers; (d) internal tables incl. worker state. Tests: near-ceiling empty-bytes, fixed, wide-decimal
   and nullable-isbits tables (§9.10/§9.11); Phase 2 scope (§12); decision 30.
3. **[major] Concatenated codec members — Adopted.** §4.9 "Members and suffixes": zstandard frames, xz
   streams and bzip2 streams are decoded member by member to exact payload exhaustion, with the decoder
   requirement checked and reserved per member and the output cap/work rule cumulative; deflate has no
   member concept, so bytes after `BFINAL` are rejected (Java/fastavro ignore them; no writer emits them;
   recorded in §14); snappy leaves no room for suffixes; truncated members and garbage suffixes remain
   `CodecError`s. Fixtures: two-frame/two-stream blocks for the three codecs (fastavro verified for
   zstandard; Java per-codec behaviour measured in Phase 0 and recorded in §8.4, whose new row notes
   commons-compress's single-stream bzip2 default), deflate `BFINAL`-suffix rejection (§8.2, §9.10, §12
   4a, decision 33).
4. **[major] Filtered Scan attempts — Adopted.** §6 and §4.9: each Scan pass is its own attempt scope
   (≤ 2 attempts per block per pass; the filter pass's retained masks are never recomputed), so a filtered
   block is decompressed at most four times against the sequential filtered scan's two — the same ≤ 2 ×
   ratio — gated with forced evictions under row-dependent filters (§9.11).
5. **[major] CPU gate — Adopted.** §4.9, §9.11, §12 4c: the parallel work bound is gated in deterministic
   units (attempt counter ≤ 2 per block per pass; decompression and value counters ≤ 2 × the sequential
   run's); measured CPU time is informational with a predeclared ≤ 2.5 × tolerance on the named host
   (decision 34).
6. **[minor] §13 summary — Adopted.** "subject to the §7 recorded exceptions" added.
7. **[minor] Worker pool — Adopted.** §4.9: `inflight = min(max_inflight_blocks == 0 ? ntasks :
   max_inflight_blocks, ntasks, nblocks)`; a fixed worker pool of `min(ntasks, Threads.nthreads(),
   inflight)` tasks is created once, never one task per block, and its state is charged (16 KiB per
   worker, documented; lazily allocated task stacks excluded and documented) (decision 34).
8. **[minor] Peak-RSS primitive — Adopted.** §4.9: the decode runs in a child process; the parent samples
   the child's current RSS (`ps -o rss=`, 10 ms) from a start line printed after warm-up and input
   faulting until a done line; baseline = first sample, peak = maximum; Windows informational via the
   child's `Sys.maxrss` (documented).

## Non-blocking follow-ups (all adopted)

* §13 qualified; worker pool capped and charged; storage/payload categories made disjoint with the
  transfer rule; RSS sampling method fixed. Deferrals unchanged.

No material objection to any round-9 finding remains on Claude's side; DRAFT v10 is submitted for round 10.
