# Response to codex-review-7 (round 7) — disposition of all items

The plan was revised in place to DRAFT v8. Every carried item and every new finding was adopted; the
parallel resource model was redesigned around actual-size reservations rather than patched. Section
references are to v8.

## Carried items

* **R1-1 / R4-2 / R5-1 / R5-2 / round-6 findings 1–3 (resource model) — Adopted.** See new findings 1–5.
* All other carried rows were marked resolved by Codex and are unchanged.

## New findings 1–10

1. **[blocker] Assembly copy outside the reservation / vector reallocation — Adopted (redesigned).**
   §4.9: stage 1 **preallocates every final column once at its exact final capacity** from the pre-scan
   row count (`rows × sizeof(e)` for isbits, `rows × 8` for references) and charges that *capacity* to
   the ceiling up front; assembly copies isbits chunk data to the block's prefix-sum offset and moves
   references, so nothing is ever reallocated and the physical peak is `capacity + committed payload +
   in-flight reservations ≤ ceiling` at all times; the chunk's reservation is released after the copy.
2. **[major] Writer-side codec memory and emitted-frame requirement — Adopted.** §4.9/§4.3 (decision 28):
   at construction the writer charges the compressor workspace (`lzma_easy_encoder_memusage`,
   `ZSTD_estimateCStreamSize`, fixed bounds for bzip2/deflate/snappy — all verified exported in the
   pinned environment: xz preset 9 ≈ 673 MiB, zstd level 22 ≈ 834 MiB) to its ceiling and checks the
   decoder requirement of the frames it will emit against `max_codec_memory` (`lzma_easy_decoder_memusage`;
   zstd windows are set explicitly so `ZSTD_estimateDStreamSize(window)` fits by construction),
   raising `LimitError` otherwise; gated at minimum, default, high and raised limits for every codec. The
   `max_codec_memory` floor is raised to 16 MiB because liblzma reports 8.06 MiB for preset 6.
3. **[major] Worst-case permits cap parallelism / parallel rejects what sequential accepts — Adopted
   (redesigned).** §4.9: reservations are **actual-size, made before every allocation** (compressed
   buffer, codec workspace from the frame's own requirement, decompressed buffer as it grows, decoded
   output as it is produced); admission has **lowest-block priority** (higher in-flight blocks are not
   counted against a lower block, and are **evicted** — abandoned and requeued — when the physical total
   would exceed the ceiling), so a block fails iff the sequential rule fails at that block: acceptance is
   identical for every `ntasks` (gated), liveness is argued (the lowest block never waits), and
   parallelism is whatever fits. The eight-thread performance gate states its explicit limits
   (`max_total_bytes = 4 GiB`) in the protocol (§10.2).
4. **[blocker] Fixed 1 GiB default unsafe — Adopted.** §4.4 (decision 12): the fixed default ceiling is
   256 MiB with smaller per-block limits (16 MiB blocks, 64 MiB block output cap, 32 MiB codec cap), and
   an **available-memory guard** sets the effective ceiling to `min(max_total_bytes, available ÷ 2)` with
   `available = min(Sys.free_memory(), Sys.total_memory())` (cgroup-constrained total; host-wide free —
   the limitation is documented), failing with `LimitError` before any allocation when the first unit of
   progress cannot fit; the guard is a safety valve outside the identical-limits invariant and is tested
   with an injected value.
5. **[major] Constructor arithmetic vs runtime reservation — Adopted.** §4.4: with actual-size
   reservations there is no worst-case `W`; the constructor validates exact relations that guarantee the
   first unit of progress fits (`max_block_bytes ≤ ceiling ÷ 4`, `max_codec_memory + 4 MiB ≤ ceiling ÷ 4`,
   `max_block_output_bytes ≤ ceiling ÷ 2`, `max_metadata_bytes + max_schema_bytes ≤ ceiling ÷ 2`,
   `max_datum_bytes ≤ max_bytes + 1 MiB`), with checked arithmetic and tests at equality and one byte
   beyond.
6. **[minor] Empty-union `minsize` — Adopted.** §4.2: `[]` parses/canonicalises (fixture 016), `minsize =
   ∞`, `juliatype([]) = UnionValue`, decode → `DataError`, encode → `EncodeError` (decision 29).
7. **[minor] Peak-RSS method — Adopted.** §4.9: fresh warmed-process baseline, streamed input, `peak −
   baseline ≤ effective ceiling + 128 MiB`, both recorded; runs on highly compressible 16 MiB blocks with
   eight workers and on half-ceiling tables.
8. **[minor] Alias normalisation — Adopted.** §4.2: type aliases normalised to fullnames at parse, raw
   text kept for re-emission; `Bar`/`a.Bar` test.
9. **[minor] Streamed compressed input — Adopted.** §4.9: the compressed block buffer and the decompressed
   buffer are both reserved before allocation for `IO` and streamed path sources.
10. **[minor] `Nothing` schema — Adopted.** §4.8: `Avro.schema(Nothing) == null`; `Union{Missing,Nothing}`
    is a union-member collision error.
11. **[follow-up] Exact sanitisation algorithm — Adopted.** §4.8 states the algorithm (join `string(p)`
    with `_`, replace non-`[A-Za-z0-9_]` by `_`, collapse runs, digit-prefix rule, 100-char truncation +
    16 hex SHA-256 suffix) and that it is identical across Julia versions for the same spelling.

No material objection to any round-7 finding remains on Claude's side; DRAFT v8 is submitted for round 8.
