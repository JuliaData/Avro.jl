# Response to codex-review-10 (round 10) — disposition of all items

The plan was revised in place to DRAFT v11. Every finding and every follow-up was adopted. Section
references are to v11. Oracle behaviour for concatenated members was measured in the authoring session
with avro-tools 1.12.2 and fastavro 1.12.2 on hand-built OCF blocks: two zstandard frames — Java and
fastavro read; two xz streams and two bzip2 streams — fastavro reads, Java `EOFException`; deflate with
three suffix bytes — both read (suffix ignored). Your xz-padding result (Java reads, fastavro rejects)
is recorded as reported.

## Carried rows

* **R1-1 / R4-1 / R4-2 / R5-2 / R5-3 and the round-6/7/8/9 partial rows — Adopted.** Each remaining
  defect is one of findings 1–5 below; the mechanisms marked resolved are unchanged.

## New findings 1–9

1. **[major] `Dict` capacity / collision rehash — Adopted (package-owned map).** Generic maps are now
   `Avro.Map{x} <: AbstractDict{String,x}` (§4.6, decision 35): insertion-ordered key and value vectors
   plus an `Int32` open-addressing index of fixed capacity `tablesz(n)` (power of two ≥ `max(16, 2n)`)
   built once from the decoded pair count, hashed with a per-instance random seed so collisions cannot be
   precomputed, never rehashed or grown; storage `128 + n × (8 + slot bytes) + 4 × tablesz(n)` depends on
   nothing private to `Base.Dict`; `Dict(m)` converts; duplicate keys last-wins (Java-compatible,
   documented). The symbol-admission table uses the same machinery (grown by doubling with the new
   capacity reserved first). Typed `Dict` targets are built outside exact accounting and documented as
   approximate (§4.4). Tests at n = 0/1/11/16/17/43/1024, adversarial equal-hash keys, duplicate keys
   (§9.10); Phases 2/4b (§12).
2. **[major] Schema/plan graphs outside the ceiling — Adopted.** §4.4 category (e): every schema node,
   frozen JSON tree, parser state, printer/canonical/fingerprint buffer, plan node and memo table is
   charged per node under the budget of the operation that builds it; new budget scopes for
   `parseschema`, `json`/`canonical`/`fingerprint`, `Avro.schema(T)` and prepared-codec construction
   (one-shot calls charge plan construction to their own budget); near-ceiling tests for shallow wide
   schemas, large defaults/props and large resolving plans (§9.10; Phase 1 gate, §12; decision 36).
3. **[major] Storage oracle vs identity-bearing values — Adopted.** The oracle is
   `Base.summarysize(x; exclude=Avro.Schema)`; schema references carried by `Fixed`, `EnumValue` and
   `Record` are charged once with the graph (category (e)); `EnumValue` has an explicit formula (16);
   the gate covers every member of `E` plus the three identity-bearing types with shared and distinct
   schema identities; nested values charge per materialisation (§4.4, §9.10).
4. **[major] XZ stream padding — Adopted.** §4.9: all-zero padding in multiples of four is accepted
   between streams and at the end of the payload; non-zero or non-multiple-of-four padding is a
   `CodecError`; positive 4/8-byte and negative 1-byte/non-zero fixtures (§8.2, §9.10); the §8.4 row
   records Java accept / fastavro reject (decision 37).
5. **[minor] Stale clauses — Adopted.** `length + 32` and the xz per-frame-estimate wording in §4.9 are
   replaced by references to the §4.4 categories; §4.14, §13 and decision 22 now say "at most two
   attempts per block per pass" and state the four-decompression absolute bound for filtered scans.
6. **[minor] Short and skippable zstandard frames — Adopted.** §4.9: the header read is "up to 18 bytes"
   (a valid empty frame is 9); `ZSTD_findFrameCompressedSize` is a required symbol (§11) and delimits
   members including skippable frames (consumed, no output, charged nothing, bounded by the payload;
   truncated skippable frames are errors); fixtures for empty and skippable frames before/between/after/
   only/truncated (§8.2, §9.10).
7. **[minor] Output-buffer and yield ownership — Adopted.** Category (c) now includes the `Encoder`
   buffer, compressor output buffers and JSON text buffers with replacement capacity reserved before
   growth; the yield-time transfer rule for `Rows`/`eachdatum`/`eachblock` is stated (charge released
   at the next iteration step, cumulative counters persist; `Avro.Table` keeps everything charged until
   return), with the more-than-the-ceiling streaming test in both retention modes (§4.4, §9.10).
8. **[minor] RSS sampler — Adopted.** §4.9 names it a sampled high-water mark, flushes the markers, and
   takes `peak_rss` as the maximum of the samples, the child's `Sys.maxrss` at the end and the child's
   `Base.gc_live_bytes()` high-water around commits; the deterministic reservation oracle (test-only
   allocation hook asserting every allocation was preceded by a reservation) is the primary gate.
9. **[minor] Layout guard — Adopted.** §4.4/§11: the storage constants are measured at `__init__` from
   canonical probe objects with `Base.summarysize` and stored in a module-level `const Ref`; a test
   asserts the recorded 1.10/1.12 values on every CI version, so a future layout change is picked up
   rather than trusted.

## Non-blocking follow-ups (all adopted)

* Stale accounting/retry text removed; short/skippable zstandard fixtures added; output and yield
  ownership stated; RSS and runtime-layout checks hardened. Deferrals unchanged.

No material objection to any round-10 finding remains on Claude's side; DRAFT v11 is submitted for round 11.
