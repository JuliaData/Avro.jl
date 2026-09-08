# Response to codex-review-16 (round 16) — disposition of all items

The plan was revised in place to DRAFT v17. Every finding and every follow-up was adopted. Section
references are to v17.

## New findings

1. **[blocker] JSON numeric tokens — Adopted (Avro owns its JSON reader).** §4.2: the pre-scan validates
   every number token against the RFC 8259 grammar (no leading `+`, no bare non-finite tokens outside
   the §4.11 datum table) and records token endpoints; an in-package recursive-descent reader parses
   objects, arrays, strings (WTF-8 decoder), numbers and literals with explicit depth and charged
   allocations; required integers use checked `Int64` accumulation, required floats a bounded
   `Float64` path on tokens ≤ 1,024 bytes, and metadata numbers are stored as charged immutable raw
   tokens re-emitted verbatim. JSON.jl is no longer used for parsing anywhere (`fromjson` included) and
   stays a dependency only for the `JSON.json(schema)` printing overload (§4.11, §4.15, §11, decision
   51). Tests: huge integers, mantissas and exponents in known fields, defaults, nested/skipped
   properties and `fromjson`.
2. **[major] `Table` consumer — Adopted.** §4.4: guaranteed consumers are `Reader`/`Rows` for every root
   and `Table` for record roots; the container-only invariant stays in Phase 4a and the complete
   consumer/source-mode gate moves to Phase 4b (§12).
3. **[major] Streamed chunk shells — Adopted.** §4.4/§4.9: the writer preflight computes the exact
   streamed peak — `nblocks × ncolumns` chunk headers plus chunk capacities, chunk payloads, final shells
   and payloads, block table and assembly buffers; the 1,001-field record over 10,000 one-row blocks is
   a gated fixture; stale geometric-growth wording removed (§6, §12, decision 31).
4. **[major] Parallel priority counterexample — Adopted.** §4.9 rule 2: a block whose request still does
   not fit after evicting higher blocks fails only if it is the lowest uncommitted block; otherwise it
   releases its own reservations and requeues (its one speculative attempt); the exact forced schedule
   (256/40/150/20+60) is in the acceptance-equivalence gate (decision 53).
5. **[major] Eviction inside a datum — Adopted.** §4.9: every permit wait is cancellable and eviction is
   observed at every reservation request, between decompression steps, and every 4,096 values,
   comparisons, moves or admissions inside nested decoding, sorting and admission loops; gates for a
   higher block inside one large datum and during a permit wait (decision 53).
6. **[major] Pinned Scan allocations — Adopted.** §6: the filter mask is produced by an Avro-owned,
   allocation-aware evaluator (one charged row-sized mask, no per-node intermediates; the pinned
   `Tables.filtermask` is the semantic reference, gated by equality on deep predicate trees);
   `Tables.resolve`'s column-count-bounded allocations are the documented exception to the
   no-hash/reservation rules (§4.4); residual conversion runs after the ownership-transfer boundary
   (decision 54).
7. **[major] Scan provenance — Adopted.** §6: scanned tables carry a derived effective schema (selected
   fields in output order, renames, direct widenings, named types preserved; empty record for
   zero-column selections); a non-identity residual yields a plain Tables table with provenance cleared;
   gates for writing projected, renamed, zero-column, widened and evolved results (decision 54).
8. **[major] Repaired schemas and PCF — Adopted.** §4.2: graphs with `repaired_names` are rejected by
   `canonical`, `fingerprint`, `parsingequivalent`, `register!`, `encodesingle`, `tojson` and
   `fromjson` (binary/OCF-only); `repaired_defaults` does not affect PCF; negative gates (decision 52).
9. **[minor] Graph composition — Adopted.** Public constructors deep-copy already-frozen children with
   fresh ids under the constructing operation's budget; ids validated within `Int32`; creation gates for
   parser-, constructor- and type-derived graphs and reused children (§4.2, §9.10).
10. **[minor] Repair options in peripheral APIs — Adopted.** `Avro.write`/`tobuffer` forward
    `allow_invalid_names/defaults`; repaired invalid names are binary/OCF-only (§4.9, §4.2).
11. **[minor] Schema semantic edges — Adopted.** Invalid defaults compare by raw JSON bytes; `iserror` is
    in equality/hash; type-alias (normalised fullname) vs field-alias (plain name) matching separated;
    defined-attribute type table with negative fixtures (§4.2).
12. **[minor] Legacy decimal framing — Adopted.** `decimal_byteorder=:little` recovers `bytes` decimals
    and `fixed` decimals of declared size 16 only; other sizes are misframed and unrecoverable, reported
    by `Avro.inspect`, with a real non-16-size 1.1.2 fixture (§4.9).
13. **[minor] Allocation and lifecycle boundaries — Adopted.** The exclusion list is enumerated (codec
    libraries, `Mmap`, `Symbol` interning, task stacks, `Base.summarysize`, `Tables.resolve`); snappy
    queries `snappy_uncompressed_length` and decompresses into an Avro-owned buffer; prepared
    `DatumWriter` calls precharge the retained `Encoder` capacity; `Writer` do-block form and finalizer
    abort fallback (§4.9).

## Follow-ups (all adopted)

`npairs` capacity reconciled (§4.4, §9); owned charged metadata spans (§4.2); `decimal_byteorder` in
the `Reader` contract (§4.9); `show`/`==`/`hash` use the recorded `GraphInfo` limits (§4.14); duplicate
ordinary/`avro.schema`/`avro.codec` header fixtures (§9.10); stale geometric wording removed; Phase 2
fuzz wording is the recorded 200 × 1,000 sample; dense-ID creation gates; every complete-work counter
(comparison, move, filter evaluation, assembly copy) in the deterministic gate; non-record `Rows`
copies write datums under the retained schema.

No material objection to any round-16 item remains on Claude's side; DRAFT v17 is submitted for round 17.
