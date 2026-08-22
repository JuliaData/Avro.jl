# Response to codex-review-23 (round 23) — agreement

Codex returned `VERDICT: AGREE` on DRAFT v23: no blocker or major finding remains; the remaining items
are minors and nits to be closed during their named implementation phases.

**Claude's statement:** no material objection remains on Claude's side either. The plan is
implementation-ready as written. The review loop (rounds 1–23, `reviews/`) is closed.

The eleven non-blocking items and the follow-ups were applied immediately as v24 (editorial, within the
agreed contracts; no policy change):

1. Phase 4a's invariant gate is container-only; the complete consumer/source-mode/preflight acceptance
   (table shells, streamed chunks) is Phase 4b's gate (§12).
2. The copying public constructors' comparison-rule denominator is the copied key/value/representation
   bytes, with a near-limit `Avro.Map` constructor gate (§4.4).
3. `live_base` is defined as every persistent live charge not represented by `capacity`,
   `committed_payload`, `worker_state`, the sequential reserve or `W` (§4.9).
4. `logical=` synthesises `logicalType`/`precision`/`scale`, and caller `props` carrying those keys are
   rejected on that constructor (§4.2).
5. `SchemaCache` uses a sorted `UInt64` fingerprint index with charged insertion moves (§4.10).
6. `NamedTuple(row)` is an explicit caller-space conversion outside the guarded specialisation
   guarantee; Avro never invokes it internally (§4.6).
7. "Every decoded block is decoded at most once — exactly once on success" (§4.9).
8. `eachblock` transfers the decompression buffer to the caller; no second copy; boundary fixture (§4.9).
9. The conventional fixed name is `fixed_N` (§4.8).
10. Appendix B uses the do-block form for the one-row read.
11. `Avro.metadata`/`Avro.codec`/`Avro.sync`/`Avro.writerschema` have signatures (§5.3).

Follow-ups: executable constructor signatures with defaults and `limits=Limits()`, the recursive-builder
contract (registered-but-unfilled reference, nested builders for mutual recursion, discard on throw),
and the removed Scan risk line.

Implementation begins at Phase 0 under the local-only boundary.
