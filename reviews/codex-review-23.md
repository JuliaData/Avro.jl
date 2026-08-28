# Codex review round 23

DRAFT v23 is agreement-ready, big dawg. No blocker or major remains.

## Disposition check (round-22 items)

| # | Round-22 item | Status | Evidence |
|---:|---|---|---|
| 1 | Admission-wave barrier | **RESOLVED** | The head cannot pass an unresolved lower worker at lines 1118–1129. Liveness and failure bounds are at lines 1144–1152 and 1178–1185. |
| 2 | Unified `reader_block_peak` | **RESOLVED** | One function serves Writer preflight, sequential decode, and `W` at lines 647–653 and 1131–1138. |
| 3 | `live_base` and final table shells | **RESOLVED** | The admission equation and shell inventory are at lines 1131–1138 and 1166–1170. |
| 4 | RSS acknowledgement and interruption gates | **RESOLVED** | Parent acknowledgement is at lines 1193–1208. Forced interruption cleanup is gated at lines 1845–1848. |
| 5 | Concrete `W` measurement timing | **RESOLVED** | Codec/value constants finish in Phase 4a; concrete `W` is measured after Phase 4b and before Phase 4c at lines 1186–1190. |
| 6 | Public constructor contract | **PARTIALLY RESOLVED** | The semantics are present at lines 272–310 and 1458–1465. The synopsis still contains ellipses and bare `props`/`limits` keywords; `Field` has no stated budget behavior. |
| 7 | Typed conversion rules | **RESOLVED** | UTC `ZonedDateTime`, `Char`, narrow integers, and `Float16` are covered at lines 872–878 and 929–940. |
| 8 | Deferred union-resolution failure | **RESOLVED** | `UnresolvableBranch` and its selection-time error gate are at lines 920–925. |
| 9 | Parser completeness | **RESOLVED** | RFC 8259 negatives, contextual attributes, and primitive-name rules are at lines 331–419. |
| 10 | Writer metadata default | **RESOLVED** | The detailed contract uses a valid typed dictionary and copies abstract byte vectors at lines 1221–1223. |
| 11 | Budgeted cache equality | **RESOLVED** | Cache collision equality uses the caller’s shared budget at lines 1304–1307. |
| 12 | Fast-validation wording | **RESOLVED** | Lines 461–471 now refer to malformed decompressed datum bytes. |
| F1 | Remaining Scan wording | **PARTIALLY RESOLVED** | Scan is deferred at lines 1566–1577, but line 2217 still calls its release timing an unresolved repository risk and cites a nonexistent §6 release rule. |
| F2 | Appendix `first_row` | **RESOLVED** | Lines 2500 and 2509 use `first_row`. |

## New findings

1. **[minor] Phase 4a still claims part of the complete consumer preflight too early.**

   **Claim:** The milestone wording does not match the revised implementation order.

   **Evidence:** Lines 675–677 place the complete consumer/source-mode gate in Phase 4b. Concrete `W` is measured after Phase 4b at lines 1186–1190. Phase 4a still requires invariant tests “with the writer’s reader-peak preflight” at line 1986.

   **Recommendation:** Mark Phase 4a’s gate as container-only. Put final table-shell, streamed-chunk, source-mode, and complete preflight acceptance in Phase 4b.

2. **[minor] Public `Avro.Map` construction lacks a comparison-work denominator.**

   **Claim:** Map sorting consumes the comparison budget, but no `input_bytes` rule exists for caller-provided pairs.

   **Evidence:** Comparison accounting is defined at lines 603–615. Public copying constructors own budgets at lines 791–800 and 1475. Map construction sorts keys at lines 579–589.

   **Recommendation:** Define constructor `input_bytes` from copied key and value representation bytes. Add a near-limit map-constructor gate.

3. **[minor] `live_base` uses incorrect category labels.**

   **Claim:** Its intended contents are complete, but they are not all category (d) or (e).

   **Evidence:** Lines 1166–1170 include header metadata and final table shells in a value described as the category-(d)/(e) total. The authoritative categories are at lines 702–763.

   **Recommendation:** Define `live_base` as every persistent live charge not represented by capacity, committed payload, worker state, sequential reserve, or `W`.

4. **[minor] Logical-property collisions are ambiguous.**

   **Claim:** The plan does not say how `logical=` interacts with `props` containing `logicalType`, `precision`, or `scale`.

   **Evidence:** The collision list at lines 272–280 omits these keys. Logical annotations and raw attributes coexist at lines 291–308 and 1458–1465.

   **Recommendation:** State which properties `logical=` synthesizes and reject duplicate caller properties for those keys.

5. **[minor] `SchemaCache` lacks a representation consistent with the no-hash contract.**

   **Claim:** Cache bounds and collision equality are defined, but its index and lookup work are not.

   **Evidence:** The guarded no-hash guarantee appears at lines 579–604 and 2135–2138. Cache behavior at lines 1297–1309 does not identify its storage structure.

   **Recommendation:** Specify a deterministic bounded index, such as a sorted `UInt64` vector, or narrow the no-hash claim and charge the chosen structure.

6. **[minor] `NamedTuple(row)` needs an explicit caller-space boundary.**

   **Claim:** Symbol admission limits names, but it does not bound the number of compiled `NamedTuple` layouts created from untrusted schemas.

   **Evidence:** Lines 200–203 say untrusted schemas do not drive compilation. Line 863 advertises `NamedTuple(row)`.

   **Recommendation:** State that this is an explicit caller-space conversion outside the guarded specialization guarantee. Assert that Avro never invokes it internally.

7. **[nit] Failure wording overstates decode completion.**

   **Claim:** “Every block is decoded exactly once” is false when a higher block is abandoned after a lower failure.

   **Evidence:** Lines 1141–1143 make the unconditional statement. Lines 1160–1165 permit abandonment.

   **Recommendation:** Say every decoded block is decoded at most once, and every block is decoded exactly once on success.

8. **[nit] `eachblock` needs an explicit peak-memory transition.**

   **Claim:** It is unclear whether the owned returned bytes take ownership of the decompression buffer or require a second buffer.

   **Evidence:** `reader_block_peak` is at lines 650–653. The owned-copy promise is at lines 1077–1080.

   **Recommendation:** Specify transfer or charge both buffers during copying. Add a boundary fixture.

9. **[nit] The conventional fixed-type name is not exact.**

   **Claim:** Literal `fixed<N>` would violate Avro’s name grammar.

   **Evidence:** Lines 929–932 use that notation; lines 945–950 define the valid-name grammar.

   **Recommendation:** Use an exact spelling such as `fixed_16`.

10. **[nit] Appendix B leaves a path-owned iterator to finalization.**

    **Claim:** `first(Avro.Rows("w.avro"))` does not close deterministically.

    **Evidence:** The close/do-block contract is at lines 1101–1106. The example is at line 2500.

    **Recommendation:** Use the do-block form or `try`/`finally`.

11. **[nit] Container accessor signatures are incomplete.**

    **Claim:** `Avro.metadata`, `Avro.codec`, and `Avro.sync` appear only in a comment.

    **Evidence:** They are named at line 1510. The DataAPI contract is at lines 1536–1541.

    **Recommendation:** Add return and ownership signatures, or use only the DataAPI interface.

## Non-blocking follow-ups (if any)

Only minor and nit work remains:

- Replace the schematic constructor synopsis with executable signatures, including defaults and `limits=Limits()`.
- Define recursive-builder context, mutual-recursion support or scope, and callback-failure cleanup.
- Correct the Phase 4a/4b gate wording and the `live_base` category description.
- Close findings 2–11 during their named implementation phases.
- Remove or recast the stale Scan risk at line 2217.

## Milestone and gate assessment

The admission-wave algorithm now has a sound liveness and failure-work bound. The memory model covers persistent state, final capacity, committed output, the direct sequential reserve, and complete higher-block reservations. No mandatory gate is impossible.

Phase 4a/4b wording needs minor synchronization. Phase 4c is executable as written. No blocker or major remains in schemas, binary or JSON encoding, resolution, OCF framing, codecs, single-object encoding, projection, resource limits, or interoperability.

Assumptions made:

- Detailed operative contracts control over stale summary text.
- Declared fixtures and gates are implementation deliverables.
- Tables.Scan remains intentionally deferred to 2.1.

Decisions made without user direction:

- I applied the requested strict severity bar. Formula, synopsis, lifecycle, and gate-wording gaps stayed minor or nit because the recorded contracts permit an implementation without changing policy.

Validation was read-only. I reviewed DRAFT v23, response-22, round-22, the local specification and dependency evidence, APIs, resource equations, milestones, decisions, risks, and gates. The package-review checklist drove the separate specification, API, resource, and interoperability passes. No file was modified.

Plan SHA-256: `f9e79c544cc5401bc938473b65851471c6fbafd04cb476001f662d4b6ab9f823`

Response SHA-256: `a54d3bcac626d51996a71c2b2958e6af590c4a0467e76d82b52599ae7af7942a`

## Verdict

VERDICT: AGREE
