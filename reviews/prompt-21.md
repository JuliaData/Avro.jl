Round 21 of the adversarial, specification-focused review of the Avro.jl 2.0 rewrite plan. Same ground rules as the previous rounds (read-only; do not create or modify files; declared artifacts/gates are deliverables, not preconditions; your entire review is your final message).

What changed since round 20:
- The plan was revised in place: /Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md (now "DRAFT v21"). Section 14 contains the review log and the decision list (now 58 decisions).
- Every round-20 item (4 majors, 12 minors, 1 nit) was dispositioned in /Users/jacob.quinn/.julia/dev/Avro/reviews/response-20.md. All were adopted. Changes: the writer tracks the exact reader-side output estimate per pending block and enforces max_block_output_bytes and max_block_count (with the four-million-empty-strings fixture); Avro.Rows has three explicit modes (generic record mode yielding Avro.Row with Tables, typed mode as a plain iterator, non-record mode); the available-memory guard reads cgroup remaining memory and live reservations and is documented as best-effort (no-OOM promise removed); counter equality limited to successful operations with a separate failure bound in every gate and summary; and all minors (signatures and scopes, UnionValue validation scope, Avro.Time removed, schema-free identity-bearing collections, remaining Scan artifacts removed, direct-lowest-block prose, ntasks/worker semantics, default W terms, narrowed constructor and no-hashing claims, successfully-closed-writer qualification, risk wording, decision order).

Your task this round:
1. For each round-20 item (majors, minors and the nit), state RESOLVED / PARTIALLY RESOLVED / NOT RESOLVED with evidence (plan section/line).
2. Re-review DRAFT v21 as a whole with the same mandate. Raise any NEW findings introduced by the revision or missed earlier, with severity tags and evidence. Apply the severity bar consistently: a finding is [blocker]/[major] only if implementing the plan as written would produce a spec violation, an unsafe default, an unexecutable gate, or an API that cannot deliver its stated guarantee; details an implementer can settle within the recorded contracts are [minor]/[nit] follow-ups.
3. Convergence request: twenty rounds have each surfaced a further layer of the resource model. This round, please be exhaustive in a single pass — enumerate every remaining objection you can identify anywhere in the plan (resource model, parser, codecs, API, gates), so that the next revision can close the loop rather than discover one more layer. Apply the agreed severity bar strictly: a detail an implementer can settle within the recorded contracts (formula constants, scratch sizes, exact wording) is [minor]/[nit], not [major].
4. Keep the bar: AGREE only if no blocker or major findings remain and the plan is implementation-ready as written. If only minor/nit items remain, say so explicitly, list them as non-blocking follow-ups to be handled during implementation, and AGREE.

Output format (markdown):
# Codex review round 21
## Disposition check (round-20 items)
## New findings
Numbered, severity-tagged [blocker]/[major]/[minor]/[nit], each with Claim / Evidence / Recommendation.
## Non-blocking follow-ups (if any)
## Milestone and gate assessment
## Verdict
Exactly one final line: `VERDICT: REVISE` or `VERDICT: AGREE`.
