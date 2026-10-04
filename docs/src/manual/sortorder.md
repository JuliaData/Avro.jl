# Sort order

Avro defines a total order over encoded data. [`Avro.comparebytes`](@ref) compares two encoded datums
byte-lockstep — without materialising values — honouring record field `order` attributes
(`ascending`, `descending`, `ignore`); [`Avro.compare`](@ref) compares two Julia values by encoding
them under a schema:

```julia
Avro.comparebytes(a_bytes, b_bytes, s)   # -1, 0, or 1
Avro.compare(s, x, y)
```

Rules of note, matching the specification and validated against the Java implementation's vectors:

* bytes/strings/fixed compare as **unsigned** bytes (lexicographic).
* floats follow Java's `Double.compare` total order (`-0.0 < 0.0`, `NaN` greatest).
* enums compare by **symbol position** in the schema, not alphabetically.
* maps are unordered: comparing them is an error unless the field's `order` is `ignore`.
* union values compare by branch index first, then by value within the branch.

Comparison work is bounded by the comparison rule of the [limits model](limits-and-security.md).
