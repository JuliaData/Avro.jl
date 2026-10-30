# Single-object encoding

The single-object encoding frames one datum with the marker `0xC3 0x01` and the writer schema's
CRC-64-AVRO fingerprint:

```julia
msg = Avro.encodesingle(s, (id=1, name="alice"))

store = Avro.SchemaCache()          # fingerprint → schema (bounded, lock-protected)
Avro.register!(store, s)
value = Avro.decodesingle(msg, store)
```

[`Avro.SchemaCache`](@ref) is the built-in store: bounded, indexed by a sorted fingerprint vector, and
idempotent for structurally equal schemas. For messages using Avro single-object encoding:

```julia
for msg in messages
    value = Avro.decodesingle(msg, store)     # reader_schema= and T= resolve/type per call site
end
```

[`Avro.SchemaStore`](@ref) is the interface for custom registries. A read-only registry needs only
[`Avro.lookup`](@ref)`(store, fingerprint::UInt64; limits=Avro.Limits())`, returning an `Avro.Schema`.
Writable registries can additionally implement [`Avro.register!`](@ref). Each registry controls its
own network requests, cache bounds and lookup resources. `decodesingle` passes the caller's limits
to the registry, then validates the returned schema's fingerprint and decodes within one Avro budget.

Single-object encoding is distinct from [Confluent Schema Registry's payload-prefix framing](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format),
which uses a magic byte and a registry-assigned schema ID instead of the marker and fingerprint above. A consumer of that framing
must resolve that ID and pass the unframed payload to `Avro.decode`.

Unknown fingerprints raise [`Avro.UnknownSchemaError`](@ref); a registry may map one fingerprint to
several parsing-equivalent schemas, and ambiguity raises [`Avro.AmbiguousSchemaError`](@ref).
