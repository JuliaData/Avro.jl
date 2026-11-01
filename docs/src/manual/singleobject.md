# Single-object encoding

The single-object encoding frames one datum with the marker `0xC3 0x01` and the writer schema's
CRC-64-AVRO fingerprint:

```julia
msg = Avro.encodesingle(s, (id=1, name="alice"))

store = Avro.SchemaCache()          # fingerprint → schema (lock-protected)
Avro.register!(store, s)
value = Avro.decodesingle(msg, store)
```

[`Avro.SchemaCache`](@ref) is the built-in store: a lock-protected dictionary with a configurable entry
cap, idempotent for structurally equal schemas.

`encodesingle` and `decodesingle` redo the schema work on every call. Keep a prepared
[`Avro.SingleObjectWriter`](@ref) and [`Avro.SingleObjectReader`](@ref) for a stream of messages. The reader
looks up and verifies each fingerprint once, so later messages only decode:

```julia
writer = Avro.SingleObjectWriter(s)                      # or Avro.SingleObjectWriter(s, T)
msg = writer((id=1, name="alice"))

reader = Avro.SingleObjectReader(store)                  # or (store, T); reader_schema= resolves
for msg in messages
    value = reader(msg)
end
```

A `SingleObjectReader` is safe to share across tasks. A `SingleObjectWriter` holds a reusable buffer, so it
has one owner at a time.

[`Avro.SchemaStore`](@ref) is the interface for custom registries. A read-only registry needs only
[`Avro.lookup`](@ref)`(store, fingerprint::UInt64)`, returning an `Avro.Schema`.
Writable registries can additionally implement [`Avro.register!`](@ref). Each registry controls its
own network requests and cache bounds. A reader validates the returned schema's fingerprint
before decoding under the caller's limits.

Single-object encoding is distinct from [Confluent Schema Registry's payload-prefix framing](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format),
which uses a magic byte and a registry-assigned schema ID instead of the marker and fingerprint above. A consumer of that framing
resolves the ID, then decodes from the byte after the five-byte prefix, keeping one prepared reader per schema ID:

```julia
id = ntoh(reinterpret(UInt32, msg[2:5])[1])
reader = get!(() -> Avro.DatumReader(fetchschema(id)), readers, id)
value, _ = reader(msg, 6)
```

Unknown fingerprints raise [`Avro.UnknownSchemaError`](@ref); a registry may map one fingerprint to
several parsing-equivalent schemas, and ambiguity raises [`Avro.AmbiguousSchemaError`](@ref).
