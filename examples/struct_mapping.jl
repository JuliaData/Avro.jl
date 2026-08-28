# Typed decoding into your own structs, customised with StructUtils field tags.

using Avro, StructUtils

# Field tags rename Avro fields and supply defaults for reader-side evolution.
@defaults struct Event
    id::Int64 = 0
    kind::Symbol = :unknown                      # Symbols pass the admission table
    payload::Union{Missing,String} = missing &(avro=(name="body",),)
end

s = Avro.schema(Event)                           # the derived record schema uses "body"

bytes = Avro.encode(s, Event(1, :click, "hello"))

dr = Avro.DatumReader(s, Event)                  # prepared, reusable, task-shareable
ev = dr(bytes)
@assert ev.kind === :click && ev.payload == "hello"
