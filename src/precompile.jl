# Precompile workload (plan §11): schema parse/print/canonical/fingerprint, prepared and one-shot
# encode/decode, container round trips over the built-in codecs, Table (with a projection), Rows,
# single-object, and the JSON encoding. Budget: ≤ 15 s precompile, ≤ 0.5 s load.

using PrecompileTools: @setup_workload, @compile_workload

struct PrecompiledRow                                  # a representative plain struct for the workload
    id::Int64
    name::String
    score::Float64
    flag::Bool
end

@setup_workload begin
    prows = [(id=Int64(1), name="a", score=1.0, flag=true), (id=Int64(2), name="b", score=2.0, flag=false)]
    pjson = """{"type":"record","name":"PC","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"score","type":"double"},{"name":"flag","type":"boolean"}]}"""
    @compile_workload begin
        s = parseschema(pjson)
        json(s)
        canonical(s)
        fingerprint(s)
        b = encode(s, prows[1])
        v = decode(s, b)
        fromjson(s, tojson(s, v))
        dr = DatumReader(s, typeof(prows[1]))
        dr(b)
        drs = DatumReader(s, PrecompiledRow)
        drs(b)
        DatumWriter(s, PrecompiledRow)(PrecompiledRow(1, "a", 1.0, true))
        dw = DatumWriter(s, typeof(prows[1]))
        dw(prows[1])
        store = SchemaCache()
        register!(store, s)
        decodesingle(encodesingle(s, prows[1]), store)
        for c in (:null, :deflate, :zstandard, :snappy)
            Tables.columntable(Table(tobuffer(prows; schema=s, codec=c)))
            for row in Rows(tobuffer(prows; schema=s, codec=c))
                row isa Row
            end
        end
        Tables.columntable(Table(tobuffer(prows; schema=s); select=(:id,)))
        schema(typeof(prows[1]))
    end
end
