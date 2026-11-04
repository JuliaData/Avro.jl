# Precompile workload: schema parse/print/canonical/fingerprint, prepared and one-shot
# encode/decode, container round trips over the built-in codecs, Table (with a projection), Rows,
# single-object, and the JSON encoding.

using PrecompileTools: @setup_workload, @compile_workload

struct PrecompiledRow                                  # a representative plain struct for the workload
    id::Int64
    name::String
    score::Float64
    flag::Bool
end

# A value of each common field type: their typed read nodes compile here, so a record of them compiles only
# its own record node.
const PRECOMPILED_FIELDS = (Int64(1), Int32(1), 1.0, 1.0f0, true, "a", :a, UInt8[1], Date(2020, 1, 1),
    DateTime(2020, 1, 1), Time(1), UUID(UInt128(1)), Union{Missing,Int64}[missing], Union{Missing,Int32}[missing],
    Union{Missing,Float64}[missing], Union{Missing,Bool}[missing], Union{Missing,String}[missing],
    Union{Missing,Date}[missing], Union{Missing,DateTime}[missing], ["a"], Int64[1], Float64[1.0], Int32[1],
    Dict("a" => "b"), Dict("a" => Int64(1)), Dict("a" => 1.0))

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
                row isa Record
            end
        end
        foreach(identity, Rows(tobuffer(prows; schema=s), typeof(prows[1])))
        foreach(identity, Rows(tobuffer(prows; schema=s), PrecompiledRow))
        fieldtypes_, fieldvalues = Any[], Any[]
        for x in PRECOMPILED_FIELDS
            fs = schema(typeof(x))
            DatumReader(fs, typeof(x))(encode(fs, x))
            push!(fieldtypes_, typeof(x))
            push!(fieldvalues, x)
            if x isa Vector && !(x isa Vector{UInt8})
                es = schema(eltype(x))
                DatumReader(es, eltype(x))(encode(es, first(x)))
                push!(fieldtypes_, eltype(x))
                push!(fieldvalues, first(x))
            end
        end
        # A record of them: each field type's shared decoding and encoding compiles here.
        R = NamedTuple{Tuple(Symbol(:f, i) for i in eachindex(fieldtypes_)),Tuple{fieldtypes_...}}
        rs = schema(R)
        row = R(Tuple(fieldvalues))
        DatumReader(rs, R)(DatumWriter(rs, R)(row))
        # File paths, memory-mapped and streamed.
        mktempdir() do dir
            path = joinpath(dir, "workload.avro")
            write(path, prows; schema=s)
            write(path, prows; schema=s, codec=:zstandard)
            Tables.columntable(Table(path))
            for row in Rows(path)
                row isa Record
            end
            foreach(identity, Rows(path, typeof(prows[1])))
            foreach(identity, Rows(path, PrecompiledRow; mmap=false))
            write(path, (id=[Int64(1)], name=["a"]))
            write(path, [PrecompiledRow(1, "a", 1.0, true)])
        end
        Tables.columntable(Table(tobuffer(prows; schema=s); select=(:id,)))
        Tables.columntable(Table(tobuffer(prows)))           # the schema derived from the rows
        schema(typeof(prows[1]))
    end
end
