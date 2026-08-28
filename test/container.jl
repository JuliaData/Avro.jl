# a sink that fails after a fixed number of bytes (writer failure injection)
mutable struct FailIO <: IO
    const io::IOBuffer
    const failafter::Int
    written::Int
end

function FailIO(n::Int)
    return FailIO(IOBuffer(), n, 0)
end

function Base.write(f::FailIO, b::UInt8)
    f.written += 1
    f.written > f.failafter && throw(ErrorException("sink failure injected at byte $(f.written)"))
    return write(f.io, b)
end

function Base.unsafe_write(f::FailIO, p::Ptr{UInt8}, n::UInt)
    f.written += Int(n)
    f.written > f.failafter && throw(ErrorException("sink failure injected at byte $(f.written)"))
    return unsafe_write(f.io, p, n)
end

function Base.flush(f::FailIO)
    return flush(f.io)
end

struct StagingLengthMismatch <: AbstractVector{Any} end

function Base.size(::StagingLengthMismatch)
    return (2,)
end

function Base.getindex(::StagingLengthMismatch, ::Int)
    return Iterators.Stateful(Int64[1])
end

function Base.iterate(::StagingLengthMismatch, state::Int=1)
    return state == 1 ? (Iterators.Stateful(Int64[1]), 2) : nothing
end

mutable struct SizedOnce{T}
    const values::Vector{T}
    done::Bool
end

function Base.IteratorSize(::Type{<:SizedOnce})
    return Base.HasLength()
end

function Base.length(values::SizedOnce)
    return length(values.values)
end

function Base.iterate(values::SizedOnce, state::Int=1)
    values.done && return nothing
    state > length(values.values) && return nothing
    state == length(values.values) && (values.done = true)
    return (values.values[state], state + 1)
end

mutable struct OnceDict{V} <: AbstractDict{String,V}
    value::V
    done::Bool
end

function Base.length(::OnceDict)
    return 1
end

function Base.iterate(values::OnceDict, state::Int=1)
    state == 1 && !values.done || return nothing
    values.done = true
    return ("a" => values.value, 2)
end

function Base.getindex(values::OnceDict, key)
    key == "a" && return values.value
    throw(KeyError(key))
end

function Base.haskey(::OnceDict, key)
    return key == "a"
end

struct InterruptIO <: IO end

function Base.eof(::InterruptIO)
    throw(InterruptException())
end

struct NoPositionIO <: IO
    io::IOBuffer
end

function Base.eof(io::NoPositionIO)
    return eof(io.io)
end

function Base.read(io::NoPositionIO, ::Type{UInt8})
    return read(io.io, UInt8)
end

function Base.unsafe_read(io::NoPositionIO, pointer::Ptr{UInt8}, bytes::UInt)
    return unsafe_read(io.io, pointer, bytes)
end

mutable struct ReaderLifecycleIO <: IO
    const io::IOBuffer
    const closes::Base.RefValue{Int}
    const failclose::Bool
end

function Base.eof(io::ReaderLifecycleIO)
    return eof(io.io)
end

function Base.read(io::ReaderLifecycleIO, ::Type{UInt8})
    return read(io.io, UInt8)
end

function Base.unsafe_read(io::ReaderLifecycleIO, pointer::Ptr{UInt8}, bytes::UInt)
    return unsafe_read(io.io, pointer, bytes)
end

function Base.close(io::ReaderLifecycleIO)
    io.closes[] += 1
    io.failclose && throw(ErrorException("reader source close sentinel"))
    return nothing
end

struct OwnedReaderLifecycleSource
    io::ReaderLifecycleIO
end

function Avro.opensource(source::OwnedReaderLifecycleSource; mmap::Bool=true)
    return Avro.StreamSource(source.io, true)
end

@noinline function closedbytesourceweakref(bytes)
    owned = copy(bytes)
    reader = Avro.Reader(owned)
    reference = WeakRef(owned)
    close(reader)
    return reference
end

@noinline function closediosourceweakref(bytes, closes)
    io = ReaderLifecycleIO(IOBuffer(bytes), closes, false)
    reader = Avro.Reader(io)
    reference = WeakRef(io)
    close(reader)
    return reference
end

@noinline function closedmappingsourceweakref(path)
    reader = Avro.Reader(path)
    reference = WeakRef((reader.source::Avro.BytesSource).buf)
    close(reader)
    return reference
end

@noinline function writerheaderownership(schema)
    key = join(("caller", "-metadata"))
    value = UInt8[0x01, 0x02, 0x03]
    keyreference = WeakRef(key)
    valuereference = WeakRef(value)
    keypointer = pointer(key)
    valuepointer = pointer(value)
    budget = Avro.Budget(Avro.Limits(); direction=:encode, available=1 << 40)
    _, entries = Avro.writerheaderentries(
        schema, :zstandard, Dict(key => value), Avro.Limits(), budget)
    return entries, keyreference, valuereference, keypointer, valuepointer, budget
end

@enum InterruptingEnum InterruptingA
const INTERRUPTING_ENUM_CALLS = Ref(0)

function Avro.avrosymbol(::Type{InterruptingEnum}, ::InterruptingEnum)
    INTERRUPTING_ENUM_CALLS[] += 1
    INTERRUPTING_ENUM_CALLS[] == 1 && throw(InterruptException())
    return "A"
end

@noinline function abandonedwriter(io, schema)
    writer = Avro.Writer(io, schema; codec=:zstandard)
    return WeakRef(writer)
end

function stringofencodedsize(encoded::Int, byte::Char='a')
    for prefix in 1:10
        payload = encoded - prefix
        payload >= 0 && Avro.varintlength(payload) == prefix &&
            return repeat(string(byte), payload)
    end
    return error("no Avro string has encoded size $encoded")
end

function mutatefirstpayload(f::Function, bytes::Vector{UInt8})
    reader = Avro.Reader(bytes)
    start = position(reader.source)
    close(reader)
    source = Avro.BytesSource(bytes, start)
    count = Avro.sourcevarint(source)
    size = Int(Avro.sourcevarint(source))
    payloadstart = position(source)
    replacement = f(Vector(view(bytes, payloadstart:payloadstart + size - 1)))
    prefix = view(bytes, 1:start - 1)
    suffix = view(bytes, payloadstart + size:length(bytes))
    return vcat(prefix, Avro.encode(Avro.LongSchema(), count),
                Avro.encode(Avro.LongSchema(), Int64(length(replacement))), replacement, suffix)
end

function oneblockcontainer(codec::Symbol)
    io = IOBuffer()
    writer = Avro.Writer(io, Avro.NullSchema(); codec=codec)
    push!(writer, missing)
    close(writer)
    return take!(io)
end

@testset "Container files" begin
    P = Avro.parseschema
    L = Avro.Limits()
    gen = joinpath(FIXTURES, "generated")
    function readall(src; kw...)
        return Avro.Reader(r -> collect(Avro.eachdatum(r)), src; kw...)
    end
    @testset "every fixture file decodes to its Java tojson expectations" begin
        cases = Tuple{String,String}[]
        for f in sort(readdir(joinpath(gen, "roots"); join=true))
            endswith(f, ".avro") && push!(cases, (f, replace(replace(basename(f), r"-[a-z]+\.avro$" => ""), r"-fastavro$" => "")))
        end
        for f in sort(readdir(joinpath(gen, "data"); join=true))
            endswith(f, ".avro") && push!(cases, (f, replace(replace(basename(f), r"-[a-z]+\.avro$" => ""), r"-fastavro$" => "")))
        end
        @test length(cases) > 150
        checked = 0
        for (file, base) in cases
            if endswith(basename(file), "-fastavro-deflate.avro")
                err = try
                    Avro.Reader(r -> collect(Avro.eachdatum(r)), file)
                    nothing
                catch e
                    e
                end
                @test err isa Avro.CodecError && occursin("bytes after the final deflate block", err.msg)
                checked += 1
                continue
            end
            dir = dirname(file)
            jsonl = joinpath(dir, base * ".jsonl")
            isfile(jsonl) || (jsonl = joinpath(dirname(dir), "data", base * ".jsonl"))
            isfile(jsonl) || continue
            vs = Avro.Reader(file) do r
                (collect(Avro.eachdatum(r)), Avro.writerschema(r))
            end
            values, ws = vs
            expected = [Avro.fromjson(ws, line) for line in readlines(jsonl) if !isempty(line)]
            if isempty(expected) && Avro.minsize(ws) == 0
                @test !isempty(values) && all(v -> isequal(v, Avro.decode(ws, UInt8[])), values)   # avro-tools' tojson prints nothing for zero-byte datums
            elseif (occursin("-fastavro-", basename(file)) || endswith(basename(file), "-xz.avro")) && ws isa Avro.RecordSchema &&
                   any(f -> f.schema isa Avro.UnionSchema && Avro.nullablebranch(f.schema) == 0, ws.fields)
                # the committed -xz.avro data (like -fastavro-*) went through fastavro, which reselects union branches
                # fastavro rewrote Java's data and reselects union branches (enum → string, float/long → double);
                # it is an oracle only where branch identity is preserved (plan §8.5)
                @test length(values) == length(expected)
                keep = [!(f.schema isa Avro.UnionSchema && Avro.nullablebranch(f.schema) == 0) for f in ws.fields]
                @test all(all(!keep[j] || isequal(values[i][j], expected[i][j]) for j in eachindex(ws.fields)) for i in eachindex(values))
            else
                @test length(values) == length(expected)
                @test all(isequal.(values, expected))
            end
            checked += 1
        end
        @test checked > 150
    end
    @testset "apache fixtures" begin
        wlines = readlines(joinpath(FIXTURES, "apache", "weather.json"))
        for name in ("weather.avro", "weather-deflate.avro", "weather-snappy.avro", "weather-zstd.avro", "weather-sorted.avro")
            values, ws = Avro.Reader(joinpath(FIXTURES, "apache", name)) do r
                (collect(Avro.eachdatum(r)), Avro.writerschema(r))
            end
            @test length(values) == length(wlines)
            expected = [Avro.fromjson(ws, l) for l in wlines]
            if name == "weather-sorted.avro"
                @test sort!([Avro.tojson(ws, v) for v in values]) == sort!([Avro.tojson(ws, v) for v in expected])
            else
                @test all(isequal.(values, expected))
            end
        end
        r = Avro.Reader(joinpath(FIXTURES, "apache", "syncInMeta.avro"))
        @test length(collect(Avro.eachdatum(r))) >= 1                        # a sync marker embedded in the metadata parses
        close(r)
        for sub in ("simple", "withUnion")
            f = joinpath(FIXTURES, "apache", "schemas", sub, "data.avro")
            @test length(readall(f)) >= 1
        end
    end
    @testset "write → read round trips (every codec, block boundaries, metadata)" begin
        rows = [(a=Int64(i), b="row $i", c=i / 2, flag=isodd(i)) for i in 1:503]
        for codecname in Avro.codecs()
            io = Avro.tobuffer(rows; codec=codecname, block_bytes=256, metadata=Dict("who" => Vector{UInt8}("me"), "bin" => UInt8[0xff, 0x00, 0xfe]))
            r = Avro.Reader(io)
            @test Avro.codec(r) === codecname
            @test Vector{UInt8}("me") == Avro.metadata(r)["who"]
            @test Avro.metadata(r)["bin"] == UInt8[0xff, 0x00, 0xfe]                        # non-UTF-8 metadata values round-trip
            @test Avro.metadata(r)["avro.codec"] == Vector{UInt8}(String(codecname))
            @test Avro.sync(r) isa NTuple{16,UInt8}
            got = collect(Avro.eachdatum(r))
            close(r)
            @test length(got) == length(rows)
            @test all(got[i].a == rows[i].a && got[i].b == rows[i].b && got[i].c == rows[i].c && got[i].flag == rows[i].flag for i in eachindex(rows))
            blocks = Avro.Reader(rr -> collect(Avro.eachblock(rr)), Avro.tobuffer(rows; codec=codecname, block_bytes=256))
            @test sum(first, blocks) == length(rows) && length(blocks) > 3
        end
        # Exact reader-header indexes at and across growth boundaries. The two reserved Avro
        # entries make these total metadata counts 8, 16, and 33.
        for nuser in (6, 14, 31)
            usermeta = Dict("user-$(lpad(i, 2, '0'))" => UInt8[mod(i, 256)] for i in 1:nuser)
            metadata_reader = Avro.Reader(Avro.tobuffer(rows[1:1]; metadata=usermeta))
            parsed_metadata = Avro.metadata(metadata_reader)
            @test length(parsed_metadata) == nuser + 2
            @test Avro.capacity(parsed_metadata.keys) == length(parsed_metadata.keys)
            @test Avro.capacity(parsed_metadata.vals) == length(parsed_metadata.vals)
            close(metadata_reader)
        end
        # a held block's bytes stay valid after iteration advances
        io = Avro.tobuffer(rows; block_bytes=256)
        r = Avro.Reader(io)
        it = Avro.eachblock(r)
        (c1, b1), _ = iterate(it)
        rest = collect(it)
        @test !isempty(rest)
        d = Avro.Budget(L; available=1 << 40)
        Avro.addinput!(d, length(b1))
        dec = Avro.Decoder(b1, d)
        @test Avro.decode(Avro.readplan(Avro.writerschema(r)), dec).a == 1
        close(r)
        # partitions become block boundaries
        parts = Tables.partitioner([rows[1:100], rows[101:503]])
        blocks = Avro.Reader(rr -> collect(Avro.eachblock(rr)), Avro.tobuffer(parts))
        @test first.(blocks[1:1]) == [100]
        # header-only file (zero datums) reads back
        empty = Avro.tobuffer(rows[1:0])
        @test readall(empty) == []
        hugefixed = P("""{"type":"record","name":"HugeFixed","fields":[
            {"name":"id","type":"int"},
            {"name":"a","type":{"type":"fixed","name":"HugeA","size":9223372036854775807}},
            {"name":"b","type":{"type":"fixed","name":"HugeB","size":9223372036854775807}}]}""")
        hugefixedio = IOBuffer()
        hugefixedwriter = Avro.Writer(hugefixedio, hugefixed)
        close(hugefixedwriter)
        hugefixedcontainer = take!(hugefixedio)
        @test isempty(readall(hugefixedcontainer))
        hugefixedtable = Avro.Table(hugefixedcontainer; select=(:id,), ntasks=1)
        @test isempty(Tables.getcolumn(hugefixedtable, :id))
        # explicit schema and non-record roots through the Writer
        arr = P("{\"type\":\"array\",\"items\":\"string\"}")
        io2 = IOBuffer()
        w = Avro.Writer(io2, arr; sync=collect(UInt8(1):UInt8(16)))
        push!(w, ["a", "b"])
        push!(w, String[])
        pending = w.encoder.pos
        @test_throws Avro.EncodeError push!(w, ArrayLengthMismatch(1, 2))
        @test w.encoder.pos == pending
        @test_throws Avro.WriterClosedError push!(w, ["c"])
        close(w)
        @test readall(io2) == []

        io2 = IOBuffer()
        w = Avro.Writer(io2, arr; sync=collect(UInt8(1):UInt8(16)))
        push!(w, ["a", "b"])
        push!(w, String[])
        close(w)
        seekstart(io2)
        r2 = Avro.Reader(io2)
        @test Avro.sync(r2) === ntuple(i -> UInt8(i), 16)
        @test isequal(collect(Avro.eachdatum(r2)), [["a", "b"], String[]])
        close(r2)

        nested = P("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"long\"}}")
        nestedwriter = Avro.Writer(IOBuffer(), nested)
        @test_throws Avro.EncodeError push!(nestedwriter, StagingLengthMismatch())
        close(nestedwriter; abort=true)

        # OCF staging uses the same composite-depth model as binary encode/decode. Union edges are
        # transparent; arrays, maps, and records are the only nodes that increase value depth.
        stageddepth = P("""{"type":"record","name":"StagedDepth","fields":[
            {"name":"next","type":["null","StagedDepth"]}]}""")
        stagedleaf = (next=missing,)
        stagedlimits = Avro.Limits(max_depth=1)
        @test Avro.decode(stageddepth,
                          Avro.encode(stageddepth, stagedleaf; limits=stagedlimits);
                          limits=stagedlimits).next === missing
        stagedio = IOBuffer()
        stagedocf = Avro.Writer(stagedio, stageddepth; codec=:null,
                                limits=stagedlimits)
        push!(stagedocf, stagedleaf)
        close(stagedocf)
        @test only(collect(Avro.eachdatum(Avro.Reader(take!(stagedio);
                                                      limits=stagedlimits)))).next === missing
        @test length(take!(Avro.tobuffer([stagedleaf]; schema=stageddepth,
                                         codec=:null, limits=stagedlimits))) > 0
        stagedtableio = IOBuffer()
        Avro.write(stagedtableio, [stagedleaf]; schema=stageddepth,
                   codec=:null, limits=stagedlimits)
        @test only(collect(Avro.eachdatum(Avro.Reader(take!(stagedtableio);
                                                      limits=stagedlimits)))).next === missing

        rootunion = Avro.UnionSchema([Avro.NullSchema(),
                                      Avro.RecordSchema("RootUnionRecord")])
        rootlimits = Avro.Limits(max_depth=0)
        rootio = IOBuffer()
        rootwriter = Avro.Writer(rootio, rootunion; codec=:null,
                                 limits=rootlimits)
        push!(rootwriter, missing)
        close(rootwriter)
        @test only(collect(Avro.eachdatum(Avro.Reader(take!(rootio);
                                                      limits=rootlimits)))) === missing
        nestedvalue = Avro.Record(stageddepth, Any[
            Avro.Record(stageddepth, Any[missing])])
        nesteddeptherror = try
            writer = Avro.Writer(IOBuffer(), stageddepth; codec=:null,
                                 limits=stagedlimits)
            try
                push!(writer, nestedvalue)
            finally
                close(writer; abort=true)
            end
            nothing
        catch caught
            caught
        end
        @test nesteddeptherror isa Avro.LimitError
        @test nesteddeptherror.limit === :max_depth
        @test nesteddeptherror.observed == 2
        @test nesteddeptherror.value == 1

        pathrecord = P("""{"type":"record","name":"WriterPath","fields":[
            {"name":"xs","type":{"type":"array","items":{"type":"map","values":"int"}}}]}""")
        pathwriter = Avro.Writer(IOBuffer(), pathrecord)
        patherror = try
            push!(pathwriter, (xs=Any[Dict("key" => "bad")],))
            nothing
        catch err
            err
        end
        @test patherror isa Avro.EncodeError
        @test patherror.path == "\$.xs[0][\"key\"]"
        @test patherror.schema === pathrecord.fields[1].schema.items.values
        close(pathwriter; abort=true)

        stagepathrecord = P("""{"type":"record","name":"WriterStagePath","fields":[
            {"name":"xs","type":{"type":"array","items":{"type":"array","items":"int"}}}]}""")
        stagebad = (xs=Any["bad"],)
        stagewriter = Avro.Writer(IOBuffer(), stagepathrecord)
        stageerror = try
            push!(stagewriter, stagebad)
            nothing
        catch err
            err
        end
        @test stageerror isa Avro.EncodeError
        @test stageerror.path == "\$.xs[0]"
        @test stageerror.schema === stagepathrecord.fields[1].schema.items
        close(stagewriter; abort=true)

        writeerror = try
            Avro.write(IOBuffer(), [stagebad]; schema=stagepathrecord)
            nothing
        catch err
            err
        end
        @test writeerror isa Avro.EncodeError
        @test writeerror.path == "\$.xs[0]"
        @test writeerror.schema === stagepathrecord.fields[1].schema.items

        estimatebudget = Avro.Budget(Avro.Limits(); direction=:encode,
                                     available=1 << 40)
        estimateplan = Avro.writeplan(stagepathrecord; budget=estimatebudget)
        estimateencoder = Avro.Encoder(estimatebudget)
        estimateerror = try
            Avro.withencoderroot!(estimateencoder, stagepathrecord) do
                Avro.estimaterootvalue(estimateplan, stagebad, estimatebudget,
                                       estimateencoder)
            end
            nothing
        catch err
            err
        end
        @test estimateerror isa Avro.EncodeError
        @test estimateerror.path == "\$.xs[0]"
        @test estimateerror.schema === stagepathrecord.fields[1].schema.items
        Avro.close!(estimatebudget)

        missingpathrecord = P("""{"type":"record","name":"WriterMissingPath","fields":[
            {"name":"xs","type":{"type":"array","items":"int"}}]}""")
        missingbad = (bad=Int32[1],)
        missingwriter = Avro.Writer(IOBuffer(), missingpathrecord)
        missingerror = try
            push!(missingwriter, missingbad)
            nothing
        catch err
            err
        end
        @test missingerror isa Avro.EncodeError
        @test missingerror.path == "\$.xs"
        @test missingerror.schema === missingpathrecord.fields[1].schema
        close(missingwriter; abort=true)
        missingwriteerror = try
            Avro.write(IOBuffer(), [missingbad]; schema=missingpathrecord)
            nothing
        catch err
            err
        end
        @test missingwriteerror isa Avro.EncodeError
        @test missingwriteerror.path == "\$.xs"
        @test missingwriteerror.schema === missingpathrecord.fields[1].schema

        boundedfixed = Avro.FixedSchema("ContainerFixed", 2)
        fixedat = Avro.Limits(max_bytes=2, max_datum_bytes=2)
        fixedover = Avro.Limits(max_bytes=1, max_datum_bytes=2)
        fixedio = IOBuffer()
        fixedwriter = Avro.Writer(fixedio, boundedfixed; limits=fixedat)
        push!(fixedwriter, UInt8[1, 2])
        close(fixedwriter)
        fixedcontainer = take!(fixedio)
        @test only(collect(Avro.eachdatum(Avro.Reader(fixedcontainer; limits=fixedat)))) ==
              Avro.Fixed(boundedfixed, UInt8[1, 2])
        @test_throws Avro.LimitError collect(Avro.eachdatum(Avro.Reader(fixedcontainer;
                                                                        limits=fixedover)))
        rejectedfixed = Avro.Writer(IOBuffer(), boundedfixed; limits=fixedover)
        @test_throws Avro.LimitError push!(rejectedfixed, UInt8[1, 2])
        close(rejectedfixed; abort=true)

        projectedfixed = P("""{"type":"record","name":"ProjectedFixed","fields":[
            {"name":"f","type":{"type":"fixed","name":"ProjectedF","size":2}},
            {"name":"x","type":"int"}]}""")
        projectedat = Avro.Limits(max_bytes=2, max_datum_bytes=3)
        projectedover = Avro.Limits(max_bytes=1, max_datum_bytes=3)
        projectedio = IOBuffer()
        projectedwriter = Avro.Writer(projectedio, projectedfixed; limits=projectedat)
        push!(projectedwriter, (f=UInt8[1, 2], x=Int32(3)))
        close(projectedwriter)
        @test_throws Avro.LimitError Avro.Table(take!(projectedio); select=[:x],
                                                limits=projectedover, ntasks=1)

        boundedrecord = P("""{"type":"record","name":"BoundedStaticDatum","fields":[
            {"name":"x","type":"string"}]}""")
        boundedcontainer = Avro.tobuffer([(x="abcdefghij",)]; schema=boundedrecord)
        boundedlimits = Avro.Limits(max_datum_bytes=4, max_bytes=64)
        for mode in (:strict, :fast), ntasks in (1, 2)
            boundederror = try
                Avro.Table(boundedcontainer; limits=boundedlimits, validate=mode,
                           ntasks=ntasks)
                nothing
            catch err
                err
            end
            @test boundederror isa Avro.LimitError
            @test boundederror.limit == boundederror.keyword == :max_datum_bytes
        end

        leafarray = Avro.ArraySchema(Avro.LongSchema())
        leafarrayio = IOBuffer()
        leafarraywriter = Avro.Writer(leafarrayio, leafarray; block_bytes=1)
        push!(leafarraywriter, SizedOnce(Int64[7], false))
        close(leafarraywriter)
        seekstart(leafarrayio)
        @test readall(leafarrayio) == [[Int64(7)]]

        leafmap = Avro.MapSchema(Avro.LongSchema())
        leafmapio = IOBuffer()
        leafmapwriter = Avro.Writer(leafmapio, leafmap; block_bytes=1)
        push!(leafmapwriter, OnceDict(Int64(7), false))
        close(leafmapwriter)
        seekstart(leafmapio)
        @test readall(leafmapio)[1]["a"] == 7

        INTERRUPTING_ENUM_CALLS[] = 0
        enumunion = Avro.UnionSchema([Avro.EnumSchema("Interrupting", ["A"]), Avro.StringSchema()])
        enumwriter = Avro.Writer(IOBuffer(), enumunion; block_bytes=1)
        @test_throws InterruptException push!(enumwriter, InterruptingA)
        @test INTERRUPTING_ENUM_CALLS[] == 1
        close(enumwriter; abort=true)

        charschema = Avro.StringSchema()
        for (char, outputbytes) in (('a', 25), ('é', 26), ('😀', 28))
            charlimits = Avro.Limits(max_block_output_bytes=outputbytes)
            chario = IOBuffer()
            charwriter = Avro.Writer(chario, charschema; limits=charlimits)
            push!(charwriter, char)
            close(charwriter)
            @test collect(Avro.eachdatum(Avro.Reader(take!(chario); limits=charlimits))) == [string(char)]
        end

        widebytes = P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":1000000000}")
        wideplan = Avro.writeplan(widebytes)
        estimatebudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        positiveestimate = Avro.estimatevalue(wideplan, Avro.WideDecimal(big(1), 0), estimatebudget)
        negativeestimate = Avro.estimatevalue(wideplan, Avro.WideDecimal(big(-1), 0), estimatebudget)
        @test positiveestimate.retained == Avro.widedecimalbytes(1)
        @test positiveestimate.peakextra == 0
        @test negativeestimate.retained == Avro.widedecimalbytes(1)
        @test negativeestimate.peakextra == Avro.bytesbytes(1)

        peaklimits = Avro.Limits(max_total_bytes=512 << 20, max_block_bytes=64 << 20,
                                 max_block_output_bytes=256 << 20)
        peakschema = Avro.ArraySchema(Avro.ArraySchema(Avro.IntSchema()))
        peakvalue = [Int32[] for _ in 1:1500]
        peakvalue[1499] = fill(Int32(1), 100_000)
        writebudget = Avro.Budget(peaklimits; direction=:encode, available=1 << 40)
        writeplan = Avro.writeplan(peakschema; budget=writebudget)
        outputestimate = Avro.estimaterootvalue(writeplan, peakvalue, writebudget)
        peakbytes = Avro.encode(peakschema, peakvalue; limits=peaklimits)
        readbudget = Avro.Budget(peaklimits; available=1 << 40)
        Avro.addinput!(readbudget, length(peakbytes))
        readplan = Avro.readplan(peakschema; budget=readbudget)
        baseline = readbudget.reserved
        Avro.decode(readplan, Avro.Decoder(peakbytes, readbudget))
        @test outputestimate[1] + outputestimate[2] >= readbudget.peak - baseline

        mappeakschema = Avro.MapSchema(Avro.ArraySchema(Avro.IntSchema()))
        mapkeys = Any["k" * lpad(string(i), 4, "0") for i in 1:1500]
        mapvalues = Any[Int32[] for _ in 1:1500]
        mapvalues[1499] = fill(Int32(1), 100_000)
        mappeakvalue = Avro.StagedWriterMap(mapkeys, mapvalues)
        mapwritebudget = Avro.Budget(peaklimits; direction=:encode, available=1 << 40)
        mapwriteplan = Avro.writeplan(mappeakschema; budget=mapwritebudget)
        mapestimate = Avro.estimaterootvalue(mapwriteplan, mappeakvalue, mapwritebudget)
        mapbytes = Avro.encode(mappeakschema, mappeakvalue; limits=peaklimits)
        mapreadbudget = Avro.Budget(peaklimits; available=1 << 40)
        Avro.addinput!(mapreadbudget, length(mapbytes))
        mapreadplan = Avro.readplan(mappeakschema; budget=mapreadbudget)
        mapbaseline = mapreadbudget.reserved
        Avro.decode(mapreadplan, Avro.Decoder(mapbytes, mapreadbudget))
        @test mapestimate[1] + mapestimate[2] >= mapreadbudget.peak - mapbaseline
    end
    @testset "sources: path, mmap=false stream, IO, bytes" begin
        rows = [(x=Int64(i),) for i in 1:100]
        buffered = Avro.tobuffer(rows)
        buffered_reader = Avro.Reader(buffered)
        @test buffered_reader.source.buf === buffered.data
        @test buffered_reader.source.stop == buffered.size
        @test [v.x for v in Avro.eachdatum(buffered_reader)] == collect(1:100)
        close(buffered_reader)
        path = joinpath(mktempdir(), "t.avro")
        Avro.write(path, rows; codec=:deflate, block_bytes=64)
        expected = collect(1:100)
        @test [v.x for v in readall(path)] == expected
        @test [v.x for v in readall(path; mmap=false)] == expected
        open(path) do io
            @test [v.x for v in readall(io)] == expected
        end
        @test [v.x for v in readall(read(path))] == expected
        got = open(io -> [v.x for v in readall(io)], path)
        @test got == expected

        sourcebytes = Vector{UInt8}(buffered.data[1:buffered.size])
        weakbytes = closedbytesourceweakref(copy(sourcebytes))
        iocloses = Ref(0)
        weakio = closediosourceweakref(copy(sourcebytes), iocloses)
        weakmapping = closedmappingsourceweakref(path)
        GC.gc(true)
        GC.gc(true)
        @test weakbytes.value === nothing
        @test weakio.value === nothing
        @test weakmapping.value === nothing
        @test iocloses[] == 0

        ownedcloses = Ref(0)
        closethrowing = ReaderLifecycleIO(IOBuffer(copy(sourcebytes)),
                                          ownedcloses, true)
        closereader = Avro.Reader(OwnedReaderLifecycleSource(closethrowing))
        closeerror = try
            close(closereader)
            nothing
        catch err
            err
        end
        @test closeerror isa ErrorException
        @test closeerror === nothing || closeerror.msg == "reader source close sentinel"
        @test closereader.source === nothing
        @test closereader.budget.pending == 0
        @test ownedcloses[] == 1
        @test close(closereader) === nothing

        badheader = UInt8[collect(b"Obj\x01")...]
        append!(badheader, Avro.encode(Avro.LongSchema(), Int64(2)))
        for (key, value) in (("avro.schema", UInt8['{']), ("padding", zeros(UInt8, 2 << 20)))
            append!(badheader, Avro.encode(Avro.LongSchema(), Int64(sizeof(key))))
            append!(badheader, codeunits(key))
            append!(badheader, Avro.encode(Avro.LongSchema(), Int64(length(value))))
            append!(badheader, value)
        end
        append!(badheader, Avro.encode(Avro.LongSchema(), Int64(0)))
        append!(badheader, zeros(UInt8, 16))
        constructorcloses = Ref(0)
        constructorio = ReaderLifecycleIO(IOBuffer(badheader), constructorcloses, true)
        guardbefore = @atomic Avro.GUARD.pending
        constructorerror = try
            Avro.Reader(OwnedReaderLifecycleSource(constructorio))
            nothing
        catch err
            err
        end
        @test constructorerror isa Avro.SchemaError
        @test constructorcloses[] == 1
        @test (@atomic Avro.GUARD.pending) == guardbefore
    end
    @testset "legacy 1.x files and decimal byte order" begin
        legacy_schema_json = """{"type":"record","name":"LegacyFixedOrdinals","fields":[
            {"name":"plain","type":"long"},
            {"name":"first","type":{"type":"fixed","size":1}},
            {"name":"nested","type":{"type":"array","items":{"type":"fixed","size":2}}}]}
            """
        function legacyvarint(n)
            return Avro.encode(P("\"long\""), Int64(n))
        end

        function legacyentry(key::String, value::Vector{UInt8})
            return vcat(legacyvarint(sizeof(key)), Vector{UInt8}(key),
                        legacyvarint(length(value)), value)
        end
        legacy_header = vcat(collect(b"Obj\x01"), legacyvarint(2),
            legacyentry("avro.schema", Vector{UInt8}(legacy_schema_json)),
            legacyentry("avro.codec", Vector{UInt8}("null")), legacyvarint(0), zeros(UInt8, 16))
        @test_throws Avro.SchemaError Avro.Reader(legacy_header)
        legacy_schema = Avro.Reader(Avro.writerschema, legacy_header; legacy=:avrojl1)
        @test Avro.fullname(legacy_schema.fields[2].schema) == "_avrojl1_fixed_1"
        @test Avro.fullname(legacy_schema.fields[3].schema.items) == "_avrojl1_fixed_2"

        leg = joinpath(gen, "legacy1x")
        @test_throws Avro.SchemaError readall(joinpath(leg, "avrojl112-null.avro"))               # 1.x wrote nameless fixed schemas
        @test_throws Avro.SchemaError readall(joinpath(leg, "avrojl112-zstd.avro"))
        vals = @test_logs (:warn, r"legacy") match_mode=:any readall(joinpath(leg, "avrojl112-null.avro"); legacy=:avrojl1, decimal_byteorder=:little)
        @test length(vals) == 2 && vals[1].a == 1 && vals[1].b == "x" && vals[2].b == "yy"
        @test vals[1].dec == Avro.Decimal(12345, 2) && vals[2].dec == Avro.Decimal(-123, 2)
        @test vals[1].u == UUID(0x123e4567e89b12d3a456426614174000)
        zvals = readall(joinpath(leg, "avrojl112-zstd.avro"); legacy=:avrojl1, decimal_byteorder=:little)
        @test length(zvals) == 2 && zvals[1].dec == Avro.Decimal(12345, 2)
        e = try; readall(joinpath(leg, "avrojl112-deflate.avro"); legacy=:avrojl1); nothing; catch err; err; end
        @test e isa Avro.DataError && occursin("precision", e.msg)                                # a :big misread of 1.x decimals fails validation rather than yielding garbage
    end
    @testset "truncation, corruption, crafted headers" begin
        defaultcodec = IOBuffer()
        write(defaultcodec, b"Obj\x01")
        write(defaultcodec, Avro.encode(Avro.LongSchema(), Int64(1)))
        write(defaultcodec, Avro.encode(Avro.LongSchema(), Int64(sizeof("avro.schema"))))
        write(defaultcodec, codeunits("avro.schema"))
        write(defaultcodec, Avro.encode(Avro.LongSchema(), Int64(sizeof("\"null\""))))
        write(defaultcodec, codeunits("\"null\""))
        write(defaultcodec, Avro.encode(Avro.LongSchema(), Int64(0)))
        write(defaultcodec, zeros(UInt8, 16))
        defaultreader = Avro.Reader(take!(defaultcodec))
        @test Avro.codec(defaultreader) === :null
        @test defaultreader.budget.reserved == 4073
        close(defaultreader)

        rows = [(x=Int64(i),) for i in 1:10]
        buf = take!(Avro.tobuffer(rows; sync=collect(UInt8(17):UInt8(32))))
        sync = collect(UInt8(17):UInt8(32))
        headerend = findfirst(i -> buf[i:i + 15] == sync, 1:length(buf) - 15) + 15   # the last byte of the header sync
        @test headerend > 20
        for cut in (1, 3, headerend - 1, headerend + 1, length(buf) - 1)
            @test_throws Avro.DataError readall(buf[1:cut])
        end
        bad = copy(buf)
        bad[end] ⊻= 0x01                                                                          # the final sync byte
        e = try; readall(bad); nothing; catch err; err; end
        @test e isa Avro.DataError && occursin("sync", e.msg)
        badmagic = copy(buf)
        badmagic[1] = UInt8('X')
        @test_throws Avro.DataError readall(badmagic)
        header = buf[1:headerend]
        function varint(n)
            return Avro.encode(P("\"long\""), n)
        end
        @test_throws Avro.DataError readall(vcat(header, varint(-1)))                             # negative count
        @test_throws Avro.DataError readall(vcat(header, varint(1), varint(-2)))                  # negative size
        @test_throws Avro.DataError readall(vcat(header, varint(typemin(Int64))))
        @test readall(vcat(header, varint(0), varint(0), sync)) == []                             # a zero-count block is valid
        zero_with_payload = vcat(header, varint(0), varint(1), UInt8[0x00], sync)
        @test_throws Avro.DataError Avro.Reader(r -> collect(Avro.eachblock(r)), zero_with_payload)
        @test_throws Avro.DataError readall(zero_with_payload)
        zerorows = Avro.Rows(zero_with_payload)
        @test_throws Avro.DataError collect(zerorows)
        close(zerorows)
        @test readall(zero_with_payload; validate=:fast) == []
        trailing = vcat(header, varint(1), varint(length(Avro.encode(P("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"), (x=Int64(7),))) + 1),
                        Avro.encode(P("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"), (x=Int64(7),)), UInt8[0x00], sync)
        lv = @test_logs (:warn, r"legacy") match_mode=:any Avro.Reader(r -> collect(Avro.eachdatum(r)), trailing; legacy=:avrojl1)
        @test length(lv) == 1 && lv[1].x == 7                                                     # the null-codec trailing tolerance
        toobig = vcat(header, varint(1), varint(Int64(L.max_block_bytes) + 1))
        @test_throws Avro.LimitError readall(toobig)
        # a block that declares more datums than it holds, and one with trailing bytes
        one = Avro.encode(P("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"), (x=Int64(7),))
        @test_throws Avro.DataError readall(vcat(header, varint(2), varint(length(one)), one, sync))
        @test_throws Avro.DataError readall(vcat(header, varint(1), varint(length(one) + 1), one, UInt8[0x00], sync))
        # no avro.schema
        noschema = vcat(collect(b"Obj\x01"), varint(0), collect(UInt8(1):UInt8(16)))
        @test_throws Avro.DataError readall(noschema)
        key = Vector{UInt8}("avro.schema")
        value = Vector{UInt8}("\"null\"")
        pair = vcat(varint(length(key)), key, varint(length(value)), value)
        function sizedmetadata(n)
            return vcat(collect(b"Obj\x01"), varint(-1), varint(n), pair,
                        varint(0), sync)
        end
        @test readall(sizedmetadata(length(pair))) == []
        @test readall(NoPositionIO(IOBuffer(sizedmetadata(length(pair))))) == []
        for declared in (0, length(pair) - 1, length(pair) + 1)
            @test_throws Avro.DataError readall(sizedmetadata(declared))
        end
        negativemetadata = vcat(collect(b"Obj\x01"), varint(-1), varint(-1))
        vectornegative = try
            Avro.Reader(negativemetadata)
            nothing
        catch err
            err
        end
        streamnegative = try
            Avro.Reader(NoPositionIO(IOBuffer(negativemetadata)))
            nothing
        catch err
            err
        end
        @test vectornegative isa Avro.DataError
        @test streamnegative isa Avro.DataError
        if vectornegative isa Avro.DataError && streamnegative isa Avro.DataError
            @test vectornegative.pos == streamnegative.pos
        end
        vectorerror = try
            readall(sizedmetadata(length(pair) - 1))
            nothing
        catch err
            err
        end
        streamerror = try
            readall(NoPositionIO(IOBuffer(sizedmetadata(length(pair) - 1))))
            nothing
        catch err
            err
        end
        @test streamerror isa Avro.DataError && vectorerror isa Avro.DataError
        @test streamerror.pos == vectorerror.pos > 0
        nullio = IOBuffer()
        nullwriter = Avro.Writer(nullio, Avro.NullSchema(); sync=zeros(UInt8, 16))
        close(nullwriter)
        hugeblock = take!(nullio)
        append!(hugeblock, Avro.encode(Avro.LongSchema(), typemax(Int64)))
        append!(hugeblock, Avro.encode(Avro.LongSchema(), Int64(0)))
        append!(hugeblock, zeros(UInt8, 16))
        hugelimits = Avro.Limits(max_block_count=typemax(Int),
                                 max_rows=typemax(Int),
                                 max_total_values=typemax(Int),
                                 max_values_per_byte=typemax(Int))
        hugeerr = try
            Avro.Reader(r -> collect(Avro.eachblock(r)), hugeblock;
                        limits=hugelimits)
            nothing
        catch caught
            caught
        end
        @test hugeerr isa Avro.LimitError
        @test hugeerr.limit === :max_total_values
        @test hugeerr.observed === typemax(Int)

        denseocfschema = Avro.UnionSchema([Avro.ArraySchema(Avro.NullSchema()),
                                           Avro.BytesSchema()])
        denseheaderio = IOBuffer()
        densewriter = Avro.Writer(denseheaderio, denseocfschema;
                                  sync=zeros(UInt8, 16))
        close(densewriter)
        denseheader = take!(denseheaderio)
        firstcount = 500
        secondbytes = 1000
        densepayload = UInt8[0x00]
        append!(densepayload, Avro.encode(Avro.LongSchema(), Int64(-firstcount)))
        append!(densepayload, UInt8[0x00, 0x00])
        push!(densepayload, 0x02)
        append!(densepayload, Avro.encode(Avro.LongSchema(), Int64(secondbytes)))
        append!(densepayload, zeros(UInt8, secondbytes))
        denseocf = copy(denseheader)
        append!(denseocf, Avro.encode(Avro.LongSchema(), Int64(2)))
        append!(denseocf, Avro.encode(Avro.LongSchema(), Int64(length(densepayload))))
        append!(denseocf, densepayload)
        append!(denseocf, zeros(UInt8, 16))
        denselimits = Avro.Limits(max_values_per_byte=1, work_allowance=0)
        observed = Int[]
        for mode in (:strict, :fast)
            reader = Avro.Reader(denseocf; limits=denselimits, validate=mode)
            err = try
                iterate(Avro.eachdatum(reader))
                nothing
            catch caught
                caught
            end
            @test err isa Avro.LimitError
            @test err.limit == :max_values_per_byte
            push!(observed, reader.budget.values)
            close(reader)
        end
        @test observed[1] == observed[2]

        staticfields = ["{\"name\":\"n$i\",\"type\":\"null\"}" for i in 1:20]
        append!(staticfields, ["{\"name\":\"s\",\"type\":\"string\"}",
                               "{\"name\":\"b\",\"type\":\"boolean\"}"])
        staticschema = P("{\"type\":\"record\",\"name\":\"DenseStaticBlock\",\"fields\":[" *
                         join(staticfields, ",") * "]}")
        staticnames = Tuple(vcat([Symbol("n$i") for i in 1:20], [:s, :b]))
        function staticvalue(text, flag)
            return NamedTuple{staticnames}(
                Tuple(vcat(fill(missing, 20), Any[text, flag])))
        end
        staticio = IOBuffer()
        staticwriter = Avro.Writer(staticio, staticschema; codec=:null,
                                   sync=zeros(UInt8, 16))
        push!(staticwriter, staticvalue("x"^1000, true))
        flush(staticwriter)
        for _ in 1:10
            push!(staticwriter, staticvalue("", true))
        end
        close(staticwriter)
        staticgood = take!(staticio)
        staticbad = copy(staticgood)
        staticbad[end - 16] = 0x02
        staticlimits = Avro.Limits(max_values_per_byte=1, work_allowance=50,
                                   max_total_bytes=1 << 30,
                                   max_block_bytes=1 << 20,
                                   max_block_output_bytes=1 << 20)
        staticcalls = (
            bytes -> Avro.Table(bytes; limits=staticlimits, validate=:strict,
                                ntasks=1),
            bytes -> Avro.Table(IOBuffer(bytes); limits=staticlimits,
                                validate=:fast, ntasks=2),
            bytes -> Avro.Rows(rows -> collect(rows), bytes; limits=staticlimits,
                               validate=:strict),
            bytes -> Avro.Reader(r -> collect(Avro.eachdatum(r)), bytes;
                                 limits=staticlimits, validate=:fast),
            bytes -> Avro.Reader(r -> collect(Avro.eachblock(r)), IOBuffer(bytes);
                                 limits=staticlimits, validate=:strict),
        )
        for call in staticcalls, bytes in (staticgood, staticbad)
            staticerror = try
                call(bytes)
                nothing
            catch caught
                caught
            end
            @test staticerror isa Avro.LimitError
            @test staticerror.limit == :max_values_per_byte
            @test staticerror.observed == 231
        end

        function encodedpair(k, v)
            return vcat(varint(sizeof(k)), Vector{UInt8}(k), varint(length(v)), v)
        end
        metadata_pairs = Vector{UInt8}[encodedpair("avro.schema", value)]
        for i in 1:64
            push!(metadata_pairs, encodedpair("key$(lpad(i, 4, '0'))", UInt8[]))
        end
        push!(metadata_pairs, encodedpair("key0001", UInt8[]))
        duplicate_header = vcat(collect(b"Obj\x01"), varint(length(metadata_pairs)),
                                reduce(vcat, metadata_pairs), varint(0), sync)
        comparison_limits = Avro.Limits(max_compare_bytes_per_byte=0, work_allowance=100)
        duplicate_error = try
            Avro.Reader(duplicate_header; limits=comparison_limits)
            nothing
        catch err
            err
        end
        @test duplicate_error isa Avro.LimitError && duplicate_error.limit == :max_compare_bytes_per_byte
    end
    @testset "writer failure injection, poisoning, atomic paths" begin
        rows = [(x=Int64(i),) for i in 1:10]
        s = Avro.schema(typeof(rows[1]))
        # a sink that fails after N bytes
        io = FailIO(200)
        w = Avro.Writer(io, s)
        e = try
            for r in rows
                push!(w, r)
                flush(w)
            end
            nothing
        catch err
            err
        end
        @test e !== nothing
        @test_throws Avro.WriterClosedError push!(w, rows[1])
        close(w)
        @test_throws Avro.WriterClosedError push!(w, rows[1])
        # the first rejected datum poisons the writer and close discards the pending block
        io2 = IOBuffer()
        w2 = Avro.Writer(io2, s)
        push!(w2, (x=Int64(1),))
        original = try
            push!(w2, (x="nope",))
            nothing
        catch err
            err
        end
        @test original isa Avro.EncodeError
        followup = try
            push!(w2, (x=Int64(2),))
            nothing
        catch err
            err
        end
        @test followup isa Avro.WriterClosedError
        if followup isa Avro.WriterClosedError
            @test followup.cause === original
        end
        close(w2)
        @test isopen(io2)
        @test readall(io2) == []
        afterclose = try
            push!(w2, (x=Int64(3),))
            nothing
        catch err
            err
        end
        @test afterclose isa Avro.WriterClosedError
        if afterclose isa Avro.WriterClosedError
            @test afterclose.cause === original
        end

        # The cached aligned path replays only failures through diagnostic frames.
        enumrecord = P("""{"type":"record","name":"FastFailure","fields":[
            {"name":"symbol","type":{"type":"enum","name":"FastSymbol",
             "symbols":["red","blue"]}}]}""")
        enumwriter = Avro.Writer(IOBuffer(), enumrecord)
        push!(enumwriter, (symbol=:red,))
        enumpending = enumwriter.encoder.pos
        enumerror = try
            push!(enumwriter, (symbol=:green,))
            nothing
        catch err
            err
        end
        @test enumerror isa Avro.EncodeError
        if enumerror isa Avro.EncodeError
            @test enumerror.path == "\$.symbol"
            @test enumerror.schema === enumrecord.fields[1].schema
        end
        @test enumwriter.encoder.pos == enumpending
        close(enumwriter)

        limitio = IOBuffer()
        limitwriter = Avro.Writer(limitio, Avro.NullSchema(); limits=Avro.Limits(max_rows=0))
        limitcause = try
            push!(limitwriter, missing)
            nothing
        catch err
            err
        end
        @test limitcause isa Avro.LimitError
        if limitcause isa Avro.LimitError
            @test limitcause.limit == :max_rows
        end
        limitfollowup = try
            push!(limitwriter, missing)
            nothing
        catch err
            err
        end
        @test limitfollowup isa Avro.WriterClosedError
        if limitfollowup isa Avro.WriterClosedError
            @test limitfollowup.cause === limitcause
        end
        close(limitwriter)
        @test isopen(limitio)
        @test readall(limitio) == []
        close(limitwriter)
        pending0 = @atomic Avro.GUARD.pending
        abandoned_io = IOBuffer()
        abandoned = abandonedwriter(abandoned_io, s)
        GC.gc(true)
        GC.gc(true)
        @test abandoned.value === nothing
        @test (@atomic Avro.GUARD.pending) == pending0
        @test isopen(abandoned_io)
        # atomic path: the destination is untouched until close, replaced on close, kept on abort
        dir = mktempdir()
        dest = joinpath(dir, "out.avro")
        Base.write(dest, "old")
        w3 = Avro.Writer(dest, s)
        push!(w3, (x=Int64(1),))
        @test read(dest, String) == "old"
        close(w3)
        @test [v.x for v in readall(dest)] == [1]
        w4 = Avro.Writer(dest, s)
        push!(w4, (x=Int64(9),))
        close(w4; abort=true)
        @test [v.x for v in readall(dest)] == [1]
        @test length(readdir(dir)) == 1                                                          # the temp file is gone

        constructionschema = Avro.ArraySchema(Avro.IntSchema())
        constructionprobe = Avro.Writer(IOBuffer(), constructionschema)
        constructioncap = constructionprobe.budget.resolution_work - 1
        close(constructionprobe; abort=true)
        constructionlimits = Avro.Limits(max_resolution_work=constructioncap)
        for constructionatomic in (true, false)
            constructionpath = joinpath(dir, "constructor-$(constructionatomic).avro")
            Base.write(constructionpath, "old")
            constructionerror = try
                Avro.Writer(constructionpath, constructionschema;
                            atomic=constructionatomic, limits=constructionlimits)
                nothing
            catch err
                err
            end
            @test constructionerror isa Avro.LimitError
            @test read(constructionpath, String) == (constructionatomic ? "old" : "")
            @test sort(readdir(dir)) == sort(["out.avro", "constructor-$(constructionatomic).avro"])
            open(constructionpath, "a") do reopened
                Base.write(reopened, "closed")
            end
            rm(constructionpath)
        end

        cleanupguard = @atomic Avro.GUARD.pending
        cleanupwriter = Avro.Writer(dest, s)
        cleanupcause = try
            push!(cleanupwriter, (x="bad",))
            nothing
        catch err
            err
        end
        cleanuptemp = cleanupwriter.temppath
        close(cleanupwriter.sink)
        rm(cleanuptemp)
        mkdir(cleanuptemp)
        Base.write(joinpath(cleanuptemp, "keep"), "make non-empty")
        try
            cleanuperror = try
                close(cleanupwriter)
                nothing
            catch err
                err
            end
            @test cleanuperror isa Exception
            @test isdir(cleanuptemp)
            @test cleanupwriter.budget.pending == 0
            @test (@atomic Avro.GUARD.pending) == cleanupguard
            cleanupfollowup = try
                push!(cleanupwriter, (x=Int64(2),))
                nothing
            catch err
                err
            end
            @test cleanupfollowup isa Avro.WriterClosedError
            if cleanupfollowup isa Avro.WriterClosedError
                @test cleanupfollowup.cause === cleanupcause
            end
        finally
            rm(cleanuptemp; recursive=true, force=true)
        end

        blockcap = 1 << 20
        blocklimits = Avro.Limits(max_block_bytes=blockcap)
        exactdatum = stringofencodedsize(blockcap)
        oversizeddatum = stringofencodedsize(blockcap + 1)
        firstdatum = stringofencodedsize(blockcap ÷ 2, 'a')
        seconddatum = stringofencodedsize(blockcap - blockcap ÷ 2 + 1, 'b')
        for blockcodec in Avro.codecs()
            exactio = IOBuffer()
            exactwriter = Avro.Writer(exactio, Avro.StringSchema(); codec=blockcodec,
                                      block_bytes=blockcap, limits=blocklimits)
            push!(exactwriter, exactdatum)
            close(exactwriter)
            @test readall(take!(exactio); limits=blocklimits) == [exactdatum]

            oversizedio = IOBuffer()
            oversizedwriter = Avro.Writer(oversizedio, Avro.StringSchema(); codec=blockcodec,
                                          block_bytes=blockcap, limits=blocklimits)
            oversizederror = try
                push!(oversizedwriter, oversizeddatum)
                nothing
            catch err
                err
            end
            @test oversizederror isa Avro.LimitError
            if oversizederror isa Avro.LimitError
                @test oversizederror.limit == :max_block_bytes
                @test oversizederror.observed == blockcap + 1
            end
            close(oversizedwriter)

            splitio = IOBuffer()
            splitwriter = Avro.Writer(splitio, Avro.StringSchema(); codec=blockcodec,
                                      block_bytes=blockcap, limits=blocklimits)
            push!(splitwriter, firstdatum)
            push!(splitwriter, seconddatum)
            close(splitwriter)
            splitbytes = take!(splitio)
            @test readall(splitbytes; limits=blocklimits) == [firstdatum, seconddatum]
            @test Avro.inspect(splitbytes; limits=blocklimits).blocks == 2
        end
        # non-atomic in-place and fsync
        dest2 = joinpath(dir, "out2.avro")
        w5 = Avro.Writer(dest2, s; atomic=false, fsync=true)
        push!(w5, (x=Int64(4),))
        close(w5)
        @test [v.x for v in readall(dest2)] == [4]
        # reserved metadata and option validation
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; metadata=Dict("avro.codec" => UInt8[]))
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; sync=UInt8[1, 2])
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; block_bytes=0)
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; codec=:snappy, level=3)
        @test_throws Avro.LimitError Avro.Writer(IOBuffer(), s; codec=:zstandard, level=22)       # ≈ 834 MB workspace > the 256 MiB ceiling
        headerentries, metadatakeyref, metadatavalueref, metadatakeypointer,
            metadatavaluepointer, headerbudget = writerheaderownership(s)
        retainedentry = only(entry for entry in headerentries if entry[1] == "caller-metadata")
        @test pointer(retainedentry[1]) != metadatakeypointer
        @test pointer(retainedentry[2]) != metadatavaluepointer
        @test headerentries[2][2] == Vector{UInt8}("zstandard")
        GC.gc(true)
        GC.gc(true)
        @test metadatakeyref.value === nothing
        @test metadatavalueref.value === nothing
        Avro.close!(headerbudget)
        rep = P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"bad-name\",\"type\":\"int\"}]}"; allow_invalid_names=true)
        @test_throws ArgumentError Avro.Writer(IOBuffer(), rep)
        @test Avro.Writer(IOBuffer(), rep; allow_invalid_names=true) isa Avro.Writer
    end
    @testset "the million-empty-string block and output-estimate caps" begin
        arr = P("{\"type\":\"array\",\"items\":\"string\"}")
        io = IOBuffer()
        w = Avro.Writer(io, arr)
        push!(w, fill("", 1_000_000))
        close(w)
        seekstart(io)
        got = readall(io)
        @test length(got) == 1 && length(got[1]) == 1_000_000 && all(isempty, got[1])
        w2 = Avro.Writer(IOBuffer(), arr)
        @test_throws Avro.LimitError push!(w2, fill("", 4_000_000))                               # ≈ 96 MiB of reader-side slots > 64 MiB
        close(w2; abort=true)
        raised = Avro.Limits(max_block_output_bytes=128 << 20)
        io3 = IOBuffer()
        w3 = Avro.Writer(io3, arr; limits=raised)
        push!(w3, fill("", 4_000_000))
        close(w3)
        seekstart(io3)
        @test_throws Avro.LimitError readall(io3)                                                 # and the default reader refuses it
        seekstart(io3)
        @test length(Avro.Reader(r -> collect(Avro.eachdatum(r)), io3; limits=raised)[1]) == 4_000_000
    end
    @testset "malformed codec blocks release transient reservations" begin
        rows = [(x=Int64(1),), (x=Int64(2),)]
        function checkmalformed(src)
            r = Avro.Reader(src)
            blocks = Avro.eachblock(r)
            baseline = r.budget.reserved
            err = try
                iterate(blocks)
                nothing
            catch e
                e
            end
            @test err isa Avro.CodecError
            @test r.budget.reserved == baseline
            next = iterate(blocks)
            @test next !== nothing
            if next !== nothing
                (count, bytes), _ = next
                @test count == 1
                @test Avro.decode(Avro.writerschema(r), bytes).x == 2
            end
            @test r.budget.reserved == baseline
            close(r)
            close(r)
            return nothing
        end
        for codecname in (:deflate, :snappy, :zstandard, :bzip2, :xz)
            bytes = take!(Avro.tobuffer(rows; codec=codecname, block_bytes=1))
            entries = Avro.Reader(r -> Avro.prescanblocks(r).entries, bytes)
            @test length(entries) == 2
            firstentry = first(entries)
            bad = copy(bytes)
            fill!(view(bad, firstentry.offset:firstentry.offset + firstentry.size - 1), 0xff)
            checkmalformed(bad)
            mktemp() do path, io
                write(io, bad)
                close(io)
                open(path) do input
                    checkmalformed(input)
                end
            end
        end
    end
    @testset "high-window codec bombs and streaming past the ceiling" begin
        hw = joinpath(gen, "highwindow")
        for name in ("zstd-window1g.avro", "xz-dict1g.avro")
            e = try; readall(joinpath(hw, name)); nothing; catch err; err; end
            @test e isa Avro.CodecError                                                           # the member requirement exceeds max_codec_memory
        end
        @test !isempty(readall(joinpath(hw, "zstd-default.avro")))
        @test !isempty(readall(joinpath(hw, "xz-default.avro")))
        if get(ENV, "AVRO_BIG_MEMORY", "true") == "true"
            raised = Avro.Limits(max_codec_memory=2 << 30, max_total_bytes=16 << 30, max_block_bytes=1 << 30, max_block_output_bytes=8 << 30, max_bytes=1 << 30, max_datum_bytes=1 << 30)
            @test !isempty(readall(joinpath(hw, "zstd-window1g.avro"); limits=raised))
            @test !isempty(readall(joinpath(hw, "xz-dict1g.avro"); limits=raised))
        end
        # mmap=false streams a file larger than the effective ceiling (176 MiB here) with one block resident
        small = Avro.Limits(max_total_bytes=176 << 20, max_block_bytes=1 << 20, max_codec_memory=16 << 20, max_block_output_bytes=8 << 20,
                            max_bytes=4 << 20, max_datum_bytes=4 << 20)
        dir = mktempdir()
        big = joinpath(dir, "big.avro")
        bs = P("\"bytes\"")
        chunk = fill(0x7a, 512 << 10)
        w = Avro.Writer(big, bs; limits=small)
        for _ in 1:400
            push!(w, chunk)
        end
        close(w)
        @test filesize(big) > 200 << 20
        n = Avro.Reader(big; limits=small, mmap=false) do r
            total = 0
            for (count, bytes) in Avro.eachblock(r)
                total += count
            end
            total
        end
        @test n == 400
        rm(dir; recursive=true, force=true)
    end
    @testset "inspect" begin
        @test_throws InterruptException Avro.inspect(InterruptIO())
        rows = [(x=Int64(i),) for i in 1:10]
        io = Avro.tobuffer(rows; codec=:deflate, block_bytes=8)
        rep = Avro.inspect(io)
        @test isempty(rep.issues) && rep.codec == "deflate" && rep.datums == 10 && rep.blocks >= 2 && rep.schema isa Avro.RecordSchema
        for inspectedcodec in filter(!=(:null), Avro.codecs())
            clean = oneblockcontainer(inspectedcodec)
            bad = mutatefirstpayload(clean) do payload
                suffix = inspectedcodec === :xz ? UInt8[0x00] : UInt8[0x7f]
                return vcat(payload, suffix)
            end
            badreport = Avro.inspect(bad)
            @test !isempty(badreport.issues)
            @test any(issue -> occursin(String(inspectedcodec), issue), badreport.issues)
        end
        legrep = Avro.inspect(joinpath(gen, "legacy1x", "avrojl112-zstd.avro"))
        @test any(occursin("zstd", i) for i in legrep.issues)
        paddedreport = Avro.inspect(joinpath(gen, "legacy1x", "avrojl112-null.avro"))
        @test paddedreport.blocks == 1 && paddedreport.datums == 2
        @test any(issue -> occursin("trailing payload bytes", issue),
                  paddedreport.issues)
        countio = IOBuffer()
        countwriter = Avro.Writer(countio, Avro.IntSchema())
        push!(countwriter, Int32(1))
        close(countwriter)
        wrongcount = take!(countio)
        wrongcount[end - 18] = 0x04                    # one datum -> two
        countreport = Avro.inspect(wrongcount)
        @test countreport.blocks == countreport.datums == 0
        @test any(issue -> occursin("unexpected end", issue), countreport.issues)
        buf = take!(Avro.tobuffer(rows))
        trep = Avro.inspect(buf[1:end - 3])
        @test !isempty(trep.issues)
        @test occursin("issues", sprint(show, MIME("text/plain"), trep))

        limitedio = IOBuffer()
        limitedwriter = Avro.Writer(limitedio, Avro.NullSchema())
        push!(limitedwriter, missing)
        flush(limitedwriter)
        push!(limitedwriter, missing)
        close(limitedwriter)
        limiteddata = take!(limitedio)
        blockreport = Avro.inspect(limiteddata; limits=Avro.Limits(max_blocks=1))
        @test blockreport.blocks == blockreport.datums == 1
        @test length(blockreport.issues) == 1 && occursin("max_blocks", only(blockreport.issues))
        rowreport = Avro.inspect(limiteddata; limits=Avro.Limits(max_rows=1))
        @test rowreport.blocks == rowreport.datums == 1
        @test length(rowreport.issues) == 1 && occursin("max_rows", only(rowreport.issues))

        shareddecimal = Avro.parseschema("{\"type\":\"record\",\"name\":\"InspectDecimal\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"fixed\",\"name\":\"D\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":8}},{\"name\":\"b\",\"type\":\"D\"}]}")
        decimalio = IOBuffer()
        close(Avro.Writer(decimalio, shareddecimal))
        decimalreport = Avro.inspect(take!(decimalio))
        @test count(issue -> occursin("misframed", issue), decimalreport.issues) == 1
    end
end
