# The Phase 4d projection equivalence matrix (plan §6/§12): names, order, eltypes, Tables.schema, row
# count and values of `Table(src; select=cols)` equal `columntable(Table(src))[cols]` over the fixture
# battery — multi-block files, empty records, select=(), nested and nullable fields, directly and
# mutually recursive roots — in both validation modes and for ntasks ∈ {1, 2, 8}; malformed data inside
# projected-away fields follows each mode's documented policy; projected tables round-trip their schema.

import Logging

@testset "Projection equivalence matrix (plan §6)" begin
    P = Avro.parseschema
    fixtures = Tuple{String,String,Vector{<:Any}}[]
    push!(fixtures, ("plain multi-block",
        "{\"type\":\"record\",\"name\":\"F1\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"score\",\"type\":[\"null\",\"double\"]},{\"name\":\"tag\",\"type\":{\"type\":\"enum\",\"name\":\"T1\",\"symbols\":[\"a\",\"b\",\"c\"]}}]}",
        [(id=Int64(i), name="n$i", score=i % 4 == 0 ? missing : i / 3, tag=("a", "b", "c")[i % 3 + 1]) for i in 1:2500]))
    push!(fixtures, ("nested and nullable",
        "{\"type\":\"record\",\"name\":\"F2\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"r\",\"type\":{\"type\":\"record\",\"name\":\"Inner2\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"},{\"name\":\"y\",\"type\":[\"null\",\"string\"]}]}},{\"name\":\"u\",\"type\":[\"long\",\"string\",\"null\"]}]}",
        [(a=Int64[i, i + 1], m=Dict("k$i" => "v$i"), r=(x=Int64(i), y=isodd(i) ? "s$i" : missing),
          u=i % 3 == 0 ? Avro.UnionValue(3, missing) : i % 3 == 1 ? Avro.UnionValue(1, Int64(i)) : Avro.UnionValue(2, "u$i")) for i in 1:600]))
    push!(fixtures, ("empty record", "{\"type\":\"record\",\"name\":\"F3\",\"fields\":[]}", [(;) for _ in 1:100]))
    push!(fixtures, ("directly recursive root",
        "{\"type\":\"record\",\"name\":\"Node\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"Node\"]}]}",
        [(v=Int64(i), next=i % 2 == 0 ? (v=Int64(-i), next=missing) : missing) for i in 1:400]))
    push!(fixtures, ("mutually recursive root",
        "{\"type\":\"record\",\"name\":\"A0\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"},{\"name\":\"b\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"B0\",\"fields\":[{\"name\":\"w\",\"type\":\"string\"},{\"name\":\"a\",\"type\":[\"null\",\"A0\"]}]}]}]}",
        [(v=Int64(i), b=i % 3 == 0 ? missing : (w="w$i", a=missing)) for i in 1:400]))
    raised = Avro.Limits(max_total_bytes=2 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                         max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)
    function selections(names)
        sels = Vector{Symbol}[]
        push!(sels, Symbol[])                                 # select=()
        push!(sels, collect(names))                            # full selection, in source order
        for nm in names
            push!(sels, [nm])                                 # every single column
        end
        length(names) > 1 && push!(sels, reverse(collect(names)))
        length(names) > 2 && push!(sels, [names[end], names[1]])
        unique!(sels)
        return sels
    end
    for (label, json, rows) in fixtures
        s = P(json)
        bytes = take!(Avro.tobuffer(rows; schema=s, block_bytes=512, limits=raised))
        @testset "$label" begin
            for val in (:strict, :fast)
                full = Avro.Table(IOBuffer(bytes); validate=val, limits=raised)
                fullct = Tables.columntable(full)
                fullsch = Tables.schema(full)
                for sel in selections(Tables.columnnames(full)), nt in (1, 2, 8)
                    pt = Avro.Table(IOBuffer(bytes); select=Tuple(sel), validate=val, ntasks=nt, limits=raised)
                    @test Tables.columnnames(pt) == sel
                    @test length(pt) == length(full)
                    psch = Tables.schema(pt)
                    @test collect(psch.names) == sel
                    for (k, nm) in enumerate(sel)
                        col = Tables.getcolumn(pt, k)
                        want = fullct[nm]
                        @test isequal(col, want) && typeof(col) == typeof(want)
                        @test psch.types[k] == fullsch.types[findfirst(==(nm), collect(fullsch.names))]
                    end
                    ps = Avro.schema(pt)
                    @test [f.name for f in ps.fields] == [String(nm) for nm in sel]
                end
                # writing a projection round-trips its derived schema (recursive roots included); for a
                # recursive root the projection is graph-wide, so nested records re-encode projected and
                # only the schema and row count round-trip value-independently
                names = collect(Tables.columnnames(full))
                sel = isempty(names) ? Symbol[] : [names[end]]
                pt = Avro.Table(IOBuffer(bytes); select=Tuple(sel), validate=val, limits=raised)
                io = IOBuffer()
                Avro.write(io, pt; limits=raised)
                seekstart(io)
                t2 = Avro.Table(io; limits=raised)
                @test Avro.json(Avro.schema(t2)) == Avro.json(Avro.schema(pt))
                @test length(t2) == length(pt)
                recursive = occursin("recursive", label)
                recursive || @test isequal(Tables.columntable(t2), Tables.columntable(pt))
            end
        end
    end
    @testset "malformed data inside projected-away fields (each mode's documented policy)" begin
        # a crafted container: {good: long, arr: array<boolean>} where arr uses the sized-block form
        # holding invalid boolean bytes — strict walks skipped fields (rejects), fast jumps the sized
        # block by its byte size (tolerates)
        s2 = P("{\"type\":\"record\",\"name\":\"M\",\"fields\":[{\"name\":\"good\",\"type\":\"long\"},{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"boolean\"}}]}")
        sync = collect(UInt8(1):UInt8(16))
        base = IOBuffer()
        w = Avro.Writer(base, s2; sync=sync)
        push!(w, (good=Int64(1), arr=[true]))
        close(w)
        orig = take!(base)
        r = Avro.Reader(IOBuffer(orig))
        e1 = Avro.prescanblocks(r).entries[1]
        close(r)
        headerend = e1.offset - Avro.varintlength(1) - Avro.varintlength(e1.size) - 1
        payload = UInt8[0x02, 0x03, 0x04, 0x02, 0x02, 0x00]     # good=1; arr sized block(count=-2, 2 bytes): two invalid bools; end
        crafted = vcat(orig[1:headerend], UInt8[0x02], UInt8[UInt8(2 * length(payload))], payload, sync)
        pt = Avro.Table(IOBuffer(crafted); select=(:good,), validate=:fast)
        @test Tables.getcolumn(pt, :good) == [1]
        @test_throws Avro.DataError Avro.Table(IOBuffer(crafted); select=(:good,), validate=:strict)
        @test_throws Avro.DataError Avro.Table(IOBuffer(crafted); select=(:arr,), validate=:fast)   # selected data is always validated
        rs = Avro.Rows(IOBuffer(crafted); select=(:good,), validate=:fast)
        @test [row.good for row in rs] == [1]
        close(rs)
        rst = Avro.Rows(IOBuffer(crafted); select=(:good,), validate=:strict)
        @test_throws Avro.DataError collect(rst)
        close(rst)
        # an invalid skipped value outside a sized block is rejected in both modes (bools are
        # domain-checked when skipped; only sized-block jumps and skipped-string UTF-8 are relaxed)
        s3 = P("{\"type\":\"record\",\"name\":\"M2\",\"fields\":[{\"name\":\"good\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"boolean\"}]}")
        rows3 = [(good=Int64(i), b=iseven(i)) for i in 1:10]
        bytes3 = take!(Avro.tobuffer(rows3; schema=s3, block_bytes=64))
        r3 = Avro.Reader(IOBuffer(bytes3))
        f1 = Avro.prescanblocks(r3).entries[1]
        close(r3)
        bad3 = copy(bytes3)
        bad3[f1.offset + 1] = 0x02                              # row 1's boolean byte
        @test_throws Avro.DataError Avro.Table(IOBuffer(bad3); select=(:good,), validate=:strict)
        @test_throws Avro.DataError Avro.Table(IOBuffer(bad3); select=(:good,), validate=:fast)
        s3fused = P("""{"type":"record","name":"M2Fused","fields":[
            {"name":"good","type":"long"},{"name":"b1","type":"boolean"},
            {"name":"b2","type":"boolean"}]}""")
        fusedbytes = take!(Avro.tobuffer([(good=Int64(1), b1=true, b2=false)];
                                         schema=s3fused))
        fusedreader = Avro.Reader(IOBuffer(fusedbytes))
        fusedblock = only(Avro.prescanblocks(fusedreader).entries)
        close(fusedreader)
        fusedbad = copy(fusedbytes)
        fusedbad[fusedblock.offset + 1] = 0x02
        for mode in (:strict, :fast)
            @test_throws Avro.DataError Avro.Table(IOBuffer(fusedbad);
                                                   select=(:good,), validate=mode)
        end
        countedwriter = P("""{"type":"record","name":"CountedFastSkip","fields":[
            {"name":"drop","type":{"type":"array","items":{"type":"record","name":"CountedFastItem","fields":[
                {"name":"e","type":{"type":"enum","name":"CountedFastEnum","symbols":["a"]}}]}}},
            {"name":"keep","type":"int"}]}""")
        countedreader = P("""{"type":"record","name":"CountedFastSkip","fields":[
            {"name":"keep","type":"int"}]}""")
        countedprefix = UInt8[0xc7, 0x01, 0xc8, 0x01]   # sized array: count=-100, size=100
        countedvalid = vcat(countedprefix, fill(UInt8(0x00), 100), UInt8[0x00, 0x02])
        countedinvalid = vcat(countedprefix, fill(UInt8(0x02), 100), UInt8[0x00, 0x02])
        countedlimits = Avro.Limits(max_total_values=10)
        CountedTarget = @NamedTuple{keep::Int32}
        for target in (nothing, CountedTarget)
            decodecounted = target === nothing ?
                bytes -> Avro.decode(countedwriter, bytes;
                                     reader_schema=countedreader, validate=:fast,
                                     limits=countedlimits) :
                bytes -> Avro.decode(countedwriter, bytes, target;
                                     reader_schema=countedreader, validate=:fast,
                                     limits=countedlimits)
            @test_throws Avro.LimitError decodecounted(countedvalid)
            @test_throws Avro.DataError decodecounted(countedinvalid)
        end
        fixedrun = P("""{"type":"record","name":"FastFixedRun","fields":[
            {"name":"id","type":"long"},{"name":"a","type":"float"},
            {"name":"b","type":"float"}]}""")
        fixedrunbytes = take!(Avro.tobuffer([(id=Int64(1), a=1.0f0, b=2.0f0)];
                                             schema=fixedrun))
        fixedrunlimits = Avro.Limits(max_bytes=4, max_datum_bytes=1 << 20)
        for mode in (:strict, :fast)
            table = Avro.Table(fixedrunbytes; select=(:id,), validate=mode,
                               limits=fixedrunlimits, ntasks=1)
            @test Tables.getcolumn(table, :id) == [1]
        end
        dynamicrun = P("""{"type":"record","name":"FastDynamicRun","fields":[
            {"name":"id","type":"long"},{"name":"s","type":"string"},
            {"name":"d","type":"double"},{"name":"f","type":"float"}]}""")
        dynamicrunbytes = take!(Avro.tobuffer([(id=Int64(1), s="", d=2.0, f=3.0f0)];
                                               schema=dynamicrun))
        for mode in (:strict, :fast)
            table = Avro.Table(dynamicrunbytes; select=(:id,), validate=mode,
                               limits=fixedrunlimits, ntasks=1)
            @test Tables.getcolumn(table, :id) == [1]
        end
        # invalid UTF-8 inside a skipped string is tolerated in both modes; selecting it rejects
        s4 = P("{\"type\":\"record\",\"name\":\"M3\",\"fields\":[{\"name\":\"good\",\"type\":\"long\"},{\"name\":\"s\",\"type\":\"string\"}]}")
        bytes4 = take!(Avro.tobuffer([(good=Int64(7), s="AB")]; schema=s4))
        r4 = Avro.Reader(IOBuffer(bytes4))
        g1 = Avro.prescanblocks(r4).entries[1]
        close(r4)
        bad4 = copy(bytes4)
        @test bad4[g1.offset + 2] == UInt8('A')
        bad4[g1.offset + 2] = 0xff                              # invalid UTF-8, length intact
        for val in (:strict, :fast)
            @test Tables.getcolumn(Avro.Table(IOBuffer(bad4); select=(:good,), validate=val), :good) == [7]
            @test_throws Avro.DataError Avro.Table(IOBuffer(bad4); select=(:s,), validate=val)
        end

        @testset "resolved skips enforce reader-side validation" begin
            cases = [
                ("time", "{\"type\":\"record\",\"name\":\"SkipTime\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":\"int\"}]}",
                 "{\"type\":\"record\",\"name\":\"SkipTime\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}}]}",
                 (good=Int32(1), bad=Int32(-1)), Avro.DataError),
                ("decimal", "{\"type\":\"record\",\"name\":\"SkipDecimal\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":\"string\"}]}",
                 "{\"type\":\"record\",\"name\":\"SkipDecimal\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":2,\"scale\":0}}]}",
                 (good=Int32(1), bad=String(UInt8[0x01, 0x00])), Avro.DataError),
                ("uuid", "{\"type\":\"record\",\"name\":\"SkipUUID\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":\"bytes\"}]}",
                 "{\"type\":\"record\",\"name\":\"SkipUUID\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}]}",
                 (good=Int32(1), bad=UInt8[0x78]), Avro.DataError),
                ("enum", "{\"type\":\"record\",\"name\":\"SkipEnumRecord\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":{\"type\":\"enum\",\"name\":\"SkipEnum\",\"symbols\":[\"A\",\"B\"]}}]}",
                 "{\"type\":\"record\",\"name\":\"SkipEnumRecord\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":{\"type\":\"enum\",\"name\":\"SkipEnum\",\"symbols\":[\"A\"]}}]}",
                 (good=Int32(1), bad="B"), Avro.ResolutionError),
                ("unresolvable union", "{\"type\":\"record\",\"name\":\"SkipUnion\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":[\"int\",\"string\"]}]}",
                 "{\"type\":\"record\",\"name\":\"SkipUnion\",\"fields\":[{\"name\":\"good\",\"type\":\"int\"},{\"name\":\"bad\",\"type\":\"long\"}]}",
                 (good=Int32(1), bad=Avro.UnionValue(2, "x")), Avro.ResolutionError),
            ]
            target = NamedTuple{(:good,),Tuple{Int32}}
            for (label, writerjson, readerjson, value, errortype) in cases
                @testset "$label" begin
                    writer = P(writerjson)
                    reader = P(readerjson)
                    datum = Avro.encode(writer, value)
                    typederror = try
                        Avro.DatumReader(writer, target; reader_schema=reader)(datum)
                        nothing
                    catch err
                        err
                    end
                    @test typederror isa errortype
                    bytes = take!(Avro.tobuffer([value]; schema=writer))
                    tableerror = try
                        Avro.Table(IOBuffer(bytes); reader_schema=reader, select=(:good,))
                        nothing
                    catch err
                        err
                    end
                    @test tableerror isa errortype
                    rows = Avro.Rows(IOBuffer(bytes); reader_schema=reader, select=(:good,))
                    rowerror = try
                        collect(rows)
                        nothing
                    catch err
                        err
                    finally
                        close(rows)
                    end
                    @test rowerror isa errortype
                end
            end
        end
    end

    @testset "corpus sweep: exact fixture scope and projection matrix (R14, D09)" begin
        fixtureroot = joinpath(@__DIR__, "fixtures")
        gen = joinpath(fixtureroot, "generated")
        apache = joinpath(fixtureroot, "apache")
        datadir = joinpath(gen, "data")
        rootsdir = joinpath(gen, "roots")
        legacydir = joinpath(gen, "legacy1x")
        highwindowdir = joinpath(gen, "highwindow")
        raised = Avro.Limits(max_total_bytes=2 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                             max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)

        function avrofiles(root)
            files = String[]
            for (dir, _, names) in walkdir(root)
                for name in names
                    endswith(name, ".avro") && push!(files, joinpath(dir, name))
                end
            end
            sort!(files)
            return files
        end

        function corpusselections(names)
            sels = Vector{Symbol}[]
            push!(sels, Symbol[])
            if !isempty(names)
                push!(sels, [names[1]])
                push!(sels, collect(names))
                push!(sels, reverse(collect(names)))
                length(names) > 1 && push!(sels, [names[end], names[1]])
                length(names) > 2 && push!(sels, collect(names[1:2:end]))
            end
            unique!(sels)
            return sels
        end

        function readoptions(data)
            startswith(data, legacydir) && return (; legacy=:avrojl1, decimal_byteorder=:little)
            return (;)
        end

        function quietly(f::Function, data)
            if startswith(data, legacydir)
                return Logging.with_logger(Logging.NullLogger()) do
                    return f()
                end
            end
            return f()
        end

        datafiles = avrofiles(datadir)
        rejected = filter(f -> endswith(f, "-fastavro-deflate.avro"), datafiles)
        rootfiles = avrofiles(rootsdir)
        legacyfiles = avrofiles(legacydir)
        highwindowfiles = avrofiles(highwindowdir)
        overlimit = filter(f -> occursin("1g.avro", basename(f)), highwindowfiles)
        highwindowcontrols = setdiff(highwindowfiles, overlimit)
        apachefiles = avrofiles(apache)
        recordfiles = sort!(vcat(setdiff(datafiles, rejected), apachefiles, legacyfiles, highwindowcontrols))
        allfiles = avrofiles(fixtureroot)

        # Plan §8.1/§8.2 defines this exact corpus. Projection applies only to generic record
        # roots; the two deliberate high-window bombs are excluded from default decoding by §4.4.
        @test length(datafiles) == 72
        @test length(rejected) == 6
        @test length(rootfiles) == 156
        @test length(legacyfiles) == 5
        @test length(apachefiles) == 8
        @test length(highwindowcontrols) == 2 && length(overlimit) == 2
        @test length(recordfiles) == 81
        @test allfiles == sort!(vcat(recordfiles, rejected, rootfiles, overlimit))
        @test sort([relpath(f, apache) for f in apachefiles]) == [
            "schemas/simple/data.avro", "schemas/withUnion/data.avro", "syncInMeta.avro",
            "weather-deflate.avro", "weather-snappy.avro", "weather-sorted.avro",
            "weather-zstd.avro", "weather.avro"]
        @test sort(basename.(legacyfiles)) == [
            "avrojl112-bzip2.avro", "avrojl112-deflate.avro", "avrojl112-null.avro",
            "avrojl112-xz.avro", "avrojl112-zstd.avro"]
        @test sort(basename.(highwindowcontrols)) == ["xz-default.avro", "zstd-default.avro"]
        @test sort(basename.(overlimit)) == ["xz-dict1g.avro", "zstd-window1g.avro"]

        rootschemas = sort(filter(f -> endswith(f, ".avsc"), readdir(rootsdir; join=true)))
        rootstems = sort([splitext(basename(f))[1] for f in rootschemas])
        @test rootstems == ["array", "boolean", "bytes", "double", "enum", "fixed", "float",
                            "int", "long", "map", "null", "string", "union"]
        for schemafile in rootschemas
            @test !(Avro.parseschema(read(schemafile, String)) isa Avro.RecordSchema)
        end
        for data in rootfiles
            @test any(stem -> startswith(basename(data), stem * "-"), rootstems)
            r = Avro.Reader(data; limits=raised)
            try
                @test !(Avro.writerschema(r) isa Avro.RecordSchema)
            finally
                close(r)
            end
        end

        for data in recordfiles
            opts = readoptions(data)
            @testset "$(relpath(data, fixtureroot))" begin
                for val in (:strict, :fast)
                    full = quietly(data) do
                        return Avro.Table(data; validate=val, ntasks=1, limits=raised, opts...)
                    end
                    names = collect(Tables.columnnames(full))
                    fullct = Tables.columntable(full)
                    fullsch = Tables.schema(full)
                    for sel in corpusselections(names)
                        expectedtypes = Type[fullsch.types[findfirst(==(nm), collect(fullsch.names))] for nm in sel]
                        rowresult = quietly(data) do
                            rl = Avro.Rows(data; select=Tuple(sel), validate=val, limits=raised, opts...)
                            try
                                return (Tables.schema(rl), collect(rl))
                            finally
                                close(rl)
                            end
                        end
                        rowschema, rows = rowresult
                        @test collect(rowschema.names) == sel
                        @test collect(rowschema.types) == expectedtypes
                        @test length(rows) == length(full)
                        for (k, nm) in enumerate(sel)
                            @test isequal([Tables.getcolumn(r, k) for r in rows], collect(fullct[nm]))
                        end
                        for nt in (1, 2, 8)
                            pt = quietly(data) do
                                return Avro.Table(data; select=Tuple(sel), validate=val, ntasks=nt,
                                                  limits=raised, opts...)
                            end
                            @test collect(Tables.columnnames(pt)) == sel
                            @test length(pt) == length(full)
                            psch = Tables.schema(pt)
                            @test collect(psch.names) == sel
                            @test collect(psch.types) == expectedtypes
                            @test isequal(Tables.columntable(pt), fullct[Tuple(sel)])
                            for (k, nm) in enumerate(sel)
                                got = Tables.getcolumn(pt, k)
                                want = fullct[nm]
                                @test isequal(got, want) && typeof(got) == typeof(want)
                            end
                        end
                    end
                end

                # One projection from every readable record fixture must remain writable. This also
                # covers the authoritative row count of empty records and `select=()`.
                full = quietly(data) do
                    return Avro.Table(data; validate=:strict, ntasks=1, limits=raised, opts...)
                end
                names = collect(Tables.columnnames(full))
                sel = isempty(names) ? Symbol[] : [names[end]]
                projected = quietly(data) do
                    return Avro.Table(data; select=Tuple(sel), validate=:strict, ntasks=1,
                                      limits=raised, opts...)
                end
                io = IOBuffer()
                Avro.write(io, projected; limits=raised)
                seekstart(io)
                roundtrip = Avro.Table(io; limits=raised)
                @test Avro.json(Avro.schema(roundtrip)) == Avro.json(Avro.schema(projected))
                @test length(roundtrip) == length(projected)
                @test isequal(Tables.columntable(roundtrip), Tables.columntable(projected))
            end
        end

        # These files are record roots, but their documented codec verdict prevents a projection.
        # Exercise every selection/mode/task combination so the exclusion cannot hide a success or
        # change the accepted language.
        for data in rejected
            r = Avro.Reader(data; limits=raised)
            s = Avro.writerschema(r)
            close(r)
            @test s isa Avro.RecordSchema
            names = Symbol[Symbol(f.name) for f in s.fields]
            for sel in corpusselections(names), val in (:strict, :fast)
                for nt in (1, 2, 8)
                    err = try
                        Avro.Table(data; select=Tuple(sel), validate=val, ntasks=nt, limits=raised)
                        nothing
                    catch e
                        e
                    end
                    @test err isa Avro.CodecError
                    err isa Avro.CodecError && @test occursin("bytes after the final deflate block", err.msg)
                end
                rl = Avro.Rows(data; select=Tuple(sel), validate=val, limits=raised)
                err = try
                    collect(rl)
                    nothing
                catch e
                    e
                finally
                    close(rl)
                end
                @test err isa Avro.CodecError
                err isa Avro.CodecError && @test occursin("bytes after the final deflate block", err.msg)
            end
        end

        # The plan deliberately excludes these two files from decode-under-defaults. They still
        # receive the full projection verdict surface under the gate's 32 MiB codec-memory limit.
        for data in overlimit
            r = Avro.Reader(data; limits=raised)
            s = Avro.writerschema(r)
            close(r)
            @test s isa Avro.RecordSchema
            names = Symbol[Symbol(f.name) for f in s.fields]
            for sel in corpusselections(names), val in (:strict, :fast)
                for nt in (1, 2, 8)
                    @test_throws Avro.CodecError Avro.Table(data; select=Tuple(sel), validate=val,
                                                            ntasks=nt, limits=raised)
                end
                rl = Avro.Rows(data; select=Tuple(sel), validate=val, limits=raised)
                try
                    @test_throws Avro.CodecError collect(rl)
                finally
                    close(rl)
                end
            end
        end
    end
end
