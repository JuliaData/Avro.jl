@testset "Writer/Reader invariant: consumers and source modes (plan §4.4)" begin
    P = Avro.parseschema
    dir = mktempdir()
    @testset "write plan nodes are bounded and field lookup is reentrant" begin
        for json in (
                "{\"type\":\"array\",\"items\":\"long\"}",
                "{\"type\":\"map\",\"values\":\"long\"}",
                "{\"type\":\"fixed\",\"name\":\"PlanFixed\",\"size\":4}",
                "{\"type\":\"enum\",\"name\":\"PlanEnum\",\"symbols\":[\"A\"]}",
                "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}")
            budget = Avro.Budget(Avro.Limits(); available=1 << 40)
            plan = Avro.writeplan(P(json); budget=budget)
            @test budget.pending == 0
            @test budget.reserved >= 64
            Avro.close!(budget)
        end
        limited = Avro.Limits(max_resolution_work=1)
        union = P("[\"null\",\"long\"]")
        for build in (Avro.readplan, Avro.writeplan)
            budget = Avro.Budget(limited; available=1 << 40)
            @test_throws Avro.LimitError build(union; budget=budget)
            @test budget.pending == budget.reserved == 0
            Avro.close!(budget)
        end
        recursive = P("{\"type\":\"record\",\"name\":\"FieldCache\",\"fields\":[{\"name\":\"child\",\"type\":[\"null\",\"FieldCache\"]},{\"name\":\"tail\",\"type\":\"int\"}]}")
        value = (child=(tail=Int32(2), child=missing), tail=Int32(1))
        @test isequal(Avro.decode(recursive, Avro.encode(recursive, value)), Avro.Record(recursive, Any[
            Avro.Record(recursive, Any[missing, Int32(2)]), Int32(1)]))
    end
    @testset "the writer preflight equals a stream reader's construction retention" begin
        s = P("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"inv\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"c\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"double\"}]}]}")
        path = joinpath(dir, "pf.avro")
        w = Avro.Writer(path, s; metadata=Dict("k" => Vector{UInt8}("v"), "blob" => zeros(UInt8, 1000)))
        @test w.preflightbase > 0
        push!(w, (a=Int64(1), b="x", c=[1.0, 2.0]))
        close(w)
        r = Avro.Reader(path; mmap=false)
        @test r.budget.reserved == w.preflightbase              # the same graph, metadata and read plan
        close(r)
    end
    @testset "byte and stream headers have identical near-ceiling acceptance" begin
        maxmetadata = (40 << 20) - 6
        smallkeys = ["k$(lpad(i, 4, '0'))" for i in 1:1024]
        largekeybytes = maxmetadata - sizeof("avro.schema") - sizeof("\"null\"") - sum(sizeof, smallkeys)
        limits = Avro.Limits(max_total_bytes=80 << 20, max_codec_memory=16 << 20,
                             max_block_bytes=1 << 20, max_block_output_bytes=1 << 20,
                             max_bytes=1 << 20, max_datum_bytes=1 << 20,
                             max_schema_bytes=6, max_metadata_bytes=maxmetadata)
        long = Avro.LongSchema()
        function varint(n)
            return Avro.encode(long, Int64(n))
        end
        header = IOBuffer()
        write(header, b"Obj\x01")
        write(header, varint(length(smallkeys) + 2))
        write(header, varint(sizeof("avro.schema")), codeunits("avro.schema"),
              varint(sizeof("\"null\"")), codeunits("\"null\""))
        for key in smallkeys
            write(header, varint(sizeof(key)), codeunits(key), varint(0))
        end
        write(header, varint(largekeybytes))
        chunk = fill(UInt8('z'), 1 << 20)
        whole, remainder = divrem(largekeybytes, length(chunk))
        for _ in 1:whole
            write(header, chunk)
        end
        write(header, view(chunk, 1:remainder), varint(0), varint(0), zeros(UInt8, 16))
        bytes = take!(header)
        path = joinpath(dir, "header-source-equivalence.avro")
        write(path, bytes)

        function outcome(src; kw...)
            try
                r = Avro.Reader(src; limits=limits, kw...)
                close(r)
                return :accepted
            catch e
                return (typeof(e), e isa Avro.LimitError ? e.limit : nothing, sprint(showerror, e))
            end
        end
        frombytes = outcome(bytes)
        fromstream = outcome(path; mmap=false)
        @test frombytes === :accepted
        @test fromstream == frombytes
    end
    @testset "strict consumers charge each datum once" begin
        lim = Avro.Limits(max_total_values=100, work_allowance=1000)
        nullio = IOBuffer()
        nullwriter = Avro.Writer(nullio, Avro.NullSchema(); limits=lim)
        for _ in 1:60
            push!(nullwriter, missing)
        end
        close(nullwriter)
        nullbytes = take!(nullio)
        @test Avro.Reader(r -> length(collect(Avro.eachdatum(r))), IOBuffer(nullbytes); limits=lim) == 60
        nullrows = Avro.Rows(IOBuffer(nullbytes); limits=lim)
        @test length(collect(nullrows)) == 60
        close(nullrows)

        empty_schema = P("{\"type\":\"record\",\"name\":\"Once\",\"fields\":[]}")
        emptyio = IOBuffer()
        emptywriter = Avro.Writer(emptyio, empty_schema; limits=lim)
        for _ in 1:60
            push!(emptywriter, (;))
        end
        close(emptywriter)
        emptybytes = take!(emptyio)
        @test length(Avro.Table(IOBuffer(emptybytes); limits=lim, ntasks=1)) == 60
        Threads.nthreads() > 1 && @test length(Avro.Table(emptybytes; limits=lim, ntasks=2)) == 60
    end
    @testset "writer and reader use identical container work counters" begin
        limits = Avro.Limits(max_total_values=16)
        schema = Avro.StringSchema()
        for codec in Avro.codecs()
            io = IOBuffer()
            writer = Avro.Writer(io, schema; codec=codec, limits=limits)
            push!(writer, "x")
            close(writer)
            bytes = take!(io)
            writerstate = (values=writer.budget.values, input_bytes=writer.budget.input_bytes,
                           rows=writer.budget.rows, blocks=writer.budget.blocks,
                           members=writer.budget.members, compare_bytes=writer.budget.compare_bytes,
                           allowance_used=Avro.allowanceused(writer.budget))
            values, readerstate = Avro.Reader(IOBuffer(bytes); limits=limits) do reader
                decoded = collect(Avro.eachdatum(reader))
                state = (values=reader.budget.values, input_bytes=reader.budget.input_bytes,
                         rows=reader.budget.rows, blocks=reader.budget.blocks,
                         members=reader.budget.members, compare_bytes=reader.budget.compare_bytes,
                         allowance_used=Avro.allowanceused(reader.budget))
                return decoded, state
            end
            @test values == ["x"]
            @test writerstate == readerstate
        end

        tight = Avro.Limits(max_total_values=3)
        tightio = IOBuffer()
        tightwriter = Avro.Writer(tightio, schema; limits=tight)
        headersize = position(tightio)
        err = try
            push!(tightwriter, "x")
            close(tightwriter)
            nothing
        catch e
            close(tightwriter; abort=true)
            e
        end
        @test err isa Avro.LimitError && err.limit === :max_total_values && err.observed == 4
        @test position(tightio) == headersize                         # no unreadable data block was emitted

        # The aligned direct-append path preserves shared counters when the next
        # row crosses a block-count boundary and must be staged during the flush.
        recordschema = P("""{"type":"record","name":"AlignedWork","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"}]}""")
        recordlimits = Avro.Limits(max_block_count=1)
        recordrows = [(id=Int64(1), name="one"), (id=Int64(2), name="two")]
        recordio = IOBuffer()
        recordwriter = Avro.Writer(recordio, recordschema; limits=recordlimits)
        for row in recordrows
            push!(recordwriter, row)
        end
        close(recordwriter)
        recordbytes = take!(recordio)
        recordwriterstate = (values=recordwriter.budget.values,
                             input_bytes=recordwriter.budget.input_bytes,
                             rows=recordwriter.budget.rows,
                             blocks=recordwriter.budget.blocks,
                             members=recordwriter.budget.members,
                             compare_bytes=recordwriter.budget.compare_bytes,
                             allowance_used=Avro.allowanceused(recordwriter.budget))
        recordvalues, recordreaderstate = Avro.Reader(recordbytes;
                                                       limits=recordlimits) do reader
            values = collect(Avro.eachdatum(reader))
            state = (values=reader.budget.values,
                     input_bytes=reader.budget.input_bytes,
                     rows=reader.budget.rows,
                     blocks=reader.budget.blocks,
                     members=reader.budget.members,
                     compare_bytes=reader.budget.compare_bytes,
                     allowance_used=Avro.allowanceused(reader.budget))
            return values, state
        end
        @test getproperty.(recordvalues, :id) == Int64[1, 2]
        @test getproperty.(recordvalues, :name) == ["one", "two"]
        function sharedwork(state)
            return (values=state.values,
                    input_bytes=state.input_bytes,
                    rows=state.rows,
                    blocks=state.blocks,
                    members=state.members,
                    allowance_used=state.allowance_used)
        end
        @test sharedwork(recordwriterstate) == sharedwork(recordreaderstate)
        @test recordwriterstate.compare_bytes >= recordreaderstate.compare_bytes

        referencewriter = Avro.Writer(IOBuffer(), recordschema)
        for row in recordrows
            push!(referencewriter, row)
        end
        close(referencewriter)
        @test recordwriterstate.compare_bytes == referencewriter.budget.compare_bytes
    end
    @testset "schema printing does not subsidise datum work" begin
        dense = fill(nothing, 67_000)
        dense_schema = Avro.ArraySchema(Avro.NullSchema())
        dense_io = IOBuffer()
        dense_writer = Avro.Writer(dense_io, dense_schema)
        err = try
            push!(dense_writer, dense)
            close(dense_writer)
            nothing
        catch e
            e
        end
        err === nothing || close(dense_writer; abort=true)
        @test err isa Avro.LimitError && err.limit === :max_values_per_byte
    end
    @testset "writer and reader enforce cumulative row and block limits" begin
        for limits in (Avro.Limits(max_block_count=0), Avro.Limits(max_rows=0))
            writer = Avro.Writer(IOBuffer(), Avro.NullSchema(); limits=limits)
            @test_throws Avro.LimitError push!(writer, missing)
            close(writer; abort=true)
        end
        blockwriter = Avro.Writer(IOBuffer(), Avro.NullSchema(); limits=Avro.Limits(max_blocks=0))
        push!(blockwriter, missing)
        @test_throws Avro.LimitError close(blockwriter)

        io = IOBuffer()
        writer = Avro.Writer(io, Avro.NullSchema())
        push!(writer, missing)
        close(writer)
        bytes = take!(io)
        @test_throws Avro.LimitError Avro.Reader(r -> collect(Avro.eachdatum(r)), IOBuffer(bytes); limits=Avro.Limits(max_rows=0))
    end
    @testset "writer preflight combines prior Table payload with the current block peak" begin
        schema = P("""{"type":"record","name":"TablePeak","fields":[
            {"name":"xs","type":{"type":"array","items":"string"}}]}""")
        limits = Avro.Limits(max_block_bytes=3 << 20,
            max_block_output_bytes=49 << 20, max_codec_memory=16 << 20,
            max_total_bytes=112 << 20, max_values_per_byte=16)
        values = fill("", (1 << 21) + 1)
        io = IOBuffer()
        writer = Avro.Writer(io, schema; block_bytes=3 << 20, limits=limits)
        push!(writer, (xs=values,))
        flush(writer)
        push!(writer, (xs=values,))
        before = position(io)
        err = try
            flush(writer)
            nothing
        catch e
            e
        end
        @test err isa Avro.LimitError
        @test err.limit === :max_total_bytes
        @test err.observed > err.value == 112 << 20
        @test position(io) == before
        @test writer.poison === err
        close(writer)
    end
    @testset "a near-ceiling record file through every source mode and guaranteed consumer" begin
        lim = Avro.Limits(max_total_bytes=96 << 20, max_block_bytes=8 << 20, max_codec_memory=16 << 20,
                          max_block_output_bytes=32 << 20, max_bytes=8 << 20, max_datum_bytes=8 << 20)
        s = P("{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"c\",\"type\":[\"null\",\"double\"]}]}")
        n = 260_000
        rows = [(a=Int64(i), b="value-$(i)-payloadpayloadpayloadpayloadpayload", c=iseven(i) ? i / 3 : missing) for i in 1:n]
        path = joinpath(dir, "near.avro")
        Avro.write(path, rows; schema=s, limits=lim, codec=:deflate)
        bytes = read(path)
        for mode in (:bytes, :mapped, :streampath, :io)
            function src()
                return mode === :bytes ? IOBuffer(bytes) : mode === :io ? open(path) : path
            end
            kw = mode === :streampath ? (; mmap=false) : (;)
            cnt = Avro.Reader(rr -> sum(first(b) for b in Avro.eachblock(rr); init=0), src(); limits=lim, kw...)
            @test cnt == n
            rl = Avro.Rows(src(); limits=lim, kw...)
            total = 0
            last = nothing
            for row in rl
                total += 1
                last = row
            end
            close(rl)
            @test total == n && last.a === Int64(n)
            t = Avro.Table(src(); limits=lim, kw...)
            @test length(t) == n && Tables.getcolumn(t, :a)[end] == n && Tables.getcolumn(t, :b)[1] == rows[1].b
            @test ismissing(Tables.getcolumn(t, :c)[1]) && Tables.getcolumn(t, :c)[2] == 2 / 3
        end
    end
    @testset "root shapes through the guaranteed consumers" begin
        shapes = [
            ("\"null\"", fill(nothing, 1000), false),
            ("{\"type\":\"record\",\"name\":\"E\",\"fields\":[]}", fill((;), 1000), true),
            ("{\"type\":\"record\",\"name\":\"AN\",\"fields\":[{\"name\":\"x\",\"type\":\"null\"},{\"name\":\"y\",\"type\":\"null\"}]}",
             fill((x=nothing, y=nothing), 1000), true),
            ("{\"type\":\"fixed\",\"name\":\"F0\",\"size\":0}", fill(UInt8[], 1000), false),
            ("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"long\"}}", fill(Vector{Int64}[], 1000), false),
            ("{\"type\":\"record\",\"name\":\"NR\",\"fields\":[{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"NI\",\"fields\":[{\"name\":\"x\",\"type\":\"null\"}]}}]}",
             fill((inner=(x=nothing,),), 1000), true),
        ]
        for (json, vals, isrecord) in shapes
            s = P(json)
            io = IOBuffer()
            w = Avro.Writer(io, s; block_bytes=256)
            for v in vals
                push!(w, v)
            end
            close(w)
            bytes = take!(io)
            cnt = Avro.Reader(rr -> sum(first(b) for b in Avro.eachblock(rr); init=0), IOBuffer(bytes))
            @test cnt == 1000
            rl = Avro.Rows(IOBuffer(bytes))
            @test count(Returns(true), rl) == 1000
            close(rl)
            if isrecord
                t = Avro.Table(IOBuffer(bytes))
                @test length(t) == 1000
            else
                @test_throws ArgumentError Avro.Table(IOBuffer(bytes))
            end
        end
    end
    @testset "wide mostly-null records over one-row blocks: chunk shells dominate" begin
        fields = join(("{\"name\":\"f$i\",\"type\":[\"null\",\"long\"]}" for i in 1:1001), ",")
        ws = P("{\"type\":\"record\",\"name\":\"W\",\"fields\":[$fields]}")
        row = Dict{String,Any}("f$i" => nothing for i in 1:1001)
        w = Avro.Writer(IOBuffer(), ws; block_bytes=1)          # every datum flushes: one-row blocks
        e = try
            for _ in 1:10_000
                push!(w, row)
            end
            close(w)
            nothing
        catch err
            err
        end
        @test e isa Avro.LimitError && e.limit === :max_total_bytes && e.direction === :encode
        raised = Avro.Limits(max_total_bytes=4 << 30, max_block_bytes=64 << 20, max_block_output_bytes=2 << 30,
                             max_codec_memory=512 << 20, max_bytes=256 << 20, max_datum_bytes=256 << 20)
        wide = joinpath(dir, "wide.avro")
        w2 = Avro.Writer(wide, ws; block_bytes=1, limits=raised)
        for _ in 1:10_000
            push!(w2, row)
        end
        close(w2)
        e2 = try
            Avro.Table(open(wide))                               # the streamed consumer: chunk shells cross the ceiling
            nothing
        catch err
            err
        end
        @test e2 isa Avro.LimitError && e2.limit === :max_total_bytes && e2.direction === :decode
        tm = Avro.Table(wide)                                    # the mapped direct path preallocates exactly and fits
        @test length(tm) == 10_000 && length(Tables.columnnames(tm)) == 1001
        @test all(ismissing, Tables.getcolumn(tm, :f1))
        rl = Avro.Rows(wide)                                     # streaming stays bounded and accepts
        @test count(Returns(true), rl) == 10_000
        close(rl)
        cnt = Avro.Reader(rr -> sum(first(b) for b in Avro.eachblock(rr); init=0), wide)
        @test cnt == 10_000
    end
    @testset "admission across repeated files; exhausted admission" begin
        s = P("{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"p\",\"type\":\"long\"},{\"name\":\"q\",\"type\":\"long\"}]}")
        function buf()
            return Avro.tobuffer([(p=Int64(1), q=Int64(2))]; schema=s)
        end
        adm = Avro.SymbolAdmission(max_names=2, max_bytes=Avro.admissionbasebytes() + 64)
        @test Tables.columnnames(Avro.Table(buf(); names=adm)) == [:p, :q]
        @test Tables.columnnames(Avro.Table(buf(); names=adm)) == [:p, :q]     # already-admitted names do not count twice
        s2 = P("{\"type\":\"record\",\"name\":\"A2\",\"fields\":[{\"name\":\"r\",\"type\":\"long\"}]}")
        buf2 = Avro.tobuffer([(r=Int64(1),)]; schema=s2)
        @test_throws Avro.LimitError Avro.Table(buf2; names=adm)               # exhausted for new names
    end
end
