@testset "Parallel decode" begin
    P = Avro.parseschema
    s = P("{\"type\":\"record\",\"name\":\"PD\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"c\",\"type\":[\"null\",\"double\"]},{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"EE\",\"symbols\":[\"X\",\"Y\"]}}]}")
    n = 20_000
    rows = [(a=Int64(i), b="row-$i-payload", c=i % 3 == 0 ? missing : i / 2, e=isodd(i) ? "X" : "Y") for i in 1:n]
    function mkbytes(codec)
        return take!(Avro.tobuffer(rows; schema=s, codec=codec, block_bytes=8192))
    end

    bytes = mkbytes(:deflate)
    nullbytes = mkbytes(:null)
    reference = Tables.columntable(Avro.Table(IOBuffer(bytes); ntasks=1))
    function geterr(f)
        try
            f()
            return nothing
        catch err
            return err
        end
    end

    function entriesof(bs)
        return Avro.Reader(IOBuffer(bs)) do r
            return Avro.prescanblocks(r).entries
        end
    end
    @testset "pre-scan reports cumulative row overflow as data" begin
        overflowio = IOBuffer()
        overflowwriter = Avro.Writer(overflowio, Avro.NullSchema(); sync=zeros(UInt8, 16))
        close(overflowwriter)
        overflowbytes = take!(overflowio)
        for count in (typemax(Int64), Int64(1))
            append!(overflowbytes, Avro.encode(Avro.LongSchema(), count))
            append!(overflowbytes, Avro.encode(Avro.LongSchema(), Int64(0)))
            append!(overflowbytes, zeros(UInt8, 16))
        end
        overflowreader = Avro.Reader(overflowbytes)
        overflowpre = Avro.prescanblocks(overflowreader)
        @test length(overflowpre.entries) == 1
        @test overflowpre.totalrows == typemax(Int)
        @test overflowpre.pending isa Avro.DataError
        @test overflowpre.pending === nothing || occursin("Int range", overflowpre.pending.msg)
        close(overflowreader)
    end
    for nt in (0, -1, big(typemax(Int)) + 1)
        @test_throws ArgumentError Avro.Table(IOBuffer(bytes); ntasks=nt)
    end
    @testset "a block over max_bytes fails identically for a stream and every ntasks" begin
        capschema = P("{\"type\":\"record\",\"name\":\"Cap\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}")
        capio = IOBuffer()
        capwriter = Avro.Writer(capio, capschema; sync=zeros(UInt8, 16))
        for i in 1:5
            push!(capwriter, (x="x"^20,))
        end
        flush(capwriter)
        for i in 6:45
            push!(capwriter, (x="x"^20,))
        end
        close(capwriter)
        capbytes = take!(capio)
        caplimits = Avro.Limits(max_bytes=500)       # the header and the 105-byte first block fit; the 840-byte second does not
        e1 = geterr(() -> Avro.Table(IOBuffer(capbytes); ntasks=1, limits=caplimits))
        e8 = geterr(() -> Avro.Table(IOBuffer(capbytes); ntasks=8, limits=caplimits))
        estream = mktemp() do path, io
            write(io, capbytes)
            close(io)
            geterr(() -> open(src -> Avro.Table(src; limits=caplimits), path))
        end
        @test e1 isa Avro.LimitError && e8 isa Avro.LimitError
        @test (e1.limit, e1.observed, e1.value) == (e8.limit, e8.observed, e8.value) == (:max_bytes, 840, 500)
        @test sprint(showerror, e1) == sprint(showerror, e8)
        @test estream isa Avro.LimitError
        @test (estream.limit, estream.observed, estream.value) == (e1.limit, e1.observed, e1.value)
        @test sprint(showerror, estream) == sprint(showerror, e1)
    end
    @testset "identical results and acceptance for ntasks ∈ {1,2,8} under both limits" begin
        for nt in (1, 2, 8), lim in (Avro.Limits(), Avro.Limits(max_bytes=64 << 10))
            ct = Tables.columntable(Avro.Table(IOBuffer(bytes); ntasks=nt, limits=lim))
            for k in keys(reference)
                @test isequal(ct[k], reference[k])
            end
        end
        nullct = Tables.columntable(Avro.Table(IOBuffer(nullbytes); ntasks=8))
        @test all(isequal(nullct[k], reference[k]) for k in keys(reference))
    end
    @testset "a dense block next to a small one decodes in parallel" begin
        dense_schema = P("""{"type":"record","name":"DenseArrays","fields":[
            {"name":"xs","type":{"type":"array","items":"string"}}]}""")
        count = (1 << 21) + 1
        io = IOBuffer()
        writer = Avro.Writer(io, dense_schema; block_bytes=3 << 20)
        push!(writer, (xs=String[],))
        flush(writer)
        push!(writer, (xs=fill("", count),))
        close(writer)
        table = Avro.Table(take!(io); ntasks=2)
        @test length(table) == 2
        @test length(Tables.getcolumn(table, :xs)[2]) == count
    end
    @testset "failure selection: the lowest failing index wins for every kind pairing" begin
        entries = entriesof(nullbytes)
        @test length(entries) > 15
        function corrupt(bs, ks...; at=entries)
            bad = copy(bs)
            for k in ks
                bad[at[k].offset] ⊻= 0x80
            end
            return bad
        end
        # content vs content: two corrupted blocks, the lower one is the error at every ntasks
        bad = corrupt(nullbytes, 5, 12)
        e1 = geterr(() -> Avro.Table(IOBuffer(bad); ntasks=1))
        e8 = geterr(() -> Avro.Table(IOBuffer(bad); ntasks=8))
        @test e1 isa Avro.DataError && e8 isa Avro.DataError
        @test e1.msg == e8.msg && e1.pos == e8.pos                              # the identical error
        # head vs worker failure: block 1 is always decoded first
        bad2 = corrupt(nullbytes, 1, 2)
        eh = geterr(() -> Avro.Table(IOBuffer(bad2); ntasks=8))
        @test eh isa Avro.DataError && eh.msg == geterr(() -> Avro.Table(IOBuffer(bad2); ntasks=1)).msg
        # content vs limit: a corrupt block 5 outranks a later block over max_bytes
        mixedio = IOBuffer()
        Avro.Writer(mixedio, s; codec=:null) do w
            for blk in 1:12
                for i in 1:50
                    push!(w, (a=Int64(i), b=blk == 9 && i == 1 ? "x"^20_000 : "b", c=missing, e="X"))
                end
                flush(w)
            end
        end
        mixed = take!(mixedio)
        mixedentries = entriesof(mixed)
        @test length(mixedentries) == 12
        smallblocks = Avro.Limits(max_bytes=16 << 10)
        elim1 = geterr(() -> Avro.Table(IOBuffer(mixed); ntasks=1, limits=smallblocks))
        elim8 = geterr(() -> Avro.Table(IOBuffer(mixed); ntasks=8, limits=smallblocks))
        @test elim1 isa Avro.LimitError && elim8 isa Avro.LimitError
        @test elim1.limit === elim8.limit === :max_bytes && elim1.observed == elim8.observed
        mixedbad = corrupt(mixed, 5; at=mixedentries)
        econtent = geterr(() -> Avro.Table(IOBuffer(mixedbad); ntasks=1))
        el1 = geterr(() -> Avro.Table(IOBuffer(mixedbad); ntasks=1, limits=smallblocks))
        el8 = geterr(() -> Avro.Table(IOBuffer(mixedbad); ntasks=8, limits=smallblocks))
        @test econtent isa Avro.DataError && el1 isa Avro.DataError && el8 isa Avro.DataError
        @test el1.msg == el8.msg == econtent.msg                                # content at 5 wins over the limit at 9
        # structural pre-scan failure vs lower content failure
        cut = nullbytes[1:entries[14].offset + 4]                                # truncated inside block 14
        es1 = geterr(() -> Avro.Table(IOBuffer(cut); ntasks=1))
        es8 = geterr(() -> Avro.Table(IOBuffer(cut); ntasks=8))
        @test es1 isa Avro.DataError && es8 isa Avro.DataError && es1.msg == es8.msg
        both = geterr(() -> Avro.Table(IOBuffer(corrupt(cut, 5)); ntasks=8))
        @test both isa Avro.DataError && both.msg == e1.msg                      # the content failure at 5 wins
    end
    @testset "GC during a parallel decode" begin
        stop = Threads.Atomic{Bool}(false)
        gctask = Threads.@spawn while !stop[]
            GC.gc(false)
            yield()
        end
        try
            ct = Tables.columntable(Avro.Table(IOBuffer(bytes); ntasks=8))
            @test isequal(ct.a, reference.a) && isequal(ct.b, reference.b)
        finally
            stop[] = true
            wait(gctask)
        end
    end
end
