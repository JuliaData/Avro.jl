@testset "Parallel decode (plan §4.9)" begin
    P = Avro.parseschema
    raised = Avro.Limits(max_total_bytes=2 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                         max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)
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
    @testset "block-table growth retains only the charged capacity" begin
        tinyrows = [(a=Int64(i), b="", c=missing, e="X") for i in 1:65]
        tinybytes = take!(Avro.tobuffer(tinyrows; schema=s, codec=:null, block_bytes=1))
        reader = Avro.Reader(IOBuffer(tinybytes))
        baseline = reader.budget.reserved
        pre = Avro.prescanblocks(reader)
        @test length(pre.entries) == 65
        @test Avro.capacity(pre.entries) == 128
        @test length(getfield(pre.entries, :storage)) == 128
        @test reader.budget.reserved - baseline == Avro.blocktablecharge(128)
        Avro.release!(reader.budget, Avro.blocktablecharge(128))
        close(reader)
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
        overflowlimits = Avro.Limits(max_block_count=typemax(Int), max_rows=typemax(Int))
        overflowreader = Avro.Reader(overflowbytes; limits=overflowlimits)
        overflowpre = Avro.prescanblocks(overflowreader)
        @test length(overflowpre.entries) == 1
        @test overflowpre.totalrows == typemax(Int)
        @test overflowpre.pending isa Avro.DataError
        @test overflowpre.pending === nothing || occursin("Int range", overflowpre.pending.msg)
        Avro.release!(overflowreader.budget,
                      Avro.blocktablecharge(Avro.capacity(overflowpre.entries)))
        close(overflowreader)
    end
    for nt in (0, -1, big(typemax(Int)) + 1)
        @test_throws ArgumentError Avro.Table(IOBuffer(bytes); ntasks=nt)
    end
    @testset "the block-output error is identical for stream and every ntasks" begin
        capschema = P("{\"type\":\"record\",\"name\":\"Cap\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}")
        capio = IOBuffer()
        capwriter = Avro.Writer(capio, capschema; sync=zeros(UInt8, 16))
        for i in 1:5
            push!(capwriter, (x=Int64(i),))
        end
        flush(capwriter)
        for i in 6:45
            push!(capwriter, (x=Int64(i),))
        end
        close(capwriter)
        capbytes = take!(capio)
        caplimits = Avro.Limits(max_block_output_bytes=80)
        e1 = geterr(() -> Avro.Table(IOBuffer(capbytes); ntasks=1, limits=caplimits))
        e8 = geterr(() -> Avro.Table(IOBuffer(capbytes); ntasks=8, limits=caplimits))
        estream = mktemp() do path, io
            write(io, capbytes)
            close(io)
            geterr(() -> open(src -> Avro.Table(src; limits=caplimits), path))
        end
        @test e1 isa Avro.LimitError && e8 isa Avro.LimitError
        @test (e1.limit, e1.observed, e1.value) == (e8.limit, e8.observed, e8.value) ==
              (:max_block_output_bytes, 88, 80)
        @test sprint(showerror, e1) == sprint(showerror, e8)
        @test estream isa Avro.LimitError
        @test (estream.limit, estream.observed, estream.value) == (e1.limit, e1.observed, e1.value)
        @test sprint(showerror, estream) == sprint(showerror, e1)
    end
    @testset "identical results and acceptance for ntasks ∈ {1,2,8}, both limits, both modes" begin
        for nt in (1, 2, 8), lim in (Avro.Limits(), raised), val in (:strict, :fast)
            t = Avro.Table(IOBuffer(bytes); ntasks=nt, limits=lim, validate=val)
            ct = Tables.columntable(t)
            for k in keys(reference)
                @test isequal(ct[k], reference[k])
            end
            st = Avro.LAST_PARALLEL_STATS[]
            @test st.peak_violations == 0
            @test st.assembly_bytes <= max(st.committed_bytes, st.nworkers == 0 ? 0 : -1)
        end
    end
    @testset "deterministic counters equal to sequential" begin
        Avro.Table(IOBuffer(bytes); ntasks=1, limits=raised)
        st1 = Avro.LAST_PARALLEL_STATS[]
        c1 = (st1.values, st1.input_bytes, st1.rows, st1.blocks)
        Avro.Table(IOBuffer(bytes); ntasks=8, limits=raised)
        st8 = Avro.LAST_PARALLEL_STATS[]
        @test (st8.values, st8.input_bytes, st8.rows, st8.blocks) == c1
        @test st8.rows == n && st8.blocks == length(entriesof(bytes))
        if Threads.nthreads() > 1
            @test st8.nworkers > 0
            @test st8.inflight_highwater >= 2
            @test st8.assembly_bytes > 0 && st8.assembly_bytes <= st8.committed_bytes
        else
            @test st8.nworkers == 0
        end
    end
    @testset "worker W includes replacement-growth overlap" begin
        if Threads.nthreads() > 1
            dense_schema = P("""{"type":"record","name":"DenseArrays","fields":[
                {"name":"xs","type":{"type":"array","items":"string"}}]}""")
            dense_limits = Avro.Limits(max_block_bytes=3 << 20,
                max_block_output_bytes=49 << 20, max_codec_memory=16 << 20,
                max_total_bytes=512 << 20, max_values_per_byte=16)
            count = (1 << 21) + 1
            io = IOBuffer()
            writer = Avro.Writer(io, dense_schema; block_bytes=3 << 20,
                                 limits=dense_limits)
            push!(writer, (xs=String[],))
            flush(writer)
            push!(writer, (xs=fill("", count),))
            close(writer)
            table = Avro.Table(take!(io); ntasks=2, limits=dense_limits)
            stats = Avro.LAST_PARALLEL_STATS[]
            @test length(table) == 2
            @test stats.nworkers == 1
            @test stats.peak_violations == 0
            @test stats.jobpeakmax > 81 << 20
        end
    end
    @testset "failed worker commit restores its reservation" begin
        emptybytes = take!(Avro.tobuffer(rows[1:0]; schema=s))
        reader = Avro.Reader(IOBuffer(emptybytes); limits=Avro.Limits(max_rows=0))
        base = reader.budget.reserved
        W = 500
        Avro.reserve!(reader.budget, W)
        jobbudget = Avro.Budget(reader.limits; available=1 << 40)
        Avro.reserve!(jobbudget, 200)
        entry = Avro.BlockEntry(1, 1, 0, 1, 1)
        job = Avro.BlockJob(entry, W, jobbudget, Threads.Event(), nothing, 0, nothing, :done)
        @test_throws Avro.LimitError Avro.commitjob!(reader, job, AbstractVector[], Int[], Avro.ParallelStats())
        @test reader.budget.reserved == base
        @test jobbudget.pending == 0
        close(reader)
    end
    @testset "forced schedules: both interleavings produce the reference" begin
        for sched in (:headslow, :workerslow)
            Avro.PARALLEL_HOOK[] = (ev, i) -> begin
                sched === :headslow && ev === :headdone && sleep(0.002)
                sched === :workerslow && ev === :workerstart && sleep(0.002)
                return nothing
            end
            try
                ct = Tables.columntable(Avro.Table(IOBuffer(bytes); ntasks=4, limits=raised))
                for k in keys(reference)
                    @test isequal(ct[k], reference[k])
                end
            finally
                Avro.PARALLEL_HOOK[] = nothing
            end
        end
    end
    @testset "worker startup failure settles and poisons the ordered job" begin
        if Threads.nthreads() > 1
            pending0 = @atomic Avro.GUARD.pending
            Avro.PARALLEL_HOOK[] = (event, index) -> begin
                event === :workerstart && error("worker startup failure $index")
                return nothing
            end
            err = try
                geterr(() -> Avro.Table(IOBuffer(bytes); ntasks=4, limits=raised))
            finally
                Avro.PARALLEL_HOOK[] = nothing
            end
            @test err isa ErrorException && occursin("worker startup failure", err.msg)
            @test (@atomic Avro.GUARD.pending) == pending0
            recovered = Tables.columntable(Avro.Table(IOBuffer(bytes); ntasks=1, limits=raised))
            @test all(isequal(recovered[name], reference[name]) for name in keys(reference))
        end
    end
    @testset "pool wrapper failure returns the pending startup reservation" begin
        if Threads.nthreads() > 1
            pending0 = @atomic Avro.GUARD.pending
            Avro.PARALLEL_HOOK[] = (event, _) -> begin
                event === :poolwrap && error("pool wrapper failure")
                return nothing
            end
            err = try
                geterr(() -> Avro.Table(IOBuffer(bytes); ntasks=4, limits=raised))
            finally
                Avro.PARALLEL_HOOK[] = nothing
            end
            @test err isa ErrorException && err.msg == "pool wrapper failure"
            @test (@atomic Avro.GUARD.pending) == pending0
        end
    end
    @testset "failure selection: the lowest failing index wins for every kind pairing" begin
        entries = entriesof(nullbytes)
        @test length(entries) > 15
        function corrupt(bs, ks...)
            bad = copy(bs)
            for k in ks
                bad[entries[k].offset] ⊻= 0x80
            end
            return bad
        end
        # content vs content: two corrupted blocks, the lower one is the error at every ntasks
        bad = corrupt(nullbytes, 5, 12)
        e1 = geterr(() -> Avro.Table(IOBuffer(bad); ntasks=1))
        e8 = geterr(() -> Avro.Table(IOBuffer(bad); ntasks=8, limits=raised))
        @test e1 isa Avro.DataError && e8 isa Avro.DataError
        @test e1.msg == e8.msg && e1.pos == e8.pos                              # the identical error
        st = Avro.LAST_PARALLEL_STATS[]
        @test st.speculative_decoded <= 7                                        # at most inflight speculative blocks
        # head vs worker failure under both forced schedules: block 1 is always the direct head
        bad2 = corrupt(nullbytes, 1, 2)
        for sched in (:headslow, :workerslow)
            Avro.PARALLEL_HOOK[] = (ev, i) -> begin
                sched === :headslow && ev === :headdone && sleep(0.002)
                sched === :workerslow && ev === :workerstart && sleep(0.002)
                return nothing
            end
            try
                eh = geterr(() -> Avro.Table(IOBuffer(bad2); ntasks=8, limits=raised))
                @test eh isa Avro.DataError && eh.msg == geterr(() -> Avro.Table(IOBuffer(bad2); ntasks=1)).msg
            finally
                Avro.PARALLEL_HOOK[] = nothing
            end
        end
        # content vs cumulative limit: the corruption at block 5 outranks a max_rows crossing at a later block
        limrows = entries[8].rowstart + entries[8].count - 1                     # crossing inside block 9
        smallrows = Avro.Limits(max_total_bytes=2 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                                max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20, max_rows=limrows)
        el = geterr(() -> Avro.Table(IOBuffer(corrupt(nullbytes, 5)); ntasks=8, limits=smallrows))
        @test el isa Avro.DataError && el.msg == e1.msg                          # content at 5 wins over the limit at 9
        elim1 = geterr(() -> Avro.Table(IOBuffer(nullbytes); ntasks=1, limits=smallrows))
        elim8 = geterr(() -> Avro.Table(IOBuffer(nullbytes); ntasks=8, limits=smallrows))
        @test elim1 isa Avro.LimitError && elim8 isa Avro.LimitError
        @test elim1.limit === elim8.limit === :max_rows && elim1.observed == elim8.observed
        # structural pre-scan failure vs lower content failure
        cut = nullbytes[1:entries[14].offset + 4]                                # truncated inside block 14
        es1 = geterr(() -> Avro.Table(IOBuffer(cut); ntasks=1))
        es8 = geterr(() -> Avro.Table(IOBuffer(cut); ntasks=8, limits=raised))
        @test es1 isa Avro.DataError && es8 isa Avro.DataError && es1.msg == es8.msg
        both = geterr(() -> Avro.Table(IOBuffer(corrupt(cut, 5)); ntasks=8, limits=raised))
        @test both isa Avro.DataError && both.msg == e1.msg                      # the content failure at 5 wins
        # a file whose total exceeds the default ceiling fails identically before any decode
        bigrows = [(a=Int64(i), b="x"^1500, c=1.0, e="X") for i in 1:250_000]
        bigraised = take!(Avro.tobuffer(bigrows; schema=s, codec=:deflate, block_bytes=1 << 20, limits=raised))
        eb1 = geterr(() -> Avro.Table(IOBuffer(bigraised); ntasks=1))
        eb8 = geterr(() -> Avro.Table(IOBuffer(bigraised); ntasks=8))
        @test eb1 isa Avro.LimitError && eb8 isa Avro.LimitError
        @test eb1.limit === eb8.limit && eb1.observed == eb8.observed
    end
    @testset "GC stress" begin
        Avro.PARALLEL_HOOK[] = (ev, i) -> (ev === :commit && GC.gc(false); nothing)
        try
            ct = Tables.columntable(Avro.Table(IOBuffer(bytes); ntasks=8, limits=raised))
            @test isequal(ct.a, reference.a) && isequal(ct.b, reference.b)
        finally
            Avro.PARALLEL_HOOK[] = nothing
        end
    end
end
