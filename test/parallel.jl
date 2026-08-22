@testset "Parallel decode (plan §4.9)" begin
    P = Avro.parseschema
    raised = Avro.Limits(max_total_bytes=2 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                         max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)
    s = P("{\"type\":\"record\",\"name\":\"PD\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"c\",\"type\":[\"null\",\"double\"]},{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"EE\",\"symbols\":[\"X\",\"Y\"]}}]}")
    n = 20_000
    rows = [(a=Int64(i), b="row-$i-payload", c=i % 3 == 0 ? missing : i / 2, e=isodd(i) ? "X" : "Y") for i in 1:n]
    mkbytes(codec) = take!(Avro.tobuffer(rows; schema=s, codec=codec, block_bytes=8192))
    bytes = mkbytes(:deflate)
    nullbytes = mkbytes(:null)
    reference = Tables.columntable(Avro.Table(IOBuffer(bytes); ntasks=1))
    geterr(f) = try
        f()
        nothing
    catch err
        err
    end
    entriesof(bs) = Avro.Reader(IOBuffer(bs)) do r
        Avro.prescanblocks(r).entries
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
    @testset "failure selection: the lowest failing index wins for every kind pairing" begin
        entries = entriesof(nullbytes)
        @test length(entries) > 15
        corrupt(bs, ks...) = (bad = copy(bs); for k in ks
            bad[entries[k].offset] ⊻= 0x80
        end; bad)
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
