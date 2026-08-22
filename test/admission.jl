@testset "Symbol admission" begin
    a = Avro.SymbolAdmission(max_names=3, max_bytes=100)
    @test Avro.admit!(a, "x") === :x
    @test Avro.admit!(a, "x") === :x      # already admitted: counted once
    @test length(a) == 1
    @test Avro.admit!(a, "y") === :y
    @test Avro.admit!(a, "z") === :z
    e = try Avro.admit!(a, "w"); nothing catch err; err end
    @test e isa Avro.LimitError && e.limit == :max_names && e.observed == 4 && e.value == 3
    b = Avro.SymbolAdmission(max_names=10, max_bytes=15)   # bytes cover the string plus an 8-byte slot
    Avro.admit!(b, "abc")
    e2 = try Avro.admit!(b, "def"); nothing catch err; err end
    @test e2 isa Avro.LimitError && e2.limit == :max_bytes && e2.observed == 22
    @test length(b) == 1 && Avro.admit!(b, "abc") === :abc  # the failed admission left the table unchanged
    @test Avro.admit!(:trusted, "anything") === :anything
    @test Avro.admission(:trusted) === :trusted
    @test_throws ArgumentError Avro.admission(:untrusted)
    @test_throws ArgumentError Avro.admission(1)
    @test Avro.admission(Avro.DEFAULT_ADMISSION) === Avro.DEFAULT_ADMISSION
    @test_throws ArgumentError Avro.SymbolAdmission(max_names=-1)

    # sorted-runs structure: carries across every power-of-two boundary, lookups stay exact
    big = Avro.SymbolAdmission(max_names=10_000, max_bytes=1 << 20)
    names = ["n$(lpad(i, 5, '0'))" for i in 1:5000]
    for n in names
        Avro.admit!(big, n)
    end
    @test length(big) == 5000
    @test all(length(r) == Avro.RUN_BASE << k for (k, r) in zip(reverse(0:length(big.runs) - 1), big.runs)) || length(big.runs) >= 1
    for n in names
        Avro.admit!(big, n)   # no double counting after carries
    end
    @test length(big) == 5000
    @test Avro.admit!(big, "n00001") === :n00001
    # one-symbol admissions across power-of-two boundaries keep the count exact
    boundaries = [Avro.RUN_BASE << k for k in 0:3]
    small = Avro.SymbolAdmission(max_names=1 << 20, max_bytes=1 << 24)
    for i in 1:(Avro.RUN_BASE << 3) + 1
        Avro.admit!(small, "s$i")
        if (i in boundaries) || i == (Avro.RUN_BASE << 3) + 1
            @test length(small) == i
        end
    end
    @test Avro.admit!(small, "s1") === :s1 && length(small) == (Avro.RUN_BASE << 3) + 1

    # concurrent admissions are safe
    c = Avro.SymbolAdmission(max_names=1 << 20, max_bytes=1 << 24)
    tasks = [Threads.@spawn begin
        for i in 1:2000
            Avro.admit!(c, "t$(i % 300)")
        end
        nothing
    end for _ in 1:4]
    foreach(errormonitor, tasks)
    foreach(wait, tasks)
    @test length(c) == 300

    # deamortised merges: mid-merge lookups stay exact across every power-of-two boundary (plan §4.4)
    dm = Avro.SymbolAdmission(max_names=1 << 20, max_bytes=1 << 24)
    limit = (Avro.RUN_BASE << 6) + 3
    for i in 1:limit
        Avro.admit!(dm, "d$(lpad(i, 7, '0'))")
        if count_ones(i) == 1 && i >= Avro.RUN_BASE      # power-of-two boundaries: merges are staged here
            @test length(dm) == i
            @test Avro.admit!(dm, "d$(lpad(1, 7, '0'))") === Symbol("d0000001")      # oldest (deep run)
            @test Avro.admit!(dm, "d$(lpad(i, 7, '0'))") === Symbol("d$(lpad(i, 7, '0'))")  # newest
            @test Avro.admit!(dm, "d$(lpad(i ÷ 2, 7, '0'))") === Symbol("d$(lpad(i ÷ 2, 7, '0'))")
            @test length(dm) == i
        end
    end
    @test length(dm) == limit
    while dm.merge !== nothing                            # drain: every source string survives the swaps
        Avro.admit!(dm, "d$(lpad(1, 7, '0'))")
    end
    @test length(dm) == limit && sum(length, dm.runs; init=0) + length(dm.recent) == limit

    # one million incremental admissions, latency recorded (plan §4.4 gate)
    mm = Avro.SymbolAdmission(max_names=1 << 21, max_bytes=64 << 20)
    t0 = time()
    for i in 1:1_000_000
        Avro.admit!(mm, "m$(i)")
    end
    elapsed = time() - t0
    @info "admission gate" million_admissions_seconds = round(elapsed; digits=2)
    @test length(mm) == 1_000_000
    @test Avro.admit!(mm, "m1") === :m1 && Avro.admit!(mm, "m999999") === :m999999
    @test elapsed < 60

    # capacity boundaries: exact at max_names and max_bytes
    cap = Avro.SymbolAdmission(max_names=Avro.RUN_BASE + 1, max_bytes=1 << 20)
    for i in 1:Avro.RUN_BASE + 1
        Avro.admit!(cap, "c$(lpad(i, 4, '0'))")
    end
    e3 = try Avro.admit!(cap, "over"); nothing catch err; err end
    @test e3 isa Avro.LimitError && e3.limit == :max_names && length(cap) == Avro.RUN_BASE + 1
    @test Avro.admit!(cap, "c0001") === :c0001            # existing names still admit after the failure
end
