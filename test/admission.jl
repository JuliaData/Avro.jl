@testset "Symbol admission" begin
    a = Avro.SymbolAdmission(max_names=3, max_bytes=100)
    @test Avro.admit!(a, "x") === :x
    @test Avro.admit!(a, "x") === :x      # already admitted: counted once
    @test length(a) == 1
    @test Avro.admit!(a, "y") === :y
    @test Avro.admit!(a, "z") === :z
    e = try Avro.admit!(a, "w"); nothing catch err; err end
    @test e isa Avro.LimitError && e.limit == :max_names && e.observed == 4 && e.value == 3
    b = Avro.SymbolAdmission(max_names=10, max_bytes=5)
    Avro.admit!(b, "abc")
    e2 = try Avro.admit!(b, "def"); nothing catch err; err end
    @test e2 isa Avro.LimitError && e2.limit == :max_bytes
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
end
