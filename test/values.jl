@testset "Value types" begin
    @test Avro.Decimal(Int128(12345), 3) == Avro.Decimal(Int128(12345), 3)
    @test Avro.Decimal(Int128(12345), 3) != Avro.Decimal(Int128(12345), 2)
    @test hash(Avro.Decimal(1, 0)) == hash(Avro.Decimal(Int128(1), 0))
    @test Avro.WideDecimal(big(2)^100, 5) == Avro.WideDecimal(big(2)^100, 5)

    ts = Avro.Timestamp{Millisecond}(0)
    @test DateTime(ts) == DateTime(1970, 1, 1)
    @test Avro.Timestamp{Millisecond}(DateTime(1970, 1, 1)) == ts
    # the spec's Helsinki example: 2016-03-17T13:13:10.123 UTC is 1458220390123 ms / 1458220390123456 µs
    @test DateTime(Avro.Timestamp{Microsecond}(1458220390123456)) == DateTime(2016, 3, 17, 13, 13, 10, 123)
    @test Avro.Timestamp{Microsecond}(DateTime(2016, 3, 17, 13, 13, 10, 123)).ticks == 1458220390123000
    @test DateTime(Avro.Timestamp{Nanosecond}(-1)) == DateTime(1969, 12, 31, 23, 59, 59, 999)   # floor
    @test DateTime(Avro.LocalTimestamp{Millisecond}(86_400_000)) == DateTime(1970, 1, 2)
    @test_throws Avro.ConversionError DateTime(Avro.Timestamp{Millisecond}(typemax(Int64)))
    @test_throws Avro.ConversionError Avro.Timestamp{Nanosecond}(DateTime(9999, 12, 31))
    @test Avro.Timestamp{Nanosecond}(DateTime(2262, 4, 11, 23, 47, 16, 854)).ticks == 9223372036854000000
    @test sprint(show, ts) == "Avro.Timestamp{Millisecond}(0)"
    @test sprint(show, Avro.LocalTimestamp{Nanosecond}(5)) == "Avro.LocalTimestamp{Nanosecond}(5)"

    @testset "timestamp ordering and hashing across units" begin
        for T in (Avro.Timestamp, Avro.LocalTimestamp)
            millis = T{Millisecond}(-1)
            micros = T{Microsecond}(-1000)
            nanos = T{Nanosecond}(-1_000_000)
            @test millis == micros == nanos
            @test isequal(millis, micros) && isequal(micros, nanos)
            @test hash(millis) == hash(micros) == hash(nanos)
            @test length(Set([millis, micros, nanos])) == 1
            @test Dict(millis => "before epoch")[nanos] == "before epoch"
            @test millis <= micros && !(millis < micros)
            @test millis < T{Nanosecond}(-999_999) < T{Microsecond}(0)
            values = [T{Millisecond}(typemax(Int64)), T{Nanosecond}(typemax(Int64)),
                      T{Nanosecond}(typemin(Int64)), T{Millisecond}(typemin(Int64))]
            @test sort(values) == reverse(values)
            @test sort([T{Microsecond}(1), T{Microsecond}(-1), T{Microsecond}(0)]) ==
                  [T{Microsecond}(-1), T{Microsecond}(0), T{Microsecond}(1)]
        end
        instant = Avro.Timestamp{Microsecond}(1)
        localtime = Avro.LocalTimestamp{Microsecond}(1)
        @test instant != localtime && !isequal(instant, localtime)
        @test length(Set([instant, localtime])) == 2
        @test_throws MethodError isless(instant, localtime)
        @test_throws MethodError instant < localtime
        @test_throws MethodError instant <= localtime
    end

    d = Avro.Duration(UInt32(1), UInt32(2), UInt32(3))
    @test d.months == 1 && d.days == 2 && d.millis == 3

    t = Time(12, 34, 56, 789, 123, 456)
    @test Avro.truncate(t, Millisecond) == Time(12, 34, 56, 789)
    @test Avro.truncate(t, Microsecond) == Time(12, 34, 56, 789, 123)
    @test Avro.round(t, Millisecond) == Time(12, 34, 56, 789)
    @test Avro.round(Time(0, 0, 0, 0, 600), Millisecond) == Time(0, 0, 0, 1)
    @test Avro.round(Time(23, 59, 59, 999, 600), Millisecond) == Time(23, 59, 59, 999)   # never rolls over the day
end
