import Durations

@testset "Value types" begin
    @test Avro.Decimal(Int128(12345), 3) == Avro.Decimal(Int128(12345), 3)
    @test Avro.Decimal(Int128(12345), 3) != Avro.Decimal(Int128(12345), 2)
    @test hash(Avro.Decimal(1, 0)) == hash(Avro.Decimal(Int128(1), 0))
    @test Avro.WideDecimal(big(2)^100, 5) == Avro.WideDecimal(big(2)^100, 5)

    ts = Avro.timestamp(Millisecond, 0)
    @test DateTime(ts) == DateTime(1970, 1, 1)
    @test Avro.timestamp(Millisecond, DateTime(1970, 1, 1)) == ts
    # the spec's Helsinki example: 2016-03-17T13:13:10.123 UTC is 1458220390123 ms / 1458220390123456 µs
    @test DateTime(Avro.timestamp(Microsecond, 1458220390123456)) == DateTime(2016, 3, 17, 13, 13, 10, 123)
    @test Avro.timestamp(Microsecond, DateTime(2016, 3, 17, 13, 13, 10, 123)) |> Dates.value == 1458220390123000
    @test DateTime(Avro.timestamp(Nanosecond, -1)) == DateTime(1969, 12, 31, 23, 59, 59, 999)   # floor
    @test DateTime(Avro.localtimestamp(Millisecond, 86_400_000)) == DateTime(1970, 1, 2)
    @test_throws InexactError DateTime(Avro.timestamp(Millisecond, typemax(Int64)))
    @test_throws Avro.ConversionError Avro.timestamp(Nanosecond, DateTime(9999, 12, 31))
    @test Avro.timestamp(Nanosecond, DateTime(2262, 4, 11, 23, 47, 16, 854)) |> Dates.value == 9223372036854000000
    @test Avro.timestamp(Millisecond, 0) isa Dates.AbstractDateTime && Avro.localtimestamp(Millisecond, 0) isa Dates.AbstractDateTime
    @test string(Avro.timestamp(Millisecond, 1_500)) == "1970-01-01T00:00:01.5+00:00[UTC]"
    @test string(Avro.localtimestamp(Nanosecond, 5)) == "1970-01-01T00:00:00.000000005"
    @test year(Avro.timestamp(Microsecond, 1458220390123456)) == 2016
    @test Avro.LocalTimestamp{Millisecond}(Avro.timestamp(Microsecond, 2_000), UTC) == Avro.localtimestamp(Millisecond, 2)

    @testset "timestamp ordering and hashing across units" begin
        for make in (Avro.timestamp, Avro.localtimestamp)
            millis = make(Millisecond, -1)
            micros = make(Microsecond, -1000)
            nanos = make(Nanosecond, -1_000_000)
            @test millis == micros == nanos
            @test isequal(millis, micros) && isequal(micros, nanos)
            @test hash(millis) == hash(micros) == hash(nanos)
            @test length(Set([millis, micros, nanos])) == 1
            @test Dict(millis => "before epoch")[nanos] == "before epoch"
            @test millis <= micros && !(millis < micros)
            @test millis < make(Nanosecond, -999_999) < make(Microsecond, 0)
            values = Any[make(Millisecond, typemax(Int64)), make(Nanosecond, typemax(Int64)),
                         make(Nanosecond, typemin(Int64)), make(Millisecond, typemin(Int64))]
            @test sort(values) == reverse(values)
            @test sort([make(Microsecond, 1), make(Microsecond, -1), make(Microsecond, 0)]) ==
                  [make(Microsecond, -1), make(Microsecond, 0), make(Microsecond, 1)]
        end
        instant = Avro.timestamp(Microsecond, 1)
        localtime = Avro.localtimestamp(Microsecond, 1)
        @test instant != localtime && !isequal(instant, localtime)
        @test length(Set([instant, localtime])) == 2
        @test_throws Exception isless(instant, localtime)    # an instant and a wall-clock time have no order
        @test_throws Exception instant < localtime
    end

    @testset "encoding accepts any zone and any exactly representable unit" begin
        ts = Avro.parseschema("""{"type":"long","logicalType":"timestamp-millis"}""")
        lts = Avro.parseschema("""{"type":"long","logicalType":"local-timestamp-micros"}""")
        decode_ms(x) = Avro.decode(ts, Avro.encode(ts, x))
        @test decode_ms(Avro.timestamp(Microsecond, 5_000)) == Avro.timestamp(Millisecond, 5)
        zoned = Durations.ZonedTimestamp{Second,Symbol("+07:00")}(Durations.Timestamp{Second}(Dates.UTInstant(Second(3))), UTC)
        @test decode_ms(zoned) == Avro.timestamp(Millisecond, 3_000)
        @test_throws Avro.EncodeError Avro.encode(ts, Avro.timestamp(Microsecond, 1))
        @test_throws Avro.EncodeError Avro.encode(ts, Avro.localtimestamp(Millisecond, 1))     # no zone: not an instant
        @test_throws Avro.EncodeError Avro.encode(lts, Avro.timestamp(Millisecond, 1))
        @test Avro.decode(lts, Avro.encode(lts, Avro.localtimestamp(Millisecond, 2))) == Avro.localtimestamp(Microsecond, 2_000)
        @test Avro.schema(typeof(zoned |> z -> Durations.ZonedTimestamp{Millisecond,Symbol("+07:00")}(z))) == ts
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
