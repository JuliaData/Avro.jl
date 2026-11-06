using Test, Avro, DataDecimals, Durations
@testset "Registered shared values" begin
    schema = Avro.BytesSchema(; logical=Avro.DecimalLogical(18, 2))
    value = DataDecimals.Decimal64{2}("12.34")
    bytes = Avro.encode(schema, value)
    decoded = Avro.decode(schema, bytes)
    @test decoded isa DataDecimals.DecimalValue{Int128}
    @test decoded == value
    @test Avro.decode(schema, bytes, typeof(value)) === value
    @test Avro.schema(typeof(value)) isa Avro.BytesSchema
    wide_schema = Avro.BytesSchema(; logical=Avro.DecimalLogical(76, 2))
    wide_value = DataDecimals.Decimal256{2}("12.34")
    @test Avro.decode(wide_schema, Avro.encode(wide_schema, wide_value), typeof(wide_value)) === wide_value
    @test Avro.decode(schema, bytes, Avro.WideDecimal) == Avro.WideDecimal(big(1234), 2)
    @test_throws Exception Avro.encode(schema, DataDecimals.Decimal64{3}("12.345"))
    @test Avro.Duration(Durations.Duration(2,3,4_000_000)) == Avro.Duration(2,3,4)
    @test_throws ArgumentError Avro.Duration(Durations.Duration(0,0,1))
    @test_throws InexactError Avro.Duration(Durations.Duration(-1,0,0))
    duration_schema = Avro.schema(Durations.Duration)
    duration = Durations.Duration(2,3,4_000_000)
    @test Avro.decode(duration_schema, Avro.encode(duration_schema, duration)) == Avro.Duration(2,3,4)
    @test Avro.decode(duration_schema, Avro.encode(duration_schema, duration), Durations.Duration) == duration
    @test_throws Avro.ConversionError Avro.decode(duration_schema, Avro.encode(duration_schema, Avro.Duration(typemax(UInt32),0,0)), Durations.Duration)
    @test_throws Avro.EncodeError Avro.encode(duration_schema, Durations.Duration(0,0,1))
    @test_throws Avro.EncodeError Avro.encode(duration_schema, Durations.Duration(-1,0,0))
    @test_throws InexactError Durations.Duration(Avro.Duration(typemax(UInt32),0,0))
end
