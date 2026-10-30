struct ByteOffsetVector <: AbstractVector{UInt8}
    data::Vector{UInt8}
    offset::Int
end

Base.size(x::ByteOffsetVector) = size(x.data)
Base.axes(x::ByteOffsetVector) = (Base.IdentityUnitRange(x.offset .+ axes(x.data, 1)),)
Base.getindex(x::ByteOffsetVector, i::Int) = x.data[i - x.offset]

@testset "Byte sources with nonstandard indices" begin
    schema = Avro.StringSchema()
    value = "hello λ"
    payload = Avro.encode(schema, value)
    store = Avro.SchemaCache()
    Avro.register!(store, schema)
    message = Avro.encodesingle(schema, value)
    reader = Avro.DatumReader(schema)
    indexedview = view(vcat(0x00, 0x00, payload), Base.IdentityUnitRange(3:length(payload) + 2))
    @test Avro.decode(schema, indexedview) == value
    @test reader(indexedview) == value
    schematext = collect(codeunits("\"string\""))
    @test Avro.parseschema(view(vcat(0x00, schematext), Base.IdentityUnitRange(2:length(schematext) + 1))) == schema
    @test Avro.decodesingle(view(vcat(0x00, message), Base.IdentityUnitRange(2:length(message) + 1)), store) == value
    for offset in (-8, 4)
        bytes = ByteOffsetVector(payload, offset)
        @test Avro.decode(schema, bytes) == value
        @test reader(bytes) == value
        @test Avro.comparebytes(schema, bytes, payload) == 0
        pair = ByteOffsetVector(vcat(payload, payload), offset)
        @test Avro.decode(schema, pair, length(payload) + 1) == (value, 2length(payload) + 1)
        @test reader(pair, length(payload) + 1) == (value, 2length(payload) + 1)
        @test Avro.parseschema(ByteOffsetVector(collect(codeunits("\"string\"")), offset)) == schema
        @test Avro.fromjson(schema, ByteOffsetVector(collect(codeunits("\"hello λ\"")), offset)) == value
        @test Avro.decodesingle(ByteOffsetVector(message, offset), store) == value
        @test_throws Avro.DataError Avro.decodesingle(ByteOffsetVector(vcat(message, 0x00), offset), store)
        @test_throws Avro.LimitError Avro.decodesingle(ByteOffsetVector(message, offset), store;
                                                    limits=Avro.Limits(max_datum_bytes=1))
    end
end
