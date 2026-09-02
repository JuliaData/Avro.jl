using Decimals: Decimals, Decimal, Decimal32, Decimal64, Decimal128, Decimal256

# `bytes` framing of a hand-built payload: the zig-zag length (< 64 bytes: one byte) then the payload.
bytesdatum(payload::Vector{UInt8}) = vcat(UInt8(2 * length(payload)), payload)

decschema(precision, scale) =
    Avro.parseschema("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":$precision,\"scale\":$scale}")

fixeddecschema(size, precision, scale; name="D$(size)_$(precision)_$(scale)") =
    Avro.parseschema("{\"type\":\"fixed\",\"name\":\"$name\",\"size\":$size,\"logicalType\":\"decimal\",\"precision\":$precision,\"scale\":$scale}")

@testset "Decimals.jl decimals" begin

@testset "schema derivation" begin
    s = Avro.schema(Decimal{9,2,Int32})
    @test s isa Avro.BytesSchema
    @test s.logical == Avro.DecimalLogical(9, 2)
    @test Avro.json(s) == "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}"
    @test Avro.json(Avro.schema(Decimal256{40})) ==
          "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":76,\"scale\":40}"
    @test Avro.schema(Decimal64{4}("1.5")) == Avro.schema(Decimal{18,4,Int64})
    # unnamed `bytes` is why two same-shaped decimals can sit in one record (a derived `fixed` would
    # define the same Avro name twice)
    rs = Avro.schema(@NamedTuple{a::Decimal{9,2,Int32}, b::Decimal{9,2,Int32}})
    @test rs.fields[1].schema.logical == rs.fields[2].schema.logical == Avro.DecimalLogical(9, 2)
    @test Avro.schema(Union{Missing,Decimal32{3}}) isa Avro.UnionSchema
end

@testset "bytes round trip" begin
    for (P, S, T) in ((9, 2, Int32), (18, 6, Int64), (38, 10, Int128), (76, 30, Decimals.Int256))
        D = Decimal{P,S,T}
        s = Avro.schema(D)
        for text in ("0", "1", "-1", "12.5", "-12.5", string(typemax(D)), string(typemin(D)))
            x = D(text)
            @test Avro.decode(s, Avro.encode(s, x), D) === x
        end
    end
    # every tier's extreme values survive the exact byte width they need
    @test Avro.decode(Avro.schema(Decimal32{0}), Avro.encode(Avro.schema(Decimal32{0}), Decimal32{0}("999999999")), Decimal32{0}) ===
          Decimal32{0}("999999999")
    d76 = Decimal256{0}("-" * "9"^76)
    s76 = Avro.schema(Decimal256{0})
    @test Avro.decode(s76, Avro.encode(s76, d76), Decimal256{0}) === d76
    @test length(Avro.encode(s76, d76)) == 33          # one length byte plus the 32-byte coefficient
end

@testset "fixed round trip" begin
    for (n, P, S, T) in ((4, 9, 2, Int32), (8, 18, 6, Int64), (16, 38, 10, Int128), (32, 76, 30, Decimals.Int256))
        D = Decimal{P,S,T}
        s = fixeddecschema(n, P, S)
        for text in ("0", "1", "-1", "12.5", "-12.5", string(typemax(D)), string(typemin(D)))
            x = D(text)
            enc = Avro.encode(s, x)
            @test length(enc) == n                     # `fixed` is always exactly `size` bytes
            @test Avro.decode(s, enc, D) === x
        end
    end
    # a value that needs more bytes than the fixed holds is an EncodeError, not a truncation
    s = fixeddecschema(2, 4, 0)
    @test_throws Avro.EncodeError Avro.encode(s, Decimal{9,0,Int32}("40000"))
end

@testset "minimal-length encoding" begin
    s = decschema(4, 2)
    D = Decimal{4,2,Int32}
    cases = ["0" => UInt8[0x00], "1.00" => UInt8[0x64], "-1.00" => UInt8[0x9c],
             "1.27" => UInt8[0x7f], "1.28" => UInt8[0x00, 0x80], "-1.28" => UInt8[0x80],
             "-1.29" => UInt8[0xff, 0x7f], "99.99" => UInt8[0x27, 0x0f], "-99.99" => UInt8[0xd8, 0xf1]]
    for (text, payload) in cases
        x = D(text)
        @test Avro.encode(s, x) == bytesdatum(payload)
        @test Avro.decode(s, bytesdatum(payload), D) === x
    end
    # non-minimal payloads are sign-extended on read (the spec only constrains what writers emit)
    @test Avro.decode(s, bytesdatum(UInt8[0x00, 0x00, 0x64]), D) === D("1.00")
    @test Avro.decode(s, bytesdatum(UInt8[0xff, 0xff, 0xff, 0x9c]), D) === D("-1.00")
    @test Avro.decode(s, bytesdatum(UInt8[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), D) === D("0")
    # `fixed` sign-extends to its declared size
    fs = fixeddecschema(4, 4, 2)
    @test Avro.encode(fs, D("-1.00")) == UInt8[0xff, 0xff, 0xff, 0x9c]
    @test Avro.encode(fs, D("1.00")) == UInt8[0x00, 0x00, 0x00, 0x64]
    @test Avro.decode(fs, UInt8[0xff, 0xff, 0xff, 0x9c], D) === D("-1.00")
end

@testset "corrupt payloads" begin
    s = decschema(4, 2)
    D = Decimal{4,2,Int32}
    @test_throws Avro.DataError Avro.decode(s, bytesdatum(UInt8[]), D)               # empty payload
    @test_throws Avro.DataError Avro.decode(s, bytesdatum(UInt8[0x27, 0x10]), D)     # 10000 > 10^4 − 1
    @test_throws Avro.DataError Avro.decode(s, bytesdatum(UInt8[0xd8, 0xf0]), D)     # −10000
    # a payload wider than the target's storage integer whose leading bytes are not sign extension
    wide = decschema(9, 0)
    @test_throws Avro.DataError Avro.decode(wide, bytesdatum(UInt8[0x01, 0x00, 0x00, 0x00, 0x00]), Decimal{9,0,Int32})
    # the generic route applies the same precision rule
    @test_throws Avro.DataError Avro.decode(s, bytesdatum(UInt8[0x27, 0x10]))
end

@testset "admission exactness" begin
    s = decschema(9, 2)
    x = Decimal{9,2,Int32}("12.34")
    enc = Avro.encode(s, x)
    # exact scale, covering precision: the direct route
    @test Avro.decode(s, enc, Decimal{9,2,Int32}) === x
    @test Avro.decode(s, enc, Decimal{18,2,Int64}) === Decimal{18,2,Int64}("12.34")
    @test Avro.decode(s, enc, Decimal{38,2,Int128}) === Decimal{38,2,Int128}("12.34")
    # a target that leaves the storage integer out takes Decimals.jl's own tier
    @test Avro.decode(s, enc, Decimal{9,2}) === x
    @test Avro.decode(s, enc, Decimal{40,2}) === Decimal{40,2}("12.34")
    @test Avro.decode(s, enc, Decimal{9,2,Int64}) === Decimal{9,2,Int64}("12.34")
    # a differing scale is a different number: never silently rescaled
    @test_throws Avro.ConversionError Avro.decode(s, enc, Decimal{9,3,Int32})
    @test_throws Avro.ConversionError Avro.decode(s, enc, Decimal{9,0,Int32})
    # a narrower precision is not admitted; the semantic route still converts what fits, per value
    @test Avro.decode(s, enc, Decimal{6,2,Int32}) === Decimal{6,2,Int32}("12.34")
    big = Avro.encode(s, Decimal{9,2,Int32}("1234567.89"))
    @test_throws Avro.ConversionError Avro.decode(s, big, Decimal{6,2,Int32})
    # writing checks the schema's scale and precision
    @test_throws Avro.EncodeError Avro.encode(s, Decimal{9,3,Int32}("12.345"))
    @test_throws Avro.EncodeError Avro.encode(decschema(4, 2), Decimal{9,2,Int32}("12345.67"))
end

@testset "records, arrays and unions" begin
    rec = Avro.parseschema("""
    {"type":"record","name":"Money","fields":[
      {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":18,"scale":4}},
      {"name":"fee","type":["null",{"type":"fixed","name":"Fee","size":8,"logicalType":"decimal","precision":18,"scale":4}]},
      {"name":"history","type":{"type":"array","items":{"type":"bytes","logicalType":"decimal","precision":18,"scale":4}}}]}""")
    D = Decimal{18,4,Int64}
    R = @NamedTuple{amount::D, fee::Union{Missing,D}, history::Vector{D}}
    for fee in (D("-0.5"), missing)
        v = (amount=D("1234.5678"), fee=fee, history=[D("1.0"), D("-2.25")])
        out = Avro.decode(rec, Avro.encode(rec, v), R)
        @test out.amount === v.amount
        @test isequal(out.fee, v.fee)
        @test out.history == v.history
    end
    # a general (non-nullable) union picks the decimal branch by `accepts`
    us = Avro.parseschema("[\"string\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}]")
    U = Union{String,Decimal{9,2,Int32}}
    for v in ("hi", Decimal{9,2,Int32}("-3.14"))
        @test Avro.decode(us, Avro.encode(us, v), U) === v
    end
    # scale 0 and scale == precision
    zero0 = Avro.schema(Decimal{9,0,Int32})
    @test Avro.decode(zero0, Avro.encode(zero0, Decimal{9,0,Int32}("-70")), Decimal{9,0,Int32}) ===
          Decimal{9,0,Int32}("-70")
    high = Avro.schema(Decimal{9,9,Int32})
    hv = Decimal{9,9,Int32}("0.123456789")
    @test Avro.decode(high, Avro.encode(high, hv), Decimal{9,9,Int32}) === hv
end

@testset "schema resolution" begin
    w = decschema(9, 2)
    x = Decimal{9,2,Int32}("12.34")
    enc = Avro.encode(w, x)
    # a reader that ignores the logical type still reads the datum as its underlying type
    plain = Avro.parseschema("\"bytes\"")
    @test Avro.decode(w, enc; reader_schema=plain) == UInt8[0x04, 0xd2]
    # ...and a plain writer read through a decimal reader takes the reader's interpretation
    @test Avro.decode(plain, Avro.encode(plain, UInt8[0x04, 0xd2]); reader_schema=w) == Avro.Decimal(1234, 2)
    @test Avro.decode(plain, Avro.encode(plain, UInt8[0x04, 0xd2]), Decimal{9,2,Int32}; reader_schema=w) === x
    # two recognised decimals must agree on both precision and scale (plan §4.7)
    @test_throws Avro.ResolutionError Avro.decode(w, enc; reader_schema=decschema(9, 3))
    @test_throws Avro.ResolutionError Avro.decode(w, enc; reader_schema=decschema(8, 2))
    @test Avro.decode(w, enc; reader_schema=decschema(9, 2)) == Avro.Decimal(1234, 2)
    # the same rule on `fixed`, where the size must match first
    fw = fixeddecschema(4, 9, 2; name="FR")
    fenc = Avro.encode(fw, x)
    @test Avro.decode(fw, fenc; reader_schema=Avro.parseschema("{\"type\":\"fixed\",\"name\":\"FR\",\"size\":4}")) ==
          Avro.Fixed(Avro.parseschema("{\"type\":\"fixed\",\"name\":\"FR\",\"size\":4}"), UInt8[0x00, 0x00, 0x04, 0xd2])
    @test_throws Avro.ResolutionError Avro.decode(fw, fenc; reader_schema=fixeddecschema(4, 9, 3; name="FR"))
    # a `string` writer promoted into a `bytes` decimal reader converts through the semantic route
    ss = Avro.parseschema("\"string\"")
    senc = Avro.encode(ss, String(UInt8[0x64]))            # a payload that is also valid UTF-8
    @test Avro.decode(ss, senc, Decimal{9,2,Int32}; reader_schema=w) === Decimal{9,2,Int32}("1.00")
end

@testset "json encoding" begin
    s = decschema(4, 2)
    D = Decimal{4,2,Int32}
    @test Avro.fromjson(s, Avro.tojson(s, D("-1.00")), D) === D("-1.00")
    fs = fixeddecschema(4, 4, 2)
    @test Avro.fromjson(fs, Avro.tojson(fs, D("12.34")), D) === D("12.34")
end

@testset "allocation gate and little-endian plans" begin
    budget = Avro.Budget(Avro.Limits(); available=1 << 40)
    Avro.addinput!(budget, 1 << 20)
    kernel(plan, d, names) = (d.pos = 1; Avro.decodetyped(plan, d, names))
    measure(plan, d, names) = (kernel(plan, d, names); @allocated(kernel(plan, d, names)))
    for D in (Decimal{9,2,Int32}, Decimal{18,2,Int64}, Decimal{38,2,Int128}, Decimal{76,2,Decimals.Int256})
        for s in (Avro.schema(D), fixeddecschema(cld(Base.precision(D), 2) + 1, Base.precision(D), 2))
            x = D("-1234.5")
            reader = Avro.DatumReader(s, D)
            enc = Avro.encode(s, x)
            @test reader(enc) === x
            @test measure(reader.plan, Avro.Decoder(enc, budget), reader.names) == 0
        end
    end
    # the 1.x native-endian recovery (`decimal_byteorder=:little`) reaches the typed route unchanged
    D = Decimal{38,6,Int128}
    s = Avro.schema(D)
    x = D("-1234.5")
    enc = Avro.encode(s, x)
    little = Avro.typedplan(D, s, Avro.littledecimals(Avro.readplan(s)), Avro.Limits())
    flipped = vcat(enc[1], reverse(enc[2:end]))
    @test kernel(little, Avro.Decoder(flipped, budget), Avro.DEFAULT_ADMISSION) === x
end

@testset "container files" begin
    D = Decimal{18,4,Int64}
    rows = [(id=Int64(i), amount=D(string(i, ".", lpad(i, 4, '0')))) for i in 1:20]
    io = IOBuffer()
    Avro.write(io, rows)
    bytes = take!(io)
    back = Avro.Rows(bytes; T=@NamedTuple{id::Int64, amount::D}) |> collect
    @test length(back) == 20
    @test all(i -> back[i] === rows[i], 1:20)
    tbl = Avro.Table(bytes)
    @test Tables.getcolumn(tbl, :amount) == [Avro.Decimal(Decimals.unscaled(r.amount), 4) for r in rows]
end

end
