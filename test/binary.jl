struct StableWriteFields
    a::Int64
end

struct ThrowingWriteFields
    a::Int64
end

@enum BranchShade branchred branchgreen

function StructUtils.fieldtags(::Avro.AvroStyle, ::Type{ThrowingWriteFields})
    return error("field tag failure")
end

function symbollookupalloc(record, iterations)
    total = Int32(0)
    for _ in 1:iterations
        total += record[:a]::Int32
        total += record.a::Int32
        total += get(record, :a, Int32(0))::Int32
        total += Tables.getcolumn(record, :a)::Int32
        haskey(record, :a) || error("missing symbol key")
    end
    return total
end

function symbollookupbytes(record::R) where {R}
    symbollookupalloc(record, 1)
    return @allocated symbollookupalloc(record, 1_000)
end

@testset "Binary core" begin
    P = Avro.parseschema
    function enc(s, x)
        return Avro.encode(P(s), x)
    end

    function dec(s, b)
        return Avro.decode(P(s), b)
    end

    function hex(x)
        return bytes2hex(x)
    end

    @testset "spec examples" begin
        @test hex(enc("\"int\"", 27)) == "36" && hex(enc("\"string\"", "foo")) == "06666f6f"
        @test hex(enc("{\"type\":\"array\",\"items\":\"long\"}", [3, 27])) == "04063600"
        @test hex(enc("{\"type\":\"map\",\"values\":\"long\"}", Dict("a" => 1))) == "02026102" * "00"
        @test hex(enc("[\"null\",\"string\"]", missing)) == "00" && hex(enc("[\"null\",\"string\"]", "a")) == "020261"
        @test hex(enc("[\"string\",\"null\"]", missing)) == "02" && hex(enc("[\"string\",\"null\"]", "a")) == "000261"
        @test hex(enc("{\"type\":\"enum\",\"name\":\"Suit\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}", :HEARTS)) == "02"
        @test dec("{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}", hex2bytes("3606666f6f")).b == "foo"
        @test dec("\"long\"", hex2bytes("36")) == 27
    end

    @testset "primitives: boundaries and bit patterns" begin
        for (v, h) in ((0, "00"), (-1, "01"), (1, "02"), (-2, "03"), (2, "04"), (-64, "7f"), (64, "8001"), (typemax(Int64), "feffffffffffffffff01"), (typemin(Int64), "ffffffffffffffffff01"))
            @test hex(enc("\"long\"", v)) == h
            @test dec("\"long\"", hex2bytes(h)) == v
        end
        @test hex(enc("\"int\"", typemax(Int32))) == "feffffff0f" && dec("\"int\"", hex2bytes("feffffff0f")) == typemax(Int32)
        @test hex(enc("\"int\"", typemin(Int32))) == "ffffffff0f" && dec("\"int\"", hex2bytes("ffffffff0f")) == typemin(Int32)
        @test_throws Avro.DataError dec("\"int\"", hex2bytes("ffffffff1f"))      # overflows 32 bits
        @test_throws Avro.DataError dec("\"int\"", hex2bytes("8080808080 00"[1:10]))   # 6th byte
        @test_throws Avro.DataError dec("\"long\"", hex2bytes("ffffffffffffffffff02"))  # bit beyond the 64th
        @test_throws Avro.DataError dec("\"long\"", hex2bytes("80808080808080808080 00"[1:20]))   # 11 bytes
        @test_throws Avro.DataError dec("\"long\"", hex2bytes("80"))             # truncated varint
        @test_throws Avro.DataError dec("\"long\"", UInt8[])
        @test_throws Avro.DataError dec("\"boolean\"", UInt8[0x02])
        @test dec("\"boolean\"", UInt8[0x01]) === true && dec("\"boolean\"", UInt8[0x00]) === false
        @test dec("\"boolean\"", UInt8[0x01]) === true
        @test_throws Avro.DataError dec("\"int\"", UInt8[0x36, 0x00])           # trailing byte
        @test_throws Avro.DataError dec("\"float\"", UInt8[0x00, 0x00, 0x00])
        @test_throws Avro.DataError dec("\"double\"", UInt8[0x00])
        nanpayload = reinterpret(Float64, 0x7ff80000deadbeef)
        @test reinterpret(UInt64, dec("\"double\"", enc("\"double\"", nanpayload))) == 0x7ff80000deadbeef
        @test reinterpret(UInt32, dec("\"float\"", enc("\"float\"", reinterpret(Float32, 0x7fc00abc)))) == 0x7fc00abc
        @test dec("\"double\"", enc("\"double\"", -0.0)) === -0.0 && dec("\"float\"", enc("\"float\"", -0.0f0)) === -0.0f0
        @test hex(enc("\"float\"", 1.0f0)) == "0000803f" && hex(enc("\"double\"", 1.0)) == "000000000000f03f"
        @test dec("\"string\"", enc("\"string\"", "héllo 😀")) == "héllo 😀"
        @test dec("\"bytes\"", enc("\"bytes\"", UInt8[0, 255])) == UInt8[0, 255]
        @test_throws Avro.DataError dec("\"string\"", UInt8[0x02, 0xff])          # invalid UTF-8
        @test_throws Avro.DataError dec("\"string\"", UInt8[0x04, 0x61])          # length beyond the data
        @test_throws Avro.DataError dec("\"string\"", UInt8[0x01])                # negative length
        @test dec("\"string\"", UInt8[0x00]) == "" && dec("\"bytes\"", UInt8[0x00]) == UInt8[]
        @test_throws Avro.EncodeError enc("\"string\"", String([0xff]))
        @test_throws Avro.EncodeError enc("\"int\"", 2147483648)
        @test_throws Avro.EncodeError enc("\"int\"", true)
        @test_throws Avro.EncodeError enc("\"float\"", 1.0)
        @test enc("\"float\"", 1) == enc("\"float\"", 1.0f0) && enc("\"double\"", 1.0f0) == enc("\"double\"", 1.0)
        @test_throws Avro.EncodeError enc("\"long\"", "1")
        @test_throws Avro.EncodeError enc("\"null\"", 0)
        @test enc("\"null\"", nothing) == UInt8[] && enc("\"null\"", missing) == UInt8[] && dec("\"null\"", UInt8[]) === missing
        @test dec("\"long\"", enc("\"long\"", UInt64(5))) == 5 && dec("\"int\"", enc("\"int\"", Int8(-3))) == -3
        @test_throws Avro.EncodeError enc("\"long\"", typemax(UInt64))
        @test dec("\"string\"", enc("\"string\"", :sym)) == "sym" && dec("\"string\"", enc("\"string\"", 'c')) == "c"
    end

    @testset "a string decodes and encodes exactly when its bytes are UTF-8" begin
        # ASCII strings of every length to 30 with a valid or invalid sequence at each position, decoded both at the
        # end of the input and followed by another field.
        pair = "{\"type\":\"record\",\"name\":\"P\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"},{\"name\":\"n\",\"type\":\"long\"}]}"
        inserts = (UInt8[0x7f], UInt8[0x80], UInt8[0xff], UInt8[0xc3], UInt8[0xc3, 0xa9], UInt8[0xc0, 0xaf],
                   UInt8[0xed, 0xa0, 0x80], UInt8[0xf0, 0x9f, 0x98, 0x80], UInt8[0xf4, 0x90, 0x80, 0x80])
        for n in 0:30, at in 1:n + 1, ins in inserts
            bytes = vcat(fill(UInt8('a'), at - 1), ins, fill(UInt8('b'), max(0, n - at + 1 - length(ins))))
            valid = isvalid(String, bytes)
            alone = vcat(UInt8(2 * length(bytes)), bytes)
            if valid
                @test dec("\"string\"", alone) == String(copy(bytes))
                r = dec(pair, vcat(alone, 0x02))
                @test r.s == String(copy(bytes)) && r.n == 1
                @test enc("\"string\"", String(copy(bytes))) == alone
            else
                @test_throws Avro.DataError dec("\"string\"", alone)
                @test_throws Avro.DataError dec(pair, vcat(alone, 0x02))
                @test_throws Avro.EncodeError enc("\"string\"", String(copy(bytes)))
            end
        end
    end

    @testset "fixed, enum, unions, branch recovery" begin
        f = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":2}"
        @test hex(enc(f, UInt8[1, 2])) == "0102" && hex(enc(f, (0x01, 0x02))) == "0102"
        @test dec(f, hex2bytes("0102")) == Avro.Fixed(P(f), UInt8[1, 2])
        @test_throws Avro.EncodeError enc(f, UInt8[1])
        @test_throws Avro.EncodeError enc(f, "ab")
        @test_throws Avro.DataError dec(f, UInt8[1])
        g = Avro.FixedSchema("G", 2)
        @test_throws Avro.EncodeError enc(f, Avro.Fixed(g, UInt8[1, 2]))      # identity mismatch
        mutable_fixed = Avro.Fixed(P(f), UInt8[1, 2])
        push!(mutable_fixed.bytes, 0x03)
        @test_throws Avro.EncodeError enc(f, mutable_fixed)
        e = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}"
        @test hex(enc(e, "B")) == "02" && hex(enc(e, :A)) == "00" && hex(enc(e, Avro.EnumValue(P(e), 2))) == "02"
        @test_throws Avro.EncodeError enc(e, "C")
        @test_throws Avro.EncodeError enc(e, 1)
        hugeenumsymbol = "X"^1_000_000
        enumschema = P(e)
        hugeenumfailure = () -> try
            Avro.encode(enumschema, hugeenumsymbol)
            nothing
        catch err
            err
        end
        hugeenumerror = hugeenumfailure()
        @test hugeenumerror isa Avro.EncodeError
        @test hugeenumerror.path == "\$"
        @test hugeenumerror.schema === enumschema
        @test sizeof(hugeenumerror.msg) < 256
        hugeinteger = big(10)^100_000
        for integerschema in (Avro.IntSchema(), Avro.LongSchema())
            integererror = try
                Avro.encode(integerschema, hugeinteger)
                nothing
            catch err
                err
            end
            @test integererror isa Avro.EncodeError
            @test occursin("integer does not fit", integererror.msg)
            @test occursin("BigInt", integererror.msg)
            @test sizeof(integererror.msg) < 256
        end
        @test_throws Avro.DataError dec(e, UInt8[0x04])                        # index out of range
        @test_throws Avro.DataError dec(e, UInt8[0x01])                        # negative index
        e2 = P("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"B\",\"A\"]}")
        @test hex(enc(e, Avro.EnumValue(e2, 1))) == "02"                        # remapped by symbol, never by index
        @test_throws Avro.DataError dec("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[]}", UInt8[0x00])
        @test_throws Avro.EncodeError enc("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[]}", "A")
        u = "[\"int\",\"string\",{\"type\":\"record\",\"name\":\"R\",\"fields\":[]},{\"type\":\"fixed\",\"name\":\"F\",\"size\":1},\"double\"]"
        @test hex(enc(u, Int32(1))) == "0002"                  # exact representation type
        @test hex(enc(u, "x")) == "020278"
        @test hex(enc(u, 1)) == "0002"                         # Int64 is not a representation type: first accepting branch (int)
        @test hex(enc(u, 2.5)) == "080000000000000440"
        @test hex(enc(u, Avro.UnionValue(5, 1))) == "08000000000000f03f"
        @test hex(enc(u, Avro.Record(P(u).branches[3], []))) == "04"
        @test hex(enc(u, Avro.Fixed(P(u).branches[4], UInt8[7]))) == "0607"
        longfixed = Avro.FixedSchema("A"^1025, 1)
        longunion = Avro.UnionSchema([longfixed, Avro.StringSchema()])
        longvalue = Avro.Fixed(longfixed, UInt8[1])
        @test Avro.encode(longunion, longvalue) == UInt8[0, 1]
        @test_throws Avro.EncodeError enc(u, Avro.UnionValue(6, 1))
        @test_throws Avro.EncodeError enc(u, Avro.Fixed(Avro.FixedSchema("F", 2), UInt8[1, 2]))   # same name, different size
        recordbranch = Avro.UnionSchema([Avro.RecordSchema("KindIdentity"), Avro.StringSchema()])
        enumimpostor = Avro.EnumSchema("KindIdentity", ["A"])
        wrongenum = Avro.EnumValue(enumimpostor, 1)
        enumbranch = Avro.UnionSchema([Avro.EnumSchema("KindIdentityEnum", ["A"]),
                                       Avro.StringSchema()])
        wrongrecord = Avro.Record(Avro.RecordSchema("KindIdentityEnum"), [])
        fixedbranch = Avro.UnionSchema([Avro.FixedSchema("KindIdentityFixed", 1),
                                        Avro.StringSchema()])
        wrongfixedrecord = Avro.Record(Avro.RecordSchema("KindIdentityFixed"), [])
        @test Avro.encode(recordbranch, wrongenum)[1] == 0x02                  # an enum value is also a string
        for (schema, value) in ((enumbranch, wrongrecord), (fixedbranch, wrongfixedrecord))
            @test_throws Avro.EncodeError Avro.encode(schema, value)
            @test_throws Avro.EncodeError Avro.tojson(schema, value)
        end
        @test_throws Avro.EncodeError enc(u, missing)
        @test_throws Avro.EncodeError enc("[]", 1)
        @test_throws Avro.DataError dec("[]", UInt8[0x00])
        @test dec(u, hex2bytes("020278")) == "x"                  # union values decode to the bare branch value
        @test dec(u, hex2bytes("0002")) === Int32(1) && dec(u, hex2bytes("04")) == Avro.Record(P(u).branches[3], [])
        @test Avro.ordinal(Avro.UnionValue(2, "x")) == 1
        @test dec("[\"null\",\"int\"]", hex2bytes("0236")) == 27 && dec("[\"null\",\"int\"]", hex2bytes("00")) === missing
        @test dec("[\"null\"]", hex2bytes("00")) === missing
        @test_throws Avro.DataError dec("[\"null\",\"int\"]", hex2bytes("04"))
        @test hex(enc("[\"null\",\"int\"]", nothing)) == "00" && hex(enc("[\"null\",\"int\"]", Avro.UnionValue(2, 3))) == "0206"
        @test Avro.encode(P("[\"int\",\"string\"]"), Avro.UnionValue(1, Int32(0))) == UInt8[0x00, 0x00]
        enumunion = "[{\"type\":\"enum\",\"name\":\"BranchRed\",\"symbols\":[\"branchred\"]},{\"type\":\"enum\",\"name\":\"BranchGreen\",\"symbols\":[\"branchgreen\"]}]"
        @test hex(enc(enumunion, branchred)) == "0000"
        @test hex(enc(enumunion, branchgreen)) == "0200"
    end

    @testset "union branch choice: records by their fields, arrays only from collections" begin
        u = P("""[{"type":"record","name":"A","fields":[{"name":"x","type":"long"}]},
                   {"type":"record","name":"B","fields":[{"name":"x","type":"long"},{"name":"y","type":"string"}]}]""")
        for v in ((x=1, y="kept"), Dict("x" => 1, "y" => "kept"))
            d = Avro.decode(u, Avro.encode(u, v))
            @test Avro.fullname(Avro.schema(d)) == "B" && d.y == "kept"
        end
        @test Avro.fullname(Avro.schema(Avro.decode(u, Avro.encode(u, (x=1,))))) == "A"
        lacks = P("""[{"type":"record","name":"A","fields":[{"name":"z","type":"long"}]},
                       {"type":"record","name":"B","fields":[{"name":"x","type":"long"}]}]""")
        @test Avro.fullname(Avro.schema(Avro.decode(lacks, Avro.encode(lacks, (x=1,))))) == "B"
        @test_throws Avro.EncodeError Avro.encode(P("""{"type":"array","items":"int"}"""), 5)
        @test_throws Avro.EncodeError Avro.encode(P("""["null",{"type":"array","items":"int"}]"""), 5)
        @test_throws Avro.EncodeError Avro.encode(P("""{"type":"array","items":"string"}"""), 'x')
        scalar = P("""[{"type":"array","items":"long"},"double"]""")
        @test Avro.decode(scalar, Avro.encode(scalar, 5)) === 5.0
        @test Avro.decode(scalar, Avro.encode(scalar, [5])) == [5]
    end

    @testset "arrays and maps: blocks, sized blocks, cross-form fixtures" begin
        arr = "{\"type\":\"array\",\"items\":\"long\"}"
        @test dec(arr, hex2bytes("00")) == Int64[] && hex(enc(arr, Int64[])) == "00"
        @test dec(arr, hex2bytes("0206" * "0236" * "00")) == [3, 27]             # two blocks
        @test dec(arr, hex2bytes("03040636" * "00")) == [3, 27]                  # sized block: count -2, size 2
        @test_throws Avro.DataError dec(arr, hex2bytes("03060636" * "00"))       # sized block not exactly consumed
        @test_throws Avro.DataError dec(arr, hex2bytes("0306"))                  # size beyond the data
        @test_throws Avro.DataError dec(arr, hex2bytes("ffffffffffffffffff01"))  # typemin count
        @test_throws Avro.DataError dec(arr, hex2bytes("8080808080808080807f"))  # overflow
        @test_throws Avro.DataError dec(arr, hex2bytes("04"))                    # declared 2 items, no bytes
        @test_throws Avro.DataError dec(arr, hex2bytes("d00f" * "00"))           # 1000 items declared, one byte left: count exceeds remaining ÷ minsize
        @test dec(arr, enc(arr, (1, 2, 3))) == [1, 2, 3] && dec(arr, enc(arr, Set([7]))) == [7] && dec(arr, enc(arr, (i for i in 1:3))) == [1, 2, 3]
        @test_throws Avro.EncodeError enc(arr, ArrayLengthMismatch(1, 2))
        @test_throws Avro.EncodeError enc(arr, ArrayLengthMismatch(2, 1))
        @test_throws Avro.EncodeError enc(arr, ArrayLengthMismatch(0, 1))
        @test_throws Avro.EncodeError enc(arr, "abc")
        @test_throws Avro.EncodeError enc(arr, Dict("a" => 1))
        @test dec(arr, enc(arr, 1:3)) == [1, 2, 3]
        arrayunion = "[\"int\",{\"type\":\"array\",\"items\":\"long\"}]"
        @test dec(arrayunion, enc(arrayunion, (i for i in 1:3))) == Int64[1, 2, 3]
        nested = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"int\"}}"
        @test dec(nested, enc(nested, [[1], Int32[]])) == Any[Int32[1], Int32[]] && typeof(dec(nested, enc(nested, [[1]]))) === Vector{Any}
        opt = "{\"type\":\"array\",\"items\":[\"null\",\"int\"]}"
        @test isequal(dec(opt, enc(opt, [1, missing, 2])), [1, missing, 2]) && typeof(dec(opt, enc(opt, [1]))) === Vector{Union{Missing,Int32}}
        mp = "{\"type\":\"map\",\"values\":\"int\"}"
        m = dec(mp, enc(mp, Dict("b" => 1, "a" => 2)))
        @test m isa Dict{String,Int32} && m == Dict("a" => 2, "b" => 1)
        @test dec(mp, enc(mp, (x=1, y=2))) == Dict{String,Int32}("x" => 1, "y" => 2)
        @test dec(mp, enc(mp, Dict(:k => 5)))["k"] == 5
        @test dec(mp, hex2bytes("00")) isa Dict{String,Int32} && isempty(dec(mp, hex2bytes("00")))
        @test dec(mp, hex2bytes("0202610202026104" * "00")) == Dict{String,Int32}("a" => 2)   # duplicate key across blocks: last wins
        @test_throws Avro.DataError dec(mp, hex2bytes("02" * "02ff" * "02" * "00"))    # invalid UTF-8 key
        @test_throws Avro.EncodeError enc(mp, Dict(1 => 2))
        @test_throws Avro.EncodeError enc(mp, [1, 2])
        @test_throws Avro.EncodeError enc(mp, Dict{Any,Int32}(:a => 1, "a" => 2))
        impossiblemap = P("{\"type\":\"map\",\"values\":[]}")
        impossiblebytes = UInt8[0x02, 0x00]
        @test_throws Avro.DataError Avro.decode(impossiblemap, impossiblebytes)
        impossibleplan = Avro.readplan(impossiblemap)
        @test_throws Avro.DataError Avro.skip(impossibleplan, Avro.Decoder(impossiblebytes))
        # Java BlockingBinaryEncoder fixtures (sized blocks at every level) decode like the positive form
        arrmap = P(read(joinpath(FIXTURES, "generated", "blocking", "arrmap.avsc"), String))
        expected = [Dict{String,Int64}("a" => 1, "b" => 2), Dict{String,Int64}(), Dict{String,Int64}("c" => 3)]
        for bs in (32, 64, 1024)
            @test Avro.decode(arrmap, read(joinpath(FIXTURES, "generated", "blocking", "arrmap-$bs.bin"))) == expected
        end
        @test Avro.decode(arrmap, Avro.encode(arrmap, expected)) == expected
        cf = joinpath(FIXTURES, "generated", "blocking", "crossform")
        cfs = P(read(joinpath(cf, "arr.avsc"), String))
        for name in ("a", "a2", "b")
            pos = Avro.decode(cfs, read(joinpath(cf, "$name.positive.bin")))
            siz = Avro.decode(cfs, read(joinpath(cf, "$name.sized.bin")))
            @test pos == siz
        end
    end

    @testset "records" begin
        r = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\",\"default\":\"x\"}]}"
        rs = P(r)
        @test hex(enc(r, (a=1, b="z"))) == "02027a" && hex(enc(r, Dict("a" => 1, "b" => "z"))) == "02027a" && hex(enc(r, Dict(:a => 1, :b => "z"))) == "02027a"
        @test hex(enc(r, Avro.Record(rs, [1, "z"]))) == "02027a"
        short_record = Avro.Record(rs, [1, "z"])
        empty!(getfield(short_record, :values))
        @test_throws Avro.EncodeError enc(r, short_record)
        long_record = Avro.Record(rs, [1, "z"])
        push!(getfield(long_record, :values), 3)
        @test_throws Avro.EncodeError enc(r, long_record)
        @test_throws Avro.EncodeError enc(r, (a=1,))                 # defaults never make a field optional when encoding
        @test_throws Avro.EncodeError enc(r, Dict("a" => 1))
        @test_throws Avro.EncodeError enc(r, 5)
        @test_throws Avro.EncodeError enc(r, "s")
        @test_throws Avro.EncodeError enc(r, (1, "z"))
        struct BinRec; b::String; a::Int32; end
        @test hex(enc(r, BinRec("z", 1))) == "02027a"                # by name, not position
        other = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"a\",\"type\":\"int\"}]}")
        @test hex(enc(r, Avro.Record(other, ["z", 1]))) == "02027a"  # reordered record of the same name: by field name
        rec = dec(r, hex2bytes("02027a"))
        @test rec.a == 1 && rec.b == "z" && rec == Avro.Record(rs, [1, "z"])
        @test rec isa Tables.AbstractRow && Tables.columnnames(rec) == [:a, :b] && Avro.schema(rec) == rs
        @test rec[1] == 1 && rec[:b] == "z" && rec["b"] == "z" && collect(rec) == Any[Int32(1), "z"]
        @test symbollookupbytes(rec) == 0
        @test_throws Avro.DataError dec(r, hex2bytes("02"))
        @test dec(r, enc(r, first(Tables.rows((a=[1], b=["q"]))))).b == "q"   # Tables.AbstractRow source
        empty = "{\"type\":\"record\",\"name\":\"E\",\"fields\":[]}"
        @test enc(empty, (;)) == UInt8[] && dec(empty, UInt8[]) == Avro.Record(P(empty), [])
        ll = "{\"type\":\"record\",\"name\":\"LongList\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"LongList\"]}]}"
        lls = P(ll)
        v = Avro.Record(lls, [1, Avro.Record(lls, [2, missing])])
        @test hex(enc(ll, v)) == "02020400" && isequal(dec(ll, hex2bytes("02020400")), v) && ismissing(dec(ll, hex2bytes("02020400")) == v)
        struct LongListNode; value::Int64; next::Union{Missing,LongListNode}; end
        @test hex(enc(ll, LongListNode(1, LongListNode(2, missing)))) == "02020400"
    end

    @testset "logical types" begin
        d = "{\"type\":\"int\",\"logicalType\":\"date\"}"
        @test dec(d, enc(d, Date(1970, 1, 2))) == Date(1970, 1, 2) && hex(enc(d, Date(1970, 1, 1))) == "00" && dec(d, hex2bytes("01")) == Date(1969, 12, 31)
        mindate = Date(1970, 1, 1) + Day(typemin(Int32))
        maxdate = Date(1970, 1, 1) + Day(typemax(Int32))
        @test dec(d, enc(d, mindate)) == mindate
        @test dec(d, enc(d, maxdate)) == maxdate
        @test_throws Avro.EncodeError enc(d, mindate - Day(1))
        @test_throws Avro.EncodeError enc(d, maxdate + Day(1))
        @test_throws Avro.EncodeError enc(d, DateTime(2020))
        tm = "{\"type\":\"int\",\"logicalType\":\"time-millis\"}"
        @test dec(tm, enc(tm, Time(1, 2, 3, 4))) == Time(1, 2, 3, 4)
        @test_throws Avro.EncodeError enc(tm, Time(1, 2, 3, 4, 5))        # not millisecond-aligned
        @test enc(tm, Avro.truncate(Time(1, 2, 3, 4, 5), Millisecond)) == enc(tm, Time(1, 2, 3, 4))
        @test_throws Avro.DataError dec(tm, enc("\"int\"", 86_400_000))
        @test_throws Avro.DataError dec(tm, enc("\"int\"", -1))
        tu = "{\"type\":\"long\",\"logicalType\":\"time-micros\"}"
        @test dec(tu, enc(tu, Time(23, 59, 59, 999, 999))) == Time(23, 59, 59, 999, 999)
        @test_throws Avro.EncodeError enc(tu, Time(0, 0, 0, 0, 0, 1))
        @test_throws Avro.DataError dec(tu, enc("\"long\"", 86_400_000_000))
        for (lt, make, P_) in (("timestamp-millis", Avro.timestamp, Millisecond), ("timestamp-micros", Avro.timestamp, Microsecond), ("timestamp-nanos", Avro.timestamp, Nanosecond),
                               ("local-timestamp-millis", Avro.localtimestamp, Millisecond), ("local-timestamp-micros", Avro.localtimestamp, Microsecond), ("local-timestamp-nanos", Avro.localtimestamp, Nanosecond))
            T(x) = make(P_, x)
            s = "{\"type\":\"long\",\"logicalType\":\"$lt\"}"
            @test dec(s, enc(s, T(-5))) == T(-5) && dec(s, enc(s, T(typemax(Int64)))) == T(typemax(Int64))
            @test dec(s, enc(s, DateTime(2016, 3, 17, 13, 13, 10, 123))) == T(DateTime(2016, 3, 17, 13, 13, 10, 123))
        end
        @test_throws Avro.EncodeError enc("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}", Avro.timestamp(Microsecond, 1))
        # Java TimeConversions vectors (text → ticks); sub-millisecond digits only survive in micros/nanos
        for line in eachline(joinpath(FIXTURES, "generated", "timevectors.tsv"))
            lt, text, val = split(line, '\t')
            startswith(val, "ERROR") && continue
            if lt == "date"
                @test dec("{\"type\":\"int\",\"logicalType\":\"date\"}", enc("\"int\"", parse(Int32, val))) == Date(text)
                continue
            end
            (startswith(lt, "timestamp-") || startswith(lt, "local-timestamp-")) || continue
            s = "{\"type\":\"long\",\"logicalType\":\"$lt\"}"
            datepart, timepart = split(rstrip(text, 'Z'), 'T')
            frac = occursin('.', timepart) ? split(timepart, '.')[2] : ""
            base = DateTime(Date(datepart), Time(split(timepart, '.')[1]))
            ms = isempty(frac) ? 0 : parse(Int, rpad(frac, 3, '0')[1:3])
            dt = base + Millisecond(ms)
            ticks = parse(Int64, val)
            v = dec(s, enc("\"long\"", ticks))
            @test DateTime(v) == dt
            @test Dates.value(v) == ticks
            length(frac) <= 3 && @test Dates.value(dec(s, enc(s, dt))) == ticks
        end
        # decimal
        db = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}"
        @test hex(enc(db, Avro.Decimal(0, 2))) == "0200" && hex(enc(db, Avro.Decimal(-1, 2))) == "02ff" && hex(enc(db, Avro.Decimal(127, 2))) == "027f" && hex(enc(db, Avro.Decimal(128, 2))) == "040080" && hex(enc(db, Avro.Decimal(-129, 2))) == "04ff7f"
        @test dec(db, hex2bytes("040000")) == Avro.Decimal(0, 2)                    # non-minimal encoding decodes
        @test dec(db, hex2bytes("02ff")) == Avro.Decimal(-1, 2) && dec(db, hex2bytes("027f")) == Avro.Decimal(127, 2)
        @test_throws Avro.DataError dec(db, hex2bytes("00"))                       # empty payload
        @test_throws Avro.DataError dec(db, hex2bytes("060186a0"))                 # 100000 exceeds precision 5
        @test_throws Avro.EncodeError enc(db, Avro.Decimal(100000, 2))
        @test_throws Avro.EncodeError enc(db, Avro.Decimal(1, 3))                   # exact scale required
        @test_throws Avro.EncodeError enc(db, 1.5)
        df = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":9}"
        @test hex(enc(df, Avro.Decimal(-1, 0))) == "ffffffff" && hex(enc(df, Avro.Decimal(1, 0))) == "00000001" && dec(df, hex2bytes("ffffffff")) == Avro.Decimal(-1, 0)
        @test_throws Avro.EncodeError enc(df, Avro.Decimal(Int128(10)^10, 0))
        @test dec("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":39}", enc("\"bytes\"", hex2bytes("7f" * "ff"^15))) == Avro.WideDecimal(big(typemax(Int128)), 0)   # 39 digits: precision > 38 always decodes as WideDecimal
        @test_throws Avro.DataError dec("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38}", enc("\"bytes\"", hex2bytes("7f" * "ff"^15)))
        wide = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":50,\"scale\":1}"
        w = Avro.WideDecimal(big(10)^45 + 1, 1)
        @test dec(wide, enc(wide, w)) == w && dec(wide, enc(wide, w)) isa Avro.WideDecimal
        @test dec(wide, enc(wide, Avro.WideDecimal(big(-1), 1))) == Avro.WideDecimal(big(-1), 1)
        @test_throws Avro.DataError dec(wide, enc("\"bytes\"", hex2bytes("01" * "00"^21)))     # 51 digits
        # uuid
        us = "{\"type\":\"string\",\"logicalType\":\"uuid\"}"
        id = UUID("123e4567-e89b-12d3-a456-426614174000")
        @test dec(us, enc(us, id)) == id && dec(us, enc("\"string\"", "123E4567-E89B-12D3-A456-426614174000")) == id
        @test_throws Avro.DataError dec(us, enc("\"string\"", "123e4567e89b12d3a456426614174000"))
        uuidschema = P(us)
        hugeuuidbytes = 1_000_000
        hugeuuid = vcat(Avro.encode(Avro.LongSchema(), Int64(hugeuuidbytes)),
                        fill(UInt8('x'), hugeuuidbytes))
        uuiderror = try
            Avro.decode(uuidschema, hugeuuid)
            nothing
        catch err
            err
        end
        @test uuiderror isa Avro.DataError
        @test sizeof(uuiderror.msg) < 128
        skipuuiderror = try
            Avro.skip(Avro.readplan(uuidschema), Avro.Decoder(hugeuuid))
            nothing
        catch err
            err
        end
        @test skipuuiderror isa Avro.DataError
        @test sizeof(skipuuiderror.msg) < 128
        @test_throws Avro.EncodeError enc(us, "nope")
        @test enc(us, string(id)) == enc(us, id)
        uf = "{\"type\":\"fixed\",\"name\":\"U\",\"size\":16,\"logicalType\":\"uuid\"}"
        @test dec(uf, enc(uf, id)) == id && hex(enc(uf, id)) == "123e4567e89b12d3a456426614174000"
        @test_throws Avro.EncodeError enc(uf, "123e4567-e89b-12d3-a456-426614174000")
        dur = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":12,\"logicalType\":\"duration\"}"
        x = Avro.Duration(UInt32(1), UInt32(2), UInt32(0xffffffff))
        @test dec(dur, enc(dur, x)) == x && hex(enc(dur, x)) == "0100000002000000ffffffff"
        @test_throws Avro.EncodeError enc(dur, UInt8[1])

        encoder = Avro.Encoder(; capacity=128)
        Avro.writeuuidstring!(encoder, id)
        Avro.reset!(encoder)
        @test @allocated(Avro.writeuuidstring!(encoder, id)) == 0
        Avro.reset!(encoder)
        Avro.writeuuidfixed!(encoder, id)
        Avro.reset!(encoder)
        @test @allocated(Avro.writeuuidfixed!(encoder, id)) == 0
    end

    @testset "positions, IO sources, prepared codecs, limits" begin
        s = P("\"long\"")
        bytes = vcat(enc("\"long\"", 1), enc("\"long\"", 2))
        @test Avro.decode(s, bytes, 1) == (1, 2) && Avro.decode(s, bytes, 2) == (2, 3)
        @test_throws Avro.DataError Avro.decode(s, bytes)
        @test_throws ArgumentError Avro.decode(s, bytes, 5)
        @test Avro.decode(s, IOBuffer(enc("\"long\"", 9))) == 9
        @test_throws Avro.DataError Avro.decode(s, IOBuffer(bytes))
        streamerror = try
            Avro.decode(s, IOBuffer(zeros(UInt8, 20)); limits=Avro.Limits(max_bytes=10))
            nothing
        catch err
            err
        end
        @test streamerror isa Avro.LimitError && streamerror.limit == :max_bytes && streamerror.value == 10
        @test Avro.decode(s, view(vcat(UInt8[0xff], enc("\"long\"", 4)), 2:2)) == 4
        reader = Avro.DatumReader(s)
        @test reader(enc("\"long\"", 5)) == 5 && reader(IOBuffer(enc("\"long\"", 6))) == 6 && reader(bytes, 2) == (2, 3)
        writer = Avro.DatumWriter(s)
        first7 = writer(7)
        @test first7 == enc("\"long\"", 7)
        @test writer(9) == enc("\"long\"", 9)
        @test first7 == enc("\"long\"", 7)                    # each call returns its own vector
        io = IOBuffer(); writer(io, 8); @test take!(io) == enc("\"long\"", 8)
        record = P("{\"type\":\"record\",\"name\":\"PreparedRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        typedwriter = Avro.DatumWriter(record, @NamedTuple{a::Int64})
        @test typedwriter((a=Int64(1),)) == UInt8[0x02]
        boundedrecord = P("{\"type\":\"record\",\"name\":\"BoundedPreparedRecord\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}")
        boundedbytes = Avro.encode(boundedrecord, (x="abcdefghij",))
        boundedoverlap = UInt8[0x14, 0x61, 0x62, 0x63]               # claims 10 string bytes, 3 remain
        streamlimits = Avro.Limits(max_bytes=4)
        for boundedreader in (
            Avro.DatumReader(boundedrecord; limits=streamlimits),
            Avro.DatumReader(boundedrecord, @NamedTuple{x::String}; limits=streamlimits),
        )
            @test boundedreader(boundedbytes, 1)[1].x == "abcdefghij"   # max_bytes bounds stream reads only
            @test_throws Avro.DataError boundedreader(boundedoverlap)
            @test_throws Avro.DataError boundedreader(view(boundedoverlap, :))
            streamerror = try
                boundedreader(IOBuffer(boundedbytes))
                nothing
            catch err
                err
            end
            @test streamerror isa Avro.LimitError && streamerror.limit == :max_bytes
        end
        reorderedrecord = P("{\"type\":\"record\",\"name\":\"ReorderedPreparedRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"long\"}]}")
        reorderedwriter = Avro.DatumWriter(reorderedrecord, @NamedTuple{a::Int64,b::Int64})
        @test_throws Avro.EncodeError reorderedwriter((b=Int64(1), a=Int64(2)))
        @test Avro.decode(reorderedrecord, Avro.DatumWriter(reorderedrecord)((b=Int64(1), a=Int64(2)))) ==
              Avro.Record(reorderedrecord, Any[Int64(2), Int64(1)])
        @test typedwriter((a=Int64(2),)) == UInt8[0x04]
        @test_throws Avro.EncodeError typedwriter((a="bad",))
        @test typedwriter((a=Int64(3),)) == UInt8[0x06]                 # a failed call leaves the writer usable
        reusableplan = Avro.DatumWriter(record)
        @test reusableplan(StableWriteFields(4)) == UInt8[0x08]
        @test_throws ErrorException reusableplan(ThrowingWriteFields(5))
        @test reusableplan(StableWriteFields(6)) == UInt8[0x0c]
        cachedrecord = P("""{"type":"record","name":"CachedPlans","fields":[
            {"name":"a","type":{"type":"fixed","name":"CachedFixed","size":1}}]}""")
        cachewriter = Avro.DatumWriter(cachedrecord)
        @test cachewriter((a=UInt8[1],)) == UInt8[1]
        @test cachewriter((a=UInt8[2],)) == UInt8[2]

        @test Avro.encode!(IOBuffer(), s, 1) === nothing
        @test Avro.encode((a=1,)) == enc("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}", (a=1,))
        @test Avro.encode(Int32(3)) == UInt8[0x06] && Avro.encode("x") == UInt8[0x02, 0x78]
        @test_throws ArgumentError Avro.DatumReader(s; reader_schema=s, union_resolution=:odd)
        # limits
        @test_throws Avro.LimitError Avro.decode(P("\"bytes\""), IOBuffer(enc("\"bytes\"", zeros(UInt8, 100))); limits=Avro.Limits(max_bytes=50))
        @test Avro.decode(P("\"bytes\""), enc("\"bytes\"", zeros(UInt8, 100)); limits=Avro.Limits(max_bytes=50)) == zeros(UInt8, 100)
        nestedpathschema = P("""{"type":"record","name":"PathRecord","fields":[
            {"name":"xs","type":{"type":"array","items":{"type":"map","values":"int"}}}]}""")
        nestedbad = (xs=Any[Dict("key" => "bad")],)
        for encodecall in (x -> Avro.encode(nestedpathschema, x),
                           x -> Avro.DatumWriter(nestedpathschema)(x),
                           x -> Avro.tojson(nestedpathschema, x))
            patherror = try
                encodecall(nestedbad)
                nothing
            catch err
                err
            end
            @test patherror isa Avro.EncodeError
            @test patherror.path == "\$.xs[0][\"key\"]"
            @test patherror.schema === nestedpathschema.fields[1].schema.items.values
        end

        deepowner = P("""{"type":"record","name":"DeepOwner","fields":[
            {"name":"value","type":"long"},
            {"name":"next","type":["null","DeepOwner"]}]}""")
        function deepownervalue(depth; bad::Bool=false)
            value = (value=bad ? "bad" : Int64(0), next=missing)
            for index in 1:depth
                value = (value=Int64(index), next=value)
            end
            return value
        end

        prepareddeep = Avro.DatumWriter(deepowner)
        for append in (e -> Avro.encode!(e, deepowner, deepownervalue(20)),
                       e -> prepareddeep(e, deepownervalue(20)))
            external = Avro.Encoder()
            append(external)
            @test take!(external) == Avro.encode(deepowner, deepownervalue(20))
        end
        for appendfailure in (e -> Avro.encode!(e, deepowner, deepownervalue(20; bad=true)),
                              e -> prepareddeep(e, deepownervalue(20; bad=true)))
            err = try
                appendfailure(Avro.Encoder())
                nothing
            catch caught
                caught
            end
            @test err isa Avro.EncodeError
        end

        arr = P("{\"type\":\"array\",\"items\":\"null\"}")
        e = try Avro.decode(arr, hex2bytes("ffffffffff0f00")); nothing catch err; err end   # 2^38 nulls declared in a sized block of 0 bytes
        @test e isa Avro.LimitError && e.limit == :max_values_per_byte
        densearray = vcat(enc("\"long\"", 1 << 20), UInt8[0])
        @test_throws Avro.LimitError Avro.decode(arr, densearray; limits=Avro.Limits(value_allowance=0))
        bigarr = Avro.parseschema("{\"type\":\"array\",\"items\":\"boolean\"}")
        @test length(Avro.encode(bigarr, fill(true, 200_000))) > 200_000
        deep = P("{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"n\",\"type\":[\"null\",\"D\"]}]}")
        function nest(k)
            v = Avro.Record(deep, [missing])
            for _ in 1:k
                v = Avro.Record(deep, [v])
            end
            return v
        end
        @test isequal(Avro.decode(deep, Avro.encode(deep, nest(10)); limits=Avro.Limits(max_depth=12)), nest(10))
        @test_throws Avro.LimitError Avro.decode(deep, Avro.encode(deep, nest(20)); limits=Avro.Limits(max_depth=12))
        @test_throws Avro.LimitError Avro.decode(deep, vcat(fill(UInt8(0x02), 2000), UInt8(0x00)))   # 2000 levels > max_depth
        # a prepared reader is shareable across tasks
        rr = Avro.DatumReader(P("\"string\""))
        tasks = [Threads.@spawn rr(enc("\"string\"", "t$i")) for i in 1:8]
        foreach(errormonitor, tasks)
        @test [fetch(t) for t in tasks] == ["t$i" for i in 1:8]
    end

    @testset "nullable-union rows: round trip and malformed blocks" begin
        nullable = P("{\"type\":\"record\",\"name\":\"StaticUnions\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"long\"]},{\"name\":\"s\",\"type\":[\"string\",\"null\"]}]}")
        rows = [(a=isodd(i) ? missing : Int64(i), s=i % 3 == 0 ? missing : "v$i") for i in 1:100]
        buf = take!(Avro.tobuffer(rows; schema=nullable))
        @test isequal(Tables.columntable(Avro.Table(buf; ntasks=1)), Tables.columntable(rows))
        hlen = length(take!(Avro.tobuffer(rows[1:0]; schema=nullable)))
        @test buf[hlen + 1:hlen + 2] == [0xc8, 0x01]   # one block of 100 datums
        badindex = copy(buf)
        badindex[hlen + 5] = 0x06                      # the first datum selects union branch 3 of 2
        @test_throws Avro.DataError Avro.Table(badindex; ntasks=1)
        @test_throws Avro.DataError Avro.Table(badindex)
        overcount = copy(buf)
        overcount[hlen + 1] = 0xca                     # 101 datums declared, 100 encoded
        @test_throws Avro.DataError Avro.Table(overcount; ntasks=1)
        @test_throws Avro.DataError Avro.DatumReader(nullable)(UInt8[0x02, 0x06])     # truncated long
    end

    @testset "sized blocks: decoding validates every item, skipping jumps the block" begin
        s = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":\"boolean\"}},{\"name\":\"b\",\"type\":\"int\"}]}")
        good = Avro.encode(s, (a=[true, false], b=1))
        @test Avro.decode(s, good).b == 1
        # a bad boolean byte inside a sized block (count -2, size 2)
        bad = vcat(hex2bytes("0304" * "0102" * "00"), enc("\"int\"", 1))
        @test_throws Avro.DataError Avro.decode(s, bad)

        # a field the reader drops is skipped: its sized block is jumped without reading the items
        tagwriter = P("""{"type":"record","name":"FastTag","fields":[
            {"name":"good","type":"long"},
            {"name":"arr","type":{"type":"array","items":["null","int"]}}]}""")
        tagreader = P("""{"type":"record","name":"FastTag","fields":[
            {"name":"good","type":"long"}]}""")
        badtag = UInt8[0x02, 0x01, 0x02, 0x04, 0x00]
        @test_throws Avro.DataError Avro.decode(tagwriter, badtag)
        @test Avro.decode(tagwriter, badtag; reader_schema=tagreader).good == 1
        @test Avro.DatumReader(tagwriter; reader_schema=tagreader)(badtag).good == 1

        enumwriter = P("""{"type":"record","name":"FastEnumTag","fields":[
            {"name":"good","type":"long"},
            {"name":"arr","type":{"type":"array","items":
                {"type":"enum","name":"FastE","symbols":["A"]}}}]}""")
        enumreader = P("""{"type":"record","name":"FastEnumTag","fields":[
            {"name":"good","type":"long"}]}""")
        badenum = UInt8[0x02, 0x01, 0x02, 0x02, 0x00]
        @test_throws Avro.DataError Avro.decode(enumwriter, badenum)
        @test Avro.decode(enumwriter, badenum; reader_schema=enumreader).good == 1

        mapwriter = P("""{"type":"record","name":"FastMap","fields":[
            {"name":"good","type":"long"},
            {"name":"m","type":{"type":"map","values":"boolean"}}]}""")
        mapreader = P("""{"type":"record","name":"FastMap","fields":[
            {"name":"good","type":"long"}]}""")
        badmap = UInt8[0x02, 0x01, 0x06, 0x02, 0x6b, 0x02, 0x00]
        @test_throws Avro.DataError Avro.decode(mapwriter, badmap)
        @test Avro.decode(mapwriter, badmap; reader_schema=mapreader).good == 1

        impossiblefixed = Avro.ArraySchema(Avro.FixedSchema("ImpossibleFastF", typemax(Int)))
        @test_throws Avro.DataError Avro.decode(impossiblefixed, UInt8[0x02, 0x00])
        saturatedrecord = P("""{"type":"record","name":"SaturatedFixedRecord","fields":[
            {"name":"a","type":{"type":"fixed","name":"SaturatedA","size":9223372036854775806}},
            {"name":"b","type":{"type":"fixed","name":"SaturatedB","size":2}}]}""")
        @test_throws Avro.DataError Avro.decode(saturatedrecord, UInt8[0x00])

        # skip() directly: the sized block is jumped, so the bad boolean is not seen
        plan = Avro.readplan(s)
        d = Avro.Decoder(bad)
        Avro.skip(plan.fields[1], d)
        @test Avro.decode(plan.fields[2], d) == 1
        # skipped strings are length-validated but not UTF-8-validated (documented exception)
        bs = P("{\"type\":\"array\",\"items\":\"string\"}")
        badstr = hex2bytes("02" * "02ff" * "00")
        d = Avro.Decoder(badstr)
        @test Avro.skip(Avro.readplan(bs), d) === nothing && d.pos == length(badstr) + 1
        @test_throws Avro.DataError Avro.decode(bs, badstr)
        # skipped logical values are domain-checked like decoded ones (fuzz findings): uuid text, time ranges, decimals
        function skipverdict(s, bytes)
            d = Avro.Decoder(bytes)
            try
                Avro.skip(Avro.readplan(s), d)
                return d.pos == length(bytes) + 1 ? :ok : :trailing
            catch e
                e isa Avro.DataError || rethrow()
                return :data
            end
        end

        function decodeverdict(s, bytes)
            try
                Avro.decode(s, bytes)
                return :ok
            catch e
                e isa Avro.DataError || rethrow()
                return :data
            end
        end
        us = P("{\"type\":\"string\",\"logicalType\":\"uuid\"}")
        for txt in ("00000000-0000-0000-0000-000000000001", "0z000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000&00001", "123e4567-e89b-12d3-a456-26614174000\x12", "ABCDEF01-2345-6789-abcd-ef0123456789", "")
            bytes = enc("\"string\"", txt)
            @test skipverdict(us, bytes) == decodeverdict(us, bytes)
        end
        @test decodeverdict(us, enc("\"string\"", "ABCDEF01-2345-6789-abcd-ef0123456789")) == :ok
        tm = P("{\"type\":\"long\",\"logicalType\":\"time-micros\"}")
        tms = P("{\"type\":\"int\",\"logicalType\":\"time-millis\"}")
        for v in (0, 86_399_999_999, 86_400_000_000, -1, 274877915135)
            bytes = enc("\"long\"", v)
            @test skipverdict(tm, bytes) == decodeverdict(tm, bytes) == (0 <= v < 86_400_000_000 ? :ok : :data)
        end
        for v in (0, 86_399_999, 86_400_000, -1)
            bytes = enc("\"int\"", v)
            @test skipverdict(tms, bytes) == decodeverdict(tms, bytes) == (0 <= v < 86_400_000 ? :ok : :data)
        end
        dec18 = P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":18,\"scale\":2}")
        dec60 = P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":60,\"scale\":2}")
        fdec = P("{\"type\":\"fixed\",\"name\":\"FD\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":5,\"scale\":0}")
        for (s, payload) in ((dec18, UInt8[]), (dec18, UInt8[0x01]), (dec18, fill(0x7f, 9)), (dec18, fill(0x7f, 17)), (dec60, fill(0x7f, 17)), (dec60, fill(0x7f, 30)))
            bytes = enc("\"bytes\"", payload)
            @test skipverdict(s, bytes) == decodeverdict(s, bytes)
        end
        @test skipverdict(fdec, UInt8[0x00, 0x01, 0x86, 0xa0]) == decodeverdict(fdec, UInt8[0x00, 0x01, 0x86, 0xa0]) == :data    # 100000: six digits exceed precision 5
        @test skipverdict(fdec, UInt8[0x00, 0x00, 0x27, 0x0f]) == decodeverdict(fdec, UInt8[0x00, 0x00, 0x27, 0x0f]) == :ok      # 9999
    end

    @testset "round-trip property (generated schemas and values)" begin
        rng = Random.Xoshiro(20260822)
        prims = ["\"null\"", "\"boolean\"", "\"int\"", "\"long\"", "\"float\"", "\"double\"", "\"bytes\"", "\"string\"",
                 "{\"type\":\"int\",\"logicalType\":\"date\"}", "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}",
                 "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":3}", "{\"type\":\"string\",\"logicalType\":\"uuid\"}"]
        counter = Ref(0)
        function genschema(depth)
            counter[] += 1
            k = rand(rng, 1:(depth > 3 ? 1 : 7))
            k == 1 && return rand(rng, prims)
            k == 2 && return "{\"type\":\"array\",\"items\":$(genschema(depth + 1))}"
            k == 3 && return "{\"type\":\"map\",\"values\":$(genschema(depth + 1))}"
            k == 4 && return "[\"null\",$(rand(rng, prims[2:end]))]"
            k == 5 && return "{\"type\":\"enum\",\"name\":\"E$(counter[])\",\"symbols\":[\"A\",\"B\",\"C\"]}"
            k == 6 && return "{\"type\":\"fixed\",\"name\":\"F$(counter[])\",\"size\":$(rand(rng, 0:5))}"
            n = rand(rng, 0:3)
            return "{\"type\":\"record\",\"name\":\"R$(counter[])\",\"fields\":[" * join(["{\"name\":\"f$i\",\"type\":$(genschema(depth + 1))}" for i in 1:n], ",") * "]}"
        end

        function genvalue(s::Avro.Schema)
            s isa Avro.NullSchema && return missing
            s isa Avro.BooleanSchema && return rand(rng, Bool)
            s isa Avro.IntSchema && return s.logical isa Avro.DateLogical ? Date(1970, 1, 1) + Day(rand(rng, Int32)) : rand(rng, Int32)
            s isa Avro.LongSchema && return s.logical isa Avro.TimestampMicros ? Avro.timestamp(Microsecond, rand(rng, Int64)) : rand(rng, Int64)
            s isa Avro.FloatSchema && return rand(rng, Bool) ? rand(rng, Float32) : reinterpret(Float32, rand(rng, UInt32))
            s isa Avro.DoubleSchema && return rand(rng, Bool) ? rand(rng, Float64) : reinterpret(Float64, rand(rng, UInt64))
            s isa Avro.BytesSchema && return s.logical isa Avro.DecimalLogical ? Avro.Decimal(Int128(rand(rng, -999999999:999999999)), 3) : rand(rng, UInt8, rand(rng, 0:8))
            s isa Avro.StringSchema && return s.logical isa Avro.UUIDLogical ? UUID(rand(rng, UInt128)) : String(rand(rng, ['a', 'é', '😀', '\0'], rand(rng, 0:6)))
            s isa Avro.FixedSchema && return Avro.Fixed(s, rand(rng, UInt8, s.size))
            s isa Avro.EnumSchema && return Avro.EnumValue(s, rand(rng, 1:3))
            s isa Avro.ArraySchema && return [genvalue(s.items) for _ in 1:rand(rng, 0:3)]
            s isa Avro.MapSchema && return Dict{String,Any}("k$i" => genvalue(s.values) for i in 1:rand(rng, 0:3))
            s isa Avro.UnionSchema && return rand(rng, Bool) ? missing : genvalue(s.branches[2])
            return Avro.Record(s, Any[genvalue(f.schema) for f in s.fields])
        end

        # map entries have no defined order, so a decoded map may re-encode its entries in another order
        hasmap(s) = s isa Avro.MapSchema || (s isa Avro.ArraySchema && hasmap(s.items)) ||
                    (s isa Avro.UnionSchema && any(hasmap, s.branches)) ||
                    (s isa Avro.RecordSchema && any(f -> hasmap(f.schema), s.fields))
        for trial in 1:150
            s = P(genschema(0))
            v = genvalue(s)
            bytes = Avro.encode(s, v)
            w = Avro.decode(s, bytes)
            @test isequal(w, v) || (v isa AbstractVector && isequal(collect(Any, w), collect(Any, v)))
            @test hasmap(s) ? isequal(Avro.decode(s, Avro.encode(s, w)), w) : Avro.encode(s, w) == bytes
            # prepared == one-shot
            @test isequal(Avro.DatumReader(s)(bytes), w) && Avro.DatumWriter(s)(v) == bytes
        end
    end
end
