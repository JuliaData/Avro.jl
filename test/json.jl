@testset "JSON reader" begin
    function err(msg, pos)
        return throw(Avro.DataError(msg, pos))
    end

    function parse(s; kw...)
        return Avro.parsejson(s; limits=Avro.Limits(max_depth=64), errfn=err, kw...)
    end
    JSONObject = Avro.JSON.Object{String,Any}

    @testset "pre-scan validates syntax, UTF-8, escapes, numbers, depth, size" begin
        @test_throws Avro.DataError parse("")
        @test_throws Avro.DataError parse("   ")
        @test_throws Avro.DataError parse("\xEF\xBB\xBF{}")                 # BOM
        @test_throws Avro.DataError parse("{\"a\":1}x")                      # trailing content
        @test_throws Avro.DataError parse("[1,]")                            # trailing comma (parser)
        @test_throws Avro.DataError parse("{\"a\":1,}")
        @test_throws Avro.DataError parse("[1 2]")
        @test_throws Avro.DataError parse("tru")                             # malformed literal
        @test_throws Avro.DataError parse("trueish")
        @test_throws Avro.DataError parse("nul")
        @test_throws Avro.DataError parse("\"a\x01b\"")                       # raw control character
        @test_throws Avro.DataError parse("\"\\x\"")                          # invalid escape
        @test_throws Avro.DataError parse("\"\\u12\"")                        # truncated \\u
        @test_throws Avro.DataError parse("\"\\u12G4\"")
        @test_throws Avro.DataError parse("\"abc")                            # unterminated
        @test_throws Avro.DataError parse("[")
        @test_throws Avro.DataError parse("]")
        @test_throws Avro.DataError parse("[}")
        @test_throws Avro.DataError parse("{1:2}")
        @test_throws Avro.DataError parse("\"\xff\"")                         # invalid UTF-8 in string
        @test_throws Avro.DataError parse("\xc3\x28")                          # invalid UTF-8 outside
        @test_throws Avro.DataError parse("\"\xed\xa0\x80\"")                 # raw surrogate code point
        @test_throws Avro.DataError parse("\"\xc0\xaf\"")                     # overlong
        @test_throws Avro.DataError parse("\"\xf4\x90\x80\x80\"")             # above U+10FFFF
        @test parse("\"\xf0\x9f\x98\x80\"") == "😀"
        @test_throws Avro.DataError parse("01")                                # number grammar
        @test_throws Avro.DataError parse("+1")
        @test_throws Avro.DataError parse("1.")
        @test_throws Avro.DataError parse(".5")
        @test_throws Avro.DataError parse("1e")
        @test_throws Avro.DataError parse("1e+")
        @test_throws Avro.DataError parse("-")
        @test_throws Avro.DataError parse("NaN")
        @test_throws Avro.DataError parse("Infinity")
        @test_throws Avro.DataError parse("1x")
        @test_throws Avro.DataError parse("1.5.2")
        @test parse("-0") == 0 && parse("12") == 12 && parse("-9223372036854775808") == typemin(Int64)
        @test parse("9223372036854775808") == Avro.JSONNumber("9223372036854775808")   # overflow stays a raw token
        @test parse("1.0") == Avro.JSONNumber("1.0") && parse("1e0") == Avro.JSONNumber("1e0") && parse("-0.0") == Avro.JSONNumber("-0.0")
        @test parse("1.0") != parse("1e0")     # lexical equality of raw tokens
        @test Avro.parsefloat(Float32, "1.000000059604644775390625827180612553027674871408692069962853565812110900878906") === reinterpret(Float32, 0x3f800001)
        @test Float32(Avro.parsefloat(Float64, "1.000000059604644775390625827180612553027674871408692069962853565812110900878906")) === reinterpret(Float32, 0x3f800000)
        @test Avro.parsefloat(Float32, "1e100") == Inf32 && Avro.parsefloat(Float32, "-1e100") == -Inf32
        @test Avro.parsefloat(Float32, "1e-100") == 0.0f0 && Avro.parsefloat(Float32, "-1e-100") === -0.0f0
        @test Avro.parsefloat(Float64, "1e400") == Inf && Avro.parsefloat(Float64, "-1e400") == -Inf
        @test Avro.parsefloat(Float64, "1e-400") == 0.0 && Avro.parsefloat(Float64, "-1e-400") === -0.0
        @test Avro.parsefloat(Float64, "4.9e-324") == 5.0e-324 && Avro.parsefloat(Float64, "-0.0") === -0.0
        @test Avro.parsefloat(Float64, "1." * "0"^2000) == 1.0
        @test_throws ArgumentError Avro.parsefloat(Float64, "abc")
        # depth and size limits
        @test_throws Avro.LimitError Avro.parsejson("[[[1]]]"; limits=Avro.Limits(max_depth=2), errfn=err)
        @test Avro.parsejson("[[[1]]]"; limits=Avro.Limits(max_depth=3), errfn=err) isa Vector{Any}
        @test_throws Avro.LimitError Avro.parsejson("[1]"; limits=Avro.Limits(max_bytes=2), errfn=err)
        deep = "["^200 * "]"^200
        @test_throws Avro.LimitError Avro.parsejson(deep; limits=Avro.Limits(max_depth=199), errfn=err)
        @test Avro.parsejson(deep; limits=Avro.Limits(max_depth=200), errfn=err) isa Vector{Any}
        @test_throws Avro.DataError parse("[" * "{"^3 * "]" * "}"^3)     # mismatched deep brackets
    end

    @testset "trees: objects keep source order, duplicates rejected over decoded keys" begin
        o = parse("{\"b\":1,\"a\":[true,null,\"s\"],\"c\":{\"z\":-2}}")
        @test o isa JSONObject
        @test collect(keys(o)) == ["b", "a", "c"]
        @test o["b"] == 1 && o["a"] isa Vector{Any} && o["a"][1] === true && o["a"][2] === nothing && o["a"][3] == "s"
        @test o["c"]["z"] == -2 && haskey(o, "c") && !haskey(o, "zz") && get(o, "zz", :d) === :d
        @test parse("{}") isa JSONObject && length(parse("{}")) == 0 && parse("[]") isa Vector{Any} && length(parse("[]")) == 0
        @test_throws Avro.DataError parse("{\"a\":1,\"a\":2}")
        @test_throws Avro.DataError parse("{\"type\":1,\"\\u0074ype\":2}")   # decoded-key equality
        longkey = "k"^100_000
        duplicate = "{\"" * longkey * "\":0,\"" * longkey * "\":1}"
        duplicateerror = try
            Avro.fromjson(Avro.MapSchema(Avro.IntSchema()), duplicate)
            nothing
        catch caught
            caught
        end
        @test duplicateerror isa Avro.DataError
        @test sizeof(duplicateerror.msg) < 256                 # the message does not carry the whole key
    end

    @testset "WTF-8 string decoding and re-escaping" begin
        @test parse("\"a\\nb\\t\\\"\\\\\\/\\b\\f\\r\"") == "a\nb\t\"\\/\b\f\r"
        @test parse("\"\\u0041\\u00e9\\u20ac\"") == "Aé€"
        @test parse("\"\\ud83d\\ude00\"") == "😀"                                 # valid pair
        @test codeunits(parse("\"\\uD800\"")) == [0xed, 0xa0, 0x80]              # lone high surrogate
        @test codeunits(parse("\"\\uDC00\"")) == [0xed, 0xb0, 0x80]              # lone low surrogate
        @test codeunits(parse("\"\\uD800\\uD800\"")) == [0xed, 0xa0, 0x80, 0xed, 0xa0, 0x80]   # high/high
        @test codeunits(parse("\"\\uFC00\"")) == [0xef, 0xb0, 0x80]              # distinct from high/high (JSON.jl merged these)
        @test codeunits(parse("\"\\uDC00\\uD800\"")) == [0xed, 0xb0, 0x80, 0xed, 0xa0, 0x80]   # low/high
        @test codeunits(parse("\"\\uDBFF\\uDC00\"")) == [0xf4, 0x8f, 0xb0, 0x80]   # pair at the top of plane 16
        @test codeunits(parse("\"\\uD800x\"")) == [0xed, 0xa0, 0x80, UInt8('x')]
        @test parse("\"\\uD800\"") != parse("\"\\uDC00\"")
        for s in ("plain", "q\"\\", "tab\t", "\x01", "😀", "é")
            @test parse(sprint(Avro.escapejson, s)) == s
        end
        lone = parse("\"\\uD800\"")
        @test sprint(Avro.escapejson, lone) == "\"\\ud800\""                       # re-escaped, code-unit exact
        @test parse(sprint(Avro.escapejson, lone)) == lone
        @test sprint(Avro.escapejson, "a\u00e9") == "\"aé\""
        @test !Avro.isstrictutf8(lone) && Avro.isstrictutf8("aé😀") && !Avro.isstrictutf8(Vector{UInt8}([0xc0, 0xaf]))
    end


    @testset "JSON.json overload" begin
        import JSON
        s = Avro.parseschema("{\"type\":\"record\",\"name\":\"J\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}")
        @test JSON.json(s) == Avro.json(s)
        io = IOBuffer()
        JSON.json(io, s)
        @test String(take!(io)) == Avro.json(s)
    end
end
