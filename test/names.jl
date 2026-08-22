@testset "Names" begin
    @test Avro.isvalidname("a") && Avro.isvalidname("_x9") && Avro.isvalidname("Ab_c1")
    @test !Avro.isvalidname("") && !Avro.isvalidname("9a") && !Avro.isvalidname("a-b") && !Avro.isvalidname("Å") && !Avro.isvalidname("a.b")
    @test Avro.isvalidnamespace("") && Avro.isvalidnamespace("a.b.c") && !Avro.isvalidnamespace(".a") && !Avro.isvalidnamespace("a..b") && !Avro.isvalidnamespace("a.9")
    @test Avro.splitfullname("a.b.C") == ("C", "a.b")
    @test Avro.splitfullname("C") == ("C", "")
    # the fullname algorithm: a dotted name wins and the namespace attribute is ignored
    @test Avro.fullname(Avro.resolvefullname("a.R", "ignored", "enc")) == "a.R"
    @test Avro.fullname(Avro.resolvefullname("R", "ns", "enc")) == "ns.R"
    @test Avro.fullname(Avro.resolvefullname("R", nothing, "enc")) == "enc.R"
    @test Avro.fullname(Avro.resolvefullname("R", "", "enc")) == "R"     # explicit empty namespace = null namespace
    @test Avro.fullname(Avro.resolvefullname("R", nothing, "")) == "R"
    @test Avro.resolvereference("x.Y", "enc") == "x.Y"
    @test Avro.resolvereference("Y", "enc") == "enc.Y"
    @test Avro.resolvereference("Y", "") == "Y"
    @test Avro.normalizealias("Bar", "a") == "a.Bar" && Avro.normalizealias("a.Bar", "a") == "a.Bar" && Avro.normalizealias("Bar", "") == "Bar"
    @test Avro.isreservedfullname(Avro.FullName("int", "")) && !Avro.isreservedfullname(Avro.FullName("int", "a"))
    f = Avro.FullName("R", "ns")
    @test f == Avro.FullName("R", "ns") && hash(f) == hash(Avro.FullName("R", "ns")) && f != Avro.FullName("R", "")
    @test Avro.FullName("A", "") < Avro.FullName("B", "")
    @test sprint(show, f) == "FullName(\"ns.R\")"
end
