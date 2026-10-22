@testset "Names" begin
    @test Avro.isvalidname("a") && Avro.isvalidname("_x9") && Avro.isvalidname("Ab_c1")
    @test !Avro.isvalidname("") && !Avro.isvalidname("9a") && !Avro.isvalidname("a-b") && !Avro.isvalidname("Å") && !Avro.isvalidname("a.b")
    @test Avro.isvalidnamespace("") && Avro.isvalidnamespace("a.b.c") && !Avro.isvalidnamespace(".a") && !Avro.isvalidnamespace("a..b") && !Avro.isvalidnamespace("a.9")
    @test Avro.isreservedfullname(Avro.FullName("int", "")) && !Avro.isreservedfullname(Avro.FullName("int", "a"))
    f = Avro.FullName("R", "ns")
    @test f == Avro.FullName("R", "ns") && hash(f) == hash(Avro.FullName("R", "ns")) && f != Avro.FullName("R", "")
    @test Avro.FullName("A", "") < Avro.FullName("B", "")
    @test sprint(show, f) == "FullName(\"ns.R\")"
end
