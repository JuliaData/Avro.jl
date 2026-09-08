@testset "Frozen containers" begin
    v = Avro.FrozenVector{Int}()
    push!(v, 1); push!(v, 2)
    @test v == [1, 2] && length(v) == 2 && v[2] == 2
    v[1] = 10
    @test v[1] == 10
    Avro.freeze!(v)
    @test Avro.isfrozen(v)
    @test_throws Avro.FrozenError push!(v, 3)
    @test_throws Avro.FrozenError (v[1] = 0)
    @test_throws Avro.FrozenError empty!(v)
    @test_throws Avro.FrozenError pop!(v)
    @test_throws Avro.FrozenError resize!(v, 0)
    @test_throws Avro.FrozenError append!(v, [1])
    @test_throws Avro.FrozenError insert!(v, 1, 1)
    @test_throws Avro.FrozenError deleteat!(v, 1)
    @test v == [10, 2]   # reads still work
    c = copy(v)
    @test !Avro.isfrozen(c) && (push!(c, 3); c == [10, 2, 3])
    @test occursin("frozen", sprint(showerror, Avro.FrozenError("vector")))

    d = Avro.FrozenDict{String,Int}()
    d["b"] = 2; d["a"] = 1; d["c"] = 3
    @test collect(keys(d)) == ["a", "b", "c"]   # sorted keys, no hashing
    @test d["b"] == 2 && get(d, "z", 0) == 0 && haskey(d, "c") && !haskey(d, "z")
    @test_throws KeyError d["z"]
    d["b"] = 20
    @test d["b"] == 20 && length(d) == 3
    delete!(d, "a")
    @test !haskey(d, "a") && length(d) == 2
    @test collect(d) == ["b" => 20, "c" => 3]
    Avro.freeze!(d)
    @test_throws Avro.FrozenError (d["x"] = 1)
    @test_throws Avro.FrozenError delete!(d, "b")
    @test_throws Avro.FrozenError empty!(d)
    d2 = Avro.FrozenDict{String,Int}(["k" => 1, "j" => 2])
    @test collect(keys(d2)) == ["j", "k"]

    r = Avro.FrozenRef{Int}()
    @test !Avro.isfilled(r)
    @test_throws Avro.FrozenError r[]
    Avro.fillonce!(r, 7)
    @test r[] == 7 && Avro.isfilled(r)
    @test_throws Avro.FrozenError Avro.fillonce!(r, 8)
    @test Avro.FrozenRef(3)[] == 3

    # transitive freezing of JSON trees
    inner = Avro.FrozenVector{Any}(Any[1, "x"], false)
    arr = Avro.JSONArray(inner)
    members = Avro.FrozenDict{String,Any}()
    members["k"] = arr
    obj = Avro.JSONObject(members, Avro.FrozenVector{String}(["k"], false))
    Avro.freeze!(obj)
    @test Avro.isfrozen(inner) && Avro.isfrozen(members) && Avro.isfrozen(obj.order)
    @test_throws Avro.FrozenError push!(inner, 2)
    @test obj["k"] === arr && haskey(obj, "k") && length(obj) == 1 && collect(keys(obj)) == ["k"]
    @test arr[1] == 1 && length(arr) == 2 && collect(arr) == Any[1, "x"]
    @test Avro.JSONNumber("1") != Avro.JSONNumber("1.0")   # lexical equality for raw numbers
    @test hash(Avro.JSONNumber("1e0")) == hash(Avro.JSONNumber("1e0"))
    @test obj == Avro.JSONObject(Avro.FrozenDict{String,Any}(["k" => Avro.JSONArray(Avro.FrozenVector{Any}(Any[1, "x"], false))]), Avro.FrozenVector{String}(["k"], false))
    @test Avro.freeze!(1) == 1
end
