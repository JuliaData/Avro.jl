using Avro, Dates, UUIDs
dir = ARGS[1]
t = [(a=1, b="x", c=1.5, d=Date(2020,1,1), dt=DateTime(2020,1,1,12), dec=Avro.Decimal{2,10}(Int128(12345)), u=UUID(0x123e4567e89b12d3a456426614174000)),
     (a=2, b="yy", c=2.5, d=Date(2020,1,2), dt=DateTime(2020,1,2,12), dec=Avro.Decimal{2,10}(Int128(-123)), u=UUID(0))]
Avro.writetable(joinpath(dir, "avrojl112-null.avro"), t)
Avro.writetable(joinpath(dir, "avrojl112-zstd.avro"), t; compress=:zstd)
Avro.writetable(joinpath(dir, "avrojl112-deflate.avro"), t; compress=:deflate)
Avro.writetable(joinpath(dir, "avrojl112-bzip2.avro"), t; compress=:bzip2)
Avro.writetable(joinpath(dir, "avrojl112-xz.avro"), t; compress=:xz)
println("wrote 1.1.2 fixtures")
