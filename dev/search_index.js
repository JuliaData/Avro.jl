var documenterSearchIndex = {"docs":
[{"location":"","page":"Home","title":"Home","text":"CurrentModule = Avro","category":"page"},{"location":"#Avro","page":"Home","title":"Avro","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"Documentation for Avro.","category":"page"},{"location":"","page":"Home","title":"Home","text":"","category":"page"},{"location":"","page":"Home","title":"Home","text":"Modules = [Avro]","category":"page"},{"location":"#Avro.Avro","page":"Home","title":"Avro.Avro","text":"The Avro.jl package provides a pure Julia implementation for reading writing data in the avro format.\n\nImplementation status\n\nIt currently supports:\n\nAll primitive types\nAll nested/complex types\nLogical types listed in the spec (Decimal, UUID, Date, Time, Timestamps, Duration)\nBinary encoding/decoding\nReading/writing object container files via the Tables.jl interface\nSupports the xz, zstd, deflate, and bzip2 compression codecs for object container files\n\nCurrently not supported are:\n\nJSON encoding/decoding of objects\nSingle object encoding or schema fingerprints\nSchema resolution\nProtocol messages, calls, handshakes\nSnappy compression\n\nPackage motivation\n\nWhy use the avro format vs. other data formats? Some benefits include:\n\nVery concise binary encoding, especially object container files with compression\nVery fast reading/writing\nObjects/data must have well-defined schema\nOne of the few \"row-oriented\" binary data formats\n\nGetting started\n\nThe Avro.jl package supports two main APIs to interact with avro data. The first is similar to the JSON3.jl struct API for interacting with json data, largely in part due to the similarities between the avro format and json. This looks like:\n\nbuf = Avro.write(obj)\nobj = Avro.read(buf, typeof(obj))\n\nIn short, we use Avro.write and provide an object obj to write out in the avro format. We can optionally provide a filename or IO as a first argument to write the data to.\n\nWe can then read the data back in using Avro.read, where the first argument must be a filename, IO, or any AbstractVector{UInt8} byte buffer containing avro data. The 2nd argument is required, and is the type of data to be read. This type can be provided as a simple Julia type (like Avro.read(buf, Vector{String})), or as a parsed avro schema, like Avro.read(buf, Avro.parseschema(\"schema.avsc\")). Avro.parseschema takes a filename or json string representing the avro schema of the data to read and returns a \"schema type\" that can be passed to Avro.read.\n\nThe second alternative API allows \"packaging\" the data's schema with the data in what the avro spec calls an \"object container\" file. While Avro.read/Avro.write require the user to already know or pass the schema externally, Avro.writetable and Avro.readtable can write/read avro object container files, and will take care of any schema writing/reading, compression, etc. automatically. These table functions unsurprisingly utilize the ubiquitous Tables.jl interface to facilitate integrations with other formats.\n\n# write our input_table out to a file named \"data.avro\" using the zstd compression codec\n# input_table can be any Tables.jl-compatible source, like CSV.File, Arrow.Table, DataFrame, etc.\nAvro.writetable(\"data.avro\", input_table; compress=:zstd)\n\n# we can also read avro data from object container files\n# if file uses compression, it will be decompressed automatically\n# the schema of the data is packaged in the object container file itself\n# and will be parsed before constructing the file table\ntbl = Avro.readtable(\"data.avro\")\n# the returned type is `Avro.Table`, which satisfies the Tables.jl interface\n# which means it can be sent to any valid sink function, like\n# Arrow.write(\"data.arrow\", tbl), CSV.write(\"data.csv\", tbl), or DataFrame(tbl)\n\n\n\n\n\n","category":"module"},{"location":"#Avro.Table","page":"Home","title":"Avro.Table","text":"Avro.Table\n\nA Tables.jl-compatible source returned from Avro.readtable. Conceptually, it can be thought of as an AbstractVector{Record}, where Record is the avro version of a \"row\" or NamedTuple. Thus, Avro.Table supports indexing/iteration like an AbstractVector.\n\n\n\n\n\n","category":"type"},{"location":"#Avro.parseschema-Tuple{Any}","page":"Home","title":"Avro.parseschema","text":"Avro.parseschema(file_or_jsonstring)\n\nParse the avro schema in a file or raw json string. The schema is expected to follow the format as described in the official spec. Returns a \"schema type\" that can be passed to Avro.read(buf, sch) as the 2nd argument.\n\n\n\n\n\n","category":"method"},{"location":"#Avro.read","page":"Home","title":"Avro.read","text":"Avro.read(source, T_or_sch) => T\n\nRead an avro-encoded object of type T or avro schema sch from source, which can be a byte buffer AbstractVector{UInt8}, file name String, or IO.\n\nThe data in source must be avro-formatted data, as no schema verification can be done. Note that \"avro object container files\" should be processed using Avro.readtable instead, where the data schema is encoded in the file itself. Also note that the 2nd argument can be a Julia type like Vector{String}, or a valid Avro.Schema type object, like is returned from Avro.parseschema(src).\n\n\n\n\n\n","category":"function"},{"location":"#Avro.readtable","page":"Home","title":"Avro.readtable","text":"Avro.readtable(file_or_io) => Avro.Table\n\nRead an avro object container file, returning an Avro.Table type, which is like an array of records, where each record follows the  schema encoded with the file. Any compression will be detected and decompressed automatically when reading.\n\n\n\n\n\n","category":"function"},{"location":"#Avro.tobuffer-Tuple{Any}","page":"Home","title":"Avro.tobuffer","text":"Avro.tobuffer(tbl; kw...)\n\nTake a Tables.jl-compatible input tbl and call Avro.writetable with an IOBuffer, which is returned, with position at the beginning.\n\n\n\n\n\n","category":"method"},{"location":"#Avro.write","page":"Home","title":"Avro.write","text":"Avro.write([filename|io,] x::T; kw...)\n\nWrite an object x of avro-supported type T in the avro format. If a file name is provided as a String as 1st argument, the avro data will be written out to disk. Similarly, an IO argument can be provided as 1st argument. If no destination 1st argument is provided, a byte buffer Vector{UInt8} will be returned with avro data written to it. Supported keyword arguments include:\n\nschema: the type that should be used when encoding the object in\n\nthe avro format; most common is providing a Union{...} type to write   the data out specifically as a \"union type\" instead of only the type of the object;   alternatively, a valid Avro.Schema can be passed, like the result of   Avro.parseschema(src)\n\n\n\n\n\n","category":"function"},{"location":"#Avro.writetable","page":"Home","title":"Avro.writetable","text":"Avro.writetable(io_or_file, tbl; kw...)\n\nWrite an input Tables.jl-compatible source table tbl out as an avro object container file. io_or_file can be a file name as a String or IO argument. If the input table supports Table.partitions, each partition will be written as a separate \"block\" in the container file.\n\nBecause avro data is strictly typed, if the input table doesn't have a well-defined schema (i.e. Tables.schema(Tables.rows(tbl)) === nothing), then Tables.dictrowtable(Tables.rows(tbl)) will be called, which scans the input table, \"building up\" the schema based on types of values found in each row.\n\nCompression is supported via the compress keyword argument, and can currently be one of :zstd, :deflate, :bzip2, or :xz.\n\n\n\n\n\n","category":"function"}]
}