module AvroCodecBzip2Ext

using Avro, CodecBzip2

function bzip2decompress(payload::AbstractVector{UInt8}, limits::Avro.Limits)
    return Avro.transcodemembers(:bzip2, Bzip2Decompressor(), payload, limits.max_bytes)
end

function bzip2writer(level)
    lvl = level === nothing ? 9 : Int(level)
    1 <= lvl <= 9 || throw(ArgumentError("the bzip2 level must be 1–9, got $lvl"))
    return Avro.WriterCodec(:bzip2, () -> Bzip2Compressor(blocksize100k=lvl))
end

function __init__()
    Avro.registercodec!(:bzip2, (decompress = bzip2decompress, writer = bzip2writer))
    return nothing
end

end # module
