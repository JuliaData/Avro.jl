module AvroCodecXzExt

using Avro, CodecXz

function xzdecompress(payload::AbstractVector{UInt8}, limits::Avro.Limits)
    # The decoder's default LZMA_CONCATENATED flag decodes every stream and validates the stream padding
    # between and after them. 256 MiB of decoder memory admits every preset (xz -9 needs 65 MiB) and
    # refuses absurd dictionaries.
    return Avro.transcodemembers(:xz, XzDecompressor(memlimit=256 * Avro.MiB), payload, limits.max_bytes)
end

function xzwriter(level)
    preset = level === nothing ? 6 : Int(level)
    0 <= preset <= 9 || throw(ArgumentError("the xz preset must be 0–9, got $preset"))
    return Avro.WriterCodec(:xz, () -> XzCompressor(level=preset))
end

function __init__()
    Avro.registercodec!(:xz, (decompress = xzdecompress, writer = xzwriter))
    return nothing
end

end # module
