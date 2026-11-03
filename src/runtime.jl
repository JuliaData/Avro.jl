# Calls `juliac --trim` cannot follow statically: run-time dispatch for the routes trimmed programs leave
# out, stream I/O, and the static call edges and finalizer registration that let it compile code which
# only the runtime calls.

# `f(args...)` dispatched when called. `juliac --trim` neither compiles nor verifies such a call, so the code
# it reaches stays out of trimmed programs, where it raises a `MethodError` for `f` instead (`notcompiled`).
Base.@nospecializeinfer function runtimecall(f, @nospecialize(args...))
    argv = Vector{Any}(undef, length(args))
    for i in eachindex(argv)
        argv[i] = args[i]
    end
    return GC.@preserve argv ccall(:jl_apply_generic, Any, (Any, Ptr{Any}, UInt32), f, pointer(argv), UInt32(length(argv)))
end

function notcompiled(err, f)
    return err isa MethodError && err.f === f
end

# `f(io, args...)` for the stream types programs mostly read and write (files and in-memory buffers); other
# `IO` types dispatch at run time.
@inline function streamcall(f::F, io::IO, args...) where {F}
    io isa IOStream && return f(io, args...)
    io isa IOBuffer && return f(io, args...)
    return runtimecall(f, io, args...)
end

# Never true and not foldable. `juliac --trim` compiles a task body or finalizer only through a static call to it.
const TRIM_CALL_EDGE = Ref(false)

# Run `f(x)` when `x` is collected. `Base.finalizer` takes its function `@nospecialize`, which `juliac --trim`
# cannot resolve, so this registers it with the runtime directly.
function addfinalizer(f::F, x) where {F}
    TRIM_CALL_EDGE[] && f(x)
    ccall(:jl_gc_add_finalizer_th, Cvoid, (Ptr{Cvoid}, Any, Any), Core.getptls(), x, f)
    return x
end
