"""
    TimeoutError

Thrown from `try_with_timeout` when the timeout is reached.
"""
struct TimeoutError <: Exception
    timeout::Float64
end

"""
    TimedOut

Helper object passed to user-provided `f` in `try_with_timeout`
that allows checking if the calling context reached a time out.
Call `x[]`, which returns a `Bool`, to check if the timeout was reached.
"""
struct TimedOut{T}
    ch::Channel{T}
end

Base.getindex(x::TimedOut) = !isopen(x.ch)

"""
    try_with_timeout(f, timeout, T=Any) -> T

Run `f` in a new task, and return its result. If `f` does not complete within
`timeout` seconds, throw a `TimeoutError`. If `f` throws an exception, rethrow
it. If `f` completes successfully, return its result.
`f` should be of the form `f(x::TimedOut)`, where `x` is a `TimedOut` object.
This allows the calling function to check whether the timeout has been reached
by checking `x[]` and if `true`, the timeout was reached and the function can
cancel/abort gracefully. The 3rd argument `T` is optional (default `Any`) and
allows passing an expected return type that `f` should return.
"""
function try_with_timeout(f, timeout, ::Type{T}=Any) where {T}
    ch = Channel{T}(0)
    x = TimedOut(ch)
    timer = Timer(tm -> close(ch, TimeoutError(timeout)), timeout)
    Threads.@spawn begin
        try
            put!(ch, $f(x)::T)
        catch e
            close(ch, CapturedException(e, catch_backtrace()))
        finally
            close(timer)
        end
    end
    return take!(ch)
end
