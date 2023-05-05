struct TimeoutError <: Exception
    timeout::Float64
end

mutable struct TimedOut{T}
    ch::Channel{T}
end

function timeout!(x::TimedOut, timeout)
    close(x.ch, TimeoutError(timeout))
    return
end

Base.getindex(x::TimedOut) = !isopen(x.ch)

"""
    try_with_timeout(f, timeout, T) -> T

Run `f` in a new task, and return its result. If `f` does not complete within
`timeout` seconds, throw a `TimeoutError`. If `f` throws an exception, rethrow
it. If `f` completes successfully, return its result.
`f` should be of the form `f(x::TimedOut)`, where `x` is a `TimedOut` object.
This allows the calling function to check whether the timeout has been reached
by checking `x[]` and if `true`, the timeout was reached and the function can
cancel/abort gracefully.
"""
function try_with_timeout(f, timeout, ::Type{T}=Any) where {T}
    ch = Channel{T}(0)
    x = TimedOut(ch)
    timer = Timer(tm -> timeout!(x, timeout), timeout)
    Threads.@spawn begin
        try
            put!(ch, $f(x))
        catch e
            close(ch, CapturedException(e, catch_backtrace()))
        finally
            close(timer)
        end
    end
    return take!(ch)
end
