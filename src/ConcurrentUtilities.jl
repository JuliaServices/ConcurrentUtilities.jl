module ConcurrentUtilities

import Base: AbstractLock, islocked, trylock, lock, unlock
export Lockable, OrderedSynchronizer, reset!, ReadWriteLock, readlock, readunlock, @wkspawn,
    Workers, remote_eval, remote_fetch, Worker, terminate!, WorkerTerminatedException,
    Pool, acquire, release, drain!, try_with_timeout, TimeoutException, FIFOLock

macro samethreadpool_spawn(expr)
    if VERSION >= v"1.9.2"
        esc(:(Threads.@spawn Threads.threadpool() $expr))
    else
        esc(:(Threads.@spawn $expr))
    end
end

include("try_with_timeout.jl")
include("workers.jl")
using .Workers
isdefined(Base, :Lockable) ? (using Base: Lockable) : include("lockable.jl") # https://github.com/JuliaLang/julia/pull/52898
include("spawn.jl")
include("synchronizer.jl")
include("rwlock.jl")
include("pools.jl")
using .Pools
include("fifolock.jl")

function clear_current_task()
    current_task().storage = nothing
    current_task().code = nothing
    return
end

"""
    @wkspawn [:default|:interactive] expr

Create a `Task` and `schedule` it to run on any available
thread in the specified threadpool (`:default` if unspecified). The task is
allocated to a thread once one becomes available. To wait for the task to
finish, call `wait` on the result of this macro, or call
`fetch` to wait and then obtain its return value.

Values can be interpolated into `@wkspawn` via `\$`, which copies the value
directly into the constructed underlying closure. This allows you to insert
the _value_ of a variable, isolating the asynchronous code from changes to
the variable's value in the current task.
"""
macro wkspawn(args...)
    e = args[end]
    expr = quote
        ret = $e
        $(clear_current_task)()
        ret
    end
@static if isdefined(Base.Threads, :maxthreadid)
    q = esc(:(Threads.@spawn $(args[1:end-1]...) $expr))
else
    q = esc(:(Threads.@spawn $expr))
end
    return q
end

end # module
