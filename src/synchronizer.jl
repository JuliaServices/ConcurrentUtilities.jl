"""
    OrderedSynchronizer(i=1)

A threadsafe synchronizer that allows ensuring concurrent work is done
in a specific order. The `OrderedSynchronizer` is initialized with an
integer `i` that represents the current "order" of the synchronizer.

Work is "scheduled" by calling `put!(f, x, i)`, where `f` is a function
that will be called like `f()` when the synchronizer is at order `i`,
and will otherwise wait until other calls to `put!` have finished
to bring the synchronizer's state to `i`. Once `f()` is called, the
synchronizer's state is incremented by 1 and any waiting `put!` calls
check to see if it's their turn to execute.

A synchronizer's state can be reset to a specific value (1 by default)
by calling `reset!(x, i)`.

`close(x, excp)` makes waiting and later `put!` calls throw `excp`
(an `InvalidStateException` by default) until `reset!` reopens `x`.
"""
mutable struct OrderedSynchronizer
    cond::Threads.Condition
    i::Int
    excp::Union{Nothing, Exception}
@static if VERSION < v"1.7"
    closed::Threads.Atomic{Bool}
else
    @atomic closed::Bool
end
end

@static if VERSION < v"1.7"
OrderedSynchronizer(i=1) = OrderedSynchronizer(Threads.Condition(), i, nothing, Threads.Atomic{Bool}(false))
else
OrderedSynchronizer(i=1) = OrderedSynchronizer(Threads.Condition(), i, nothing, false)
end

"""
    reset!(x::OrderedSynchronizer, i=1)

Reset the state of `x` to `i`.
"""
function reset!(x::OrderedSynchronizer, i=1)
    Base.@lock x.cond begin
        x.i = i
        x.excp = nothing
@static if VERSION < v"1.7"
        x.closed[] = false
else
        @atomic :monotonic x.closed = false
end
    end
end

function Base.close(x::OrderedSynchronizer, excp::Exception=closed_exception())
    cleanup_lock(x.cond) do
        x.excp = excp
@static if VERSION < v"1.7"
        x.closed[] = true
else
        @atomic :release x.closed = true
end
        Base.notify_error(x.cond, excp)
    end
    return
end

@static if VERSION < v"1.7"
    Base.isopen(x::OrderedSynchronizer) = !x.closed[]
else
Base.isopen(x::OrderedSynchronizer) = !(@atomic :monotonic x.closed)
end
closed_exception() = InvalidStateException("OrderedSynchronizer is closed.", :closed)

function check_closed(x::OrderedSynchronizer)
    if !isopen(x)
        # if the monotonic load succeed, now do an acquire fence
@static if VERSION < v"1.7"
        !x.closed[] && Base.concurrency_violation()
else
        !(@atomic :acquire x.closed) && Base.concurrency_violation()
end
        excp = x.excp
        throw(excp === nothing ? closed_exception() : excp)
    end
end

"""
    put!(f::Function, x::OrderedSynchronizer, i::Int, incr::Int=1)

Schedule `f` to be called when `x` is at order `i`. Note that `put!`
will block until `f` is executed. The typical usage involves something
like:

```julia
x = OrderedSynchronizer()
@sync for i = 1:N
    Threads.@spawn begin
        # do some concurrent work
        # once work is done, schedule synchronization
        put!(x, \$i) do
            # report back result of concurrent work
            # won't be executed until all `i-1` calls to `put!` have already finished
        end
    end
end
```

The `incr` argument controls how much the synchronizer's state is
incremented after `f` is called. By default, `incr` is 1.

If `f` throws, `put!` closes `x` with that exception and rethrows it, so
waiting and later `put!` calls throw it too instead of waiting forever.
"""
function Base.put!(f, x::OrderedSynchronizer, i, incr=1)
    check_closed(x)
    Base.@lock x.cond begin
        # wait until we're ready to execute f
        while x.i != i
            check_closed(x)
            wait(x.cond)
        end
        check_closed(x)
        try
            f()
        catch e
            # later steps can never run in order, so fail them now rather than leave them waiting
            e isa Exception ? close(x, e) : close(x)
            rethrow()
        end
        x.i += incr
        notify(x.cond)
    end
    return nothing
end
