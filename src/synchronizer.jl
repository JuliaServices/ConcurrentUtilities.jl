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
"""
mutable struct OrderedSynchronizer
    coordinating_task::Task
    cond::Threads.Condition
    i::Int
@static if VERSION < v"1.7"
    closed::Threads.Atomic{Bool}
else
    @atomic closed::Bool
end
end

@static if VERSION < v"1.7"
OrderedSynchronizer(i=1) = OrderedSynchronizer(current_task(), Threads.Condition(), i, Threads.Atomic{Bool}(false))
else
OrderedSynchronizer(i=1) = OrderedSynchronizer(current_task(), Threads.Condition(), i, false)
end

"""
    reset!(x::OrderedSynchronizer, i=1)

Reset the state of `x` to `i`.
"""
function reset!(x::OrderedSynchronizer, i=1)
    Base.@lock x.cond begin
        x.i = i
@static if VERSION < v"1.7"
        x.closed[] = false
else
        @atomic :monotonic x.closed = false
end
    end
end

function Base.close(x::OrderedSynchronizer, excp::Exception=closed_exception())
    Base.@lock x.cond begin
@static if VERSION < v"1.7"
        x.closed[] = true
else
        @atomic :monotonic x.closed = true
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
        throw(closed_exception())
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
            Base.throwto(x.coordinating_task, e)
        end
        x.i += incr
        notify(x.cond)
    end
end
