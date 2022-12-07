module WorkerUtilities

export Lockable, OrderedSynchronizer, reset!, ReadWriteLock, readlock, readunlock, @wkspawn

const WORK_QUEUE = Channel{Task}(0)
const WORKER_TASKS = Task[]

"""
  WorkerUtilities.@spawn expr
  WorkerUtilities.@spawn passthroughstorage::Bool expr

Similar to `Threads.@spawn`, schedule and execute a task (given by `expr`)
that will be run on a "background worker" (see [`WorkerUtilities.init`]((@ref))).

In the 2-argument invocation, `passthroughstorage` controls whether the task-local storage of the
`current_task()` should be "passed through" to the spawned task.
"""
macro spawn(thunk)
    esc(quote
        tsk = @task $thunk
        tsk.storage = current_task().storage
        put!(WorkerUtilities.WORK_QUEUE, tsk)
        tsk
    end)
end

"""
  WorkerUtilities.@spawn expr
  WorkerUtilities.@spawn passthroughstorage::Bool expr

Similar to `Threads.@spawn`, schedule and execute a task (given by `expr`)
that will be run on a "background worker" (see [`WorkerUtilities.init`]((@ref))).

In the 2-argument invocation, `passthroughstorage` controls whether the task-local storage of the
`current_task()` should be "passed through" to the spawned task.
"""
macro spawn(passthroughstorage, thunk)
    esc(quote
        tsk = @task $thunk
        if $passthroughstorage
            tsk.storage = current_task().storage
        end
        put!(WorkerUtilities.WORK_QUEUE, tsk)
        tsk
    end)
end

"""
  WorkerUtilities.init(nworkers=Threads.nthreads() - 1)

Initialize background workers that will execute tasks spawned via
[`WorkerUtilities.@spawn`](@ref). If `nworkers == 1`, a single worker
will be started on thread 1 where tasks will be executed in contention
with other thread 1 work. Background worker tasks can be inspected by
looking at `WorkerUtilities.WORKER_TASKS`.
"""
function init(nworkers=Threads.nthreads()-1)
    maxthreadid = nworkers + 1
    tids = Threads.nthreads() == 1 ? (1:1) : 2:maxthreadid
    resize!(WORKER_TASKS, max(nworkers, 1))
    Threads.@threads for tid in 1:maxthreadid
        if tid in tids
            WORKER_TASKS[tid == 1 ? 1 : (tid - 1)] = Base.@async begin
                for task in WORK_QUEUE
                    schedule(task)
                    wait(task)
                end
            end
        end
    end
    return
end

"""
  Lockable(value, lock = ReentrantLock())

Creates a `Lockable` object that wraps `value` and
associates it with the provided `lock`.
"""
struct Lockable{T, L <: Base.AbstractLock}
    value::T
    lock::L
end

Lockable(value) = Lockable(value, ReentrantLock())

"""
  lock(f::Function, l::Lockable)

Acquire the lock associated with `l`, execute `f` with the lock held,
and release the lock when `f` returns. `f` will receive one positional
argument: the value wrapped by `l`. If the lock is already locked by a
different task/thread, wait for it to become available.
When this function returns, the `lock` has been released, so the caller should
not attempt to `unlock` it.
"""
function Base.lock(f, l::Lockable)
    lock(l.lock) do
        f(l.value)
    end
end

# implement the rest of the Lock interface on Lockable
Base.islocked(l::Lockable) = islocked(l.lock)
Base.lock(l::Lockable) = lock(l.lock)
Base.trylock(l::Lockable) = trylock(l.lock)
Base.unlock(l::Lockable) = unlock(l.lock)

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
    @atomic closed::Bool
end

OrderedSynchronizer(i=1) = OrderedSynchronizer(current_task(), Threads.Condition(), i, false)

"""
    reset!(x::OrderedSynchronizer, i=1)

Reset the state of `x` to `i`.
"""
function reset!(x::OrderedSynchronizer, i=1)
    Base.@lock x.cond begin
        x.i = i
        @atomic :monotonic x.closed = false
    end
end

function Base.close(x::OrderedSynchronizer, excp::Exception=closed_exception())
    Base.@lock x.cond begin
        @atomic :monotonic x.closed = true
        Base.notify_error(x.cond, excp)
    end
    return
end

Base.isopen(x::OrderedSynchronizer) = !(@atomic :monotonic x.closed)
closed_exception() = InvalidStateException("OrderedSynchronizer is closed.", :closed)

function check_closed(x::OrderedSynchronizer)
    if !isopen(x)
        # if the monotonic load succeed, now do an acquire fence
        !(@atomic :acquire x.closed) && Base.concurrency_violation()
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

mutable struct ReadWriteLock
    writelock::ReentrantLock
    @atomic waitingwriter::Union{Nothing, Task}
    readwait::Base.ThreadSynchronizer
    @atomic readercount::Int
    @atomic readerwait::Int
end

ReadWriteLock() = ReadWriteLock(ReentrantLock(), nothing, Base.ThreadSynchronizer(), 0, 0)

const MaxReaders = 1 << 30

function readlock(rw::ReadWriteLock)
    if (@atomic :acquire_release rw.readercount += 1) < 0
        # A writer is active or pending, so we need to wait
        Base.@lock rw.readwait wait(rw.readwait)
    end
    return
end

function readunlock(rw::ReadWriteLock)
    if (@atomic :acquire_release rw.readercount -= 1) < 0
        # there's a pending write, check if we're the last reader
        if (@atomic :acquire_release rw.readerwait -= 1) == 0
            # Last reader, wake up the writer.
            schedule(rw.waitingwriter)
        end
    end
    return
end

function Base.lock(rw::ReadWriteLock)
    lock(rw.writelock) # only a single writer allowed at a time
    # ok, here's how we do this: we subtract MaxReaders from readercount
    # to make readercount negative; this will prevent any further readers
    # from locking, while maintaining our actual reader count so we
    # can track when we're able to write
    r = (@atomic :acquire_release rw.readercount -= MaxReaders) + MaxReaders
    # if r == 0, that means there were no readers,
    # so we can proceed directly with the write lock
    # if r == 1, this is an interesting case because there's only 1 reader
    # and we might be racing to acquire the write lock and the reader
    # unlocking; so we _also_ atomically set and check readerwait;
    # if readerwait == 0, then the reader won the race and decremented readerwait
    # to -1, and we increment by 1 to 0, so we know the reader is done and can proceed
    # without waiting. If _we_ win the race, then we'll continue to waiting
    # and the reader will decrement and then schedule us
    if r != 0 && (@atomic :acquire_release rw.readerwait += r) != 0
        # otherwise, there are readers, so we need to wait for them to finish
        # we do this by setting ourselves as the waiting writer
        # and wait for the last reader to re-schedule us
        @atomic rw.waitingwriter = current_task()
        wait()
    end
    return
end

Base.islocked(rw::ReadWriteLock) = islocked(rw.writelock)

function Base.unlock(rw::ReadWriteLock)
    r = (@atomic :acquire_release rw.readercount += MaxReaders)
    if r > 0
        # wake up waiting readers
        Base.@lock rw.readwait notify(rw.readwait)
    end
    unlock(rw.writelock)
    return
end

# mostly copied from Base definitions in task.jl and threadingconstructs.jl
# but adapted to wrap interpolated mutable arguments in WeakRef
function _lift_one_interp!(e)
    letargs = Any[]  # store the new gensymed arguments
    unwrapargs = Any[]
    _lift_one_interp_helper(e, false, letargs, unwrapargs) # Start out _not_ in a quote context (false)
    letargs, unwrapargs
end
_lift_one_interp_helper(v, _, _, _) = v
function _lift_one_interp_helper(expr::Expr, in_quote_context, letargs, unwrapargs)
    if expr.head === :$
        if in_quote_context  # This $ is simply interpolating out of the quote
            # Now, we're out of the quote, so any _further_ $ is ours.
            in_quote_context = false
        else
            letarg = gensym()
            newarg = gensym()
            push!(letargs, :($(esc(letarg)) = ismutable($(esc(expr.args[1]))) ? WeakRef($(esc(expr.args[1]))) : $(esc(expr.args[1]))))
            push!(unwrapargs, :($newarg = $unwrap($letarg)))
            return newarg  # Don't recurse into the lifted $() exprs
        end
    elseif expr.head === :quote
        in_quote_context = true   # Don't try to lift $ directly out of quotes
    elseif expr.head === :macrocall
        return expr  # Don't recur into macro calls, since some other macros use $
    end
    for (i,e) in enumerate(expr.args)
        expr.args[i] = _lift_one_interp_helper(e, in_quote_context, letargs, unwrapargs)
    end
    expr
end

unwrap(@nospecialize(val)) = val isa WeakRef ? val.value : val

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
the variable's value in the current task. Interpolating a _mutable_ variable
will also cause it to be wrapped in a `WeakRef`, so that Julia's internal
references to these arguments won't prevent them from being garbage collected
once the `Task` has finished running.
"""
macro wkspawn(args...)
    tpid = Int8(0)
    na = length(args)
    if na == 2
        ttype, ex = args
        if ttype isa QuoteNode
            ttype = ttype.value
        elseif ttype isa Symbol
            # TODO: allow unquoted symbols
            ttype = nothing
        end
        if ttype === :interactive
            tpid = Int8(1)
        elseif ttype !== :default
            throw(ArgumentError("unsupported threadpool in @spawn: $ttype"))
        end
    elseif na == 1
        ex = args[1]
    else
        throw(ArgumentError("wrong number of arguments in @spawn"))
    end

    letargs, unwrapargs = _lift_one_interp!(ex)
    _ex = quote
        $(unwrapargs...)
        $ex
    end
    thunk = esc(:(()->($_ex)))
    var = esc(Base.sync_varname)
    quote
        let $(letargs...)
            local task = Task($thunk)
            task.sticky = false
            @static if isdefined(Base.Threads, :maxthreadid)
                ccall(:jl_set_task_threadpoolid, Cint, (Any, Int8), task, $tpid)
            end
            if $(Expr(:islocal, var))
                put!($var, task)
            end
            schedule(task)
            task
        end
    end
end


end # module
