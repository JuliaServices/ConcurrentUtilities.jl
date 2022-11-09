module WorkerUtilities

export Lockable, OrderedSynchronizer, reset!, ReadWriteLock, readlock, readunlock

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
end

OrderedSynchronizer(i=1) = OrderedSynchronizer(current_task(), Threads.Condition(), i)

"""
    reset!(x::OrderedSynchronizer, i=1)

Reset the state of `x` to `i`.
"""
function reset!(x::OrderedSynchronizer, i=1)
    Base.@lock x.cond begin
        x.i = i
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
    Base.@lock x.cond begin
        # wait until we're ready to execute f
        while x.i != i
            wait(x.cond)
        end
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
    waitingwriter::Union{Nothing, Task} # guarded by writelock
    readwait::Base.ThreadSynchronizer
    @atomic readercount::Int
    @atomic readerwait::Int
end

ReadWriteLock() = ReadWriteLock(ReentrantLock(), nothing, Base.ThreadSynchronizer(), 0, 0)

const MaxReaders = 1 << 30

function readlock(rw::ReadWriteLock)
    if (@atomic rw.readercount += 1) < 0
        # A writer is pending, wait for it.
        Base.@lock rw.readwait wait(rw.readwait)
    end
    return
end

function readunlock(rw::ReadWriteLock)
    if (@atomic rw.readercount -= 1) < 0
        # there's a pending write, check if we're the last reader
        @assert rw.waitingwriter !== nothing
        if (@atomic rw.readerwait -= 1) == 0
            # Last reader, wake up the writer.
            schedule(rw.waitingwriter)
        end
    end
    return
end

function Base.lock(rw::ReadWriteLock)
    lock(rw.writelock)
    r = (@atomic rw.readercount -= MaxReaders) + MaxReaders
    if r != 0 && (@atomic rw.readerwait += r) != 0
        # wait for active readers
        rw.waitingwriter = current_task()
        wait()
    end
    return
end

Base.islocked(rw::ReadWriteLock) = islocked(rw.writelock)

function Base.unlock(rw::ReadWriteLock)
    r = (@atomic rw.readercount += MaxReaders)
    if r > 0
        # wake up waiting readers
        Base.@lock rw.readwait notify(rw.readwait)
    end
    unlock(rw.writelock)
    return
end

end # module
