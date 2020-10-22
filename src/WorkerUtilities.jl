module WorkerUtilities

const WORK_QUEUE = Channel{Task}(0)
const WORKER_TASKS = Task[]

macro spawn(thunk)
    esc(quote
        tsk = @task $thunk
        tsk.storage = current_task().storage
        put!(WorkerUtilities.WORK_QUEUE, tsk)
        tsk
    end)
end

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

function init(nworkers=Threads.nthreads()-1)
    maxthreadid = nworkers + 1
    tids = nworkers == 1 ? (1:1) : 2:maxthreadid
    resize!(WORKER_TASKS, nworkers)
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

"""
Lockable(value)
Creates a `Lockable` object that wraps `value` and
associates it with a newly created `ReentrantLock`.
"""
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
function lock(f, l::Lockable)
    lock(l.lock) do
        f(l.value)
    end
end

# implement the rest of the Lock interface on Lockable
islocked(l::Lockable) = islocked(l.lock)
lock(l::Lockable) = lock(l.lock)
trylock(l::Lockable) = trylock(l.lock)
unlock(l::Lockable) = unlock(l.lock)

end # module
