const WORK_QUEUE = Channel{Task}(0)
const WORKER_TASKS = Task[]

"""
  ConcurrentUtilities.@spawn expr
  ConcurrentUtilities.@spawn passthroughstorage::Bool expr

Similar to `Threads.@spawn`, schedule and execute a task (given by `expr`)
that will be run on a "background worker" (see [`ConcurrentUtilities.init`]((@ref))).

In the 2-argument invocation, `passthroughstorage` controls whether the task-local storage of the
`current_task()` should be "passed through" to the spawned task.
"""
macro spawn(thunk)
    esc(quote
        tsk = @task $thunk
        tsk.storage = current_task().storage
        put!(ConcurrentUtilities.WORK_QUEUE, tsk)
        tsk
    end)
end

"""
  ConcurrentUtilities.@spawn expr
  ConcurrentUtilities.@spawn passthroughstorage::Bool expr

Similar to `Threads.@spawn`, schedule and execute a task (given by `expr`)
that will be run on a "background worker" (see [`ConcurrentUtilities.init`]((@ref))).

In the 2-argument invocation, `passthroughstorage` controls whether the task-local storage of the
`current_task()` should be "passed through" to the spawned task.
"""
macro spawn(passthroughstorage, thunk)
    esc(quote
        tsk = @task $thunk
        if $passthroughstorage
            tsk.storage = current_task().storage
        end
        put!(ConcurrentUtilities.WORK_QUEUE, tsk)
        tsk
    end)
end

"""
  ConcurrentUtilities.init(nworkers=Threads.nthreads() - 1)

Initialize background workers that will execute tasks spawned via
[`ConcurrentUtilities.@spawn`](@ref). If `nworkers == 1`, a single worker
will be started on thread 1 where tasks will be executed in contention
with other thread 1 work. Background worker tasks can be inspected by
looking at `ConcurrentUtilities.WORKER_TASKS`.
"""
function init(nworkers=Threads.nthreads()-1)
    maxthreadid = nworkers + 1
    tids = Threads.nthreads() == 1 ? (1:1) : 2:maxthreadid
    resize!(WORKER_TASKS, max(nworkers, 1))
@static if VERSION < v"1.8.0"
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
else
    Threads.@threads :static for tid in 1:maxthreadid
        if tid in tids
            WORKER_TASKS[tid == 1 ? 1 : (tid - 1)] = Base.@async begin
                for task in WORK_QUEUE
                    schedule(task)
                    wait(task)
                end
            end
        end
    end
end
    return
end