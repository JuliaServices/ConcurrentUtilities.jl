const WORK_QUEUE = Channel{Task}(0)
const WORKER_TASKS = Task[]

"""
    ConcurrentUtilities.@spawn expr
    ConcurrentUtilities.@spawn passthroughstorage::Bool expr

Like `Threads.@spawn`, but run `expr` on one of the background workers started by
[`ConcurrentUtilities.init`](@ref). `@spawn` waits until a worker takes the task, so
call `init` first. The task shares the caller's task-local storage unless
`passthroughstorage` is `false`.
"""
macro spawn(thunk)
    esc(quote
        tsk = @task $thunk
        tsk.storage = current_task().storage
        put!($WORK_QUEUE, tsk)
        tsk
    end)
end

macro spawn(passthroughstorage, thunk)
    esc(quote
        tsk = @task $thunk
        if $passthroughstorage
            tsk.storage = current_task().storage
        end
        put!($WORK_QUEUE, tsk)
        tsk
    end)
end

"""
    ConcurrentUtilities.init(nworkers=Threads.nthreads() - 1)

Start `nworkers` worker tasks that each run one [`ConcurrentUtilities.@spawn`](@ref)
task at a time. With a single thread, the one worker shares it with other work.
The worker tasks are stored in `ConcurrentUtilities.WORKER_TASKS`.
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
                    try
                        wait(task)
                    catch e
                        e isa TaskFailedException || rethrow()
                    end
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
                    try
                        wait(task)
                    catch e
                        e isa TaskFailedException || rethrow()
                    end
                end
            end
        end
    end
end
    return
end
