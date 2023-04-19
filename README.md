
# ConcurrentUtilities.jl

*Utilities for working with multithreaded workers for Julia services and applications*

## Installation

The package is registered in the [`General`](https://github.com/JuliaRegistries/General) registry and so can be installed at the REPL with `] add ConcurrentUtilities`.

## Usage

### `ConcurrentUtilities.init`

    ConcurrentUtilities.init(nworkers=Threads.nthreads() - 1)

Initialize background workers that will execute tasks spawned via
[`ConcurrentUtilities.@spawn`](@ref). If `nworkers == 1`, a single worker
will be started on thread 1 where tasks will be executed in contention
with other thread 1 work. Background worker tasks can be inspected by
looking at `ConcurrentUtilities.WORKER_TASKS`.

### `ConcurrentUtilities.@spawn`
    ConcurrentUtilities.@spawn expr
    ConcurrentUtilities.@spawn passthroughstorage expr

Similar to `Threads.@spawn`, schedule and execute a task (given by `expr`)
that will be run on a "background worker" (see [`ConcurrentUtilities.init`]((@ref))).

In the 2-argument invocation, `passthroughstorage` controls whether the task-local storage of the
`current_task()` should be "passed through" to the spawned task.

### `Lockable`

    Lockable(value, lock = ReentrantLock())

Creates a `Lockable` object that wraps `value` and
associates it with the provided `lock`.

    lock(f::Function, l::Lockable)

Acquire the lock associated with `l`, execute `f` with the lock held,
and release the lock when `f` returns. `f` will receive one positional
argument: the value wrapped by `l`. If the lock is already locked by a
different task/thread, wait for it to become available.
When this function returns, the `lock` has been released, so the caller should
not attempt to `unlock` it.