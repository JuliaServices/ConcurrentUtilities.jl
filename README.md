
# ConcurrentUtilities.jl

*Utilities for working with multithreaded workers for Julia services and applications*

## Installation

The package is registered in the [`General`](https://github.com/JuliaRegistries/General) registry and so can be installed at the REPL with `] add ConcurrentUtilities`.

## Usage

Each utility has a docstring with details, for example `?Pool` in the REPL.

- `Pool`, `acquire`, `release`, `drain!`: a thread-safe pool of reusable objects,
  optionally keyed, that limits how many objects are in use at once.
- `ReadWriteLock`, `readlock`, `readunlock`: a lock that allows many readers or one
  writer (Julia 1.8+).
- `FIFOLock`: a reentrant lock that waiting tasks acquire in arrival order (Julia 1.10+).
- `Lockable`: a value paired with the lock that guards it; `lock(f, l)` calls
  `f(value)` with the lock held.
- `OrderedSynchronizer`: lets concurrent tasks run a step in a fixed order with
  `put!(f, sync, i)`.
- `try_with_timeout`: runs `f` in a task and throws `TimeoutException` if it does not
  finish in time.
- `Worker`, `remote_eval`, `remote_fetch`, `terminate!`: run code in a separate Julia
  process.
- `@wkspawn`: like `Threads.@spawn`, but the finished task drops its code and
  task-local storage so they can be garbage collected.
- `ConcurrentUtilities.init` and `ConcurrentUtilities.@spawn`: run tasks on a fixed set
  of background worker tasks. `@spawn` waits for a free worker, so call `init` first.
