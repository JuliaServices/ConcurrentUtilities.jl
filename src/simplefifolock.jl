"""
    SimpleFIFOLock()

A reentrant lock similar to Base.ReentrantLock, but with strict FIFO ordering.

This lock supports "barging", in which a thread can jump to the front of the queue of tasks:
```
lock(fifolock; first=true)
```

Calling `lock` inhibits running on finalizers on that thread.

Implementation note: The implementation uses a Condition.  Conditions provide FIFO
notification order, as well as the ability to jump to the front, which makes the
implementation straightforward.
"""
mutable struct SimpleFIFOLock <: AbstractLock
    cond::Threads.Condition
    reentrancy_count::UInt # 0 iff the lock is not held
    locked_by::Union{Task,Nothing} # nothing iff the lock is not held
    SimpleFIFOLock() = new(Threads.Condition(), 0, nothing)
end
    
"""
    trylock(l::SimpleFIFOLock)

Try to acquire lock `l`.  If successful, return `true`.  If the lock is held by another
task, do not wait and return `false`.

Each successful `trylock` must be matched by an `unlock`.
"""
@inline function Base.trylock(l::SimpleFIFOLock)
    GC.disable_finalizers()
    ct = current_task()
    lock(l.cond)
    locked_by = l.locked_by
    if locked_by === nothing || locked_by === ct
        l.reentrancy_count += 1
        l.locked_by = ct
        unlock(l.cond)
        return true
     end
     unlock(l.cond)
     GC.enable_finalizers()
     return false
end

"""
    lock(l::SimpleFIFOLock; first=false)

Acquire lock `l`. The lock is reentrant, so if the calling task has already acquired the
lock then return immediately.

Each `lock` must be matched by an `unlock`.
"""
@inline function Base.lock(l::SimpleFIFOLock; first=false)
    GC.disable_finalizers()
    ct = current_task()
    lock(l.cond)
    while true
        if l.locked_by === nothing || l.locked_by === ct
            l.reentrancy_count += 1
            l.locked_by = ct
            unlock(l.cond)
            return nothing
        end
        # Don't pay for the try-catch unless we `wait`.
        try
            wait(l.cond; first)
        catch
            unlock(l.cond)
            rethrow()
        end
    end
end
        
"""
    unlock(lock::SimpleFIFOLock)

Releases ownerhsip of `lock`.

Note if the has been more than once by the same thread, it will need to be unlocked the same
number of times.
"""
@inline function Base.unlock(l::SimpleFIFOLock)
    ct = current_task()
    lock(l.cond)
    if l.locked_by === nothing
        unlock(l.cond)
        error("unlocking an unlocked lock")
    end
    if l.locked_by !== ct
        unlock(l.cond)
        error("unlock from wrong thread")
    end
    l.reentrancy_count += -1
    if l.reentrancy_count == 0
        l.locked_by = nothing
        if !isempty(l.cond.waitq)
            # Don't pay for the try-catch unless we `notify`.
            try
                notify(l.cond; all=false)
            catch
                unlock(l.cond)
                rethrow()
            end
        end
    end
    unlock(l.cond)
    return nothing
end

# Performance note: for `@btime begin lock($l); unlock($l); end`.
#   13ns for SpinLock
#   17ns for ReentrantLock
#   33ns for FIFOLock
#   35ns for SimpleFIFOLock
#   57ns for the alternative versions below that use `@lock` (and hence have too much try-finally code)

# @inline function Base.lock(l::SimpleFIFOLock)
#     GC.disable_finalizers()
#     ct = current_task()
#     @lock l.cond begin
#         while true
#             if l.locked_by === nothing || l.locked_by === ct
#                 l.reentrancy_count += 1
#                 l.locked_by = ct
#                 return nothing
#             end
#             wait(l.cond)
#         end
#     end
# end
        
# @inline function Base.unlock(l::SimpleFIFOLock)
#     ct = current_task()
#     @lock l.cond begin
#         if l.locked_by === nothing
#             error("unlocking an unlocked lock")
#         end
#         if l.locked_by !== ct
#             error("unlock from wrong thread")
#         end
#         @assert l.reentrancy_count > 0
#         l.reentrancy_count += -1
#         if l.reentrancy_count == 0
#             l.locked_by = nothing
#             notify(l.cond; all=false)
#         end
#     end
#     return nothing
# end
