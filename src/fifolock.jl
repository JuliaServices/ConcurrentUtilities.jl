@static if VERSION >= v"1.10-"

const LOCKED_BIT = 0b01

"""
    FIFOLock()

A reentrant lock similar to Base.ReentrantLock, but with strict FIFO ordering.

Base.ReentrantLock allows tasks to "barge the lock", i.e. it is occasionally
possible for a task to jump the queue of tasks waiting for the lock; this is
intentional behavior to increase throughput as described
[here](https://webkit.org/blog/6161/locking-in-webkit/).

When fairness is more important than throughput, use this FIFOLock.
"""
mutable struct FIFOLock <: AbstractLock
    @atomic locked_by::Union{Task, Nothing}
    reentrancy_cnt::UInt32
    @atomic havelock::UInt8
    cond_wait::Base.ThreadSynchronizer

    FIFOLock() = new(nothing, 0x0000_0000, 0x00, Base.ThreadSynchronizer())
end

assert_havelock(l::FIFOLock) = assert_havelock(l, l.locked_by)
islocked(l::FIFOLock) = (@atomic :monotonic l.havelock) & LOCKED_BIT != 0

# Correctness reasoning:
# 
# `havelock` can only be unset with the `cond_wait` lock held.
# Locking then first tries to set `havelock`; on failure, we
# acquire the `cond_wait` lock and try to set `havelock` again
# to ensure that we didn't coincide with an `unlock`. If we
# fail again, then we are assured that we will not miss an
# `unlock`, because we hold the `cond_wait` lock. Thus we can
# safely wait on `cond_wait`.
#
# FIFO ordering is ensured in `unlock`, which first acquires
# the `cond_wait` lock. If `cond_wait`'s wait queue is empty,
# the lock is released. Otherwise, we pop the first task in
# the wait queue, transfer ownership to it, schedule it, and
# return. Thus when one or more tasks are waiting,`havelock`
# is never reset.

"""
    trylock(l::FIFOLock)

Try to acquire lock `l`. If successful, return `true`. If the lock is
held by another task, do not wait and return `false`. 
"""
@inline function trylock(l::FIFOLock)
    ct = current_task()
    if l.locked_by === ct
        l.reentrancy_cnt += 0x0000_0001
        return true
    end
    return _trylock(l, ct)
end
@noinline function _trylock(l::FIFOLock, ct::Task)
    GC.disable_finalizers()
    if (@atomicreplace :acquire l.havelock 0x00 => LOCKED_BIT).success
        l.reentrancy_cnt = 0x0000_0001
        @atomic :release l.locked_by = ct
        return true
    end
    GC.enable_finalizers()
    return false
end

"""
    lock(l::FIFOLock)

Acquire lock `l`. If the calling task has already acquired the lock
previously, increment an internal counter and return to support
reentrancy. Each `lock` call must be matched with an `unlock` call.

As with `Base.ReentrantLock`, acquiring a lock will inhibit running
finalizers on that thread until the lock is released.

FIFO behavior is handled in `unlock`.
"""
@inline function lock(l::FIFOLock)
    trylock(l) || _lock(l)
end
@noinline function _lock(l::FIFOLock)
    ct = current_task()
    c = l.cond_wait
    lock(c)
    try
        _trylock(l, ct) && return
        GC.disable_finalizers()
        wait(c)
        # l.locked_by and l.reentrancy_cnt are set in unlock
    finally
        unlock(c)
    end
    return
end

"""
    unlock(l::FIFOLock)

Release ownership of the lock `l`. If the lock was acquired recursively,
the number of unlocks must match the number of locks before `l` is
actually released.

FIFO behavior is enforced here, in `unlock`: if one or more tasks are
waiting on the lock, we do not actually unlock; just hand ownership to
the first waiting task and schedule it.
"""
@inline function unlock(l::FIFOLock)
    ct = current_task()
    if l.locked_by !== ct
        error("unlock from wrong thread")
    end
    if l.reentrancy_cnt == 0x0000_0000
        error("unlock count must match lock count")
    end
    _unlock(l)
end
@noinline function _unlock(l::FIFOLock)
    ct = current_task()
    c = l.cond_wait
    n = l.reentrancy_cnt - 0x0000_0001
    if n == 0x0000_0000
        lock(c)
        try
            if isempty(c.waitq)
                l.reentrancy_cnt = n
                @atomic :release l.locked_by = nothing
                @atomic :release l.havelock = 0x00
            else
                t = popfirst!(c.waitq)
                @atomic :release l.locked_by = t
                schedule(t)
                # Leave l.reentrancy_cnt at 1
            end
        finally
            unlock(c)
        end
        GC.enable_finalizers()
    else
        l.reentrancy_cnt = n
    end
    return
end

end
