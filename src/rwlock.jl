@static if VERSION >= v"1.8"

"""
    ReadWriteLock()

A threadsafe lock that allows multiple readers or a single writer.

The read side is acquired/released via `readlock(rw)` and `readunlock(rw)`,
while the write side is acquired/released via `lock(rw)`, `trylock(rw)`, and
`unlock(rw)`.
Use `readlock(rw) do ... end` or `lock(rw) do ... end` to scope lock
ownership to a function call.

While a writer is active, all readers will block. Once the writer is finished,
all pending readers will be allowed to acquire/release before the next writer.
"""
mutable struct ReadWriteLock <: Base.AbstractLock
    writelock::ReentrantLock
    readwait::Threads.Condition
    writeready::Threads.Event
    # `readercount` keeps track of how many readers are active or pending
    # if > 0, then that's the number of active readers
    # if < 0, then there is a writer active or pending, and
    # `readercount + MaxReaders` is the number of active or pending readers
    @atomic readercount::Int
    @atomic readerwait::Int
end

function ReadWriteLock()
@static if VERSION < v"1.8"
    throw(ArgumentError("ReadWriteLock requires Julia v1.8 or greater"))
else
    return ReadWriteLock(ReentrantLock(), Threads.Condition(), Threads.Event(true), 0, 0)
end
end

const MaxReaders = 1 << 30

function readlock(rw::ReadWriteLock)
    # first step is to increment readercount atomically
    if (@atomic :acquire_release rw.readercount += 1) < 0
        # if we observe from our atomic operation that readercount is < 0,
        # a writer was active or pending, so we need initiate the "slowpath"
        # by acquiring the readwait lock
        Base.@lock rw.readwait begin
            # check our condition again, if it holds, then we get in line
            # to be notified once the writer is done (the writer also acquires
            # the readwait lock, so we'll be waiting until it releases it)
            if rw.readercount < 0
                # writer still active
                wait(rw.readwait)
            end
        end
    end
    return
end

"""
    readlock(f, rw::ReadWriteLock)

Acquire the read side of `rw`, execute `f`, and release the read side when `f`
returns or throws.
"""
function readlock(f, rw::ReadWriteLock)
    readlock(rw)
    try
        return f()
    finally
        readunlock(rw)
    end
end

function readunlock(rw::ReadWriteLock)
    # similar to `readlock`, the first step is to decrement `readercount` atomically
    if (@atomic :acquire_release rw.readercount -= 1) < 0
        # if we observe from our atomic operation that readercount is < 0,
        # there's a pending write, so check if we're the last reader
        if (@atomic :acquire_release rw.readerwait -= 1) == 0
            # we observed that there was a pending write AND we just
            # observed that we were the last reader, so it's our
            # responsibility to notify the writer that it can proceed
            notify(rw.writeready)
        end
    end
    return
end

function Base.lock(rw::ReadWriteLock)
    lock(rw.writelock) # only a single writer allowed at a time
    # first, we subtract MaxReaders from readercount
    # to make readercount negative; this will prevent any further readers
    # from locking, while maintaining our actual reader count so we
    # can track when we're able to write
    r = (@atomic :acquire_release rw.readercount -= MaxReaders) + MaxReaders
    # we also _add_ MaxReaders so that `r` is the number of active readers
    # if r == 0, that means there were no readers,
    # so we can proceed directly with the write lock (the 1st check below in the if)
    # if there _are_ active readers, then we need to wait until they all unlock
    # by incrementing `readerwait` by `r`, we're atomically setting the # of read
    # unlocks we expect to happen before we can proceed with the write lock
    # (readers decrement `readerwait` if they observe `readercount` is negative)
    # if, by chance, the last reader manages to unlock before we increment `readerwait`,
    # then `readerwait` will actually be negative and we'll increment it back to 0
    # in that case, we can proceed directly with the write lock (the 2nd check below)
    if r != 0 && (@atomic :acquire_release rw.readerwait += r) != 0
        # otherwise, there are still pending readers, so we need to wait for them to finish
        # we do this by waiting on the `writeready` event, which will be
        # notified when the last reader unlocks; if the last reader happens
        # to be racing with us, then `writeready` will already be set and
        # we'll proceed immediately
        wait(rw.writeready)
    end
    return
end

"""
    trylock(rw::ReadWriteLock)

Attempt to acquire the write side of `rw` without blocking. Return `true` on
success and `false` if either the read or write side is already held.
"""
function Base.trylock(rw::ReadWriteLock)
    trylock(rw.writelock) || return false
    result = @atomicreplace :acquire_release :acquire rw.readercount 0 => -MaxReaders
    result.success || unlock(rw.writelock)
    return result.success
end

"""
    lock(f, rw::ReadWriteLock)

Acquire the write side of `rw`, execute `f`, and release the write side when
`f` returns or throws.
"""
function Base.lock(f, rw::ReadWriteLock)
    lock(rw)
    try
        return f()
    finally
        unlock(rw)
    end
end

"""
    islocked(rw::ReadWriteLock)

Return `true` if either the read or write side of `rw` is held.
"""
function Base.islocked(rw::ReadWriteLock)
    return islocked(rw.writelock) || (@atomic :monotonic rw.readercount) != 0
end

function Base.unlock(rw::ReadWriteLock)
    r = (@atomic :acquire_release rw.readercount += MaxReaders)
    if r > 0
        # wake up waiting readers
        Base.@lock rw.readwait notify(rw.readwait)
    end
    unlock(rw.writelock)
    return
end

end
