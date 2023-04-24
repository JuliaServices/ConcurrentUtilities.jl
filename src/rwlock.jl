"""
    ReadWriteLock()

A threadsafe lock that allows multiple readers or a single writer.

The read side is acquired/released via `readlock(rw)` and `readunlock(rw)`,
while the write side is acquired/released via `lock(rw)` and `unlock(rw)`.

While a writer is active, all readers will block. Once the writer is finished,
all pending readers will be allowed to acquire the lock before the next writer.
"""
mutable struct ReadWriteLock
    writelock::ReentrantLock
    waitingwriter::Union{Nothing, Task}
    readwait::Threads.Condition
@static if VERSION < v"1.7"
    readercount::Threads.Atomic{Int}
    readerwait::Threads.Atomic{Int}
else
    @atomic readercount::Int
    @atomic readerwait::Int
end
end

@static if VERSION < v"1.7"
ReadWriteLock() = ReadWriteLock(ReentrantLock(), nothing, Threads.Condition(), Threads.Atomic{Int}(0), Threads.Atomic{Int}(0))
else
ReadWriteLock() = ReadWriteLock(ReentrantLock(), nothing, Threads.Condition(), 0, 0)
end

const MaxReaders = 1 << 30

function readlock(rw::ReadWriteLock)
@static if VERSION < v"1.7"
    Threads.atomic_add!(rw.readercount, 1)
    if rw.readercount[] < 0
        # A writer is active or pending, so we need to wait
        Base.@lock rw.readwait begin
            # check our condition again
            if rw.readercount[] < 0
                # writer still active
                wait(rw.readwait)
            end
        end
    end
else
    if (@atomic :acquire_release rw.readercount += 1) < 0
        # A writer is active or pending, so we need to wait
        Base.@lock rw.readwait begin
            # check our condition again
            if rw.readercount < 0
                # writer still active
                wait(rw.readwait)
            end
        end
    end
end
    return
end

function readunlock(rw::ReadWriteLock)
@static if VERSION < v"1.7"
    Threads.atomic_sub!(rw.readercount, 1)
    if rw.readercount[] < 0
        # there's a pending write, check if we're the last reader
        Threads.atomic_sub!(rw.readerwait, 1)
        if rw.readerwait[] == 0
            # Last reader, wake up the writer.
            @assert rw.waitingwriter !== nothing
            schedule(rw.waitingwriter)
            rw.waitingwriter = nothing
        end
    end
else
    if (@atomic :acquire_release rw.readercount -= 1) < 0
        # there's a pending write, check if we're the last reader
        if (@atomic :acquire_release rw.readerwait -= 1) == 0
            # Last reader, wake up the writer.
            @assert rw.waitingwriter !== nothing
            schedule(rw.waitingwriter)
            rw.waitingwriter = nothing
        end
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
@static if VERSION < v"1.7"
    Threads.atomic_sub!(rw.readercount, MaxReaders)
    r = rw.readercount[] + MaxReaders
else
    r = (@atomic :acquire_release rw.readercount -= MaxReaders) + MaxReaders
end
    # if r == 0, that means there were no readers,
    # so we can proceed directly with the write lock
    # if r == 1, this is an interesting case because there's only 1 reader
    # and we might be racing to acquire the write lock and the reader
    # unlocking; so we _also_ atomically set and check readerwait;
    # if readerwait == 0, then the reader won the race and decremented readerwait
    # to -1, and we increment by 1 to 0, so we know the reader is done and can proceed
    # without waiting. If _we_ win the race, then we'll continue to waiting
    # and the reader will decrement and then schedule us
@static if VERSION < v"1.7"
    if r != 0
        Threads.atomic_add!(rw.readerwait, r)
        if rw.readerwait[] != 0
            # otherwise, there are readers, so we need to wait for them to finish
            # we do this by setting ourselves as the waiting writer
            # and wait for the last reader to re-schedule us
            rw.waitingwriter = current_task()
            wait()
        end
    end
else
    if r != 0 && (@atomic :acquire_release rw.readerwait += r) != 0
        # otherwise, there are readers, so we need to wait for them to finish
        # we do this by setting ourselves as the waiting writer
        # and wait for the last reader to re-schedule us
        @assert rw.waitingwriter === nothing
        rw.waitingwriter = current_task()
        wait()
    end
end
    return
end

Base.islocked(rw::ReadWriteLock) = islocked(rw.writelock)

function Base.unlock(rw::ReadWriteLock)
@static if VERSION < v"1.7"
    Threads.atomic_add!(rw.readercount, MaxReaders)
    r = rw.readercount[]
else
    r = (@atomic :acquire_release rw.readercount += MaxReaders)
end
    if r > 0
        # wake up waiting readers
        Base.@lock rw.readwait notify(rw.readwait)
    end
    unlock(rw.writelock)
    return
end