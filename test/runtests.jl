using Test, ConcurrentUtilities

@testset "ConcurrentUtilities" begin

    @testset "ConcurrentUtilities.@spawn" begin

        ConcurrentUtilities.init()
        threadid = fetch(ConcurrentUtilities.@spawn(Threads.threadid()))
        @show Threads.nthreads(), threadid
        @test Threads.nthreads() == 1 ? (threadid == 1) : (threadid != 1)
        @test ConcurrentUtilities.@spawn(false, 1 + 1).storage === nothing
        failed = ConcurrentUtilities.@spawn error("expected failure")
        @test_throws TaskFailedException wait(failed)
        @test all(t -> !istaskdone(t), ConcurrentUtilities.WORKER_TASKS)
        @test fetch(ConcurrentUtilities.@spawn(1 + 1)) == 2

    end # @testset "ConcurrentUtilities.@spawn"

    @testset "Lockable" begin
        # Lockable{T, L<:AbstractLock}
        let # test the constructor `Lockable(value, lock)`
            lockable = Lockable(Dict("foo" => "hello"), ReentrantLock())
            @test lockable.value["foo"] == "hello"
            lock(lockable) do d
                @test d["foo"] == "hello"
            end
            lock(lockable) do d
                d["foo"] = "goodbye"
            end
            @test lockable.value["foo"] == "goodbye"
            lock(lockable) do d
                @test d["foo"] == "goodbye"
            end
        end
        let # test the constructor `Lockable(value)`
            lockable = Lockable(Dict("foo" => "hello"))
            @test lockable.value["foo"] == "hello"
            lock(lockable) do d
                @test d["foo"] == "hello"
            end
            lock(lockable) do d
                d["foo"] = "goodbye"
            end
            @test lockable.value["foo"] == "goodbye"
            lock(lockable) do d
                @test d["foo"] == "goodbye"
            end
        end
    end # @testset "Lockable"

    @testset "OrderedSynchronizer" begin

        x = OrderedSynchronizer()
        A = Vector{Int}(undef, 10)
        @sync for i = 10:-1:1
            @async put!(x, i) do
                A[i] = i
            end
        end
        @test A == 1:10

        reset!(x)
        A = Vector{Int}(undef, 10)
        @sync for i = 1:10
            @async put!(x, i) do
                A[i] = i
            end
        end
        @test A == 1:10

        reset!(x)
        A = Vector{Int}(undef, 10)
        @sync for i in (2, 1, 4, 3, 6, 5, 8, 7, 10, 9)
            @async put!(x, i) do
                A[i] = i
            end
        end
        @test A == 1:10

        reset!(x)
        A = Vector{Int}(undef, 4)
        @sync for (i, j) in zip((2, 1, 4, 3), (3, 1, 7, 5))
            @async put!(x, j, 2) do
                A[i] = j
            end
        end
        @test A == [1, 3, 5, 7]

        reset!(x)
        ref = Ref(false)
        ch = Channel(0)
        t = @async begin
            put!(ch, true)
            put!(x, 2) do
                ref[] = true
            end
        end
        # wait until the task is blocked
        take!(ch)
        # test put! hasn't run yet and task isn't done
        @test !ref[]
        @test !istaskdone(t)
        # cancel put! by closing the sync
        close(x)
        e = try
            fetch(t)
        catch e
            e.task.result
        end
        @test e == ConcurrentUtilities.closed_exception()
    end

    @testset "ReadWriteLock" begin
@static if VERSION < v"1.8"
        @warn "skipping ReadWriteLock tests since VERSION ($VERSION) < v\"1.8\""
else
        rw = ReadWriteLock()
        @test rw isa Base.AbstractLock

        @test trylock(rw)
        @test islocked(rw)
        unlock(rw)
        @test !islocked(rw)

        readlock(rw)
        @test islocked(rw)
        @test !trylock(rw)
        @test trylock(() -> error("unreachable"), rw) === false
        @test (@atomic rw.readercount) == 1
        @test (@atomic rw.readerwait) == 0
        readunlock(rw)
        @test !islocked(rw)

        try_result = trylock(rw) do
            @test islocked(rw)
            :write
        end
        @test try_result === :write
        @test !islocked(rw)

        lock(rw)
        @test !trylock(rw)
        @test islocked(rw)
        unlock(rw)
        @test !islocked(rw)
        @test (@atomic rw.readercount) == 0
        @test (@atomic rw.readerwait) == 0

        if Threads.nthreads() > 1
            observed_writer_state = Threads.Atomic{Bool}(false)
            stop_observing = Threads.Atomic{Bool}(false)
            reader_started = Channel(1)
            reader = Threads.@spawn begin
                readlock(rw)
                put!(reader_started, nothing)
                while !stop_observing[]
                    if (@atomic :acquire rw.readercount) < 0
                        observed_writer_state[] = true
                    end
                end
                readunlock(rw)
            end
            take!(reader_started)
            all_failed = all(!trylock(rw) for _ in 1:100_000)
            stop_observing[] = true
            wait(reader)
            @test all_failed
            @test !observed_writer_state[]
            @test (@atomic rw.readerwait) == 0
        end

        if isdefined(Base, :Lockable)
            lockable = Base.Lockable(Ref(1), rw)
            lock(lockable) do value
                value[] += 1
            end
            @test lockable.value[] == 2
        end

        read_result = readlock(rw) do
            @test (@atomic rw.readercount) == 1
            :read
        end
        @test read_result === :read
        @test (@atomic rw.readercount) == 0

        write_result = lock(rw) do
            @test islocked(rw)
            :write
        end
        @test write_result === :write
        @test !islocked(rw)
        @test (@atomic rw.readercount) == 0

        @test_throws ErrorException begin
            readlock(rw) do
                error("read scope failed")
            end
        end
        @test (@atomic rw.readercount) == 0

        @test_throws ErrorException begin
            lock(rw) do
                error("write scope failed")
            end
        end
        @test !islocked(rw)
        @test (@atomic rw.readercount) == 0

        println("test read is blocked while writing")
        lock(rw)
        c = Channel()
        t = @async begin
            put!(c, nothing)
            readlock(rw)
            take!(c)
            readunlock(rw)
            true
        end
        take!(c)
        @test !istaskdone(t)
        unlock(rw)
        @test !istaskdone(t)
        put!(c, nothing)
        @test fetch(t)

        println("test write is blocked until reader done")
        readlock(rw)
        c = Channel()
        t = @async begin
            put!(c, nothing)
            lock(rw)
            take!(c)
            @test islocked(rw)
            unlock(rw)
            true
        end
        take!(c)
        @test !istaskdone(t)
        readunlock(rw)
        @test !istaskdone(t)
        put!(c, nothing)
        @test fetch(t)

        println("test new reads blocked on pending write, and vice versa")
        readlock(rw)
        @test islocked(rw)
        # start another reader
        secondReaderLocked = Ref(false)
        c = Channel()
        r2 = @async begin
            put!(c, nothing)
            readlock(rw)
            secondReaderLocked[] = true
            take!(c)
            readunlock(rw)
            true
        end
        take!(c)
        wc = Channel()
        t = @async begin
            put!(wc, nothing)
            lock(rw)
            take!(wc)
            unlock(rw)
            true
        end
        take!(wc)
        # write task not done
        @test !istaskdone(t)
        # first reader not done
        @test !istaskdone(r2)
        # but first reader did lock
        @test secondReaderLocked[]
        # start a third reader
        thirdReaderLocked = Ref(false)
        c2 = Channel()
        r3 = @async begin
            put!(c2, nothing)
            readlock(rw)
            thirdReaderLocked[] = true
            take!(c2)
            readunlock(rw)
            true
        end
        take!(c2)
        # no tasks have finished yet
        @test !istaskdone(t)
        @test !istaskdone(r2)
        @test !istaskdone(r3)
        # but third reader didn't lock because it's blocked
        # on a _pending_ write
        @test !thirdReaderLocked[]
        # second writer, which should wait til after the already-queued third reader
        wc2 = Channel()
        t2 = @async begin
            put!(wc2, nothing)
            lock(rw)
            take!(wc2)
            unlock(rw)
            true
        end
        take!(wc2)
        # unblock r2
        put!(c, nothing)
        # it should finish
        @test fetch(r2)
        # now unlock 1st reader so write can happen
        readunlock(rw)
        # write task should finish
        put!(wc, nothing)
        @test fetch(t)
        # now that write has finished, r3 should have lock
        put!(c2, nothing)
        @test thirdReaderLocked[]
        @test fetch(r3)
        # only now r3 has finished should t2 have lock
        put!(wc2, nothing)
        @test fetch(t2)
        @test !islocked(rw)

        # A reader released by one writer must not wait behind the next writer,
        # since the next writer includes that reader in readerwait.
        rw = ReadWriteLock()
        lock(rw)
        lock(rw.readwait)
        delayed_reader = @async begin
            readlock(rw)
            readunlock(rw)
            true
        end
        while (@atomic rw.readercount) != -ConcurrentUtilities.MaxReaders + 1
            yield()
        end
        unlock(rw)
        next_writer = @async begin
            lock(rw)
            unlock(rw)
            true
        end
        while (@atomic rw.readerwait) != 1
            yield()
        end
        unlock(rw.readwait)
        @test fetch(delayed_reader)
        @test fetch(next_writer)
        @test (@atomic rw.readercount) == 0
        @test (@atomic rw.readerwait) == 0
        @test !islocked(rw)
end # @static if VERSION < v"1.8"
    end

    @testset "FIFOLock" begin
@static if VERSION < v"1.10-"
        @warn "skipping FIFOLock tests since VERSION ($VERSION) < v\"1.10\""
else
        ctr_out = Threads.Atomic{Int}(1)
        test_tasks = Task[]
        sizehint!(test_tasks, 16)
        tasks_out = zeros(Int, 16)
        tot = zeros(Int, 1)
        fl = FIFOLock()
        c = Base.GenericCondition{FIFOLock}(fl)
        lock(c)
        try
            @test notify(c) == 0
        finally
            unlock(c)
        end
        waitq_tail = let fl = fl
            function ()
                c = fl.cond_wait
                lock(c)
                try
                    return c.waitq.tail
                finally
                    unlock(c)
                end
            end
        end
        lock(fl)
        try
            tail = waitq_tail()
            for i in 1:16
                # Queue each task before starting the next one so the FIFO
                # assertion tracks lock wait order, not scheduler timing.
                t = Threads.@spawn begin
                    lock(fl)
                    try
                        tot[1] += 1
                        tasks_out[i] = Threads.atomic_add!(ctr_out, 1)
                    finally
                        unlock(fl)
                    end
                end
                push!(test_tasks, t)
                while waitq_tail() === tail
                    yield()
                end
                tail = waitq_tail()
            end
        finally
            unlock(fl)
        end
        for t in test_tasks
            @test try
                wait(t)
                true
            catch
                false
            end
        end
        @test tot[1] == 16
        @test tasks_out == 1:16

@static if isdefined(Base, :CancellationTokenSource)
        fl = FIFOLock()
        lock(fl)
        source = Base.CancellationTokenSource()
        cancelled = Base.ScopedValues.with(
            () -> Threads.@spawn(lock(fl)),
            Base.CANCEL_TOKEN => Base.CancellationToken(source),
        )
        while isempty(fl.cond_wait)
            yield()
        end
        Base.cancel!(source)
        @test timedwait(() -> istaskdone(cancelled), 5) == :ok
        @test_throws TaskFailedException wait(cancelled)
        unlock(fl)
        @test lock(() -> true, fl)
        @test !islocked(fl)
end
end # @static if VERSION < v"1.10"
    end

    # track all workers every created
    ALL_WORKERS = []
    ConcurrentUtilities.Workers.GLOBAL_CALLBACK_PER_WORKER[] = w -> push!(ALL_WORKERS, w)
    include("workers.jl")
    # After all tests have run, check we didn't leave any workers running.
    for w in ALL_WORKERS
        if process_running(w.process) || !Workers.terminated(w)
            @show w
        end
        @test !process_running(w.process)
        @test !isopen(w.pipe)
        @test Workers.terminated(w)
        @test istaskstarted(w.messages) && istaskdone(w.messages)
        @test istaskstarted(w.output) && istaskdone(w.output)
        @test istaskstarted(w.worksubmission) && istaskdone(w.worksubmission)
        @test !isopen(w.workqueue)
        @test isempty(w.futures)
    end
    include("pools.jl")
    include("try_with_timeout.jl")
end

    # @testset "@wkspawn" begin
        # basics
        @test fetch(@wkspawn(1 + 1)) == 2
        
        if isdefined(Base.Threads, :maxthreadid)
            # interactive threadpool
            @test fetch(@wkspawn(:interactive, 1 + 1)) == 2
        end
        
        # show incorrect behavior
        ref = Ref(10)
        ansref = Ref(0)
        wkref = WeakRef(ref)
        t = let ref=ref
            Threads.@spawn begin
                ansref[] = $ref[]
            end
        end
        wait(t)
        @test ansref[] == 10
        t = nothing; ref = nothing; GC.gc(true); GC.gc(true); GC.gc(true)
        # Older Julia versions can retain this captured value; newer Julia
        # versions may collect it. `@wkspawn` below should collect it either way.
        base_spawn_value = wkref.value
        @test base_spawn_value === nothing || base_spawn_value.x == 10
        
        # and now with @wkspawn
        ref = Ref(10)
        ansref = Ref(0)
        wkref = WeakRef(ref)
        t = let ref=ref
            @wkspawn begin
                ansref[] = $ref[]
            end
        end
        wait(t)
        @test ansref[] == 10
        t = nothing; ref = nothing; GC.gc(true); GC.gc(true); GC.gc(true)
        @show wkref
        # correctly GCed
        @test wkref.value === nothing

        # captures are also cleared when the spawned expression fails
        ref = Ref(10)
        wkref = WeakRef(ref)
        t = let ref=ref
            @wkspawn begin
                $ref[]
                error("expected failure")
            end
        end
        @test_throws TaskFailedException wait(t)
        @test t.code === nothing
        ref = nothing
        GC.gc(true); GC.gc(true); GC.gc(true)
        @test wkref.value === nothing
#     end

# end # @testset "ConcurrentUtilities"
