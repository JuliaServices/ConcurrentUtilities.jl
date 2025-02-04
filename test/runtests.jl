using Test, ConcurrentUtilities

@testset "ConcurrentUtilities" begin

    @testset "ConcurrentUtilities.@spawn" begin

        ConcurrentUtilities.init()
        threadid = fetch(ConcurrentUtilities.@spawn(Threads.threadid()))
        @show Threads.nthreads(), threadid
        @test Threads.nthreads() == 1 ? (threadid == 1) : (threadid != 1)
        @test ConcurrentUtilities.@spawn(false, 1 + 1).storage === nothing

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
        # readlock doesn't count as "locked"
        @test !islocked(rw)
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
end # @static if VERSION < v"1.8"
    end

    @testset "FIFOLock" begin
@static if VERSION < v"1.10-"
        @warn "skipping FIFOLock tests since VERSION ($VERSION) < v\"1.10\""
else
        ctr_in = Threads.Atomic{Int}(1)
        ctr_out = Threads.Atomic{Int}(1)
        test_tasks = Task[]
        sizehint!(test_tasks, 16)
        tasks_in = zeros(Int, 16)
        tasks_out = zeros(Int, 16)
        tot = zeros(Int, 1)
        fl = FIFOLock()
        lock(fl)
        try
            for i in 1:16
                t = Threads.@spawn begin
                    tasks_in[i] = Threads.atomic_add!(ctr_in, 1)
                    lock(fl)
                    try
                        tot[1] += 1
                        tasks_out[i] = Threads.atomic_add!(ctr_out, 1)
                    finally
                        unlock(fl)
                    end
                end
                push!(test_tasks, t)
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
        @test tasks_out == tasks_in
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
        # there should be no program references to ref, and 3 GC calls
        # should have collected it, but it's still alive
        @test wkref.value.x == 10
        
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
#     end

# end # @testset "ConcurrentUtilities"
