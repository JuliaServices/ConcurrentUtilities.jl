using Test, WorkerUtilities

@testset "WorkerUtilities" begin

    @testset "WorkerUtilities.@spawn" begin

        WorkerUtilities.init()
        threadid = fetch(WorkerUtilities.@spawn(Threads.threadid()))
        @test Threads.nthreads() == 1 ? (threadid == 1) : (threadid != 1)
        @test WorkerUtilities.@spawn(false, 1 + 1).storage === nothing

    end # @testset "WorkerUtilities.@spawn"

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

    end

end # @testset "WorkerUtilities"