using ConcurrentUtilities, Test

@testset "try_with_timeout" begin
    # test non time out
    @test try_with_timeout(_ -> 1, 1) === 1
    # test time out
    @test_throws TimeoutException try_with_timeout(1) do timedout
        sleep(3)
        # this is a weird place to test, but it _does_
        # print a failure if it doesn't work
        @test timedout[]
    end
    # test exception
    @test_throws ErrorException try
        try_with_timeout(_ -> error("hey"), 1)
    catch e
        @test e isa CapturedException
        rethrow(e.ex)
    end
    # test return type
    @inferred try_with_timeout(_ -> 1, 1, Int)
    # bad return type
    @test_throws TypeError try
        try_with_timeout(_ -> 1, 1, String)
    catch e
        @test e isa CapturedException
        rethrow(e.ex)
    end

    # try_with_timeout should not migrate the task to a different thread pool
    if isdefined(Base.Threads, :threadpool)
        @test try_with_timeout(_ -> Threads.threadpool(), 1) == Threads.threadpool()
        @test read(`julia -t 1,1 -E 'using ConcurrentUtilities; try_with_timeout(_ -> Threads.threadpool(), 1)'`, String) == ":interactive\n"
        @test read(`julia -t 1,1 -E 'using ConcurrentUtilities; fetch(Threads.@spawn begin try_with_timeout(_ -> Threads.threadpool(), 1) end)'`, String) == ":default\n"
    end
end
