using ConcurrentUtilities, Test

@testset "try_with_timeout" begin
    # test non time out
    @test try_with_timeout(_ -> 1, 1) === 1
    # test time out
    @test_throws TimeoutError try_with_timeout(1) do timedout
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
end
