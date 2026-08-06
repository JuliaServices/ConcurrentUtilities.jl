using ConcurrentUtilities.Workers
using Test, IOCapture

@testset "Worker basics" verbose=true begin

    w = Worker()
    @testset "correct connected/running states ($w)" begin
        @test w.pid > 0
        @test process_running(w.process)
        @test isopen(w.pipe)
        @test !Workers.terminated(w)
        background_tasks = (w.messages, w.output, w.worksubmission)
        @test timedwait(() -> all(istaskstarted, background_tasks), 10) == :ok
        @test all(t -> !istaskdone(t), background_tasks)
        @test isempty(w.futures)
    end
    @testset "clean shutdown ($w)" begin
        close(w)
        @test !process_running(w.process)
        @test !isopen(w.pipe)
        @test Workers.terminated(w)
        @test istaskstarted(w.messages) && istaskdone(w.messages)
        @test istaskstarted(w.output) && istaskdone(w.output)
        @test istaskstarted(w.worksubmission) && istaskdone(w.worksubmission)
        @test !isopen(w.workqueue)
        @test isempty(w.futures)
    end

    w = Worker()
    @testset "more forceful shutdown ($w)" begin
        @test w.pid > 0
        terminate!(w)
        wait(w)
        @test !process_running(w.process)
        @test !isopen(w.pipe)
        @test Workers.terminated(w)
        @test istaskstarted(w.messages) && istaskdone(w.messages)
        @test istaskstarted(w.output) && istaskdone(w.output)
        @test istaskstarted(w.worksubmission) && istaskdone(w.worksubmission)
        @test !isopen(w.workqueue)
        @test isempty(w.futures)
    end

    w = Worker()
    @testset "remote_eval/remote_fetch ($w)" begin
        expr = quote
            global x
            x = 101
        end
        ret = remote_fetch(w, expr)
        @test ret == 101
        @test isempty(w.futures) # should be empty since we're not waiting for a response
        # now fetch the remote value
        expr = quote
            global x
            x
        end
        fut = remote_eval(w, expr)
        @test fetch(fut) == 101
        @test isempty(w.futures) # should be empty since we've received all expected responses

        # test remote_call w/ exception
        expr = quote
            error("oops")
        end
        fut = remote_eval(w, expr)
        @test_throws CapturedException fetch(fut)
        close(w)
    end

    # avoid crash logs escaping to stdout, as it confuses PkgEval
    # https://github.com/JuliaTesting/ReTestItems.jl/issues/38
    w = Worker(; worker_redirect_io=devnull)
    @testset "worker crashing ($w)" begin
        expr = quote
            ccall(:abort, Cvoid, ())
        end
        fut = remote_eval(w, expr)
        @test_throws Workers.WorkerTerminatedException fetch(fut)
        wait(w)
        @test !process_running(w.process)
        @test !isopen(w.pipe)
        @test Workers.terminated(w)
        @test istaskstarted(w.messages) && istaskdone(w.messages)
        @test istaskstarted(w.output) && istaskdone(w.output)
        @test istaskstarted(w.worksubmission) && istaskdone(w.worksubmission)
        @test !isopen(w.workqueue)
        @test isempty(w.futures)
        close(w)
    end

    w = Worker()
    @testset "remote_eval ($w)" begin
        fut = remote_eval(w, :(1 + 2))
        @test fetch(fut) == 3
        # test remote module loading
        fut = remote_eval(w, :(using Test; @test 1 == 1))
        @test fetch(fut) isa Test.Pass
        close(w)
    end
end

@testset "Worker connection failure cleanup" begin
    if Sys.isunix()
        mktempdir() do dir
            withenv("TMPDIR" => dir) do
                @test_throws ConcurrentUtilities.TimeoutException Worker(
                    exeflags=`--version`,
                    connect_timeout=1,
                    worker_redirect_io=devnull,
                )
                @test isempty(readdir(dir))
            end
        end
    end
end

@testset "stale response after terminate! does not kill message loop" begin
    w = Worker(worker_redirect_io=devnull)
    fut = remote_eval(w, :(sleep(0.2); 1 + 1))
    while Base.@lock(w.lock, isempty(w.futures))
        yield()
    end
    lock(w.lock)
    # while we hold the coordinator lock, the worker's response arrives and
    # the messages task blocks behind us before it can look up the future
    sleep(1)
    # simulate a concurrent terminate! winning the race to the futures table
    wte = WorkerTerminatedException(w)
    for (_, f) in w.futures
        close(f.value, wte)
    end
    empty!(w.futures)
@static if VERSION < v"1.7"
    w.terminated[] = true
else
    @atomic w.terminated = true
end
    unlock(w.lock)
    @test_throws WorkerTerminatedException fetch(fut)
    # finish what terminate! would have done, then wait for a clean shutdown;
    # the stale response must be dropped rather than kill the messages task
    close(w.workqueue, wte)
    kill(w.process, Base.SIGKILL)
    while !process_exited(w.process)
        sleep(0.1)
    end
    close(w.pipe)
    close(w.server)
    @test wait(w)
    @test istaskstarted(w.messages) && istaskdone(w.messages)
    @test isempty(w.futures)
end

@testset "Workers print in color" begin
    project_path = pkgdir(ConcurrentUtilities)
    code = """
    using ConcurrentUtilities.Workers
    remote_fetch(Worker(), :(printstyled("this better ber red\n", color=:red)))
    """
    # Need to run in a separate process to force --color=yes in CI.
    logs = IOCapture.capture(color=true) do
        run(`$(Base.julia_cmd()) --project=$project_path --color=yes -e $code`)
    end
    @test contains(logs.output,  "\e[31mthis better ber red\e[39m\n")
end
