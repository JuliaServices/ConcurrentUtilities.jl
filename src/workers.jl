module Workers

using Sockets, Serialization

export Worker, remote_eval, remote_fetch, terminate!, WorkerTerminatedException

import ..try_with_timeout

# RPC framework
struct Request
    mod::Symbol # module in which we should eval
    expr::Expr # expression to eval
    id::UInt64 # unique id for this request
    # if true, worker should terminate immediately after receiving this Request
    # ignoring other fields
    shutdown::Bool
end

# worker executes Request and returns a serialized Response object *if* Request wasn't a shutdown
struct Response
    result
    error::Union{Nothing, Exception}
    id::UInt64 # matches a corresponding Request.id
end

# simple FutureResult that coordinator can wait on until a Response comes back for a Request
struct FutureResult
    id::UInt64 # matches a corresponding Request.id
    value::Channel{Any} # size 1
end

Base.fetch(f::FutureResult) = fetch(f.value)

mutable struct Worker
    lock::ReentrantLock # protects the .futures field; no other fields are modified after construction
    pid::Int
    process::Base.Process
    server::Sockets.PipeServer
    pipe::Base.PipeEndpoint
    messages::Task
    output::Task
    process_watch::Task
    worksubmission::Task
    workqueue::Channel{Tuple{Request, FutureResult}}
    futures::Dict{UInt64, FutureResult} # Request.id -> FutureResult
@static if VERSION < v"1.7"
    terminated::Threads.Atomic{Bool}
else
    @atomic terminated::Bool
end
end

# used to close FutureResult.value channels when a worker terminates
struct WorkerTerminatedException <: Exception
    worker::Worker
end

# atomics compat
macro atomiccas(ex, cmp, val)
    @static if VERSION < v"1.7"
        return esc(quote
            _ret = Threads.atomic_cas!($ex, $cmp, $val)
            (; success=(_ret === $cmp))
        end)
    else
        return esc(:(@atomicreplace $ex $cmp => $val))
    end
end

macro atomicget(ex)
    @static if VERSION < v"1.7"
        return esc(Expr(:ref, ex))
    else
        return esc(:(@atomic :acquire $ex))
    end
end

terminated(w::Worker) = @atomicget(w.terminated)

# performs all the "closing" tasks of worker fields
# but does not *wait* for a final close state
# so typically callers should call wait(w) after this
function terminate!(w::Worker, from::Symbol=:manual)
    if @atomiccas(w.terminated, false, true).success
        # we won getting to close down the worker
        @debug "terminating worker $(w.pid) from $from"
        wte = WorkerTerminatedException(w)
        Base.@lock w.lock begin
            for (_, fut) in w.futures
                close(fut.value, wte)
            end
            empty!(w.futures)
        end
        signal = Base.SIGTERM
        while true
            kill(w.process, signal)
            signal = signal == Base.SIGTERM ? Base.SIGINT : Base.SIGKILL
            process_exited(w.process) && break
            sleep(0.1)
            process_exited(w.process) && break
        end
        close(w.pipe)
        close(w.server)
    end
    return
end

# Base.Process has a nifty .exitnotify Condition
# so we might as well get notified when the process exits
# as one of our ways of detecting the worker has gone away
function watch_and_terminate!(w::Worker)
    wait(w.process)
    terminate!(w, :watch_and_terminate)
    true
end

# gracefully terminate a worker by sending a shutdown message
# and waiting for the other tasks to perform worker shutdown
function Base.close(w::Worker)
    if !terminated(w) && isopen(w.pipe)
        req = Request(Symbol(), :(), rand(UInt64), true)
        Base.@lock w.lock begin
            serialize(w.pipe, req)
        end
    end
    wait(w)
    return
end

# wait until our spawned tasks have all finished
Base.wait(w::Worker) = fetch(w.process_watch) && fetch(w.messages) && fetch(w.output)

Base.show(io::IO, w::Worker) = print(io, "Worker(pid=$(w.pid)", terminated(w) ? ", terminated=true, termsignal=$(w.process.termsignal)" : "", ")")

# used in testing to ensure all created workers are
# eventually cleaned up properly
const GLOBAL_CALLBACK_PER_WORKER = Ref{Any}()

function Worker(;
    env::AbstractDict=ENV,
    dir::String=pwd(),
    threads::String="auto",
    exeflags=`--threads=$threads`,
    connect_timeout::Int=60,
    worker_redirect_io::IO=stdout,
    worker_redirect_fn=(io, pid, line)->println(io, "  Worker $pid:  $line")
    )
    # below copied from Distributed.launch
    env = Dict{String, String}(env)
    pathsep = Sys.iswindows() ? ";" : ":"
    if get(env, "JULIA_LOAD_PATH", nothing) === nothing
        env["JULIA_LOAD_PATH"] = join(LOAD_PATH, pathsep)
    end
    if get(env, "JULIA_DEPOT_PATH", nothing) === nothing
        env["JULIA_DEPOT_PATH"] = join(DEPOT_PATH, pathsep)
    end
    # Set the active project on workers using JULIA_PROJECT.
    # Users can opt-out of this by (i) passing `env = ...` or (ii) passing
    # `--project=...` as `exeflags` to addprocs(...).
    project = Base.ACTIVE_PROJECT[]
    if project !== nothing && get(env, "JULIA_PROJECT", nothing) === nothing
        env["JULIA_PROJECT"] = project
    end
    # end copied from Distributed.launch
    ## start the worker process
    file = tempname()
    server = Sockets.listen(file)
    color = get(worker_redirect_io, :color, false) ? "yes" : "no" # respect color of target io
    exec = "include(\"$(@__DIR__)/ConcurrentUtilities.jl\"); using ConcurrentUtilities: Workers; Workers.startworker(\"$file\")"
    cmd = `$(Base.julia_cmd()) $exeflags --startup-file=no --color=$color -e $exec`
    proc = open(detach(setenv(addenv(cmd, env), dir=dir)), "r+")
    pid = Libc.getpid(proc)

    ## connect to the worker process with timeout
    try
        pipe = try_with_timeout(x -> Sockets.accept(server), connect_timeout)
        # create worker
@static if VERSION < v"1.7"
        w = Worker(ReentrantLock(), pid, proc, server, pipe, Task(nothing), Task(nothing), Task(nothing), Task(nothing), Channel{Tuple{Request, FutureResult}}(), Dict{UInt64, FutureResult}(), Threads.Atomic{Bool}(false))
else
        w = Worker(ReentrantLock(), pid, proc, server, pipe, Task(nothing), Task(nothing), Task(nothing), Task(nothing), Channel{Tuple{Request, FutureResult}}(), Dict{UInt64, FutureResult}(), false)
end
        ## start a task to watch for worker process termination
        w.process_watch = Threads.@spawn watch_and_terminate!(w)
        ## start a task to redirect worker output
        w.output = Threads.@spawn redirect_worker_output(worker_redirect_io, w, worker_redirect_fn, proc)
        ## start a task to listen for worker messages
        w.messages = Threads.@spawn process_responses(w)
        ## start a task to process eval requests and send to worker
        w.worksubmission = Threads.@spawn process_work(w)
        # add a finalizer
        finalizer(x -> @async(terminate!(x, :finalizer)), w) # @async to allow a task switch
        if isassigned(GLOBAL_CALLBACK_PER_WORKER)
            GLOBAL_CALLBACK_PER_WORKER[](w)
        end
        return w
    catch
        # cleanup in case connect fails/times out
        kill(proc, Base.SIGKILL)
        @isdefined(sock) && close(sock)
        @isdefined(w) && terminate!(w, :Worker_catch)
        rethrow()
    end
end

function redirect_worker_output(io::IO, w::Worker, fn, proc)
    try
        while !process_exited(proc) && !@atomicget(w.terminated)
            line = readline(proc)
            if !isempty(line)
                fn(io, w.pid, line)
                flush(io)
            end
        end
    catch e
        # @error "Error redirecting worker output $(w.pid)" exception=(e, catch_backtrace())
        terminate!(w, :redirect_worker_output)
        e isa EOFError || e isa Base.IOError || rethrow()
    end
    true
end

function process_responses(w::Worker)
    lock = w.lock
    reqs = w.futures
    try
        while isopen(w.pipe) && !@atomicget(w.terminated)
            # get the next Response from the worker
            r = deserialize(w.pipe)
            @assert r isa Response "Received invalid response from worker $(w.pid): $(r)"
            # println("Received response $(r) from worker $(w.pid)")
            Base.@lock lock begin
                # look up the FutureResult for this request
                fut = pop!(reqs, r.id)
                @assert !isready(fut.value) "Received duplicate response for request $(r.id) from worker $(w.pid)"
                if r.error !== nothing
                    # this allows rethrowing the exception from the worker to the caller
                    close(fut.value, r.error)
                else
                    put!(fut.value, r.result)
                end
            end
        end
    catch e
        # @error "Error processing responses from worker $(w.pid)" exception=(e, catch_backtrace())
        terminate!(w, :process_responses)
        e isa EOFError || e isa Base.IOError || rethrow()
    end
    true
end

function process_work(w::Worker)
    try
        for (req, fut) in w.workqueue
            # println("Sending request $(req) to worker $(w.pid)")
            Base.@lock w.lock begin
                w.futures[req.id] = fut
            end
            serialize(w.pipe, req)
        end
    catch e
        # @error "Error processing work for worker $(w.pid)" exception=(e, catch_backtrace())
        terminate!(w, :process_work)
        # e isa EOFError || e isa Base.IOError || rethrow()
    end
end

remote_eval(w::Worker, expr) = remote_eval(w, Main, expr.head == :block ? Expr(:toplevel, expr.args...) : expr)

function remote_eval(w::Worker, mod, expr)
    terminated(w) && throw(WorkerTerminatedException(w))
    # we only send the Symbol module name to the worker
    req = Request(nameof(mod), expr, rand(UInt64), false)
    fut = FutureResult(req.id, Channel(1))
    put!(w.workqueue, (req, fut))
    return fut
end

# convenience call to eval and fetch in one step
remote_fetch(w::Worker, args...) = fetch(remote_eval(w, args...))

# compat for `Threads.@spawn :interactive expr`
@static if hasmethod(getfield(Threads, Symbol("@spawn")), Tuple{LineNumberNode, Module, Symbol, Expr})
    macro _spawn_interactive(ex)
        esc(:(Threads.@spawn :interactive $ex))
    end
else
    macro _spawn_interactive(ex)
        esc(:(@async $ex))
    end
end

function startworker(file)
    # don't need stdin (copied from Distributed.start_worker)
    redirect_stdin(devnull)
    close(stdin)
    redirect_stderr(stdout) # redirect stderr so coordinator reads everything from stdout
    pipe = Sockets.connect(file)
    try
        wait(@_spawn_interactive serve_requests(pipe))
    catch e
        @error "Error serving requests from coordinator" exception=(e, catch_backtrace())
    finally
        close(pipe)
        exit(0)
    end
end

# we need to lookup the module to eval in for this request
# so we loop through loaded modules until we find it
function getmodule(nm::Symbol)
    # fast-path Main/Base/Core
    nm == :Main && return Main
    nm == :Base && return Base
    nm == :Core && return Core
    for mod in Base.loaded_modules_array()
        if nameof(mod) == nm
            return mod
        end
    end
    error("module $nm not found")
end

function execute(r::Request)
    # @show r.mod, r.expr
    return Core.eval(getmodule(r.mod), r.expr)
end

function serve_requests(io)
    iolock = ReentrantLock()
    while true
        req = deserialize(io)
        @assert req isa Request
        req.shutdown && break
        # println("received request: $(req)")
        Threads.@spawn begin
            r = $req
            local resp
            try
                result = execute(r)
                resp = Response(result, nothing, r.id)
            catch e
                resp = Response(nothing, CapturedException(e, catch_backtrace()), r.id)
            finally
                Base.@lock iolock begin
                    # println("sending response: $(resp)")
                    serialize(io, resp)
                end
            end
        end
        yield()
    end
end

end # module Workers
