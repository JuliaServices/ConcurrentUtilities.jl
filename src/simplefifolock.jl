mutable struct SimpleFIFOLock <: AbstractLock
    cond_wait::Base.ThreadSynchronizer
    reentrancy_count::UInt # 0 iff the lock is not held
    locked_by::Union{Task,Nothing}  # nothing iff the lock is not held
    SimpleFIFOLock() = new(Base.ThreadSynchronizer(), 0, nothing)
end

@inline function Base.trylock(l::SimpleFIFOLock)
    GC.disable_finalizers(c)
    ct = current_task()
    lock(l.cond_wait)
    locked_by = l.locked_by
    if locked_by === nothing || locked_by === ct
        l.rentrancy_count += 1
        l.locked_by = ct
        unlock(l.cond_wait)
        return true
    end
    unlock(l.cond_wait)
    GC.enable_finalizers(c)
    return false
end

@inline function Base.lock(l::SimpleFIFOLock)
    GC.disable_finalizers()
    ct = current_task()
    lock(l.cond_wait)
    locked_by = l.locked_by
    if locked_by === nothing || locked_by == ct
        # We unroll the first iteration of the loop to avoid the overyhead of `try-finally`
        # in the uncontended lock case.
        l.reentrancy_count += 1
        l.locked_by = ct
        unlock(l.cond_wait)
    else
        try
            while l.locked_by !== nothing && l.locked_by !== ct
                wait(l.cond_wait)
            end
            l.reentrancy_count += 1
            l.locked_by = ct
        finally
            unlock(l.cond_wait)
        end
    end
    return nothing
end
        
@inline function Base.unlock(l::SimpleFIFOLock)
    ct = current_task()
    lock(l.cond_wait)
    #@info ":$(@__LINE__()) unlocking" task=current_task() locked_by=l.locked_by count=l.reentrancy_count ql=length(l.cond_wait.waitq)
    println(@__LINE__())
    println(@__LINE__())
    if l.locked_by !== ct
        @info ":$(@__LINE__())"
        error("unlock from wrong thread")
    end
    if l.reentrancy_count == 0
        @info ":$(@__LINE__())"
        error("unlock count must equal lock count")
    end
    @info ":$(@__LINE__())"
    l.reentrancy_count -= 1
    @info ":$(@__LINE__())"
    @info ":$(@__LINE__()) now count is" task=current_task() count=l.reentrancy_count
    if l.reentrancy_count == 0
        @info "count becomes zero" task=current_task() ql=length(l.cond_wait.waitq)
        l.locked_by = nothing
        if !isempty(l.cond_wait.waitq)
            @info "waitq not empty"  ql=length(l.cond_wait.waitq)
            t = popfirst!(l.cond_wait.waitq)
            l.locked_by = t
            l.reentrancy_count = 1
            schedule(t)
        end
        GC.enable_finalizers()
    end
    unlock(l.cond_wait)
    return nothing
end

# mutable struct FLock2 <: AbstractLock
#     lock::ReentrantLock
#     locked_by::Union{Task,Nothing} # nothing iff the lock is not held
#     rentrancy_count::UInt          # 0 iff the lock is not held
#     tasks::Vector{Task}
#     FLock2() = new(ReentrantLock(), nothing, 0, Task[])
# end

# @inline function Base.trylock(l::SimpleFIFOLock)
#     GC.disable_finalizers(c)
#     ct = current_task()
#     lock(l.lock)
#     locked_by = l.locked_by
#     if locked_by === nothing || locked_by === ct
#         l.rentrancy_count += 1
#         l.locked_by = ct
#         unlock(l.lock)
#         return true
#     end
#     unlock(l.lock)
#     GC.enable_finalizers(c)
#     return false
# end

# @inline function Base.lock(l::SimpleFIFOLock)
#     GC.disable_finalizers()
#     ct = current_task()
#     lock(l.lock)
#     locked_by = l.locked_by
#     if locked_by === nothing || locked_by == ct
#         l.reentrancy_count += 1
#         l.locked_by = ct
#         unlock(l.lock)
#     else
#         push!(l.tasks, ct)
#         unlock(l.lock)  # Little race here since someone could sneak in and yield to me.
#         wait()
#         l.reentrancy_count += 1
#         l.locked_by = ct
#     end
#     return nothing
# end
