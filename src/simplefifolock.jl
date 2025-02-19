mutable struct SimpleFIFOLock <: AbstractLock
    cond::Threads.Condition
    reentrancy_count::UInt # 0 iff the lock is not held
    locked_by::Union{Task,Nothing} # nothing iff the lock is not held
    SimpleFIFOLock() = new(Threads.Condition(), 0, nothing)
end
    
@inline function Base.trylock(l::SimpleFIFOLock)
    GC.disable_finalizers()
    ct = current_task()
    lock(l.cond)
    locked_by = l.locked_by
    if locked_by === nothing || locked_by === ct
        l.reentrancy_count += 1
        l.locked_by = ct
        unlock(l.cond)
        return true
     end
     unlock(l.cond)
     GC.enable_finalizers()
     return false
end

@inline function Base.lock(l::SimpleFIFOLock)
    GC.disable_finalizers()
    ct = current_task()
    lock(l.cond)
    while true
        if l.locked_by === nothing || l.locked_by === ct
            l.reentrancy_count += 1
            l.locked_by = ct
            unlock(l.cond)
            return nothing
        end
        # Don't pay for the try-catch unless we `wait`.
        try
            wait(l.cond)
        catch
            unlock(l.cond)
            rethrow()
        end
    end
end
        
@inline function Base.unlock(l::SimpleFIFOLock)
    ct = current_task()
    lock(l.cond)
    if l.locked_by === nothing
        unlock(l.cond)
        error("unlocking an unlocked lock")
    end
    if l.locked_by !== ct
        unlock(l.cond)
        error("unlock from wrong thread")
    end
    l.reentrancy_count += -1
    if l.reentrancy_count == 0
        l.locked_by = nothing
        if !isempty(l.cond.waitq)
            # Don't pay for the try-catch unless we `notify`.
            try
                notify(l.cond; all=false)
            catch
                unlock(l.cond)
                rethrow()
            end
        end
    end
    unlock(l.cond)
    return nothing
end

# Performance note: for `@btime begin lock($l); unlock($l); end`.
#   13ns for SpinLock
#   17ns for ReentrantLock
#   33ns for FIFOLock
#   35ns for SimpleFIFOLock
#   57ns for the alternative versions below that use `@lock` (and hence have too much try-finally code)

# @inline function Base.lock(l::SimpleFIFOLock)
#     GC.disable_finalizers()
#     ct = current_task()
#     @lock l.cond begin
#         while true
#             if l.locked_by === nothing || l.locked_by === ct
#                 l.reentrancy_count += 1
#                 l.locked_by = ct
#                 return nothing
#             end
#             wait(l.cond)
#         end
#     end
# end
        
# @inline function Base.unlock(l::SimpleFIFOLock)
#     ct = current_task()
#     @lock l.cond begin
#         if l.locked_by === nothing
#             error("unlocking an unlocked lock")
#         end
#         if l.locked_by !== ct
#             error("unlock from wrong thread")
#         end
#         @assert l.reentrancy_count > 0
#         l.reentrancy_count += -1
#         if l.reentrancy_count == 0
#             l.locked_by = nothing
#             notify(l.cond; all=false)
#         end
#     end
#     return nothing
# end
