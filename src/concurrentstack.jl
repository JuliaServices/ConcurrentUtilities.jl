# copied from https://github.com/JuliaConcurrent/ConcurrentCollections.jl/blob/09a8cbe25a1a0d3cb9d0fb0d03cad60a7d5ccebd/src/stack.jl
@static if VERSION < v"1.7"

mutable struct Node{T}
    value::T
    next::Union{Node{T},Nothing}
end

Node{T}(value::T) where {T} = Node{T}(value, nothing)

mutable struct ConcurrentStack{T}
    lock::ReentrantLock
    next::Union{Node{T},Nothing}
end

ConcurrentStack{T}() where {T} = ConcurrentStack{T}(ReentrantLock(), nothing)

function Base.push!(stack::ConcurrentStack{T}, v) where {T}
    v === nothing && throw(ArgumentError("cannot push nothing onto a ConcurrentStack"))
    v = convert(T, v)
    node = Node{T}(v)
    lock(stack.lock) do
        node.next = stack.next
        stack.next = node
    end
    return stack
end

function Base.pop!(stack::ConcurrentStack)
    lock(stack.lock) do
        node = stack.next
        node === nothing && return nothing
        stack.next = node.next
        return node.value
    end
end
    
else
    
mutable struct Node{T}
    value::T
    @atomic next::Union{Node{T},Nothing}
end

Node{T}(value::T) where {T} = Node{T}(value, nothing)

mutable struct ConcurrentStack{T}
    @atomic next::Union{Node{T},Nothing}
end

ConcurrentStack{T}() where {T} = ConcurrentStack{T}(nothing)

function Base.push!(stack::ConcurrentStack{T}, v) where {T}
    v === nothing && throw(ArgumentError("cannot push nothing onto a ConcurrentStack"))
    v = convert(T, v)
    node = Node{T}(v)
    next = @atomic stack.next
    while true
        @atomic node.next = next
        next, ok = @atomicreplace(stack.next, next => node)
        ok && break
    end
    return stack
end

function Base.pop!(stack::ConcurrentStack)
    while true
        node = @atomic stack.next
        node === nothing && return nothing
        next = @atomic node.next
        next, ok = @atomicreplace(stack.next, node => next)
        ok && return node.value
    end
end

end # @static if VERSION < v"1.7"