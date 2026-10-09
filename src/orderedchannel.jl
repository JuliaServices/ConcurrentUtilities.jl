mutable struct OrderedChannel{T} <: AbstractChannel{T}
    cond::Threads.Condition
    values::Dict{Int, T}
end

function OrderedChannel{T}(sz::Integer = 0) where T
    return OrderedChannel{T}(Threads.Condition(), Dict{Int, T}())
end

function Base.put!(c::OrderedChannel{T}, value::T, order::Int) where T
    @lock c.cond begin
        c.values[order] = value
        notify(c.cond)
    end
end

function Base.take!(c::OrderedChannel{T}) where T
    lock(c.cond) do
        while isempty(c.values)
            wait(c.cond)
        end
        return popfirst!(c.values)
    end
end

function Base.isready(c::OrderedChannel{T}) where T
    lock(c.cond) do
        return !isempty(c.values)
    end
end