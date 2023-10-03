module Pools

export Pool, acquire, release, drain!
import Base: acquire, release

"""
    Pool{T}(max::Int=4096)
    Pool{K, T}(max::Int=4096)

A threadsafe object for managing a pool of objects of type `T`, optionally keyed by objects
of type `K`. Objects can be requested by calling `acquire(f, pool, [key])`, where `f` is a
function that returns a new object of type `T`.
The `key` argument is optional and can be used to lookup objects that match a certain criteria
(a Dict is used internally, so matching is `isequal`).

The `max` argument will limit the number of objects
that can be acquired at any given time. If the limit has been reached, `acquire` will
block until an object is returned to the pool via `release`.

By default, `release(pool, obj)` will return the object to the pool for reuse.
`release(pool)` will return the "permit" to the pool while not returning
any object for reuse.

`drain!` can be used to remove any cached objects for reuse, but it does *not* release
any active acquires.
"""
mutable struct Pool{K, T}
    lock::Threads.Condition
    max::Int
    cur::Int
    keyedvalues::Dict{K, Vector{T}}
    values::Vector{T}

    function Pool{K, T}(max::Int=4096) where {K, T}
        T === Nothing && throw(ArgumentError("Pool type can not be `Nothing`"))
        x = new(Threads.Condition(), max, 0)
        if K === Nothing
            x.values = T[]
            safesizehint!(x.values, max)
        else
            x.keyedvalues = Dict{K, Vector{T}}()
        end
        return x
    end
end

Pool{T}(max::Int=4096) where {T} = Pool{Nothing, T}(max)

safesizehint!(x, n) = sizehint!(x, min(4096, n))

# determines whether we'll look up object caches in .keyedvalues or .values
iskeyed(::Pool{K}) where {K} = K !== Nothing

Base.keytype(::Type{<:Pool{K}}) where {K} = K
Base.keytype(p::Pool) = keytype(typeof(p))

Base.valtype(::Type{<:Pool{<:Any, T}}) where {T} = T
Base.valtype(p::Pool) = valtype(typeof(p))

"""
    Pools.max(pool::Pool) -> Int

Return the maximum number of objects permitted to be in use at the same time.
See `Pools.permits(pool)` for the number of objects currently in use.
"""
max(pool::Pool) = Base.@lock pool.lock pool.max

"""
    Pools.permits(pool::Pool) -> Int

Return the number of objects currently in use. Less than or equal to `Pools.max(pool)`.
"""
permits(pool::Pool) = Base.@lock pool.lock pool.cur

"""
    Pools.depth(pool::Pool) -> Int

Return the number of objects in the pool available for reuse.
"""
depth(pool::Pool) = Base.@lock pool.lock mapreduce(length, +, values(pool.keyedvalues); init=0)
depth(pool::Pool{Nothing}) = Base.@lock pool.lock length(pool.values)

"""
    drain!(pool)

Remove all objects from the pool for reuse, but do not release any active acquires.
"""
function drain!(pool::Pool{K}) where {K}
    Base.@lock pool.lock begin
        if iskeyed(pool)
            for objs in values(pool.keyedvalues)
                empty!(objs)
            end
        else
            empty!(pool.values)
        end
    end
end

# in VERSION >= v"1.7", we can replace `TRUE` with `Returns(true)`
TRUE(x) = true

@noinline keyerror(key, K) = throw(ArgumentError("invalid key `$key` provided for pool key type $K"))
@noinline releaseerror() = throw(ArgumentError("cannot release permit when pool is empty"))

# NOTE: assumes you have the lock!
function releasepermit(pool::Pool)
    pool.cur > 0 || releaseerror()
    pool.cur -= 1
    notify(pool.lock; all=false)
    return
end

"""
    acquire(f, pool::Pool{K, T}, [key::K]; forcenew::Bool=false, isvalid::Function) -> T

Get an object from a `pool`, optionally keyed by the provided `key`.
The provided function `f` must create a new object instance of type `T`.
Each `acquire` call MUST be matched by exactly one `release` call.
The `forcenew` keyword argument can be used to force the creation of a new object, ignoring any existing objects in the pool.
The `isvalid` keyword argument can be used to specify a function that will be called to determine if an object is still valid
for reuse. By default, all objects are considered valid.
If there are no objects available for reuse, `f` will be called to create a new object.
If the pool is already at its maximum capacity, `acquire` will block until an object is returned to the pool via `release`.
"""
function Base.acquire(f, pool::Pool{K, T}, key=nothing; forcenew::Bool=false, isvalid::Function=TRUE) where {K, T}
    key isa K || keyerror(key, K)
    Base.@lock pool.lock begin
        # first get a permit
        while pool.cur >= pool.max
            wait(pool.lock)
        end
        pool.cur += 1
        # now see if we can get an object from the pool for reuse
        if !forcenew
            objs = iskeyed(pool) ? get!(() -> safesizehint!(T[], pool.max), pool.keyedvalues, key) : pool.values
            while !isempty(objs)
                obj = pop!(objs)
                isvalid(obj) && return obj
            end
        end
    end
    try
        # if there weren't any objects to reuse or we were forcenew, we'll create a new one
        return f()
    catch
        # if we error creating a new object, it's critical we return the permit to the pool
        Base.@lock pool.lock releasepermit(pool)
        rethrow()
    end
end

"""
    release(pool::Pool{K, T}, key::K, obj::Union{T, Nothing}=nothing)
    release(pool::Pool{K, T}, obj::T)
    release(pool::Pool{K, T})

Return an object to a `pool`, optionally keyed by the provided `key`.
If `obj` is provided, it will be returned to the pool for reuse.
Otherwise, if `nothing` is returned, or `release(pool)` is called,
just the "permit" will be returned to the pool.
"""
function Base.release(pool::Pool{K, T}, key, obj::Union{T, Nothing}=nothing) where {K, T}
    key isa K || keyerror(key, K)
    keyed = iskeyed(pool)
    Base.@lock pool.lock begin
        # if keyed && !haskey(pool.keyedvalues, key)
        #     throw(Base.KeyError(key))
        # end
        # return the permit
        releasepermit(pool)
        # if we're given an object, we'll put it back in the pool
        if obj !== nothing
            # if an invalid key is provided, we let the KeyError propagate
            objs = keyed ? pool.keyedvalues[key] : pool.values
            push!(objs, obj)
        end
    end
    return
end

Base.release(pool::Pool{K, T}, obj::T) where {K, T} = release(pool, nothing, obj)
Base.release(pool::Pool{K, T}) where {K, T} = release(pool, nothing, nothing)

end # module
