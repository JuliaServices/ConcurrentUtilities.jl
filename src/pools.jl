module Pools

export Pool, acquire, release, drain!
import Base: acquire, release

"""
    Pool{T}(limit::Int=4096)
    Pool{K, T}(limit::Int=4096)

A threadsafe object for managing a pool of objects of type `T`, optionally keyed by objects
of type `K`.

Objects can be requested by calling `acquire(f, pool, [key])`, where `f` is a
function that returns a new object of type `T`.
The `key` argument is optional and can be used to lookup objects that match a certain criteria
(a `Dict` is used internally, so matching is `isequal`).

The `limit` argument will limit the number of objects that can be in use at any given time.
If the limit has been reached, `acquire` will block until an object is released
via `release`.

- `release(pool, obj)` will return the object to the pool for reuse.
- `release(pool)` will decrement the number in use but not return any object for reuse.
- `drain!` can be used to remove objects that have been returned to the pool for reuse;
  it does *not* release any objects that are in use.

See also `acquire`, `release`, `Pools.limit`, `Pools.in_use`, `Pools.in_pool`, `drain!`.
The key and object types can be inspected with `keytype` and `valtype` respectively.
"""
mutable struct Pool{K, T}
    lock::Threads.Condition
    limit::Int
    cur::Int
    keyedvalues::Dict{K, Vector{T}}
    values::Vector{T}

    function Pool{K, T}(limit::Int=4096) where {K, T}
        T === Nothing && throw(ArgumentError("Pool type can not be `Nothing`"))
        x = new(Threads.Condition(), limit, 0)
        if K === Nothing
            x.values = T[]
            safesizehint!(x.values, limit)
        else
            x.keyedvalues = Dict{K, Vector{T}}()
        end
        return x
    end
end

Pool{T}(limit::Int=4096) where {T} = Pool{Nothing, T}(limit)

safesizehint!(x, n) = sizehint!(x, min(4096, n))

# determines whether we'll look up object caches in .keyedvalues or .values
iskeyed(::Pool{K}) where {K} = K !== Nothing

"""
    keytype(::Pool)

Return the type of the keys for the pool.
If the pool is not keyed, this will return `Nothing`.
"""
Base.keytype(::Type{<:Pool{K}}) where {K} = K
Base.keytype(p::Pool) = keytype(typeof(p))

"""
    valtype(::Pool)

Return the type of the objects that can be stored in the pool.
"""
Base.valtype(::Type{<:Pool{<:Any, T}}) where {T} = T
Base.valtype(p::Pool) = valtype(typeof(p))

"""
    Pools.limit(pool::Pool) -> Int

Return the maximum number of objects permitted to be in use at the same time.
See `Pools.in_use(pool)` for the number of objects currently in use.
"""
limit(pool::Pool) = Base.@lock pool.lock pool.limit

"""
    Pools.in_use(pool::Pool) -> Int

Return the number of objects currently in use. Less than or equal to `Pools.limit(pool)`.
"""
in_use(pool::Pool) = Base.@lock pool.lock pool.cur

"""
    Pools.in_pool(pool::Pool) -> Int

Return the number of objects in the pool available for reuse.
"""
in_pool(pool::Pool) = Base.@lock pool.lock mapreduce(length, +, values(pool.keyedvalues); init=0)
in_pool(pool::Pool{Nothing}) = Base.@lock pool.lock length(pool.values)

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
@noinline releaseerror() = throw(ArgumentError("cannot release when no objects are in use"))

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
If the pool is already at its usage limit, `acquire` will block until an object is returned to the pool via `release`.
"""
function Base.acquire(f, pool::Pool{K, T}, key=nothing; forcenew::Bool=false, isvalid::Function=TRUE) where {K, T}
    key isa K || keyerror(key, K)
    Base.@lock pool.lock begin
        # first get a permit
        while pool.cur >= pool.limit
            wait(pool.lock)
        end
        pool.cur += 1
        # now see if we can get an object from the pool for reuse
        if !forcenew
            objs = iskeyed(pool) ? get!(() -> safesizehint!(T[], pool.limit), pool.keyedvalues, key) : pool.values
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

Release an object from usage by a `pool`, optionally keyed by the provided `key`.
If `obj` is provided, it will be returned to the pool for reuse.
Otherwise, if `nothing` is returned, or `release(pool)` is called,
the usage count will be decremented without an object being returned to the pool for reuse.
"""
function Base.release(pool::Pool{K, T}, key, obj::Union{T, Nothing}=nothing) where {K, T}
    key isa K || keyerror(key, K)
    Base.@lock pool.lock begin
        # return the permit
        releasepermit(pool)
        # if we're given an object, we'll put it back in the pool
        if obj !== nothing
            # if an invalid key is provided, we let the KeyError propagate
            objs = iskeyed(pool) ? pool.keyedvalues[key] : pool.values
            push!(objs, obj)
        end
    end
    return
end

Base.release(pool::Pool{K, T}, obj::T) where {K, T} = release(pool, nothing, obj)
Base.release(pool::Pool{K, T}) where {K, T} = release(pool, nothing, nothing)

end # module
