module Pools

import ..ConcurrentStack, ..Lockable

export Pool, acquire, release

import Base: Semaphore, acquire, release

"""
    Pool(T; max::Int=typemax(Int))
    Pool(K, T; max::Int=typemax(Int))

A threadsafe object for managing a pool of objects of type `T`, optionally keyed by objects
of type `K`. Objects can be requested by calling `acquire(f, pool, [key])`, where `f` is a
function that returns a new object of type `T`.
The `key` argument is optional and can be used to lookup objects that match a certain criteria.
If no pool exists for the given key, one will be created.

If `max` is specified, the pool will limit the number of objects
that can be checked out at any given time. If the limit has been reached, `acquire` will
block until an object is returned to the pool via `release`.

By default, `release(pool, obj)` will return the object to the pool for reuse.
`release(pool)` will return the "permit" to the pool while not returning
any object for reuse.
"""
struct Pool{K, T}
    sem::Semaphore
    values::Lockable{Dict{K, ConcurrentStack{T}}}
end

Pool(T; max::Int=typemax(Int)) = Pool{Nothing, T}(Semaphore(max), Lockable(Dict{Nothing, ConcurrentStack{T}}()))
Pool(K, T; max::Int=typemax(Int)) = Pool{K, T}(Semaphore(max), Lockable(Dict{K, ConcurrentStack{T}}()))

Base.empty!(pool::Pool) = Base.@lock pool.values empty!(pool.values[])

TRUE(x) = true

"""
    acquire(f, pool::Pool{K, T}, [key::K]; forcenew::Bool=false, isvalid::Function) -> T

Get an object from a `pool`, optionally keyed by the provided `key`. If no pool exists for the given key, one will be created.
The provided function `f` must create a new object instance of type `T`.
The acquired object MUST be returned to the pool by calling `release(pool, key, obj)` exactly once.
The `forcenew` keyword argument can be used to force the creation of a new object, ignoring any existing objects in the pool.
The `isvalid` keyword argument can be used to specify a function that will be called to determine if an object is still valid
for reuse. By default, all objects are considered valid.
If there are no objects available for reuse, `f` will be called to create a new object.
If the pool is already at its maximum capacity, `acquire` will block until an object is returned to the pool via `release`.
"""
function Base.acquire(f, pool::Pool{K, T}, key=nothing; forcenew::Bool=false, isvalid::Function=TRUE) where {K, T}
    key isa K || throw(ArgumentError("invalid key `$key` provided for pool key type $K"))
    acquire(pool.sem)
    try
        # once we've received our permit, we can figure out where the object should come from
        # if we're forcing a new object, we can just call f() and return it
        forcenew && return f()
        # otherwise, check if there's an existing object in the pool to reuse
        objs = Base.@lock pool.values get!(() -> ConcurrentStack{T}(), pool.values[], key)
        obj = pop!(objs)
        while obj !== nothing
            # if the object is valid, return it
            isvalid(obj) && return obj
            # otherwise, try the next object
            obj = pop!(objs)
        end
        # if we get here, we didn't find any valid objects, so we'll just create a new one
        return f()
    catch
        # an error occurred while acquiring, so make sure we return the semaphore permit
        release(pool.sem)
        rethrow()
    end
end

"""
    release(pool::Pool{K, T}, key::K, obj::Union{T, Nothing}=nothing)
    release(pool::Pool{K, T}, obj::T)
    release(pool::Pool{K, T})

Return an object to a `pool`, optionally keyed by the provided `key`.
If `obj` is provided, it will be returned to the pool for reuse. Otherwise, if `nothing` is returned,
just the "permit" will be returned to the pool.
"""
function Base.release(pool::Pool{K, T}, key, obj::Union{T, Nothing}=nothing) where {K, T}
    key isa K || throw(ArgumentError("invalid key `$key` provided for pool key type $K"))
    # if we're given an object, we'll put it back in the pool
    # otherwise, we'll just return the permit
    if obj !== nothing
        # first though, we repeat Base.Semaphore's error check in the case of an invalid release
        # where we don't want to push an object for reuse for an invalid release
        Base.@lock pool.sem.cond_wait begin
            if pool.sem.curr_cnt > 0
                # if the key is invalid, we'll just let the KeyError propagate
                objs = Base.@lock pool.values pool.values[][key]
                push!(objs, obj)
            end
        end
        # we don't throw an error or unlock in the invalid case, because we'll let
        # the release call below do all that for us
    end
    release(pool.sem)
    return
end

Base.release(pool::Pool{K, T}, obj::T) where {K, T} = release(pool, nothing, obj)
Base.release(pool::Pool{K, T}) where {K, T} = release(pool, nothing, nothing)

end # module
