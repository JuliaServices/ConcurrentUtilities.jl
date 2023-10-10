using ConcurrentUtilities.Pools, Test

@testset "Pools" begin
    pool_size = length∘Pools.values
    @testset "nonkeyed and pool basics" begin
        pool = Pool{Int}(3)
        @test keytype(pool) === Nothing
        @test valtype(pool) === Int

        @test Pools.limit(pool) == 3
        @test Pools.in_use(pool) == 0
        @test Pools.in_pool(pool) == 0

        # acquire an object from the pool
        x1 = acquire(() -> 1, pool)
        # no existing objects in the pool, so our function was called to create a new one
        @test x1 == 1
        @test Pools.limit(pool) == 3
        @test Pools.in_use(pool) == 1
        @test Pools.in_pool(pool) == 0

        # release back to the pool for reuse
        release(pool, x1)
        @test Pools.in_use(pool) == 0
        @test Pools.in_pool(pool) == 1

        # acquire another object from the pool
        x1 = acquire(() -> 2, pool)
        # this time, the pool had an existing object, so our function was not called
        @test x1 == 1
        @test Pools.in_use(pool) == 1
        @test Pools.in_pool(pool) == 0

        # but now there are no objects to reuse again, so the next acquire will call our function
        x2 = acquire(() -> 2, pool)
        @test x2 == 2
        @test Pools.in_use(pool) == 2
        @test Pools.in_pool(pool) == 0

        x3 = acquire(() -> 3, pool)
        @test x3 == 3
        @test Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 0

        # the pool is now at `Pools.limit`, so the next acquire will block until an object is released
        @test Pools.in_use(pool) == Pools.limit(pool)
        tsk = @async acquire(() -> 4, pool; forcenew=true)
        yield()
        @test !istaskdone(tsk)
        # release an object back to the pool
        release(pool, x1)
        # now the acquire can complete
        x1 = fetch(tsk)
        # even though we released 1 for reuse, we passed forcenew, so our function was called to create new
        @test x1 == 4
        @test Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 1

        # error to try and provide a key to a non-keyed pool
        @test_throws ArgumentError acquire(() -> 1, pool, 1)

        # release objects back to the pool
        release(pool, x1)
        release(pool, x2)
        release(pool, x3)
        @test Pools.in_use(pool) == 0
        @test Pools.in_pool(pool) == 4

        # acquire an object, but checking isvalid
        x1 = acquire(() -> 5, pool; isvalid=x -> x == 1)
        @test x1 == 1
        @test Pools.in_use(pool) == 1

        # no valid objects, so our function was called to create a new one
        x2 = acquire(() -> 6, pool; isvalid=x -> x == 1)
        @test x2 == 6
        @test Pools.in_use(pool) == 2

        # we have one permit left, we now throw while creating a new object
        # and we want to test that the permit isn't permanently lost for the pool
        @test_throws ErrorException acquire(() -> error("oops"), pool; forcenew=true)
        @test Pools.in_use(pool) == 2

        # we can still acquire a new object
        x3 = acquire(() -> 7, pool; forcenew=true)
        @test x3 == 7
        @test Pools.in_use(pool) == 3

        # release objects back to the pool
        drain!(pool)
        release(pool, x1)
        release(pool, x2)
        release(pool, x3)
        @test Pools.in_use(pool) == 0
        @test Pools.in_pool(pool) == 3

        # try to do an invalid release
        @test_throws ArgumentError release(pool, 10)

        # test that the invalid release didn't push the object to our pool for reuse
        x1 = acquire(() -> 8, pool)
        @test x1 == 7
        @test Pools.in_use(pool) == 1
        @test Pools.in_pool(pool) == 2
        # calling drain! removes all objects for reuse
        drain!(pool)
        @test Pools.in_use(pool) == 1
        @test Pools.in_pool(pool) == 0

        x2 = acquire(() -> 9, pool)
        @test x2 == 9
        @test Pools.in_use(pool) == 2
        @test Pools.in_pool(pool) == 0
    end

    @testset "keyed pool" begin
        # now test a keyed pool
        pool = Pool{String, Int}(3)
        @test keytype(pool) === String
        @test valtype(pool) === Int

        @test Pools.limit(pool) == 3
        @test Pools.in_use(pool) == 0
        @test Pools.in_pool(pool) == 0

        # acquire an object from the pool
        x1 = acquire(() -> 1, pool, "a")
        # no existing objects in the pool, so our function was called to create a new one
        @test x1 == 1
        @test Pools.in_use(pool) == 1
        @test Pools.in_pool(pool) == 0

        # release back to the pool for reuse
        release(pool, "a", x1)
        @test Pools.in_use(pool) == 0
        @test Pools.in_pool(pool) == 1

        # test for a different key
        x2 = acquire(() -> 2, pool, "b")
        # there's an existing object, but for a different key, so we don't reuse
        @test x2 == 2
        @test Pools.in_use(pool) == 1
        @test Pools.in_pool(pool) == 1

        # acquire another object from the pool
        x1 = acquire(() -> 2, pool, "a")
        # this time, the pool had an existing object, so our function was not called
        @test x1 == 1
        @test Pools.in_use(pool) == 2
        @test Pools.in_pool(pool) == 0

        x3 = acquire(() -> 3, pool, "a")
        @test x3 == 3
        @test Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 0

        # the pool is now at capacity, so the next acquire will block until an object is released
        # even though we've acquired using different keys, the capacity is shared across the pool
        @test Pools.in_use(pool) == Pools.limit(pool)
        tsk = @async acquire(() -> 4, pool, "c"; forcenew=true)
        yield()
        @test !istaskdone(tsk)
        # release an object back to the pool
        release(pool, "a", x1)
        # now the acquire can complete
        x1 = fetch(tsk)
        # even though we released 1 for reuse, we passed forcenew, so our function was called to create new
        @test x1 == 4
        @test Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 1

        # error to try and provide an invalid key to a keyed pool
        @test_throws ArgumentError acquire(() -> 1, pool, 1)
        @test Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 1

        # error to *not* provide a key to a keyed pool
        @test_throws ArgumentError acquire(() -> 1, pool)
        @test Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 1

        # error to *not* provide a key when releasing to a keyed pool
        @test_throws ArgumentError release(pool)
        @test Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 1

        # error to release an invalid key back to the pool
        @test_throws KeyError release(pool, "z", 1)
        @test_broken Pools.in_use(pool) == 3
        @test Pools.in_pool(pool) == 1
    end
end
