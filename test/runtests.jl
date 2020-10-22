using Test, WorkerUtilities

# Lockable{T, L<:AbstractLock}
let # test the constructor `Lockable(value, lock)`
    lockable = Lockable(Dict("foo" => "hello"), ReentrantLock())
    @test lockable.value["foo"] == "hello"
    lock(lockable) do d
        @test d["foo"] == "hello"
    end
    lock(lockable) do d
        d["foo"] = "goodbye"
    end
    @test lockable.value["foo"] == "goodbye"
    lock(lockable) do d
        @test d["foo"] == "goodbye"
    end
end
let # test the constructor `Lockable(value)`
    lockable = Lockable(Dict("foo" => "hello"))
    @test lockable.value["foo"] == "hello"
    lock(lockable) do d
        @test d["foo"] == "hello"
    end
    lock(lockable) do d
        d["foo"] = "goodbye"
    end
    @test lockable.value["foo"] == "goodbye"
    lock(lockable) do d
        @test d["foo"] == "goodbye"
    end
en