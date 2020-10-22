
# ExpiringCaches.jl

*A Dict type with expiring values and a `@cacheable` macro to cache function results in an expiring cache*

## Installation

The package is registered in the [`General`](https://github.com/JuliaRegistries/General) registry and so can be installed at the REPL with `] add ExpiringCaches`.

## Usage


### `Cache`
    ExpiringCaches.Cache{K, V}(timeout::Dates.Period)

Create a thread-safe, expiring cache where values older than `timeout`
are "invalid" and will be deleted.

An `ExpiringCaches.Cache` is an `AbstractDict` and tries to emulate a regular
`Dict` in all respects. It is most useful when the cost of retrieving or
calculating a value is expensive and is able to be "cached" for a certain
amount of time. To avoid using the cache (i.e. to invalidate the cache),
a `Cache` supports the `delete!` and `empty!` methods to remove values
manually.


### `@cacheable`
    @cacheable timeout function_definition::ReturnType

For a function definition (`function_definition`, either short-form
or full), create an `ExpiringCaches.Cache` and store results for `timeout`
(hashed by the exact input arguments obviously).

Note that the function definition _MUST_ include the `ReturnType` declartion
as this is used as the value (`V`) type in the `Cache`.# WorkerUtilities.jl
