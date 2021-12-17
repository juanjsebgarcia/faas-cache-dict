# faas-cache-dict
A Python dictionary implementation designed to act as an in-memory cache for FaaS
environments.

Formally you would describe this a memory constrained LRU TTL cache dict.

This is implementation only uses core Python stdlib with no external dependencies.

## Background
This was originally designed to be used as an in-memory cache for AWS Lambda.

In most FaaS environments, successive quick calls of the function persists variables in
the global scope. We can leverage this to cache data for future calls.

FaaS runtimes have limited RAM capacities so this library allows you to set a max
`mebibyte` size for the dict. It also allows setting an optional max items length.

Items are kept in order with the LRU at the HEAD of the list.

Items are deleted if they expire, or from the head (LRU) if the cache is out of space.

## Usage

## Limitations
- Performance degrades with size, you will need to test this for your use case. Though
 in most circumstances this will be much faster than performing a network call to a cache.
- The library _should_ be thread-safe, but limited testing has gone into this.

## Support
CPython 3.7 or greater. No extra dependencies are required.

## Contributions
This code is distributed under an open license. Feel free to fork it or preferably open
a PR.

## Inspirations
Thanks to `mobilityhouse/ttldict` for their implementation which served as a starting
point.
