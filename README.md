# faas-cache-dict
A Python dictionary implementation designed to act as an in-memory RAM constrained LRU
TTL cache dict for FaaS environments.

This implementation uses only core Python stdlib.

## Background
This was originally designed to be used as an in-memory cache for AWS Lambda.

In most FaaS environments, successive quick invocations of the function persists
variables in the global scope. We can leverage this to cache data in global for future
calls.

FaaS runtimes have limited RAM capacities so this library allows you to set a max byte
size for the dict. It also allows setting an optional max items length.

Items are kept in order with the LRU at the HEAD of the list.

Items are deleted if they expire, or from the head (LRU) if the cache is out of space.

## Usage
Simple usage guide:
```
from faas_cache_dict import FaaSCacheDict

cache = FaaSCacheDict(default_ttl=10, max_size_bytes='128M', max_items=10)

cache['foo'] = 'bar'
print(cache)
>>> <FaaSCacheDict@0x10a9daec0; default_ttl=10, max_memory=128M, max_items=10, current_memory_bytes=496, current_items=1>

print(cache['foo'])
>>> 'bar'

(wait 10 seconds TTL)

print(cache['foo'])
>>> KeyError
```

## Limitations
- Performance degrades with size, you will need to test this for your use case. Though
 in most circumstances this will be much faster than performing a network call to an
 external cache.
- The library _should_ be thread-safe, but limited testing has gone into this.

## Support
CPython 3.7 or greater. No extra external dependencies are required.

## Contributions
This code is distributed under an open license. Feel free to fork it or preferably open
a PR.

## Inspirations
Thanks to `mobilityhouse/ttldict` for their implementation which served as a starting
point.
