# faas-cache-dict
A fast thread-safe Python dictionary implementation designed to act as an in-memory RAM
constrained LRU TTL cache dict for FaaS environments. Though it has many valuable use
cases outside FaaS.

This is a Pythonic dict implementation with all the typical methods working `.get`
`.keys` `.values` `.items` `len` etc. This package uses only core Python stdlib +
[objsize](https://pypi.org/project/objsize/).

If used in a serverless FaaS environment then this package works best by supporting an
existing caching strategy, as there is no guarantee that any in-memory data will persist
between calls.

## Background
This was originally designed to be a performant in-memory cache dict for AWS Lambda,
preventing repeated invocations making "slow" network calls to a connected ElastiCache
Redis cluster.

In most FaaS environments, successive quick invocations of the function persists
variables in the global scope. We can leverage this to cache data in global for future
calls.

FaaS runtimes have limited RAM capacities so this library allows you to set a max byte
size for the cache dict. It also allows setting an optional max items length, and a TTL
for each item.

Items are kept in order with the LRU at the HEAD of the list.

Items are deleted if they expire, or from the head (LRU) if the cache dict is out of
space.

## Expiry Dimensions
Several dimensions exist to constrain the longevity of the data the cache dict stores.
These can all be combined as your use case demands. You can also use none, if you so
wish.

### Memory size
A max memory (RAM) size the cache dict can use before it starts deleting the LRU values.
This can be expressed in bytes (`1024`) or "human" format `1K` (kibibyte). Supported
"human" expressions are `K`, `M`, `G`, `T`.

```
from faas_cache_dict import FaaSCacheDict

cache = FaaSCacheDict(max_size_bytes='128M')
cache.change_byte_size('64M')  # If data is too large, LRU will be trimmed until it fits

cache.get_byte_size()  # Returns actual size of data and cache dict structure (bytes)
```

### TTL
The number of `*seconds*` to hold a data point before making it unavailable and then
later purging it. This can be sub-second by using float values. This can be configured
as a default across the cache dict, or on a per key basis.

```
from faas_cache_dict import FaaSCacheDict

cache = FaaSCacheDict(default_ttl=60)  # Setting it to None (default) means no expiry

cache['key'] = 'value'  # Will expire in 60 seconds
cache.set_ttl('key', 120)  # Will now expire in 120 seconds from now
cache.get_ttl('key')
>>> 120
cache.set_ttl('key', None)  # Will now never expire

from datetime import time
cache.expire_at('key', time.time()) # Expire now, or a chosen time (epoch) in future

cache.default_ttl = 30  # Now all *new* keys will expire in 30 seconds by default
cache['another_key'] = 'value'  # Expires in 30 seconds as per new default

<Wait 31 seconds>

cache['another_key']
>>> KeyError  # Expired

cache.is_expired('another_key')
>>> True
```

### LRU
A max list length constraint which deletes the least recently accessed item once the max
size is reached.

```
from faas_cache_dict import FaaSCacheDict

cache = FaaSCacheDict(max_items=10)  # Setting it to None (default) means sys.maxsize
cache.change_max_items(5)  # If data is too large, LRU will be trimmed until it fits
```

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
<!--- TODO: Better docs to come --->

## Known limitations
- The memory constraint applies to the whole cache dict object not just its contents.
The cache dict itself consumes a small amount of memory in overheads, so eg. `1K` of
requested memory will yield slightly less than `1K` of available internal storage.
- Due to extra processing, performance does **slowly** degrade with size (item count),
you will need to test this for your situation. In 99% of use cases this will still be
an order of magnitude faster than doing network calls to an external cache (and more
reliable).

## Support
CPython 3.8 or greater.

## Contributions
This code is distributed under an open license. Feel free to fork it or preferably open
a PR.

## Inspirations
Thanks to `mobilityhouse/ttldict` for their implementation which served as a proof of
concept, which has since been much extended.
