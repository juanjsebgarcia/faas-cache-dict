# faas-cache-dict

[PyPi package repository](https://pypi.org/project/faas-cache-dict/)

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
from faas_cache_dict import FaaSCacheDict, DataTooLarge

cache = FaaSCacheDict(max_size_bytes='128M')
cache.change_byte_size('64M')  # If over limit, LRU items are trimmed until it fits

cache.get_byte_size()  # Returns actual size of data and cache dict structure (bytes)

# If a single item exceeds max_size_bytes, DataTooLarge is raised
try:
    cache['huge_item'] = 'x' * (129 * 1024 * 1024)  # 129MB item
except DataTooLarge:
    print("Item too large for cache")
```

### TTL
The number of `*seconds*` to hold a data point before making it unavailable and then
later purging it. This can be sub-second by using float values. This can be configured
as a default across the cache dict, or on a per key basis.

```
from faas_cache_dict import FaaSCacheDict
import time

cache = FaaSCacheDict(default_ttl=60)  # Setting it to None (default) means no expiry

cache['key'] = 'value'  # Will expire in 60 seconds
cache.set_ttl('key', 120)  # Will now expire in 120 seconds from now
cache.get_ttl('key')
>>> 119.9
cache.set_ttl('key', None)  # Will now never expire

cache.expire_at('key', time.time() + 5) # Expire in 5 seconds time (epoch)

cache.default_ttl = 30  # Now all *new* keys will expire in 30 seconds by default
cache['another_key'] = 'value'  # Expires in 30 seconds as per new default

<Wait 31 seconds>

cache['another_key']
>>> KeyError  # Expired

cache.is_expired('another_key')
>>> None  # Returns None because the key was purged (state unknown)
```

Note: `get_ttl()`, `set_ttl()`, and `expire_at()` all raise `KeyError` if the key
is missing or expired. Also, `set_ttl()` and `expire_at()` do not affect LRU order -
modifying a key's TTL will not promote it to the most-recently-used position.

`is_expired(key)` returns three possible values:
- `False` — key exists and is not expired
- `True` — key exists but has expired
- `None` — key doesn't exist (either never added or already purged)

Note: `pop(key)` returns the default value (`None`) for expired keys, unlike
`cache[key]` which raises `KeyError`. This matches standard dict behavior.

### LRU
A max list length constraint which deletes the least recently accessed item once the max
size is reached.

```
from faas_cache_dict import FaaSCacheDict

cache = FaaSCacheDict(max_items=10)  # Default is sys.maxsize (effectively unlimited)
cache.change_max_items(5)  # If data is too large, LRU will be trimmed until it fits
cache.change_max_items(None)  # Disable max items constraint entirely
```

### Lifecycle hooks
A hook exists enabling post-deletion lifecycle events, for example if a networked resource
is deleted from the cache dict you may wish to perform dependency clean up.

```
from faas_cache_dict import FaaSCacheDict

def post_deletion_hook(key, value):
    pass  # do stuff here

cache = FaaSCacheDict(max_items=5, on_delete_callable=post_deletion_hook)
```

Note that even if the post_deletion_callable fails, the item will still be purged
from the cache dict. You are responsible for implementing your own error handling.

Note that lifecycle hooks are run synchronously, so time costly operations will degrade
the performance of the faas cache dict.

### Additional Methods

#### `setdefault(key, default=None)`
Standard dict method. Returns the value if the key exists and is not expired,
otherwise sets the key to the default value and returns it.

```
cache.setdefault('key', 'default_value')
>>> 'default_value'
cache.setdefault('key', 'other_value')  # Key already exists
>>> 'default_value'
```

#### `move_to_end(key, last=True)`
Move an existing key to either end of the LRU order. Useful for manual cache
management.

```
cache.move_to_end('key')  # Move to MRU (most recently used) position
cache.move_to_end('key', last=False)  # Move to LRU (least recently used) position
```

Raises `KeyError` if the key is missing or expired.

#### `close()`
Stop the background purge thread. Call this when you're done with the cache to
cleanly release resources.
The purge thread is also stopped automatically when the instance is deleted.

```
cache.close()
```

#### `fromkeys()` and `copy()` - Not Supported
Both `fromkeys()` and `copy()` raise `NotImplementedError`. Use the constructor
and add items individually instead.

### Pickling Support
The cache dict can be pickled and unpickled. TTL expiry timestamps are preserved,
meaning items will expire at their original intended time after unpickling.

```
import pickle
from faas_cache_dict import FaaSCacheDict

cache = FaaSCacheDict(default_ttl=60)
cache['key'] = 'value'

# Pickle and unpickle
data = pickle.dumps(cache)
restored_cache = pickle.loads(data)

# TTL is preserved - will expire at the same time as original
restored_cache['key']
>>> 'value'
```

Note: The background purge thread is automatically restarted after unpickling.

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

## Thread Safety
The cache is fully thread-safe. All operations are protected by a reentrant lock
(RLock), allowing safe concurrent access from multiple threads.

A background daemon thread automatically purges expired items every 5 seconds. This
thread uses a weak reference to allow the cache to be garbage collected when no longer
in use. Call `stop_purge_thread()` to explicitly stop the purge thread when done.

## Known limitations
- The memory constraint applies to the whole cache dict object not just its contents.
The cache dict itself consumes a small amount of memory in overheads, so eg. `1K` of
requested memory will yield slightly less than `1K` of available internal storage.
- Due to extra processing, performance does **slowly** degrade with size (item count),
you will need to test this for your situation. In 99% of use cases this will still be
an order of magnitude faster than doing network calls to an external cache (and more
reliable).
- `on_delete_callable` hooks are invoked outside the lock to prevent deadlocks. Note
that the item has already been deleted from the cache when your callback executes.
- Iteration (`for key in cache`) takes a snapshot of keys under the lock, but yields
outside the lock. In multi-threaded code, keys may be deleted between iteration and
access—wrap `cache[key]` in try/except if needed.

## Support
CPython 3.9 or greater.

## Contributions
This code is distributed under an open license. Feel free to fork it or preferably open
a PR.

## Inspirations
Thanks to `mobilityhouse/ttldict` for their implementation which served as a proof of
concept, which has since been much extended.
