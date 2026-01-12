"""
Threading and concurrency tests for faas-cache-dict.

Tests for thread safety and concurrent access patterns.
"""

import gc
import threading
import time

from faas_cache_dict import FaaSCacheDict


def test_concurrent_read_write():
    """Multiple threads reading and writing should not corrupt data."""
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="10M")
    errors = []
    iterations = 100

    def writer(thread_id):
        try:
            for i in range(iterations):
                faas[f"key_{thread_id}_{i}"] = f"value_{thread_id}_{i}"
        except Exception as e:
            errors.append(e)

    def reader(thread_id):
        try:
            for i in range(iterations):
                key = f"key_{thread_id}_{i}"
                _ = faas.get(key)
        except Exception as e:
            errors.append(e)

    threads = []
    for i in range(5):
        t1 = threading.Thread(target=writer, args=(i,))
        t2 = threading.Thread(target=reader, args=(i,))
        threads.extend([t1, t2])

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0


def test_concurrent_access_same_key():
    """Multiple threads accessing the same key should be thread-safe."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["shared"] = 0
    errors = []

    def increment():
        try:
            for _ in range(100):
                val = faas.get("shared", 0)
                faas["shared"] = val + 1
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=increment) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    # Value may not be exactly 1000 due to race conditions, but no errors
    assert faas["shared"] > 0


def test_concurrent_purge_and_iteration():
    """Iteration should be safe during background purge."""
    faas = FaaSCacheDict(default_ttl=0.5)

    for i in range(100):
        faas[f"key_{i}"] = i

    errors = []

    def iterate():
        try:
            for _ in range(10):
                list(faas.keys())
                time.sleep(0.1)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=iterate) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0


def test_on_delete_callable_thread_safety():
    """on_delete_callable should be safely invoked from background thread."""
    call_count = {"count": 0}
    lock = threading.Lock()

    def on_delete(key, value):
        with lock:
            call_count["count"] += 1

    faas = FaaSCacheDict(default_ttl=1, on_delete_callable=on_delete)
    for i in range(10):
        faas[f"key_{i}"] = i

    # Wait for background purge
    time.sleep(faas._auto_purge_seconds + 2)
    gc.collect()

    assert call_count["count"] == 10


def test_concurrent_delete_operations():
    """Multiple threads deleting items should be thread-safe."""
    faas = FaaSCacheDict(default_ttl=60)
    errors = []

    # Add many items
    for i in range(1000):
        faas[f"key_{i}"] = i

    def deleter(start, end):
        try:
            for i in range(start, end):
                try:
                    del faas[f"key_{i}"]
                except KeyError:
                    pass  # Already deleted by another thread
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=deleter, args=(0, 500)),
        threading.Thread(target=deleter, args=(250, 750)),
        threading.Thread(target=deleter, args=(500, 1000)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0


def test_concurrent_pop_operations():
    """Multiple threads popping items should be thread-safe."""
    faas = FaaSCacheDict(default_ttl=60)
    errors = []
    popped_values = []
    lock = threading.Lock()

    # Add items
    for i in range(100):
        faas[f"key_{i}"] = i

    def popper():
        try:
            for i in range(100):
                val = faas.pop(f"key_{i}", None)
                if val is not None:
                    with lock:
                        popped_values.append(val)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=popper) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    # Each value should only be popped once
    assert len(popped_values) == 100


def test_concurrent_popitem_operations():
    """Multiple threads calling popitem should be thread-safe."""
    faas = FaaSCacheDict(default_ttl=60)
    errors = []
    popped_items = []
    lock = threading.Lock()

    # Add items
    for i in range(50):
        faas[f"key_{i}"] = i

    def popitem_worker():
        try:
            while True:
                try:
                    k, v = faas.popitem()
                    with lock:
                        popped_items.append((k, v))
                except KeyError:
                    break  # Cache is empty
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=popitem_worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    # All items should be popped exactly once
    assert len(popped_items) == 50


def test_concurrent_lru_eviction():
    """LRU eviction under concurrent access should be thread-safe."""
    faas = FaaSCacheDict(max_items=10)
    errors = []

    def writer(thread_id):
        try:
            for i in range(100):
                faas[f"key_{thread_id}_{i}"] = f"value_{thread_id}_{i}"
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    assert len(faas) <= 10


def test_concurrent_clear_and_write():
    """Clearing cache while writing should be thread-safe."""
    faas = FaaSCacheDict(default_ttl=60)
    errors = []

    def writer():
        try:
            for i in range(100):
                faas[f"key_{i}"] = i
                time.sleep(0.001)
        except Exception as e:
            errors.append(e)

    def clearer():
        try:
            for _ in range(10):
                faas.clear()
                time.sleep(0.01)
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=clearer)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert len(errors) == 0


def _create_cache_with_expired_items():
    """Helper to create a cache with items that are already expired."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    time.sleep(0.02)  # Let items expire
    return faas


def _raw_len(faas):
    """Get raw length bypassing expiry check."""
    return len(list(super(FaaSCacheDict, faas).__iter__()))


def test_purge_on_iter():
    """__iter__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3  # Items still in raw storage
    list(faas)  # Trigger __iter__
    assert _raw_len(faas) == 0  # Items purged


def test_purge_on_contains():
    """__contains__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = "a" in faas  # Trigger __contains__
    assert _raw_len(faas) == 0


def test_purge_on_len():
    """__len__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = len(faas)  # Trigger __len__
    assert _raw_len(faas) == 0


def test_purge_on_repr():
    """__repr__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = repr(faas)  # Trigger __repr__
    assert _raw_len(faas) == 0


def test_purge_on_sizeof():
    """__sizeof__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas.__sizeof__()  # Trigger __sizeof__
    assert _raw_len(faas) == 0


def test_purge_on_reversed():
    """__reversed__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = list(reversed(faas))  # Trigger __reversed__
    assert _raw_len(faas) == 0


def test_purge_on_eq():
    """__eq__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas == {}  # Trigger __eq__
    assert _raw_len(faas) == 0


def test_purge_on_ne():
    """__ne__ should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas != {}  # Trigger __ne__
    assert _raw_len(faas) == 0


def test_purge_on_keys():
    """keys() should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas.keys()  # Trigger keys()
    assert _raw_len(faas) == 0


def test_purge_on_items():
    """items() should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas.items()  # Trigger items()
    assert _raw_len(faas) == 0


def test_purge_on_values():
    """values() should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas.values()  # Trigger values()
    assert _raw_len(faas) == 0


def test_purge_on_pop():
    """pop() should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas.pop("nonexistent", None)  # Trigger pop()
    assert _raw_len(faas) == 0


def test_purge_on_popitem():
    """popitem() should purge expired items before raising EmptyCache."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    try:
        faas.popitem()  # Trigger popitem() - will raise since all expired
    except KeyError:
        pass
    assert _raw_len(faas) == 0


def test_purge_on_get_byte_size():
    """get_byte_size() should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    _ = faas.get_byte_size()  # Trigger get_byte_size()
    assert _raw_len(faas) == 0


def test_purge_on_delete_oldest_item():
    """delete_oldest_item() should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    try:
        faas.delete_oldest_item()  # Trigger delete_oldest_item() - will raise since all expired
    except KeyError:
        pass
    assert _raw_len(faas) == 0


def test_purge_on_change_max_items():
    """change_max_items() should purge expired items via __len__."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    faas.change_max_items(10)  # Trigger change_max_items()
    assert _raw_len(faas) == 0


def test_purge_on_change_byte_size():
    """change_byte_size() should purge expired items."""
    faas = _create_cache_with_expired_items()
    assert _raw_len(faas) == 3
    faas.change_byte_size("1M")  # Trigger change_byte_size()
    assert _raw_len(faas) == 0


def test_purge_on_setitem_via_shrink():
    """__setitem__ should purge expired items via _shrink_to_fit_byte_size."""
    faas = FaaSCacheDict(default_ttl=0.01, max_size_bytes="1M")
    faas["a"] = 1
    faas["b"] = 2
    time.sleep(0.02)  # Let items expire
    assert _raw_len(faas) == 2
    faas["c"] = 3  # Trigger __setitem__ which calls _shrink_to_fit_byte_size
    assert _raw_len(faas) == 1  # Only "c" remains
