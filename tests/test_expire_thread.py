import gc
import pickle
import time
from unittest.mock import Mock

import objsize

from faas_cache_dict import FaaSCacheDict


def test_purge_thread_alive():
    faas = FaaSCacheDict()
    time.sleep(1)
    assert faas._purge_thread.is_alive


def test_purge_thread_working():
    faas = FaaSCacheDict(default_ttl=2)
    for i in range(1000):
        faas[i] = i * 10
    time.sleep(1)
    assert faas._purge_thread.is_alive
    prev_size = objsize.get_deep_size(faas)
    assert prev_size > 40000
    time.sleep(faas._auto_purge_seconds + 2)
    gc.collect()
    assert objsize.get_deep_size(faas) < prev_size / 1.25


def test_purge_thread_is_daemon():
    """Purge thread should be a daemon so it doesn't prevent program exit"""
    faas = FaaSCacheDict()
    assert faas._purge_thread.daemon is True


def test_purge_thread_removes_expired_without_access():
    """Expired items should be removed by background thread without explicit access"""
    faas = FaaSCacheDict(default_ttl=2)

    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    # Items exist initially (using super to bypass expiry check)
    assert len(list(super(FaaSCacheDict, faas).__iter__())) == 3

    # Wait for expiry + purge cycle (default is 5 seconds)
    time.sleep(faas._auto_purge_seconds + 3)
    gc.collect()

    # Items should be purged by background thread
    assert len(list(super(FaaSCacheDict, faas).__iter__())) == 0


def test_purge_thread_calls_on_delete_callable():
    """Background purge should trigger on_delete_callable for expired items"""
    mock = Mock(return_value=None)
    faas = FaaSCacheDict(default_ttl=2, on_delete_callable=mock.delete)

    faas["a"] = 1
    faas["b"] = 2

    mock.delete.assert_not_called()

    # Wait for expiry + purge cycle (default is 5 seconds)
    time.sleep(faas._auto_purge_seconds + 3)

    # on_delete_callable should have been called for both items
    assert mock.delete.call_count == 2


def test_purge_thread_alive_after_unpickle():
    """Purge thread should be running after unpickling"""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    assert loaded._purge_thread.is_alive()
    assert loaded._purge_thread.daemon is True


def test_pickle_preserves_values():
    """Pickling and unpickling should preserve values correctly"""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    faas["b"] = "hello"
    faas["c"] = {"nested": "dict"}

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    assert loaded["a"] == 1
    assert loaded["b"] == "hello"
    assert loaded["c"] == {"nested": "dict"}
    assert len(loaded) == 3


def test_pickle_preserves_ttl():
    """Pickling and unpickling should preserve TTL expiry times"""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    original_ttl = faas.get_ttl("a")

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    loaded_ttl = loaded.get_ttl("a")

    # TTL should be very close (within 1 second due to test execution time)
    assert abs(original_ttl - loaded_ttl) < 1


def test_pickle_preserves_lru_order():
    """Pickling and unpickling should preserve LRU order"""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    # Access "a" to make it most recently used
    _ = faas["a"]

    # Order should now be: b, c, a (oldest to newest)
    assert faas.keys() == ["b", "c", "a"]

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    # LRU order should be preserved
    assert loaded.keys() == ["b", "c", "a"]


def test_unpickle_with_empty_pickled_items():
    """Unpickling cache with no items should work correctly."""
    faas = FaaSCacheDict(default_ttl=60)
    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)
    assert len(loaded) == 0


def test_unpickle_mixed_ttl_and_no_ttl_items():
    """Unpickling should preserve mixed TTL and no-TTL items."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["with_ttl"] = 1
    faas.set_ttl("with_ttl", 120)

    # Add item with no TTL
    faas["no_ttl"] = 2
    faas.set_ttl("no_ttl", None)

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    assert loaded.get_ttl("with_ttl") is not None
    assert loaded.get_ttl("no_ttl") is None
    assert loaded["no_ttl"] == 2


def test_unpickle_items_expired_after_serialization():
    """Items that expire after serialization but before unpickle should be expired."""
    faas = FaaSCacheDict(default_ttl=0.1)
    faas["short_lived"] = 1
    dumped = pickle.dumps(faas, protocol=5)
    time.sleep(0.2)  # Wait for TTL to expire
    loaded = pickle.loads(dumped)
    assert loaded.is_expired("short_lived") is True


def test_multiple_pickle_unpickle_cycles():
    """Multiple pickle/unpickle cycles should maintain integrity."""
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    faas["b"] = 2

    for _ in range(5):
        dumped = pickle.dumps(faas, protocol=5)
        faas = pickle.loads(dumped)

    assert faas["a"] == 1
    assert faas["b"] == 2
    assert faas._purge_thread.is_alive()


def test_pickle_preserves_max_size_bytes():
    """Pickling should preserve max_size_bytes configuration."""
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="2M")
    faas["a"] = 1

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    assert loaded._max_size_user == "2M"


def test_pickle_preserves_max_items():
    """Pickling should preserve max_items configuration."""
    faas = FaaSCacheDict(default_ttl=60, max_items=100)
    faas["a"] = 1

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    assert loaded._max_items == 100


def test_pickle_recalculates_byte_size():
    """Unpickling should recalculate _self_byte_size, not use stale pickled value."""
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = "x" * 1000  # Add some data

    original_size = faas.get_byte_size()
    assert original_size > 0

    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    # Byte size should be recalculated and be similar to original
    loaded_size = loaded._self_byte_size
    assert loaded_size > 0
    # Should be close to original (allowing some variance for object overhead)
    assert abs(loaded_size - original_size) < 1000


def test_cache_can_be_garbage_collected():
    """FaaSCacheDict should be garbage collected when no references remain."""
    import weakref as wr

    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    weak_ref = wr.ref(faas)

    del faas
    gc.collect()

    # Give thread time to notice and exit
    time.sleep(0.1)
    gc.collect()

    assert weak_ref() is None, "FaaSCacheDict was not garbage collected"


def test_close_stops_purge_thread():
    """close() should stop the background purge thread."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    assert faas._purge_thread.is_alive()

    faas.close()
    # Give thread time to exit (it checks on next iteration)
    time.sleep(faas._auto_purge_seconds + 1)

    assert not faas._purge_thread.is_alive()


def test_close_is_idempotent():
    """Calling close() multiple times should not cause errors."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    faas.close()
    faas.close()
    faas.close()

    assert faas._stop_purge is True


def test_cache_still_works_after_close():
    """Cache operations should still work after close(), just no background purge."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    faas.close()

    # Basic operations should still work
    faas["b"] = 2
    assert faas["a"] == 1
    assert faas["b"] == 2
    del faas["a"]
    assert "a" not in faas
