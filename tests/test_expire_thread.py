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
