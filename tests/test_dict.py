import pickle
from threading import _RLock

import pytest

from faas_cache_dict import FaaSCacheDict


def test_basic_read_write_op():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    faas["b"] = 2
    assert faas["a"] == 1
    assert faas["b"] == 2


def test_delete_op():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    faas["b"] = 2
    assert len(faas) == 2

    del faas["b"]
    assert len(faas) == 1
    assert faas["a"] == 1
    with pytest.raises(KeyError):
        assert faas["b"] == 1
    with pytest.raises(KeyError):
        del faas["unknown"]


def test_get_op():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    faas["b"] = 2
    assert faas.get("a") == 1
    assert faas.get("b") == 2
    assert faas.get("non-exist", 9000) == 9000


def test_keys_op():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    faas["b"] = 2
    assert faas.keys() == ["a", "b"]


def test_values_op():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    assert faas.values() == [1]
    faas["b"] = 2
    assert faas.values() == [1, 2]


def test_items_op():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    assert faas.items() == [("a", 1)]
    faas["b"] = 2
    assert faas.items() == [("a", 1), ("b", 2)]


def test_len_op():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    assert len(faas) == 0
    faas["a"] = 1
    assert len(faas) == 1
    faas["b"] = 2
    assert len(faas) == 2
    faas["a"] = 3
    assert len(faas) == 2
    del faas["a"]
    assert len(faas) == 1


def test_iterator():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    for x in faas:
        assert x in faas.keys()


def test_reducer():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    faas["a"] = 1
    dumped = pickle.dumps(faas, protocol=5)
    loaded = pickle.loads(dumped)

    assert type(faas._lock) == type(loaded._lock)

    assert loaded.default_ttl == 60
    assert loaded._max_size_user == "1M"
