import time

import pytest

from faas_cache_dict import FaaSCacheDict


def test_unexpired_key_available():
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    assert faas["a"] == 1


def test_expired_key_not_available():
    faas = FaaSCacheDict(default_ttl=0.25)
    faas["a"] = 1
    assert faas["a"] == 1
    time.sleep(0.3)
    with pytest.raises(KeyError):
        assert faas["a"] == 1


def test_is_expired():
    faas = FaaSCacheDict(default_ttl=0.25)
    faas["a"] = 1
    assert not faas.is_expired("a")
    time.sleep(0.3)
    assert faas.is_expired("a")


def test_unknown_key_is_null_expired():
    faas = FaaSCacheDict()
    faas["a"] = 1
    assert faas.is_expired("a") is False
    assert faas.is_expired("b") is None


def test_expire_at():
    faas = FaaSCacheDict(default_ttl=0.1)
    faas["a"] = 1
    faas.expire_at("a", time.time() + 0.2)
    time.sleep(0.1)
    assert faas["a"] == 1
    assert not faas.is_expired("a")
    time.sleep(0.15)
    assert faas.is_expired("a")
    with pytest.raises(KeyError):
        assert faas["a"] == 1


def test_set_new_default_ttl():
    faas = FaaSCacheDict(default_ttl=1)
    assert faas.default_ttl == 1
    faas.default_ttl = 10
    assert faas.default_ttl == 10


def test_set_ttl():
    faas = FaaSCacheDict(default_ttl=0.1)
    faas["a"] = 1
    time.sleep(0.05)
    faas.set_ttl("a", 0.2)
    assert faas["a"] == 1
    time.sleep(0.2)
    with pytest.raises(KeyError):
        assert faas["a"] == 1


def test_get_ttl():
    faas = FaaSCacheDict(default_ttl=1)
    faas["a"] = 1
    assert faas.get_ttl("a")
    assert faas.get_ttl("a") < 1
    faas.default_ttl = 10
    faas["b"] = 2
    assert 9.8 < faas.get_ttl("b") < 10


def test_get_ttl_returns_none_when_no_default_ttl():
    """get_ttl should return None when key has no TTL set"""
    faas = FaaSCacheDict()  # No default_ttl
    faas["a"] = 1
    assert faas.get_ttl("a") is None
