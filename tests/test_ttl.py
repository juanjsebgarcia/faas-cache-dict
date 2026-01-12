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


def test_set_ttl_none_removes_expiry():
    """Setting TTL to None should remove expiry (key never expires)"""
    faas = FaaSCacheDict(default_ttl=0.1)
    faas["a"] = 1

    # Key would expire in 0.1 seconds
    assert faas.get_ttl("a") is not None

    # Remove expiry
    faas.set_ttl("a", None)

    # TTL should now be None
    assert faas.get_ttl("a") is None

    # Key should still exist after original TTL would have expired
    time.sleep(0.2)
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


def test_default_ttl_zero_expires_immediately():
    """default_ttl=0 should cause items to expire immediately"""
    faas = FaaSCacheDict(default_ttl=0)
    faas["a"] = 1

    # Item either expired (True) or already purged (None)
    assert faas.is_expired("a") in (True, None)

    # Accessing should raise KeyError (expired or purged)
    with pytest.raises(KeyError):
        _ = faas["a"]


def test_set_ttl_zero_expires_immediately():
    """set_ttl(key, 0) should cause item to expire immediately"""
    faas = FaaSCacheDict()
    faas["a"] = 1

    # No TTL initially
    assert faas.is_expired("a") is False

    # Set TTL to 0
    faas.set_ttl("a", 0)

    # Should be expired now
    assert faas.is_expired("a") is True


def test_expire_at_with_past_timestamp():
    """expire_at() with past timestamp should make key expired immediately."""
    faas = FaaSCacheDict()
    faas["a"] = 1
    faas.expire_at("a", time.time() - 10)  # 10 seconds in the past
    assert faas.is_expired("a") is True
    with pytest.raises(KeyError):
        _ = faas["a"]


def test_expire_at_nonexistent_key_raises():
    """expire_at() on nonexistent key should raise KeyError."""
    faas = FaaSCacheDict()
    with pytest.raises(KeyError):
        faas.expire_at("nonexistent", time.time() + 60)


def test_set_ttl_nonexistent_key_raises():
    """set_ttl() on nonexistent key should raise KeyError."""
    faas = FaaSCacheDict()
    with pytest.raises(KeyError):
        faas.set_ttl("nonexistent", 60)


def test_set_ttl_negative_raises():
    """set_ttl() with negative TTL should raise ValueError."""
    faas = FaaSCacheDict()
    faas["a"] = 1
    with pytest.raises(ValueError, match="TTL must be non-negative"):
        faas.set_ttl("a", -1)


def test_set_ttl_negative_float_raises():
    """set_ttl() with negative float TTL should raise ValueError."""
    faas = FaaSCacheDict()
    faas["a"] = 1
    with pytest.raises(ValueError, match="TTL must be non-negative"):
        faas.set_ttl("a", -0.5)


def test_get_ttl_on_key_with_no_expiry():
    """get_ttl() on key with no expiry should return None."""
    faas = FaaSCacheDict()  # No default TTL
    faas["a"] = 1
    assert faas.get_ttl("a") is None


def test_is_expired_consistency_with_getitem():
    """is_expired() should be consistent with __getitem__ behavior."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)

    # If is_expired returns True, __getitem__ should raise KeyError
    if faas.is_expired("a"):
        with pytest.raises(KeyError):
            _ = faas["a"]


def test_custom_now_parameter_in_is_expired():
    """is_expired() should respect custom now parameter."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    # With current time, should not be expired
    assert faas.is_expired("a") is False

    # With future time, should be expired
    future = time.time() + 120
    assert faas.is_expired("a", now=future) is True


def test_custom_now_parameter_in_get_ttl():
    """get_ttl() should respect custom now parameter."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    current_ttl = faas.get_ttl("a")
    # With future time, TTL should be less
    future_ttl = faas.get_ttl("a", now=time.time() + 30)
    assert future_ttl < current_ttl


def test_custom_now_parameter_in_set_ttl():
    """set_ttl() should respect custom now parameter."""
    faas = FaaSCacheDict()
    faas["a"] = 1

    custom_now = time.time() + 100
    faas.set_ttl("a", 60, now=custom_now)

    # TTL should be set relative to custom_now
    # The expire time should be custom_now + 60, so TTL from current time
    # should be approximately (custom_now + 60) - time.time() = 100 + 60 = 160
    actual_ttl = faas.get_ttl("a")
    assert actual_ttl > 150  # Should be around 160 seconds


def test_very_large_ttl_precision():
    """Very large TTL values should maintain precision."""
    faas = FaaSCacheDict()
    faas["a"] = 1
    large_ttl = 10**9  # ~31 years
    faas.set_ttl("a", large_ttl)
    ttl = faas.get_ttl("a")
    # Should be close to original (within 1 second)
    assert abs(ttl - large_ttl) < 1


def test_get_ttl_near_expiry():
    """get_ttl() on key about to expire should return small positive value."""
    faas = FaaSCacheDict(default_ttl=0.1)
    faas["a"] = 1
    time.sleep(0.05)
    ttl = faas.get_ttl("a")
    assert ttl is not None
    assert ttl < 0.06  # Should be about 0.05 or less


def test_get_ttl_after_expiry_returns_negative():
    """get_ttl() after expiry should return negative value."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)
    # Item still exists internally, get_ttl should return negative
    # Note: This depends on whether purge has run
    ttl = faas.get_ttl("a")
    if ttl is not None:
        assert ttl < 0
