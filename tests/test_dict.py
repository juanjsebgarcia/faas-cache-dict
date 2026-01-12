import pickle
import time

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

    assert type(faas._lock) is type(loaded._lock)

    assert loaded.default_ttl == 60
    assert loaded._max_size_user == "1M"


def test_contains():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    assert "a" in faas
    time.sleep(2)
    assert "a" not in faas


def test_dict():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    fdict = faas.__dict__
    assert fdict["default_ttl"] == 2


def test_bitwise_or_raises():
    with pytest.raises(NotImplementedError):
        FaaSCacheDict(default_ttl=1) | FaaSCacheDict(default_ttl=2)


def test_equal():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas2 = FaaSCacheDict(default_ttl=2)
    faas2["a"] = 1

    assert faas is not faas2
    assert faas == faas2


def test_not_equal():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas2 = FaaSCacheDict(default_ttl=3)
    faas2["a"] = 2

    assert faas is not faas2
    assert faas != faas2


def test_equal_with_incompatible_type_returns_false():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1

    assert (faas == 42) is False
    assert (faas == "string") is False
    assert (faas is None) is False
    assert (faas == [1, 2, 3]) is False


def test_not_equal_with_incompatible_type():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1

    assert faas != 42
    assert faas != "string"
    assert faas is not None
    assert faas != [1, 2, 3]


def test_equal_with_regular_dict():
    """Test that FaaSCacheDict compares equal to a regular dict with same contents."""
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas["b"] = 2

    regular_dict = {"a": 1, "b": 2}

    assert faas == regular_dict
    assert regular_dict == faas


def test_not_equal_with_regular_dict():
    """Test that FaaSCacheDict compares not equal to a regular dict with different contents."""
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas["b"] = 2

    assert faas != {"a": 1, "b": 3}
    assert faas != {"a": 1}
    assert faas != {"a": 1, "b": 2, "c": 3}
    assert faas != {}


def test_equal_with_empty_dict():
    """Test that empty FaaSCacheDict compares equal to empty dict."""
    faas = FaaSCacheDict(default_ttl=2)

    assert faas == {}
    assert {} == faas


def test_getitem_keyerror_includes_key():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1

    try:
        _ = faas["nonexistent"]
        assert False, "Should have raised KeyError"
    except KeyError as e:
        assert e.args[0] == "nonexistent"


def test_getitem_expired_keyerror_includes_key():
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["expiring_key"] = 1

    time.sleep(0.02)

    try:
        _ = faas["expiring_key"]
        assert False, "Should have raised KeyError"
    except KeyError as e:
        assert e.args[0] == "expiring_key"


def test_clear():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas["b"] = 2
    faas.clear()
    assert len(faas) == 0


def test_reversed():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas["b"] = 2
    assert [x for x in reversed(faas)] == ["b", "a"]


def test_pop():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas["b"] = 2
    v = faas.pop("a")
    assert v == 1
    v = faas.pop("unknown", 9000)
    assert v == 9000
    assert len(faas) == 1


def test_popitem():
    faas = FaaSCacheDict(default_ttl=2)
    faas["a"] = 1
    faas["b"] = 2
    k, v = faas.popitem()
    assert v == 2
    assert k == "b"
    assert len(faas) == 1


def test_copy():
    with pytest.raises(NotImplementedError):
        faas = FaaSCacheDict(default_ttl=2)
        faas.copy()


def test_setdefault_existing_key():
    """setdefault should return existing value without modifying it."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    result = faas.setdefault("a", 999)
    assert result == 1
    assert faas["a"] == 1


def test_setdefault_missing_key():
    """setdefault should set and return default for missing key."""
    faas = FaaSCacheDict(default_ttl=60)
    result = faas.setdefault("a", 42)
    assert result == 42
    assert faas["a"] == 42


def test_setdefault_expired_key():
    """setdefault should treat expired key as missing."""
    import time

    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)
    result = faas.setdefault("a", 999)
    assert result == 999
    assert faas["a"] == 999


def test_setdefault_respects_constraints():
    """setdefault should respect max_items constraint."""
    faas = FaaSCacheDict(default_ttl=60, max_items=2)
    faas["a"] = 1
    faas["b"] = 2
    # This should evict "a" to make room
    faas.setdefault("c", 3)
    assert "a" not in faas
    assert faas["b"] == 2
    assert faas["c"] == 3


def test_setdefault_none_default():
    """setdefault with no default should use None."""
    faas = FaaSCacheDict(default_ttl=60)
    result = faas.setdefault("a")
    assert result is None
    assert faas["a"] is None


def test_fromkeys_raises():
    """fromkeys should raise NotImplementedError."""
    with pytest.raises(NotImplementedError):
        FaaSCacheDict.fromkeys(["a", "b", "c"])


def test_move_to_end():
    """move_to_end should move key to end of cache."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    assert list(faas.keys()) == ["a", "b", "c"]
    faas.move_to_end("a")
    assert list(faas.keys()) == ["b", "c", "a"]


def test_move_to_end_first():
    """move_to_end(key, last=False) should move key to beginning."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas.move_to_end("c", last=False)
    assert list(faas.keys()) == ["c", "a", "b"]


def test_move_to_end_expired_key():
    """move_to_end should raise KeyError for expired key."""
    import time

    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)
    with pytest.raises(KeyError):
        faas.move_to_end("a")


def test_move_to_end_missing_key():
    """move_to_end should raise KeyError for missing key."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    with pytest.raises(KeyError):
        faas.move_to_end("nonexistent")


def test_create_with_values():
    faas = FaaSCacheDict(default_ttl=2, a=1, b=2)
    assert faas["a"] == 1
    assert faas["b"] == 2


def test_init_invalid_ttl_string_raises():
    """String TTL values should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid TTL config"):
        FaaSCacheDict(default_ttl="60")


def test_init_invalid_ttl_list_raises():
    """List TTL values should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid TTL config"):
        FaaSCacheDict(default_ttl=[60])


def test_init_invalid_ttl_dict_raises():
    """Dict TTL values should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid TTL config"):
        FaaSCacheDict(default_ttl={"ttl": 60})


def test_init_negative_ttl_raises():
    """Negative TTL should raise ValueError."""
    with pytest.raises(ValueError, match="TTL must be >=0"):
        FaaSCacheDict(default_ttl=-1)


def test_init_negative_float_ttl_raises():
    """Negative float TTL should raise ValueError."""
    with pytest.raises(ValueError, match="TTL must be >=0"):
        FaaSCacheDict(default_ttl=-0.5)


def test_init_invalid_max_size_bytes_float_raises():
    """Float max_size_bytes should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid byte size"):
        FaaSCacheDict(max_size_bytes=1.5)


def test_init_invalid_max_size_bytes_list_raises():
    """List max_size_bytes should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid byte size"):
        FaaSCacheDict(max_size_bytes=[1024])


def test_init_invalid_max_size_bytes_dict_raises():
    """Dict max_size_bytes should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid byte size"):
        FaaSCacheDict(max_size_bytes={"size": 1024})


def test_init_invalid_max_items_float_raises():
    """Float max_items should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid max items limit"):
        FaaSCacheDict(max_items=3.5)


def test_init_invalid_max_items_string_raises():
    """String max_items should raise ValueError."""
    with pytest.raises(ValueError, match="Invalid max items limit"):
        FaaSCacheDict(max_items="10")


def test_init_max_items_zero_raises():
    """max_items=0 should raise ValueError."""
    with pytest.raises(ValueError, match="Max items limit must >0"):
        FaaSCacheDict(max_items=0)


def test_init_max_items_negative_raises():
    """Negative max_items should raise ValueError."""
    with pytest.raises(ValueError, match="Max items limit must >0"):
        FaaSCacheDict(max_items=-1)


def test_init_with_dict_args():
    """Constructor should accept dict as positional argument."""
    faas = FaaSCacheDict(None, None, None, None, {"a": 1, "b": 2})
    assert faas["a"] == 1
    assert faas["b"] == 2


def test_init_with_list_of_tuples():
    """Constructor should accept list of tuples as positional argument."""
    faas = FaaSCacheDict(None, None, None, None, [("a", 1), ("b", 2)])
    assert faas["a"] == 1
    assert faas["b"] == 2


def test_get_expired_key_returns_default():
    """get() should return default when key has expired."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)
    assert faas.get("a", "default") == "default"


def test_get_expired_key_returns_none_by_default():
    """get() should return None for expired key when no default provided."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)
    assert faas.get("a") is None


def test_get_nonexistent_vs_expired_both_return_default():
    """Both nonexistent and expired keys should return the same default."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["existing"] = 1
    time.sleep(0.02)

    default = "my_default"
    assert faas.get("nonexistent", default) == default
    assert faas.get("existing", default) == default


def test_get_with_none_as_explicit_default():
    """get(key, None) should work correctly."""
    faas = FaaSCacheDict(default_ttl=60)
    assert faas.get("nonexistent", None) is None


def test_pop_expired_key_returns_default():
    """pop() on expired key should return default."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)
    assert faas.pop("a", "default") == "default"


def test_pop_expired_key_no_default_returns_none():
    """pop() on expired key with no default should return None."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    time.sleep(0.02)
    assert faas.pop("a") is None


def test_popitem_empty_cache_error_message():
    """popitem() on empty cache should raise KeyError with 'EmptyCache' message."""
    faas = FaaSCacheDict()
    with pytest.raises(KeyError) as exc_info:
        faas.popitem()
    assert exc_info.value.args[0] == "EmptyCache"


def test_popitem_all_items_expired():
    """popitem() when all items are expired should raise KeyError."""
    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    faas["b"] = 2
    time.sleep(0.02)
    with pytest.raises(KeyError) as exc_info:
        faas.popitem()
    assert exc_info.value.args[0] == "EmptyCache"


def test_repr_with_none_max_size():
    """__repr__ should handle None max_size correctly."""
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes=None)
    repr_str = repr(faas)
    assert "max_memory=None" in repr_str


def test_repr_with_max_size_set():
    """__repr__ should display max_size when set."""
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    repr_str = repr(faas)
    assert "max_memory=1M" in repr_str


def test_str_equals_repr():
    """__str__ should return same as __repr__."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    assert str(faas) == repr(faas)


def test_sizeof_returns_int():
    """__sizeof__ should return an integer."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1
    size = faas.__sizeof__()
    assert isinstance(size, int)
    assert size > 0


def test_reversed_empty_cache():
    """__reversed__ on empty cache should return empty iterator."""
    faas = FaaSCacheDict()
    result = list(reversed(faas))
    assert result == []


def test_eq_with_object_having_non_callable_items():
    """__eq__ with object having items attribute but not callable should raise TypeError."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = 1

    class FakeDict:
        items = "not callable"

    # The __eq__ implementation calls other.items() without checking if it's callable
    # This results in a TypeError when items is not a method
    with pytest.raises(TypeError):
        _ = faas == FakeDict()


def test_ior_raises_not_implemented():
    """__ior__ (|=) should raise NotImplementedError."""
    faas = FaaSCacheDict(default_ttl=60)
    faas2 = FaaSCacheDict(default_ttl=60)
    with pytest.raises(NotImplementedError):
        faas |= faas2


def test_ror_raises_not_implemented():
    """__ror__ (right |) should raise NotImplementedError."""
    faas = FaaSCacheDict(default_ttl=60)
    with pytest.raises((NotImplementedError, TypeError)):
        {"a": 1} | faas


def test_unhashable_key_list_raises_type_error():
    """Using unhashable key (list) should raise TypeError."""
    faas = FaaSCacheDict(default_ttl=60)
    with pytest.raises(TypeError):
        faas[[1, 2, 3]] = "value"


def test_unhashable_key_dict_raises_type_error():
    """Using unhashable key (dict) should raise TypeError."""
    faas = FaaSCacheDict(default_ttl=60)
    with pytest.raises(TypeError):
        faas[{"key": "value"}] = "value"


def test_none_value_storage_and_retrieval():
    """None should be a valid value to store and retrieve."""
    faas = FaaSCacheDict(default_ttl=60)
    faas["key_with_none"] = None
    assert faas["key_with_none"] is None
    assert faas.get("key_with_none", "default") is None


def test_empty_string_key():
    """Empty string should be a valid key."""
    faas = FaaSCacheDict(default_ttl=60)
    faas[""] = "empty key value"
    assert faas[""] == "empty key value"


def test_numeric_keys():
    """Numeric keys should work correctly."""
    faas = FaaSCacheDict(default_ttl=60)
    faas[0] = "zero"
    faas[1] = "one"
    faas[-1] = "negative one"
    faas[3.14] = "pi"

    assert faas[0] == "zero"
    assert faas[1] == "one"
    assert faas[-1] == "negative one"
    assert faas[3.14] == "pi"


def test_tuple_key():
    """Tuple keys (hashable) should work correctly."""
    faas = FaaSCacheDict(default_ttl=60)
    faas[(1, 2, 3)] = "tuple key"
    assert faas[(1, 2, 3)] == "tuple key"


def test_purge_clears_all_items():
    """purge() should clear all items."""
    faas = FaaSCacheDict()
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    faas.purge()

    assert len(faas) == 0
