import time
import uuid

import pytest

from faas_cache_dict.constants import BYTES_PER_MEBIBYTE
from faas_cache_dict.faas_cache_dict import DataTooLarge, FaaSCacheDict

one_mb_text = open("tests/1_mebibyte.txt").read()


def load_with_mebibyte_of_data(faas, mb):
    faas[str(uuid.uuid4())] = one_mb_text * mb
    return faas


def test_max_size_set():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE

    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="2M")
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE * 2


def test_change_byte_size():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M")
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE
    faas.change_byte_size("2M")
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE * 2
    faas = load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 1
    faas.change_byte_size("1M")
    assert len(faas) == 0


def test_shrink_to_fit_byte_size():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="10M")
    faas = load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 1
    faas.change_byte_size("1M")
    assert len(faas) == 0


def test_get_byte_size():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="10M")
    faas = load_with_mebibyte_of_data(faas, 1)
    assert faas.get_byte_size() > BYTES_PER_MEBIBYTE


def test_byte_size_set_purge_expired():
    faas = FaaSCacheDict(default_ttl=0.5, max_size_bytes="10M")
    original_size = faas.get_byte_size()
    faas["a"] = 1
    loaded_size = faas.get_byte_size()
    assert original_size < loaded_size
    time.sleep(0.5)
    faas._purge_expired()
    assert original_size < faas.get_byte_size() < loaded_size


def test_byte_size_set_delete():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes="10M")
    faas["a"] = 1
    loaded_size = faas.get_byte_size()
    del faas["a"]
    assert faas.get_byte_size() < loaded_size


def test_byte_size_set_add():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes="10M")
    faas["a"] = "a"
    loaded_size = faas.get_byte_size()
    faas["b"] = "bb"
    assert loaded_size < faas.get_byte_size()


def test_byte_size_set_modification():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes="10M")
    faas["a"] = "a"
    loaded_size = faas.get_byte_size()
    faas["a"] = "aaa"
    assert loaded_size < faas.get_byte_size()


def test_raises_if_data_oversized():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes="2M")
    load_with_mebibyte_of_data(faas, 1)  # No error
    with pytest.raises(DataTooLarge):
        # Expected to raise as dict consumes some space
        faas["a"] = load_with_mebibyte_of_data(faas, 2)
    with pytest.raises(DataTooLarge):
        faas["a"] = load_with_mebibyte_of_data(faas, 3)


def test_memory_size_none():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes=None)
    load_with_mebibyte_of_data(faas, 1)
    load_with_mebibyte_of_data(faas, 1)
    load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 3


def test_memory_size_none_then_limited():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes=None)
    load_with_mebibyte_of_data(faas, 1)
    load_with_mebibyte_of_data(faas, 1)
    load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 3
    faas.change_byte_size("1M")
    assert len(faas) == 0


def test_memory_size_then_none():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes="2M")
    load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 1
    load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 2
    faas.change_byte_size(None)
    assert len(faas) == 2
    load_with_mebibyte_of_data(faas, 1)
    load_with_mebibyte_of_data(faas, 1)
    load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 5


def test_change_max_size():
    faas = FaaSCacheDict(max_items=20)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4
    faas["e"] = 5

    faas.change_max_items(2)

    assert len(faas) == 2

    assert faas.keys() == ["d", "e"]


def test_change_max_size_with_expired():
    faas = FaaSCacheDict(max_items=20)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4
    faas["e"] = 5

    faas.expire_at("d", 10)

    faas.change_max_items(2)

    assert len(faas) == 2

    assert faas.keys() == ["c", "e"]


def test_change_byte_size_zero_raises():
    """change_byte_size(0) should raise ValueError for consistency with __init__."""
    faas = FaaSCacheDict(max_size_bytes="1M")
    faas["a"] = 1
    with pytest.raises(ValueError, match="Byte size must be >0"):
        faas.change_byte_size(0)


def test_change_byte_size_negative_int_raises():
    """change_byte_size(-1) should raise ValueError."""
    faas = FaaSCacheDict(max_size_bytes="1M")
    with pytest.raises(ValueError, match="Byte size must be >0"):
        faas.change_byte_size(-1)


def test_change_byte_size_negative_string_raises():
    """change_byte_size('-1M') should raise ValueError."""
    faas = FaaSCacheDict(max_size_bytes="1M")
    with pytest.raises(ValueError, match="Memory size must be >0"):
        faas.change_byte_size("-1M")


def test_get_byte_size_skip_purge_true():
    """get_byte_size(skip_purge=True) should not purge expired items."""
    faas = FaaSCacheDict(default_ttl=0.01, max_size_bytes="1M")
    faas["a"] = 1
    time.sleep(0.02)
    # Skip purge should still return cached size
    size = faas.get_byte_size(skip_purge=True)
    assert size > 0


def test_setitem_replace_key_with_larger_value():
    """Replacing existing key with larger value should handle size correctly."""
    faas = FaaSCacheDict(max_size_bytes="1M")
    faas["a"] = "small"
    original_size = faas.get_byte_size()
    faas["a"] = "this is a much larger string value" * 100
    new_size = faas.get_byte_size()
    assert new_size > original_size


def test_shrink_to_fit_with_many_items():
    """_shrink_to_fit_byte_size should handle many items and evict when needed."""
    faas = FaaSCacheDict(max_size_bytes="1M")
    # Add items with larger values to ensure we exceed the threshold
    for i in range(100):
        faas[f"key_{i}"] = f"value_{i}" * 1000  # Larger values
    initial_len = len(faas)
    _ = faas.get_byte_size()
    # Reduce size dramatically - some items must be evicted
    faas.change_byte_size("100K")
    # Should have shrunk (fewer items)
    assert len(faas) < initial_len
    # Should be at or under the limit
    assert faas.get_byte_size() <= 100 * 1024 + 5000  # Some overhead tolerance


def test_combined_ttl_memory_maxitems_constraints():
    """Cache should handle TTL, memory, and max_items constraints together."""
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes="10K", max_items=5)

    # Add items
    for i in range(10):
        faas[f"key_{i}"] = f"value_{i}" * 10

    # max_items=5 should limit count
    assert len(faas) <= 5

    # Memory constraint should be respected
    assert faas.get_byte_size() <= 10 * 1024 + 1000

    # Wait for TTL expiry
    time.sleep(1.5)

    # All items should be expired or purged
    assert len(faas) == 0
