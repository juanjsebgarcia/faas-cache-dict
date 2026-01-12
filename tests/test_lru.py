import pytest

from faas_cache_dict import FaaSCacheDict


def test_respects_max_size_constraint():
    faas = FaaSCacheDict(max_items=2)
    faas["a"] = 1
    assert len(faas) == 1
    faas["b"] = 2
    assert len(faas) == 2
    faas["c"] = 3
    assert len(faas) == 2
    faas["d"] = 4
    assert len(faas) == 2

    assert list(faas.keys()) == ["c", "d"]


def test_oldest_item_at_head():
    faas = FaaSCacheDict(max_items=4)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4

    assert list(faas.keys())[0] == "a"
    assert list(faas.keys())[-1] == "d"


def test_getting_item_resets_to_end():
    faas = FaaSCacheDict(max_items=4)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4

    assert list(faas.keys())[0] == "a"
    _ = faas["a"]
    assert list(faas.keys())[0] == "b"
    assert list(faas.keys())[-1] == "a"
    assert len(faas.keys()) == 4


def test_delete_oldest_item():
    faas = FaaSCacheDict(max_items=4)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4

    assert list(faas.keys())[0] == "a"
    faas.delete_oldest_item()
    assert list(faas.keys())[0] == "b"
    assert len(faas.keys()) == 3


def test_delete_oldest_item_empty_cache():
    """delete_oldest_item() on empty cache should raise KeyError."""
    faas = FaaSCacheDict()
    with pytest.raises(KeyError) as exc_info:
        faas.delete_oldest_item()
    assert exc_info.value.args[0] == "EmptyCache"


def test_delete_oldest_item_all_expired():
    """delete_oldest_item() when all items expired should raise KeyError."""
    import time

    faas = FaaSCacheDict(default_ttl=0.01)
    faas["a"] = 1
    faas["b"] = 2
    time.sleep(0.02)
    with pytest.raises(KeyError) as exc_info:
        faas.delete_oldest_item()
    assert exc_info.value.args[0] == "EmptyCache"


def test_change_max_items_none_disables_limit():
    """change_max_items(None) should disable the item limit."""
    faas = FaaSCacheDict(max_items=2)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3  # Should evict "a"
    assert len(faas) == 2

    faas.change_max_items(None)
    faas["d"] = 4
    faas["e"] = 5
    faas["f"] = 6
    # With limit disabled, all items should remain
    assert len(faas) >= 4


def test_change_max_items_zero_raises():
    """change_max_items(0) should raise ValueError for consistency with __init__."""
    faas = FaaSCacheDict(max_items=2)
    faas["a"] = 1
    faas["b"] = 2
    with pytest.raises(ValueError, match="Max items limit must be >0"):
        faas.change_max_items(0)


def test_setting_existing_key_does_not_trigger_eviction():
    """Updating an existing key should not count as a new item."""
    faas = FaaSCacheDict(max_items=2)
    faas["a"] = 1
    faas["b"] = 2
    # Update existing key
    faas["a"] = 100
    assert len(faas) == 2
    assert faas["a"] == 100
    assert faas["b"] == 2


def test_lru_order_maintained_after_get():
    """Getting an item should move it to the end (most recently used)."""
    faas = FaaSCacheDict(max_items=3)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    # Access "a" to make it most recently used
    _ = faas["a"]

    # Add new item - should evict "b" (oldest)
    faas["d"] = 4

    assert "a" in faas
    assert "b" not in faas
    assert "c" in faas
    assert "d" in faas


def test_setitem_with_all_expired_items_and_max_items():
    """Adding to cache where all items are expired should not raise KeyError."""
    import time

    faas = FaaSCacheDict(default_ttl=0.01, max_items=3)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    time.sleep(0.02)  # Let all items expire

    # This should NOT raise KeyError - expired items should be purged first
    faas["d"] = 4

    assert len(faas) == 1
    assert faas["d"] == 4


def test_setitem_with_some_expired_items_and_max_items():
    """Adding to full cache with some expired items should evict expired first."""
    import time

    faas = FaaSCacheDict(default_ttl=0.01, max_items=3)
    faas["a"] = 1
    faas["b"] = 2

    time.sleep(0.02)  # Let "a" and "b" expire

    # Add non-expiring item
    faas.set_ttl("c", None) if "c" in faas else None
    faas["c"] = 3
    faas.set_ttl("c", None)

    # Add another item - should purge expired "a" and "b", not evict "c"
    faas["d"] = 4

    assert "a" not in faas
    assert "b" not in faas
    assert "c" in faas
    assert "d" in faas
    assert len(faas) == 2
