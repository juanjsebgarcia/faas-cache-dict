from unittest.mock import Mock, call

from faas_cache_dict import FaaSCacheDict


def test_assert_callable_is_called():
    mock = Mock(return_value=None)

    faas = FaaSCacheDict(on_delete_callable=mock.delete, max_items=3)

    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4

    mock.delete.assert_called()
    mock.delete.assert_called_with("a", 1)

    faas["e"] = 5
    faas["a"] = 2
    mock.delete.assert_has_calls([call("a", 1), call("b", 2), call("c", 3)])


def test_assert_excepting_callable_does_not_break_faas():
    def uh_oh_bad_times_ahead(key, value):
        raise Exception("Silly mistakes happen")

    mock = Mock(return_value=None)
    mock.side_effect = uh_oh_bad_times_ahead

    faas = FaaSCacheDict(on_delete_callable=mock, max_items=2)

    faas["a"] = 1
    faas["b"] = 2

    assert len(faas) == 2

    faas["c"] = 3

    mock.assert_called()
    mock.assert_called_with("a", 1)
    assert len(faas) == 2


def test_non_terminal_deletes_do_not_hook():
    mock = Mock(return_value=None)

    faas = FaaSCacheDict(on_delete_callable=mock.delete, max_items=3)

    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["a"] = 1  # Make MRU

    mock.delete.assert_not_called()

    faas["e"] = 2

    mock.delete.assert_called_with("b", 2)


def test_pop_calls_on_delete_callable():
    mock = Mock(return_value=None)

    faas = FaaSCacheDict(on_delete_callable=mock.delete)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    assert len(faas) == 3
    mock.delete.assert_not_called()

    result = faas.pop("a")

    assert result == 1
    assert len(faas) == 2
    mock.delete.assert_called_once_with("a", 1)


def test_pop_with_default_does_not_call_on_delete_callable():
    mock = Mock(return_value=None)

    faas = FaaSCacheDict(on_delete_callable=mock.delete)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    assert len(faas) == 3

    result = faas.pop("nonexistent", "default_value")

    assert result == "default_value"
    assert len(faas) == 3
    mock.delete.assert_not_called()


def test_popitem_calls_on_delete_callable():
    mock = Mock(return_value=None)

    faas = FaaSCacheDict(on_delete_callable=mock.delete)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    assert len(faas) == 3
    mock.delete.assert_not_called()

    k, v = faas.popitem()

    assert k == "c"
    assert v == 3
    assert len(faas) == 2
    mock.delete.assert_called_once_with("c", 3)


def test_popitem_first_calls_on_delete_callable():
    mock = Mock(return_value=None)

    faas = FaaSCacheDict(on_delete_callable=mock.delete)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    assert len(faas) == 3

    k, v = faas.popitem(last=False)

    assert k == "a"
    assert v == 1
    assert len(faas) == 2
    mock.delete.assert_called_once_with("a", 1)


def test_on_delete_callable_with_none():
    """on_delete_callable=None should work correctly."""
    faas = FaaSCacheDict(on_delete_callable=None)
    faas["a"] = 1
    del faas["a"]  # Should not raise
    assert "a" not in faas


def test_on_delete_callable_with_lambda():
    """Lambda should work as on_delete_callable."""
    results = []
    faas = FaaSCacheDict(on_delete_callable=lambda k, v: results.append((k, v)))
    faas["a"] = 1
    del faas["a"]
    assert ("a", 1) in results


def test_on_delete_callable_exception_during_purge():
    """Exception in on_delete_callable during purge should not break cache."""
    import time

    def bad_callback(key, value):
        raise RuntimeError("Callback error")

    faas = FaaSCacheDict(default_ttl=0.01, on_delete_callable=bad_callback)
    faas["a"] = 1
    faas["b"] = 2

    time.sleep(0.02)

    # Manual purge should succeed despite callback errors
    faas._purge_expired()
    # Verify items were still removed
    assert len(list(super(FaaSCacheDict, faas).__iter__())) == 0


def test_on_delete_callable_with_non_callable_fails_silently():
    """Non-callable on_delete_callable should fail silently when deletion occurs."""
    # Note: Constructor doesn't validate, so it accepts anything
    faas = FaaSCacheDict(on_delete_callable="not a callable")
    faas["a"] = 1

    # Should not raise due to exception handling - just logs warning
    del faas["a"]
    assert "a" not in faas


def test_purge_calls_on_delete_callable():
    """purge() should call on_delete_callable for each item."""
    mock = Mock(return_value=None)

    faas = FaaSCacheDict(on_delete_callable=mock.delete)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3

    mock.delete.assert_not_called()

    faas.purge()

    assert mock.delete.call_count == 3
    assert len(faas) == 0


def test_lru_eviction_calls_on_delete_callable():
    """LRU eviction should call on_delete_callable."""
    deleted_items = []

    def on_delete(key, value):
        deleted_items.append((key, value))

    faas = FaaSCacheDict(max_items=3, on_delete_callable=on_delete)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4  # Should evict "a"

    assert ("a", 1) in deleted_items


def test_memory_eviction_calls_on_delete_callable():
    """Memory-based eviction should call on_delete_callable."""
    deleted_items = []

    def on_delete(key, value):
        deleted_items.append(key)

    faas = FaaSCacheDict(max_size_bytes="100K", on_delete_callable=on_delete)
    # Add items with large values that will exceed 100K
    for i in range(50):
        faas[f"key_{i}"] = "x" * 5000  # 5KB per item

    # Some items should have been evicted (50 * 5KB = 250KB > 100KB)
    assert len(deleted_items) > 0


def test_rapid_on_delete_callable_invocations():
    """Rapid deletions should not corrupt on_delete_callable execution."""
    call_count = {"count": 0}

    def on_delete(key, value):
        call_count["count"] += 1

    faas = FaaSCacheDict(on_delete_callable=on_delete)

    # Add and delete rapidly
    for i in range(100):
        faas[f"key_{i}"] = i

    for i in range(100):
        del faas[f"key_{i}"]

    assert call_count["count"] == 100
