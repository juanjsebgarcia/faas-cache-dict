"""
Performance regression tests for faas-cache-dict.

These lock in the O(n) write path: a write sizes only the entry it
touches and never re-scans the whole cache, so per-insert cost stays flat as the
cache grows.

`test_insert_sizes_only_one_entry_not_whole_cache` is the rigorous, deterministic
proof - it counts the work objsize does rather than wall-clock time, so it cannot
flake. The timing tests are illustrative and use generous, ratio-based thresholds
so they distinguish O(n) from O(n^2) without being flaky under load.
"""

import time
from unittest.mock import MagicMock

import faas_cache_dict.faas_cache_dict as fcd
from faas_cache_dict import FaaSCacheDict


def test_insert_sizes_only_one_entry_not_whole_cache(monkeypatch):
    """
    Deterministic proof of the fix: the number of bytes objsize traverses per
    insert must not grow with cache size.

    The old code re-measured the entire cache on every write (via
    get_deep_byte_size(self)), so an insert into a 2000-item cache traversed
    ~2000x the data of an insert into a near-empty one. Incremental sizing only
    measures the single entry being written, so per-insert work is flat.
    """
    traversed = {"bytes": 0}
    real_get_deep_byte_size = fcd.get_deep_byte_size

    def counting(obj, exclude=None):
        size = real_get_deep_byte_size(obj, exclude=exclude)
        traversed["bytes"] += size
        return size

    monkeypatch.setattr(fcd, "get_deep_byte_size", counting)

    # Only byte-limited caches size entries at all
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="500M")
    value = "v" * 500

    # One insert into a near-empty cache.
    faas["warmup"] = value
    traversed["bytes"] = 0
    faas["probe-small"] = value
    small = traversed["bytes"]

    # Grow the cache substantially, then probe one insert into the large cache.
    for i in range(2000):
        faas[i] = value
    traversed["bytes"] = 0
    faas["probe-large"] = value
    large = traversed["bytes"]

    assert small > 0
    assert large < small * 3, (
        f"per-insert sizing work grew {large / small:.0f}x with cache size "
        f"(small={small} bytes, large={large} bytes) - looks like a full-cache rescan"
    )
    faas.close()


def test_cache_without_byte_limit_never_deep_sizes(monkeypatch):
    """
    A cache with no byte limit must not walk its keys or values with objsize.

    Regression: every insert and removal deep-sized the entry even with no
    limit set. objsize follows every reference, so caching an object that
    reaches a logger, a test session or a MagicMock walked far more than the
    entry itself, or never terminated (a MagicMock spawns new children each
    time objsize probes it).
    """
    calls = {"count": 0}
    real_get_deep_byte_size = fcd.get_deep_byte_size

    def counting(obj, exclude=None):
        calls["count"] += 1
        return real_get_deep_byte_size(obj, exclude=exclude)

    monkeypatch.setattr(fcd, "get_deep_byte_size", counting)

    faas = FaaSCacheDict(60, None, 2, None, {"seed": 1})
    faas["a"] = 1
    faas["a"] = 2  # replace
    faas["b"] = 3  # evicts the LRU entry
    del faas["a"]
    faas.pop("b")
    faas["c"] = 4
    faas.purge()
    repr(faas)
    faas.close()

    assert calls["count"] == 0


def test_cache_without_byte_limit_accepts_unmeasurable_values():
    """
    Values objsize cannot measure, or would walk forever, are fine to cache
    when no byte limit is set.
    """
    mock = MagicMock()
    faas = FaaSCacheDict(default_ttl=60, max_items=1)
    faas["mock"] = mock
    assert faas["mock"] is mock
    faas["other"] = 1  # evicts the mock
    assert "mock" not in faas
    faas.close()


def test_enabling_byte_limit_later_seeds_running_total():
    """
    Turning a byte limit on after inserting into an unlimited cache must
    measure exactly, so later inserts enforce against a correct total.
    """
    faas = FaaSCacheDict(default_ttl=60)
    faas["a"] = "x" * 1000
    assert faas._self_byte_size == 0  # untracked while unlimited
    faas.change_byte_size("1M")
    assert faas._self_byte_size == faas.get_byte_size()
    faas.change_byte_size(None)
    assert faas._self_byte_size == 0
    assert "current_memory_bytes=None" in repr(faas)
    faas.close()


def _mean_insert_seconds(make, fill_to, sample=200):
    """Mean wall-clock seconds per insert into a cache already holding fill_to items."""
    faas = make()
    for i in range(fill_to):
        faas[("fill", i)] = i
    start = time.perf_counter()
    for i in range(sample):
        faas[("sample", i)] = i
    elapsed = time.perf_counter() - start
    faas.close()
    return elapsed / sample


def test_insert_throughput_is_flat_across_sizes():
    """
    Per-insert time stays roughly constant as the cache grows (O(1) amortised).

    The old O(n^2) path was ~20x slower at the larger size; a 6x ceiling cleanly
    separates O(n) from O(n^2) while tolerating timing noise.
    """
    small = _mean_insert_seconds(lambda: FaaSCacheDict(default_ttl=60), fill_to=200)
    large = _mean_insert_seconds(lambda: FaaSCacheDict(default_ttl=60), fill_to=4000)
    assert large < small * 6, f"per-insert time grew {large / small:.1f}x with cache size"


def test_byte_limited_insert_throughput_is_flat():
    """
    The byte-limited write path (DataTooLarge precheck + limit enforcement) must
    also stay flat - it was the slowest configuration before the fix.
    """

    def make():
        return FaaSCacheDict(default_ttl=60, max_size_bytes="500M")

    small = _mean_insert_seconds(make, fill_to=200)
    large = _mean_insert_seconds(make, fill_to=4000)
    assert large < small * 6, f"byte-limited per-insert time grew {large / small:.1f}x"
