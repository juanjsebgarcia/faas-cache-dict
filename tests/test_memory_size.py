import time
import uuid

import pytest

from faas_cache_dict.constants import BYTES_PER_MEBIBYTE
from faas_cache_dict.faas_cache_dict import DataTooLarge, FaaSCacheDict
from faas_cache_dict.size_utils import get_deep_byte_size

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
    # 3M of headroom so two ~1MB items comfortably fit regardless of whether the
    # byte total counts a shared value once (exact) or per entry (the running
    # total). This exercises change_byte_size(None) disabling the limit, not the
    # exact-vs-conservative sizing boundary.
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes="3M")
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


def test_empty_cache_byte_size_excludes_thread_and_lock():
    """
    The reported cache size should reflect stored data, not library plumbing.

    Regression: objsize traversed the background purge thread (~135 KB of
    reachable interpreter state) and the lock, so an empty cache reported a
    wildly inflated size.
    """
    faas = FaaSCacheDict()
    assert faas.get_byte_size() < 10_000


def test_small_max_size_bytes_allows_insert():
    """
    A small byte limit must not be entirely consumed by library plumbing.

    Regression: because the purge thread (~135 KB) counted toward the cache
    size, any limit below that made the first insert evict everything - including
    the item itself - and raise KeyError('EmptyCache') out of __setitem__.
    """
    faas = FaaSCacheDict(max_size_bytes="1K")
    faas["a"] = 1
    assert faas["a"] == 1
    assert len(faas) == 1


def test_small_max_size_bytes_never_raises_keyerror():
    """Inserting into a small cache must never surface KeyError('EmptyCache')."""
    faas = FaaSCacheDict(max_size_bytes="2K")
    for i in range(5):
        faas[f"k{i}"] = i
    assert len(faas) >= 1


def test_nested_cache_size_excludes_nested_thread():
    """A nested FaaSCacheDict value must not add its own ~135 KB thread graph."""
    outer = FaaSCacheDict()
    base = outer.get_byte_size()
    outer["nested"] = FaaSCacheDict()
    delta = outer.get_byte_size() - base
    assert delta < 10_000


def test_setitem_oversized_with_overhead_raises_not_silent():
    """
    An item that fits by raw key+value size but not once cache overhead is
    counted must raise DataTooLarge - it must not be silently dropped (finding #2).

    This exercises the authoritative enforcement path: the cheap precheck passes
    (the item is smaller than the limit), but the item cannot fit once cache
    overhead is included, so _shrink_to_fit_byte_size must reject it.
    """
    probe = FaaSCacheDict()
    overhead = probe.get_byte_size()

    value = "x" * 600
    item_cost = get_deep_byte_size("a") + get_deep_byte_size((None, value))
    # A budget the raw item fits within, but not alongside the cache overhead.
    max_bytes = item_cost + overhead // 2
    assert item_cost <= max_bytes  # would pass the cheap precheck

    faas = FaaSCacheDict(max_size_bytes=max_bytes)
    with pytest.raises(DataTooLarge):
        faas["a"] = value
    # The item must not be silently present either
    assert "a" not in faas
    assert len(faas) == 0


def test_setitem_obviously_oversized_preserves_existing_items():
    """
    A value larger than the whole budget raises DataTooLarge without evicting
    existing items to make room for something that can never fit (no collateral).
    """
    faas = FaaSCacheDict(max_size_bytes="4K")
    faas["keep"] = "small"
    with pytest.raises(DataTooLarge):
        faas["huge"] = "y" * 10_000
    assert faas["keep"] == "small"
    assert "huge" not in faas


def test_setitem_evicts_oldest_to_fit_new_item():
    """A new item that fits is kept; the oldest items are evicted to make room."""
    faas = FaaSCacheDict(max_size_bytes="4K")
    for key in ("a", "b", "c", "d", "e", "f"):
        # Distinct runtime strings (a variable multiplicand is not constant-folded,
        # so objsize counts each value separately).
        faas[key] = key * 800
    # The newest item is retained and the cache stays within budget...
    assert "f" in faas
    assert faas.get_byte_size() <= 4 * 1024
    # ...while the oldest item was evicted (LRU), not the newest.
    assert "a" not in faas


def test_cached_byte_size_fresh_after_purge_expired():
    """
    After _purge_expired removes items, the cached _self_byte_size must reflect
    the reduced size, not a stale pre-purge value (finding #6).
    """
    faas = FaaSCacheDict(default_ttl=0.05, max_size_bytes="10M")
    faas["big"] = "x" * 200_000
    assert faas._self_byte_size > 100_000  # cached reflects the big item
    time.sleep(0.1)
    faas._purge_expired()
    assert faas._self_byte_size < 10_000  # cached now reflects the empty cache


def test_cached_byte_size_fresh_after_clear():
    """purge()/clear() must update the cached size, not leave it stale (finding #6)."""
    faas = FaaSCacheDict(max_size_bytes="10M")
    faas["k"] = "y" * 200_000
    assert faas._self_byte_size > 100_000
    faas.clear()
    assert faas._self_byte_size < 10_000


def test_cached_byte_size_set_at_construction():
    """__init__ must compute the cached size, not leave it at 0 (finding #6)."""
    faas = FaaSCacheDict(None, "10M", None, None, {"a": 1, "b": 2})
    assert faas._self_byte_size > 0


def test_get_byte_size_skip_purge_does_not_remove_expired():
    """
    get_byte_size(skip_purge=True) recomputes WITHOUT purging expired items - the
    recompute must not leak a purge through __sizeof__ (finding #6).
    """
    faas = FaaSCacheDict(default_ttl=0.02)
    faas.stop_purge_thread()  # isolate from the background purge thread
    faas["a"] = 1
    time.sleep(0.05)
    raw_before = len(list(super(FaaSCacheDict, faas).__iter__()))
    size = faas.get_byte_size(skip_purge=True)
    raw_after = len(list(super(FaaSCacheDict, faas).__iter__()))
    assert raw_before == 1
    assert raw_after == 1  # the expired item was NOT purged
    assert size > 0


def test_byte_size_counts_shared_values_per_entry():
    """
    The running byte total maintained on the hot path counts each entry
    independently, so two entries sharing one large object are counted twice -
    conservative for a size limit. This is the documented trade-off of
    incremental sizing (finding #8); get_byte_size() gives an exact measure.
    """
    big = "x" * 100_000
    faas = FaaSCacheDict(max_size_bytes="10M")
    faas["a"] = big
    exact_with_one = faas.get_byte_size()  # exact: counts `big` once, resyncs total
    faas["b"] = big  # same object
    # The running total grew by ~another 100K even though an exact measure would
    # dedup the shared object.
    assert faas._self_byte_size - exact_with_one > 50_000


def _exact_size_without_resync(faas):
    """
    Exact deep size of the cache, measured without get_byte_size (which would
    resync the running total under test) and without purging.
    """
    with faas._locked():
        faas._suppress_sizeof_purge = True
        try:
            return get_deep_byte_size(faas, exclude=[faas._pending_on_delete])
        finally:
            faas._suppress_sizeof_purge = False


def test_running_byte_total_tracks_dict_storage():
    """
    The running byte total must include the dict's own storage, not just the
    entries, through inserts and deletes alike.

    Regression: only each entry's key and value were counted, missing the
    hash table and the OrderedDict's per-entry ordering nodes. For small
    entries the total was ~50% low, and after mass deletes ~95% low.
    """
    faas = FaaSCacheDict(max_size_bytes="10M")
    faas.stop_purge_thread()
    # Keys and values are distinct ints beyond the small-int cache, so no object
    # is shared (sharing makes the per-entry total over-count, a separate issue)
    for i in range(1000, 6000):
        faas[i] = i * 3
    exact = _exact_size_without_resync(faas)
    assert abs(faas._self_byte_size - exact) <= exact * 0.02

    for i in range(1000, 5900):
        del faas[i]
    exact = _exact_size_without_resync(faas)
    assert abs(faas._self_byte_size - exact) <= exact * 0.02
    faas.close()


def test_insert_cannot_empty_cache_when_overhead_exceeds_low_water():
    """
    When fixed overhead sits above the low-water mark, an insert that crosses
    the limit must evict only a few entries, not empty the cache trying to
    reach a target it can never reach.

    The dict's table does not shrink on delete, so a cache that grew large and
    then had its limit lowered keeps most of the new limit as fixed overhead.
    """
    faas = FaaSCacheDict(max_size_bytes="10M")
    faas.stop_purge_thread()
    for i in range(1000, 6000):
        faas[i] = i * 3
    for i in range(1000, 5950):  # 50 entries left; the table stays large
        del faas[i]
    faas.change_byte_size(int(faas.get_byte_size() * 1.005))
    assert (
        faas._shrink_low_water * faas._max_size_bytes
        < super(FaaSCacheDict, faas).__sizeof__()
    )  # the precondition: overhead alone is above low-water

    for i in range(10_000, 10_020):
        faas[i] = i * 3
    # A pass may evict up to a quarter of the entries, but must not empty it
    assert len(faas) >= 25
    assert faas.get_byte_size() <= faas._max_size_bytes
    faas.close()


def test_insert_cannot_empty_cache_when_fixed_overhead_tips_over_low_water():
    """
    When the size eviction cannot reduce (the instance's attributes plus the
    dict's table and node index) sits between the low-water mark and the
    limit, an overflowing insert must evict only part of the entries.

    Regression: the low-water target was then below anything a non-empty
    cache could reach, so every overflowing insert evicted all entries but
    the new one (and an earlier fix that looked only at the dict's storage,
    not the instance's own attributes, missed this case).
    """
    faas = FaaSCacheDict(max_size_bytes="10M")
    faas.stop_purge_thread()
    for i in range(1000, 1200):
        faas[i] = i * 3
    for i in range(1000, 1190):  # 10 entries left; the table stays grown
        del faas[i]
    limit = int(faas._irreducible_byte_size() / 0.93)
    faas.change_byte_size(limit)
    irreducible = faas._irreducible_byte_size()
    assert limit * faas._shrink_low_water < irreducible < limit  # the precondition

    sizes = []
    for i in range(5000, 5030):
        faas[i] = i * 3
        sizes.append(len(faas))
    assert min(sizes[1:]) >= 5, sizes
    assert faas.get_byte_size() <= limit
    faas.close()


def test_irreducible_size_ignores_entries_reached_through_bound_hook():
    """
    An on_delete_callable bound to an object that holds the cache must not
    make every entry look irreducible.

    Regression: the irreducible-overhead measure walked from the cache's
    attributes through the bound method to its owner, back to the cache and
    into every entry, so the eviction cap applied on every pass.
    """

    class Service:
        def __init__(self):
            self.cache = FaaSCacheDict(
                max_size_bytes="10M", on_delete_callable=self.on_evict
            )
            self.cache.stop_purge_thread()

        def on_evict(self, key, value):
            pass

    service = Service()
    for i in range(1000, 3000):
        service.cache[i] = "v" * 50
    irreducible = service.cache._irreducible_byte_size()
    assert irreducible < service.cache.get_byte_size() * 0.5
    # Evicting every entry would leave about this much, measured directly
    for i in range(1000, 3000):
        del service.cache[i]
    assert abs(irreducible - service.cache.get_byte_size()) <= 64
    service.cache.close()


@pytest.mark.parametrize("remove", ["pop", "popitem"])
def test_running_byte_total_exact_through_pop_and_popitem(remove):
    """
    pop() and popitem() must keep the running byte total exact, as del does.

    Regression: both removed entries without going through __delitem__, so
    they skipped the container-size tracking and the total drifted ~70% high
    after mass removals.
    """
    faas = FaaSCacheDict(max_size_bytes="10M")
    faas.stop_purge_thread()
    for i in range(1000, 2500):
        faas[i] = i * 3
    for i in range(1000, 2450):
        if remove == "pop":
            faas.pop(i)
        else:
            faas.popitem(last=False)
    exact = _exact_size_without_resync(faas)
    assert abs(faas._self_byte_size - exact) <= exact * 0.02
    faas.close()


def test_small_entries_never_exceed_byte_limit():
    """
    A cache of many small entries must stay within its byte limit after every
    insert, not only after an exact resync.

    Regression: the undercounted running total let such a cache reach ~1.7x
    its limit before enforcement noticed.
    """
    limit = 64 * 1024
    faas = FaaSCacheDict(max_size_bytes=limit)
    faas.stop_purge_thread()
    for i in range(1000, 2500):
        faas[i] = i * 3
        assert _exact_size_without_resync(faas) <= limit, f"over limit at key {i}"
    faas.close()


def test_byte_limit_survives_in_place_value_mutation():
    """
    A value mutated in place after insertion and then removed must not corrupt
    the running byte total enough to disable enforcement.

    Regression: removal recomputed the entry's (now larger) size, so a
    grown-then-removed value over-subtracted and drove _self_byte_size negative,
    after which the `> max_size_bytes` check never fired and the byte limit
    stopped working entirely.
    """
    faas = FaaSCacheDict(max_size_bytes="1M")
    # Churn: insert a value, grow it in place, delete it (over-subtracts).
    for i in range(15):
        v = []
        faas[f"tmp{i}"] = v
        v.extend(range(40000))
        del faas[f"tmp{i}"]
    # The running total must never go negative (which disabled enforcement).
    assert faas._self_byte_size >= 0
    # The byte limit must still be enforced when real data is added.
    for i in range(80):
        faas[("real", i)] = ("x" * 50000) + str(i)  # ~4MB of distinct data
    actual = get_deep_byte_size(faas)  # exact measure; does not resync the total
    assert actual <= 2 * 1024 * 1024, f"byte limit not enforced: {actual} bytes"


def test_byte_total_clamped_when_replacing_mutated_value():
    """Replacing a value mutated in place must not drive the total negative either."""
    faas = FaaSCacheDict(max_size_bytes="10M")
    v = [1]
    faas["k"] = v
    v.extend(range(50000))  # grow in place after insertion
    faas["k"] = "small"  # replace: the old (grown) contribution is over-subtracted
    assert faas._self_byte_size >= 0


def test_byte_limit_is_a_hard_exact_ceiling_at_capacity():
    """
    Filling past the limit with distinct values must leave the EXACT size at or
    under the limit - enforcement re-measures exactly when at capacity, so the
    ceiling is hard (not a conservative estimate).
    """
    faas = FaaSCacheDict(max_size_bytes="200K")
    for i in range(100):
        faas[i] = ("x" * 5000) + str(i)  # distinct ~5KB values, ~500K total
    assert get_deep_byte_size(faas) <= 200 * 1024


def test_shared_value_items_not_evicted_when_they_fit():
    """
    Entries sharing one large object that fits the limit must NOT be evicted:
    enforcement uses an exact (deduped) measure, so the per-entry over-count of
    the running total never discards a fitting item.
    """
    faas = FaaSCacheDict(max_size_bytes="1M")
    shared = "s" * 300_000  # ~300KB; all entries reference the same object
    for k in ("a", "b", "c", "d"):
        faas[k] = shared
    assert len(faas) == 4
    assert get_deep_byte_size(faas) <= 1024 * 1024


def test_in_place_growth_enforced_on_next_sweep():
    """
    A value grown in place is invisible to the O(1) hot path, but the exact
    enforcement sweep (run by the background thread each cycle) catches it. Called
    directly here to avoid a timing-dependent sleep.
    """
    faas = FaaSCacheDict(max_size_bytes="500K")
    v = list(range(100))
    faas["k"] = v
    v.extend(range(200000))  # grow in place, well past the 500K limit
    faas._shrink_to_fit_byte_size()  # what the background thread runs each cycle
    assert get_deep_byte_size(faas) <= 500 * 1024


def test_failed_oversize_update_preserves_existing_value():
    """
    Updating an existing key with a value that cannot fit must raise
    DataTooLarge AND leave the previously stored value intact.

    Regression: the old entry was replaced before byte-limit enforcement ran,
    so a failed update destroyed it. Its neighbours, evicted while trying to
    make room, must come back too, in their original order. The updated key's
    LRU recency is refreshed rather than restored, so it ends up newest.
    """
    faas = FaaSCacheDict()
    faas["a"] = 1
    faas["k"] = "small"
    faas["b"] = 2
    seeded_size = faas.get_byte_size()

    big = "x" * 800
    big_entry_size = get_deep_byte_size("k") + get_deep_byte_size((None, big))
    # The oversized value fits alone (passing the cheap up-front reject) but
    # not together with the cache's structural overhead, even once every other
    # entry has been evicted.
    faas.change_byte_size(seeded_size + big_entry_size // 2)

    with pytest.raises(DataTooLarge):
        faas["k"] = big

    # The previous value and its neighbours all survive the failed update
    assert faas.keys() == ["a", "b", "k"]
    assert faas["k"] == "small"
    assert faas["a"] == 1
    assert faas["b"] == 2
    faas.close()


def test_rejected_insert_keeps_existing_items_and_skips_their_hooks():
    """
    An insert rejected with DataTooLarge must leave the cache exactly as it
    was, and must not fire on_delete for entries it evicted along the way.

    Regression: the up-front check ignores the cache's own overhead, so an item
    that passes it but cannot fit evicted every older entry (firing their
    hooks) before DataTooLarge was raised, emptying the cache.
    """
    fired = []
    faas = FaaSCacheDict(
        max_size_bytes=4000,
        on_delete_callable=lambda key, value: fired.append(key),
    )
    for i in range(5):
        faas[i] = i

    with pytest.raises(DataTooLarge):
        faas["big"] = "y" * 3800

    assert faas.keys() == [0, 1, 2, 3, 4]
    assert fired == []
    # The running total was resynced, so normal inserts still work
    faas["small"] = "ok"
    assert faas["small"] == "ok"
    faas.close()


def test_rejected_insert_at_item_capacity_keeps_evicted_item():
    """
    At max_items capacity an insert evicts the LRU entry before byte-limit
    enforcement runs; if the insert is then rejected, that entry comes back.
    """
    fired = []
    faas = FaaSCacheDict(
        max_size_bytes=4000,
        max_items=3,
        on_delete_callable=lambda key, value: fired.append(key),
    )
    for i in range(3):
        faas[i] = i

    with pytest.raises(DataTooLarge):
        faas["big"] = "y" * 3800

    assert faas.keys() == [0, 1, 2]
    assert fired == []
    # A real eviction afterwards still fires its hook as normal
    faas[3] = 3
    assert faas.keys() == [1, 2, 3]
    assert fired == [0]
    faas.close()
