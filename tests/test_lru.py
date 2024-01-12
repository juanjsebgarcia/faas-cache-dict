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


def test_pop_oldest_item():
    faas = FaaSCacheDict(max_items=4)
    faas["a"] = 1
    faas["b"] = 2
    faas["c"] = 3
    faas["d"] = 4

    assert list(faas.keys())[0] == "a"
    faas._pop_oldest_item()
    assert list(faas.keys())[0] == "b"
