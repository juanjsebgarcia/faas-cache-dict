from faas_cache_dict.faas_cache_dict import FaaSCacheDict, get_deep_byte_size


def test_get_deep_byte_size_faas_dict():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes="1M", max_items=2)
    original_size = get_deep_byte_size(faas)
    faas["a"] = 1
    minimal_size = get_deep_byte_size(faas)
    assert original_size < minimal_size
    faas["b"] = 2
    assert original_size < get_deep_byte_size(faas)


def test_get_deep_byte_size_nested_faas_dict():
    top_level = FaaSCacheDict(default_ttl=60, max_size_bytes="1M", max_items=2)
    top_level_size = get_deep_byte_size(top_level)
    top_level["nested"] = FaaSCacheDict(
        default_ttl=60, max_size_bytes="1M", max_items=2
    )
    with_nested_size = get_deep_byte_size(top_level)
    assert with_nested_size > top_level_size
    top_level["nested"]["a"] = "a" * 10
    assert get_deep_byte_size(top_level) > with_nested_size
