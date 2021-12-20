from faas_cache_dict.faas_cache_dict import FaaSCacheDict, get_deep_byte_size


def test_get_deep_byte_size_faas_dict():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes='1M', max_items=2)
    original_size = get_deep_byte_size(faas)
    faas['a'] = 1
    minimal_size = get_deep_byte_size(faas)
    assert original_size < minimal_size
    faas['b'] = 2
    assert original_size < get_deep_byte_size(faas)


def test_get_deep_byte_size_nested_faas_dict():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes='1M', max_items=2)
    original_size = get_deep_byte_size(faas)
    faas['nested'] = FaaSCacheDict(default_ttl=60, max_size_bytes='1M', max_items=2)
    nested_size = get_deep_byte_size(faas)
    assert nested_size > (original_size * 2)
    faas['nested']['a'] = 'a' * 10
    assert get_deep_byte_size(faas) > nested_size
