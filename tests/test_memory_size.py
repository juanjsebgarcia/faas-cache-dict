import time
import uuid

import pytest

from faas_cache_dict.faas_cache_dict import (
    BYTES_PER_MEBIBYTE,
    DataTooLarge,
    FaaSCacheDict,
)

one_mb_text = open('tests/1_mebibyte.txt').read()


def load_with_mebibyte_of_data(faas, mb):
    faas[str(uuid.uuid4())] = one_mb_text * mb
    return faas


def test_max_size_set():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes='1M')
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE

    faas = FaaSCacheDict(default_ttl=60, max_size_bytes='2M')
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE * 2


def test_change_byte_size():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes='1M')
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE
    faas.change_byte_size('2M')
    assert faas._max_size_bytes == BYTES_PER_MEBIBYTE * 2
    faas = load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 1
    faas.change_byte_size('1M')
    assert len(faas) == 0


def test_shrink_to_fit_byte_size():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes='10M')
    faas = load_with_mebibyte_of_data(faas, 1)
    assert len(faas) == 1
    faas.change_byte_size('1M')
    assert len(faas) == 0


def test_get_byte_size():
    faas = FaaSCacheDict(default_ttl=60, max_size_bytes='10M')
    faas = load_with_mebibyte_of_data(faas, 1)
    assert faas.get_byte_size() > BYTES_PER_MEBIBYTE


def test_byte_size_set_purge_expired():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes='10M')
    original_size = faas.get_byte_size()
    faas['a'] = 1
    loaded_size = faas.get_byte_size()
    assert original_size < loaded_size
    time.sleep(1)
    faas._purge_expired()
    assert original_size < faas.get_byte_size() < loaded_size


def test_byte_size_set_delete():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes='10M')
    faas['a'] = 1
    loaded_size = faas.get_byte_size()
    del faas['a']
    assert faas.get_byte_size() < loaded_size


def test_byte_size_set_add():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes='10M')
    faas['a'] = 'a'
    loaded_size = faas.get_byte_size()
    faas['b'] = 'bb'
    assert loaded_size < faas.get_byte_size()


def test_byte_size_set_modification():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes='10M')
    faas['a'] = 'a'
    loaded_size = faas.get_byte_size()
    faas['a'] = 'aaa'
    assert loaded_size < faas.get_byte_size()


def test_raises_if_data_oversized():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes='2M')
    load_with_mebibyte_of_data(faas, 1)  # No error
    with pytest.raises(DataTooLarge):
        # Expected to raise as dict consumes some space
        faas['a'] = load_with_mebibyte_of_data(faas, 2)
    with pytest.raises(DataTooLarge):
        faas['a'] = load_with_mebibyte_of_data(faas, 3)


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
    faas.change_byte_size('1M')
    assert len(faas) == 0


def test_memory_size_then_none():
    faas = FaaSCacheDict(default_ttl=1, max_size_bytes='2M')
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
