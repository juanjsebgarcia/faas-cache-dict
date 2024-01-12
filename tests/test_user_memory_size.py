import pytest

from faas_cache_dict.constants import (
    BYTES_PER_GIBIBYTE,
    BYTES_PER_KIBIBYTE,
    BYTES_PER_MEBIBYTE,
)
from faas_cache_dict.faas_cache_dict import user_input_byte_size_to_bytes


def test_bytes_int_accepted():
    assert 10 == user_input_byte_size_to_bytes(10)
    assert 100000 == user_input_byte_size_to_bytes(100000)
    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes(0)
    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes(-1)
    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes(1.0)


def test_bytes_k_accepted():
    assert BYTES_PER_KIBIBYTE == user_input_byte_size_to_bytes("1K")
    assert BYTES_PER_KIBIBYTE == user_input_byte_size_to_bytes("1.0K")
    assert (BYTES_PER_KIBIBYTE * 1.5) == user_input_byte_size_to_bytes("1.5K")
    assert (BYTES_PER_KIBIBYTE * 100) == user_input_byte_size_to_bytes("100K")
    assert (BYTES_PER_KIBIBYTE * 1000) == user_input_byte_size_to_bytes("1000K")

    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes("K")
    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes("-1K")


def test_bytes_m_accepted():
    assert BYTES_PER_MEBIBYTE == user_input_byte_size_to_bytes("1M")
    assert BYTES_PER_MEBIBYTE == user_input_byte_size_to_bytes("1.0M")
    assert (BYTES_PER_MEBIBYTE * 1.5) == user_input_byte_size_to_bytes("1.5M")
    assert (BYTES_PER_MEBIBYTE * 100) == user_input_byte_size_to_bytes("100M")
    assert (BYTES_PER_MEBIBYTE * 1000) == user_input_byte_size_to_bytes("1000M")

    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes("M")
    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes("-1M")


def test_bytes_g_accepted():
    assert BYTES_PER_GIBIBYTE == user_input_byte_size_to_bytes("1G")
    assert BYTES_PER_GIBIBYTE == user_input_byte_size_to_bytes("1.0G")
    assert (BYTES_PER_GIBIBYTE * 1.5) == user_input_byte_size_to_bytes("1.5G")
    assert (BYTES_PER_GIBIBYTE * 100) == user_input_byte_size_to_bytes("100G")
    assert (BYTES_PER_GIBIBYTE * 1000) == user_input_byte_size_to_bytes("1000G")

    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes("G")
    with pytest.raises(ValueError):
        user_input_byte_size_to_bytes("-1G")
