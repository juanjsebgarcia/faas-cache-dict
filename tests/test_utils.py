"""
Tests for utility functions in faas-cache-dict.

Tests for _assert helper and get_deep_byte_size function.
"""

import pytest

from faas_cache_dict.size_utils import get_deep_byte_size
from faas_cache_dict.utils import _assert


class TestAssertFunction:
    """Tests for the _assert helper function."""

    def test_assert_raises_value_error_when_false(self):
        """_assert should raise ValueError when condition is False."""
        with pytest.raises(ValueError, match="test error"):
            _assert(False, "test error")

    def test_assert_passes_when_true(self):
        """_assert should not raise when condition is True."""
        _assert(True, "should not raise")  # Should not raise

    def test_assert_with_empty_message(self):
        """_assert should work with empty error message."""
        with pytest.raises(ValueError):
            _assert(False, "")

    def test_assert_with_no_message(self):
        """_assert should work with default empty message."""
        with pytest.raises(ValueError):
            _assert(False)

    def test_assert_truthy_values(self):
        """_assert should pass for truthy values."""
        _assert(1, "should not raise")
        _assert("non-empty", "should not raise")
        _assert([1, 2, 3], "should not raise")
        _assert({"key": "value"}, "should not raise")

    def test_assert_falsy_values_raise(self):
        """_assert should raise for falsy values."""
        with pytest.raises(ValueError):
            _assert(0, "zero is falsy")
        with pytest.raises(ValueError):
            _assert("", "empty string is falsy")
        with pytest.raises(ValueError):
            _assert([], "empty list is falsy")
        with pytest.raises(ValueError):
            _assert(None, "None is falsy")


class TestGetDeepByteSize:
    """Tests for the get_deep_byte_size function."""

    def test_get_deep_byte_size_returns_int(self):
        """get_deep_byte_size should return an integer."""
        result = get_deep_byte_size({"a": 1, "b": [1, 2, 3]})
        assert isinstance(result, int)
        assert result > 0

    def test_get_deep_byte_size_empty_dict(self):
        """get_deep_byte_size should handle empty dict."""
        result = get_deep_byte_size({})
        assert isinstance(result, int)
        assert result > 0  # Even empty dict has overhead

    def test_get_deep_byte_size_string(self):
        """get_deep_byte_size should handle strings."""
        small = get_deep_byte_size("small")
        large = get_deep_byte_size("this is a much larger string" * 100)
        assert large > small

    def test_get_deep_byte_size_nested_structure(self):
        """get_deep_byte_size should handle nested structures."""
        nested = {"level1": {"level2": {"level3": [1, 2, 3, {"level4": "deep"}]}}}
        result = get_deep_byte_size(nested)
        assert isinstance(result, int)
        assert result > 0

    def test_get_deep_byte_size_list(self):
        """get_deep_byte_size should handle lists."""
        empty_list = get_deep_byte_size([])
        small_list = get_deep_byte_size([1, 2, 3])
        large_list = get_deep_byte_size(list(range(1000)))

        assert empty_list < small_list < large_list

    def test_get_deep_byte_size_none(self):
        """get_deep_byte_size should handle None."""
        result = get_deep_byte_size(None)
        assert isinstance(result, int)
        # None has 0 size in objsize library
        assert result >= 0

    def test_get_deep_byte_size_integer(self):
        """get_deep_byte_size should handle integers."""
        result = get_deep_byte_size(42)
        assert isinstance(result, int)
        assert result > 0
