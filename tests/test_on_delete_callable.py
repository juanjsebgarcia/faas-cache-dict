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
    import ipdb

    ipdb.set_trace()

    faas["a"] = 1  # Make MRU

    mock.delete.assert_not_called()

    faas["e"] = 2

    mock.delete.assert_called_with("b", 2)
