def _assert(bool_, err_string=''):
    """
    Avoid using asserts in production code
    https://juangarcia.co.uk/python/python-smell-assert/
    """
    try:
        assert bool_
    except AssertionError:
        raise ValueError(err_string)
