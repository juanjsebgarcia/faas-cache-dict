def _assert(bool_, err_string=''):
    """
    Avoid using asserts in production code
    https://juangarcia.co.uk/python/python-smell-assert/
    """
    if not bool_:
        raise ValueError(err_string)
