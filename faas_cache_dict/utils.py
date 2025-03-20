def _assert(bool_: bool, err_string: str = "") -> None:
    """
    Avoid using asserts in production code
    https://juangarcia.co.uk/python/python-smell-assert/
    """
    if not bool_:
        raise ValueError(err_string)
