from typing import Any


def _assert(bool_: bool, err_string: str = "") -> None:
    """
    Avoid using asserts in production code
    https://juangarcia.co.uk/python/python-smell-assert/
    """
    if not bool_:
        raise ValueError(err_string)


def _is_number(value: Any) -> bool:
    """
    True if value is an int or float, but not a bool.

    bool subclasses int in Python, so isinstance(True, int) is True; numeric
    config such as TTLs and timestamps must reject booleans explicitly.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_int(value: Any) -> bool:
    """
    True if value is an int, but not a bool.

    Excludes True/False where a genuine integer (max item count, raw byte size)
    is required, since bool subclasses int.
    """
    return isinstance(value, int) and not isinstance(value, bool)
