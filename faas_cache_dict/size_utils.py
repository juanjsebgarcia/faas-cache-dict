import math
import threading
from typing import Any

import objsize

from .constants import BYTE_SIZE_CONVERSIONS
from .utils import _assert, _is_int

# objsize traverses an object's entire reference graph. For a FaaSCacheDict that
# graph reaches the background purge Thread and the RLock, whose reachable
# interpreter/module state weighs ~135 KB - unrelated to the cached data and
# unstable between runs. Excluding them by type keeps the measurement focused on
# stored data, and means nested FaaSCacheDict values are handled the same way.
_SIZE_EXCLUDED_TYPES = (threading.Thread, type(threading.RLock()))


def _cache_size_filter(obj: Any) -> bool:
    """
    objsize traversal filter.

    Returns False (skip object and its subtree) for the cache's background thread
    and lock, otherwise defers to objsize's default filter which already excludes
    shared objects such as types, modules and functions.
    """
    if isinstance(obj, _SIZE_EXCLUDED_TYPES):
        return False
    return objsize.default_object_filter(obj)


def get_deep_byte_size(obj: Any) -> int:
    """
    Get the deep byte size of an object, this uses `objsize` to determine an accurate byte size
    of the object in system memory.

    The cache's background purge thread and lock are excluded from the traversal,
    so the result reflects the cached data rather than fixed library overhead
    (objsize would otherwise attribute ~135 KB of interpreter state to them).
    """
    return objsize.get_deep_size(obj, filter_func=_cache_size_filter)


def user_input_byte_size_to_bytes(user_bytes: int | str) -> int:
    """
    Convert the user input to integer bytes

    User input may be bytes directly or a suffixed string amount such as '128.0M'
    """
    _assert(
        isinstance(user_bytes, str) or _is_int(user_bytes),
        "Invalid byte size input",
    )

    if _is_int(user_bytes):
        _assert(user_bytes > 0, "Byte size must be >0")
        return user_bytes

    # A valid suffixed string needs at least one quantity character plus a suffix.
    _assert(len(user_bytes) >= 2, "Invalid byte size")
    _assert(
        user_bytes[-1].upper() in BYTE_SIZE_CONVERSIONS.keys(),
        "Unknown byte size suffix",
    )

    try:
        quantity = float(user_bytes[0:-1])
    except ValueError:
        raise ValueError("Invalid byte size") from None

    _assert(math.isfinite(quantity), "Byte size must be finite")
    _assert(quantity > 0, "Memory size must be >0")

    result = int(BYTE_SIZE_CONVERSIONS[user_bytes[-1].upper()] * quantity)
    # Quantities small enough to round down to zero bytes are not a usable limit.
    _assert(result > 0, "Byte size must be >0")
    return result
