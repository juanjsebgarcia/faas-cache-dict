import objsize

from .constants import BYTE_SIZE_CONVERSIONS
from .utils import _assert


def get_deep_byte_size(obj):
    return objsize.get_deep_size(obj)


def user_input_byte_size_to_bytes(user_bytes):
    """
    Convert the user input to integer bytes

    User input may be bytes directly or a suffixed string amount such as '128.0M'
    """
    _assert(isinstance(user_bytes, (int, str)), "Invalid byte size input")

    if isinstance(user_bytes, int):
        _assert(user_bytes > 0, "Byte size must be >0")
        return user_bytes

    _assert(
        user_bytes[-1].upper() in BYTE_SIZE_CONVERSIONS.keys(),
        "Unknown byte size suffix",
    )

    quantity = float(user_bytes[0:-1])

    _assert(quantity > 0, "Memory size must be >0")

    return BYTE_SIZE_CONVERSIONS[user_bytes[-1].upper()] * quantity
