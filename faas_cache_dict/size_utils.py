import sys


def get_deep_byte_size(obj, seen=None):
    """
    Recursively dive into an objects contents to discover true memory size.
    """
    if seen is None:
        seen = set()  # prevents self-referential objects causing an infinite loop

    if id(obj) in seen:
        return 0

    seen.add(id(obj))

    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        size += sum(
            get_deep_byte_size(v, seen) for v in obj.values(is_calculating_size=True)
        )
        size += sum(
            get_deep_byte_size(k, seen) for k in obj.keys(is_calculating_size=True)
        )

    elif hasattr(obj, '__dict__'):
        size += get_deep_byte_size(obj.__dict__, seen)

    elif hasattr(obj, '__iter__') and not (isinstance(obj, (str, bytes, bytearray))):
        size += sum(get_deep_byte_size(i, seen) for i in obj)

    return size
