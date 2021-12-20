import sys
import time
from collections import OrderedDict
from threading import RLock

__all__ = ['FaaSCacheDict']

BYTES_PER_MEBIBYTE = 1048576


class FaaSCacheDict(OrderedDict):
    """
    Python Dictionary with TTL, max size and max length constraints
    """

    def __init__(
        self, default_ttl=None, max_size_mb=None, max_items=sys.maxsize, *args, **kwargs
    ):
        """
        :param default_ttl: (int|float) optional: Default object TTL in seconds
        :param max_size_mb: (int) optional: Max mebibyte size of cache
        :param max_items: (int) optional: Max length/count of items in cache
        :param args: (any) OrderedDict args
        :param kwargs: (any) OrderedDict kwargs
        """
        if sys.version_info < (3, 7):
            raise SystemError('Python 3.7 or newer required.')

        # CACHE TTL
        _assert(
            isinstance(default_ttl, int) or (default_ttl is None), 'Invalid TTL config'
        )
        if default_ttl:
            _assert(default_ttl >= 0, 'TTL must be >=0')
        self.default_ttl = default_ttl

        # CACHE MEMORY SIZE
        _assert(
            isinstance(max_size_mb, int) or (max_size_mb is None), 'Invalid byte size'
        )
        if max_size_mb:
            _assert(max_size_mb > 0, 'Byte size must be >0')
        self._max_size_mb = max_size_mb
        self._max_size_bytes = None
        if max_size_mb:
            self._max_size_bytes = mebibytes_to_bytes(max_size_mb)
        self._self_byte_size = 0

        # CACHE LENGTH
        _assert(
            isinstance(max_items, int) or (max_items is None), 'Invalid max items limit'
        )
        if max_items:
            _assert(max_items > 0, 'Max items limit must >0')
        self._max_items = max_items

        self._lock = RLock()
        super().__init__()
        self.update(*args, **kwargs)
        self._set_self_byte_size()

    def __getitem__(self, key):
        with self._lock:
            if self.is_expired(key):
                self.__delitem__(key)
                raise KeyError
            value_with_expiry = super().__getitem__(key)
            self._push_to_queue_end(key, value_with_expiry)
            return value_with_expiry[1]

    def __setitem__(self, key, value, override_ttl=None):
        if self._max_size_bytes:
            if (
                get_deep_byte_size(key) + get_deep_byte_size(value)
            ) > self._max_size_bytes:
                raise DataTooLarge

        with self._lock:
            if override_ttl:
                expire = override_ttl
            elif self.default_ttl is None:
                expire = None
            else:
                expire = time.time() + self.default_ttl

            if self._max_items:
                while self._max_items <= self.__len__():
                    self._pop_oldest_item()

            super().__setitem__(key, (expire, value))
            self._shrink_to_fit_byte_size()
            self._set_self_byte_size()

    def __delitem__(self, key):
        with self._lock:
            super().__delitem__(key)
            self._set_self_byte_size()

    def __iter__(self):
        """Yield non-expired keys, without purging the expired ones"""
        with self._lock:
            for key in super().__iter__():
                if not self.is_expired(key):
                    yield key

    def __len__(self):
        with self._lock:
            self._purge_expired()
            return super().__len__()

    def __repr__(self):
        return '<FaaSCacheDict@{:#08x}; ttl={}, max_mb={}, max_items={}, length={}>'.format(
            id(self), self.default_ttl, self._max_size_mb, self._max_items, len(self),
        )

    ###
    # Dict functions
    ###
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self, is_calculating_size=False):
        with self._lock:
            self._purge_expired(is_calculating_size)
            return list(super().keys())

    def items(self, is_calculating_size=False):
        with self._lock:
            self._purge_expired(is_calculating_size)
            return [(k, v[1]) for (k, v) in super().items()]

    def values(self, is_calculating_size=False):
        with self._lock:
            self._purge_expired(is_calculating_size)
            return [v[1] for v in super().values()]

    ###
    # TTL functions
    ###
    def get_ttl(self, key, now=None):
        """Return remaining TTL for a key"""
        if now is None:
            now = time.time()
        with self._lock:
            expire, _value = super().__getitem__(key)
            return expire - now

    def set_ttl(self, key, ttl, now=None):
        """Set TTL for the given key"""
        if now is None:
            now = time.time()
        with self._lock:
            # Set new TTL and reset to bottom of queue (MRU)
            value = self.__getitem__(key)
            self.__delitem__(key)
            super().__setitem__(key, (now + ttl, value))

    def expire_at(self, key, timestamp):
        """Set the key expire timestamp (epoch seconds - ie `time.time()`)"""
        with self._lock:
            value = self.__getitem__(key)
            self.__delitem__(key)
            super().__setitem__(key, (timestamp, value))

    def is_expired(self, key, now=None):
        """Check if key has expired, and return it if so"""
        with self._lock:
            if now is None:
                now = time.time()

            expire, _value = super().__getitem__(key)

            if expire:
                if expire < now:
                    return key

    def _purge_expired(self, is_calculating_size=False):
        """Iterate through all cache items and prune all expired"""
        _keys = list(super().__iter__())
        _remove = [key for key in _keys if self.is_expired(key)]  # noqa
        [self.__delitem__(key) for key in _remove]
        if not is_calculating_size:
            self._set_self_byte_size()

    ###
    # Memory size functions
    ###
    def get_byte_size(self):
        """Get self size in bytes"""
        return get_deep_byte_size(self)

    def change_mb_size(self, max_size_mb):
        """Set new max MB size and delete objects if required"""
        with self._lock:
            self._max_size_mb = max_size_mb
            self._max_size_bytes = None
            if max_size_mb:
                self._max_size_bytes = mebibytes_to_bytes(max_size_mb)
            self._shrink_to_fit_byte_size()
            self._set_self_byte_size()

    def _set_self_byte_size(self):
        self._self_byte_size = self.get_byte_size()

    def _shrink_to_fit_byte_size(self):
        with self._lock:
            self._purge_expired()
            if self._max_size_bytes:
                while self.get_byte_size() > self._max_size_bytes:
                    self._pop_oldest_item()

    ###
    # LRU functions
    ###
    def _push_to_queue_end(self, key, value_with_expiry):
        """Reset the item to the end of the queue (MRU)"""
        self.__delitem__(key)
        self.__setitem__(key, value_with_expiry[1], override_ttl=value_with_expiry[0])

    def _pop_oldest_item(self):
        _keys = list(super().__iter__())
        self.__delitem__(_keys[0])


class DataTooLarge(ValueError):
    """
    Raised if the data being added exceeds the dicts limit

    This being raised means that it was not possible to store the data within the
    user set max size constraint of the dict
    """

    pass


def mebibytes_to_bytes(mb):
    return mb * BYTES_PER_MEBIBYTE


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


def _assert(bool_, err_string=''):
    """
    Avoid using asserts in production code
    https://juangarcia.co.uk/python/python-smell-assert/
    """
    try:
        assert bool_
    except AssertionError:
        raise ValueError(err_string)
