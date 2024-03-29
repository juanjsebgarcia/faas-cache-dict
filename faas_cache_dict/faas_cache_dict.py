import sys
import time
from collections import OrderedDict
from threading import RLock

from .exceptions import DataTooLarge
from .size_utils import get_deep_byte_size, user_input_byte_size_to_bytes
from .utils import _assert

__all__ = ["FaaSCacheDict"]


class FaaSCacheDict(OrderedDict):
    """
    Python Dictionary with TTL, max size and max length constraints
    """

    def __init__(
        self,
        default_ttl=None,
        max_size_bytes=None,
        max_items=sys.maxsize,
        on_delete_callable=None,
        *args,
        **kwargs,
    ):
        """
        :param default_ttl: (int|float) optional: Default object TTL in seconds
        :param max_size_bytes: (int|str) optional: Max byte size of cache (1024 or '1K')
        :param max_items: (int) optional: Max length/count of items in cache
        :param on_delete_callable (callable) optional: Hook which is called on object deletion
        :param args: (any) OrderedDict args
        :param kwargs: (any) OrderedDict kwargs
        """
        # CACHE TTL
        _assert(
            isinstance(default_ttl, (int, float)) or (default_ttl is None),
            "Invalid TTL config",
        )
        if default_ttl:
            _assert(default_ttl >= 0, "TTL must be >=0")
        self.default_ttl = default_ttl

        # CACHE MEMORY SIZE
        _assert(
            isinstance(max_size_bytes, (int, str)) or (max_size_bytes is None),
            "Invalid byte size",
        )
        self._max_size_user = max_size_bytes
        self._max_size_bytes = None
        if self._max_size_user:
            self._max_size_bytes = user_input_byte_size_to_bytes(self._max_size_user)
        self._self_byte_size = 0

        # CACHE LENGTH
        _assert(
            isinstance(max_items, int) or (max_items is None), "Invalid max items limit"
        )
        if max_items is not None:
            _assert(max_items > 0, "Max items limit must >0")
        self._max_items = max_items

        # Lifecycle callables
        self.on_delete_callable = on_delete_callable

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
            super().move_to_end(key)
            return value_with_expiry[1]

    def __setitem__(self, key, value, override_ttl=None):
        if self._max_size_bytes:
            if (
                get_deep_byte_size(key) + get_deep_byte_size(value)
            ) > self._max_size_bytes:
                raise DataTooLarge

        with self._lock:
            expire = None
            if override_ttl:
                expire = override_ttl
            elif self.default_ttl:
                expire = time.time() + self.default_ttl

            if self._max_items:
                if (
                    key not in self.keys()
                ):  # If refreshing an existing key then size remains constant
                    while self._max_items <= self.__len__():
                        self.delete_oldest_item()

            super().__setitem__(key, (expire, value))
            super().move_to_end(key)
            self._shrink_to_fit_byte_size()

    def __delitem__(self, key, is_terminal=True, ignore_missing=False):
        with self._lock:
            try:
                if self.on_delete_callable and is_terminal:
                    try:
                        self.on_delete_callable(key, super().__getitem__(key)[1])
                    except Exception as err:
                        # Prevent user code from breaking FaasCacheDict ops
                        print(f"FaasCacheDict: on_delete_callable caused exc: {err}")
                        pass
                super().__delitem__(key)
            except KeyError as err:
                if not ignore_missing:
                    raise err
            finally:
                self._set_self_byte_size()

    def __iter__(self):
        """Yield non-expired keys, without purging the expired ones"""
        with self._lock:
            for key in super().__iter__():
                if self.is_expired(key) is False:
                    yield key

    def __len__(self):
        with self._lock:
            self._purge_expired()
            return super().__len__()

    def __repr__(self):
        return (
            "<FaaSCacheDict@{:#08x}; default_ttl={}, max_memory={}, "
            "max_items={}, current_memory_bytes={}, current_items={}>"
        ).format(
            id(self),
            self.default_ttl,
            self._max_size_user,
            self._max_items,
            self._self_byte_size,
            len(self),
        )

    def __reduce__(self):
        """
        This allows the FaasCache object to be correctly pickled

        It is based on the OrderedDict reducer
        """
        inst_dict = vars(self).copy()
        for k in vars(OrderedDict()):
            inst_dict.pop(k, None)

        inst_dict.pop("_lock")

        return self.__class__, (), inst_dict or None, None, iter(super().items())

    def __setstate__(self, new_state):
        """
        This allows the FaasCache object to be correctly un-pickled
        The RLock is renewed when un-pickled
        """
        new_state["_lock"] = RLock()
        self.__dict__.update(new_state)

    ###
    # Dict functions
    ###
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        with self._lock:
            self._purge_expired()
            return list(super().keys())

    def items(self):
        with self._lock:
            self._purge_expired()
            return [(k, v[1]) for (k, v) in super().items()]

    def values(self):
        with self._lock:
            self._purge_expired()
            return [v[1] for v in super().values()]

    def purge(self):
        """Delete all data in the cache"""
        with self._lock:
            _keys = list(super().__iter__())
            [self.__delitem__(key, ignore_missing=True) for key in _keys]
            self._set_self_byte_size()

    ###
    # TTL functions
    ###
    def get_ttl(self, key, now=None):
        """Return remaining delta TTL for a key from now"""
        if now is None:
            now = time.time()
        with self._lock:
            expire, _value = super().__getitem__(key)
            return expire - now

    def set_ttl(self, key, ttl, now=None):
        """Set TTL for the given key, this will be set ttl seconds ahead of now"""
        if now is None:
            now = time.time()
        _assert(ttl >= 0, "TTL must be in the future")
        with self._lock:
            # Set new TTL and reset to bottom of queue (MRU)
            value = self.__getitem__(key)
            if ttl is None:  # No expiry
                super().__setitem__(key, (None, value))
            else:
                super().__setitem__(key, (now + ttl, value))

    def expire_at(self, key, timestamp):
        """Set the key expire absolute timestamp (epoch seconds - ie `time.time()`)"""
        with self._lock:
            value = self.__getitem__(key)
            super().__setitem__(key, (timestamp, value))

    def is_expired(self, key, now=None):
        """
        Check if key has expired, and return it if so.

        Note: A historic key may have expired and have since been
        deleted in which case this will return `None` as its state is unknown.
        """
        with self._lock:
            if now is None:
                now = time.time()

            try:
                expire, _value = super().__getitem__(key)
            except KeyError:
                return None  # unknown

            if expire:
                if expire < now:
                    return True

        return False

    def _purge_expired(self):
        """Iterate through all cache items and prune all expired keys"""
        _keys = list(super().__iter__())
        _remove = [key for key in _keys if self.is_expired(key)]  # noqa
        [self.__delitem__(key, ignore_missing=True) for key in _remove]
        self._set_self_byte_size()

    ###
    # Memory size functions
    ###
    def get_byte_size(self):
        """Get self size in bytes"""
        byte_size = get_deep_byte_size(self)
        self._self_byte_size = byte_size  # May as well!
        return byte_size

    def change_byte_size(self, max_size_bytes):
        """
        Set new max byte size and delete objects if required

        :param max_size_bytes: (int|str) optional: Max byte size of cache (1024 or '1K')
        """
        with self._lock:
            self._max_size_user = max_size_bytes
            self._max_size_bytes = None
            if self._max_size_user:
                self._max_size_bytes = user_input_byte_size_to_bytes(
                    self._max_size_user
                )
            self._shrink_to_fit_byte_size()

    def _set_self_byte_size(self):
        """Calculate and set the new internal cache size"""
        self._self_byte_size = self.get_byte_size()

    def _shrink_to_fit_byte_size(self):
        """As required delete the oldest LRU items in the cache dict until size criteria is met"""
        with self._lock:
            self._purge_expired()
            if self._max_size_bytes:
                while self.get_byte_size() > self._max_size_bytes:
                    self.delete_oldest_item()
            self._set_self_byte_size()

    ###
    # LRU functions
    ###
    def change_max_items(self, max_items):
        """
        Set new max item length and trim as required

        :param max_items: (int) optional: Max length of cache, `None` to disable max-length
        """
        with self._lock:
            self._max_items = max_items
            if self._max_items:
                while self._max_items < self.__len__():
                    self.delete_oldest_item()

    def delete_oldest_item(self):
        """
        Remove the oldest item in the cache, which is the HEAD of the OrderedDict
        """
        with self._lock:
            _keys = list(super().__iter__())
            if _keys:
                self.__delitem__(_keys[0])
            else:
                raise KeyError("EmptyCache")
