import gc
import logging
import sys
import time
import weakref
from collections import OrderedDict
from threading import RLock, Thread
from typing import Any, Callable, Iterable

from .exceptions import DataTooLarge
from .size_utils import get_deep_byte_size, user_input_byte_size_to_bytes
from .utils import _assert

__all__ = ["FaaSCacheDict"]

logger = logging.getLogger(__name__)


class FaaSCacheDict(OrderedDict):
    """
    Python Dictionary with TTL, max size and max length constraints
    """

    _auto_purge_seconds = 5

    def __init__(
        self,
        default_ttl: int | float | None = None,
        max_size_bytes: int | str | None = None,
        max_items: int | None = sys.maxsize,
        on_delete_callable: Callable | None = None,
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
        if default_ttl is not None:
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
        self._set_self_byte_size(skip_purge=True)

        # Thread to purge expired data
        self._stop_purge = False
        self._purge_thread = Thread(
            target=FaaSCacheDict._purge_thread_func, args=(weakref.ref(self),)
        )
        self._purge_thread.daemon = True
        self._purge_thread.start()

    def __getitem__(self, key: Any) -> Any:
        with self._lock:
            if self.is_expired(key):
                self.__delitem__(key)
                raise KeyError(key)
            value_with_expiry = super().__getitem__(key)
            super().move_to_end(key)
            return value_with_expiry[1]

    def __setitem__(
        self, key: Any, value: Any, expire_at: float | int | None = None
    ) -> None:
        with self._lock:
            if self._max_size_bytes:
                if (
                    get_deep_byte_size(key) + get_deep_byte_size(value)
                ) > self._max_size_bytes:
                    raise DataTooLarge

            expire = None
            if expire_at is not None:
                expire = expire_at
            elif self.default_ttl is not None:
                expire = time.time() + self.default_ttl

            if self._max_items:
                if (
                    key not in super().keys()
                ):  # If refreshing an existing key then size remains constant
                    # Purge expired items first so we don't evict valid items unnecessarily
                    self._purge_expired()
                    while self._max_items <= super().__len__():
                        self.delete_oldest_item()

            super().__setitem__(key, (expire, value))
            super().move_to_end(key)
            self._shrink_to_fit_byte_size()

    def __delitem__(
        self,
        key: Any,
        is_terminal: bool = True,
        ignore_missing: bool = False,
        skip_byte_size_update: bool = False,
    ) -> None:
        with self._lock:
            try:
                if self.on_delete_callable and is_terminal:
                    try:
                        self.on_delete_callable(key, super().__getitem__(key)[1])
                    except Exception as err:
                        # Prevent user code from breaking FaasCacheDict ops
                        logger.warning("on_delete_callable raised exception: %s", err, exc_info=True)
                super().__delitem__(key)
            except KeyError as err:
                if not ignore_missing:
                    raise err
            finally:
                if not skip_byte_size_update:
                    self._set_self_byte_size()

    def __iter__(self) -> Iterable[Any]:
        """Yield non-expired keys, without purging the expired ones"""
        with self._lock:
            self._purge_expired()
            keys = list(super().__iter__())

        for key in keys:
            if self.is_expired(key) is False:
                yield key

    def __contains__(self, key: Any) -> bool:
        with self._lock:
            self._purge_expired()
            return True if key in super().keys() else False

    def __len__(self) -> int:
        with self._lock:
            self._purge_expired()
            return super().__len__()

    def __repr__(self) -> str:
        with self._lock:
            self._purge_expired()
            return (
                "<FaaSCacheDict@{:#08x}; default_ttl={}, max_memory={}, "
                "max_items={}, current_memory_bytes={}, current_items={}>"
            ).format(
                id(self),
                self.default_ttl,
                self._max_size_user,
                self._max_items,
                self._self_byte_size,
                super().__len__(),
            )

    def __str__(self) -> str:
        return self.__repr__()

    def __reduce__(self) -> tuple:
        """
        This allows the FaasCache object to be correctly pickled

        It is based on the OrderedDict reducer
        """
        with self._lock:
            self._purge_expired()
            inst_dict = vars(self).copy()
            for k in vars(OrderedDict()):
                inst_dict.pop(k, None)

            inst_dict.pop("_lock")
            inst_dict.pop("_purge_thread")
            inst_dict.pop("_stop_purge", None)

            # Store items separately to restore with original expiry times
            inst_dict["_pickled_items"] = list(super().items())

            return self.__class__, (), inst_dict or None, None, None

    def __setstate__(self, new_state: dict) -> None:
        """
        This allows the FaasCache object to be correctly un-pickled
        The RLock and purge thread are renewed when un-pickled
        """
        # Extract pickled items before updating __dict__
        pickled_items = new_state.pop("_pickled_items", [])

        new_state["_lock"] = RLock()
        new_state["_stop_purge"] = False
        self.__dict__.update(new_state)

        # Restore items directly to preserve original expiry times
        for key, value in pickled_items:
            super().__setitem__(key, value)

        # Create and start purge thread after self is fully initialized
        self._purge_thread = Thread(
            target=FaaSCacheDict._purge_thread_func, args=(weakref.ref(self),)
        )
        self._purge_thread.daemon = True
        with self._lock:
            self._purge_thread.start()

    def __sizeof__(self) -> int:
        with self._lock:
            self._purge_expired()
            return super().__sizeof__()

    def __reversed__(self) -> Iterable[Any]:
        with self._lock:
            self._purge_expired()
            keys = list(super().__reversed__())

        for key in keys:
            if self.is_expired(key) is False:
                yield key

    def __eq__(self, other: Any) -> bool:
        with self._lock:
            self._purge_expired()
            if not hasattr(other, "items"):
                return False
            self_items = [(k, v[1]) for (k, v) in super().items()]
            return self_items == other.items()

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __or__(self, other: Any) -> None:
        raise NotImplementedError

    def __ior__(self, other: Any) -> None:
        raise NotImplementedError

    def __ror__(self, other: Any) -> None:
        raise NotImplementedError

    ###
    # Dict functions
    ###
    def get(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            try:
                return self[key]
            except KeyError:
                return default

    def keys(self) -> list[Any]:
        with self._lock:
            self._purge_expired()
            return list(super().keys())

    def items(self) -> list[tuple[Any, Any]]:
        with self._lock:
            self._purge_expired()
            return [(k, v[1]) for (k, v) in super().items()]

    def values(self) -> list[Any]:
        with self._lock:
            self._purge_expired()
            return [v[1] for v in super().values()]

    def pop(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            self._purge_expired()
            if key not in super().keys():
                return default
            value = super().__getitem__(key)[1]
            if self.on_delete_callable:
                try:
                    self.on_delete_callable(key, value)
                except Exception as err:
                    logger.warning("on_delete_callable raised exception: %s", err, exc_info=True)
            super().__delitem__(key)
            self._set_self_byte_size()
            return value

    def popitem(self, last: bool = True) -> tuple[Any, Any]:
        with self._lock:
            self._purge_expired()
            if not super().__len__():
                raise KeyError("EmptyCache")
            k = list(super().keys())[-1 if last else 0]
            value = super().__getitem__(k)[1]
            if self.on_delete_callable:
                try:
                    self.on_delete_callable(k, value)
                except Exception as err:
                    logger.warning("on_delete_callable raised exception: %s", err, exc_info=True)
            super().__delitem__(k)
            self._set_self_byte_size()
            return k, value

    def clear(self) -> None:
        return self.purge()

    def purge(self) -> None:
        """Delete all data in the cache, can't just call clear as it needs to call on_delete_callable"""
        with self._lock:
            [
                self.__delitem__(key, ignore_missing=True)
                for key in list(super().__iter__())
            ]

    def close(self) -> None:
        """Stop the background purge thread and release resources."""
        self._stop_purge = True

    def copy(self):
        raise NotImplementedError

    ###
    # TTL functions
    ###
    def get_ttl(self, key: Any, now: float | int | None = None) -> float | None:
        """Return remaining delta TTL for a key from now, or None if no TTL is set"""
        if now is None:
            now = time.time()

        with self._lock:
            expire, _value = super().__getitem__(key)
            if expire is None:
                return None
            return expire - now

    def set_ttl(self, key, ttl: float | int | None, now: float | int | None = None) -> None:
        """Set TTL for the given key, this will be set ttl seconds ahead of now. Pass None to remove expiry."""
        if now is None:
            now = time.time()

        if ttl is not None:
            _assert(ttl >= 0, "TTL must be non-negative")

        with self._lock:
            # Set new TTL and reset to bottom of queue (MRU)
            value = self.__getitem__(key)
            if ttl is None:  # No expiry
                super().__setitem__(key, (None, value))
            else:
                super().__setitem__(key, (now + ttl, value))

    def expire_at(self, key: Any, timestamp: float | int) -> None:
        """Set the key expire absolute timestamp (epoch seconds - ie `time.time()`)"""
        with self._lock:
            value = self.__getitem__(key)
            super().__setitem__(key, (timestamp, value))

    def is_expired(self, key: Any, now: float | int | None = None) -> bool | None:
        """
        Check if key has expired, and return it if so.

        Note: A historic key may have expired and have since been
        deleted in which case this will return `None` as its state is unknown.
        """
        if now is None:
            now = time.time()

        with self._lock:
            try:
                expire, _value = super().__getitem__(key)
            except KeyError:
                return None  # unknown

            if expire is not None:
                if expire < now:
                    return True

            return False

    def _purge_expired(self) -> None:
        """Iterate through all cache items and prune all expired keys"""
        with self._lock:
            _keys = list(super().__iter__())
            _remove = [key for key in _keys if self.is_expired(key)]  # noqa
            [
                self.__delitem__(key, ignore_missing=True, skip_byte_size_update=True)
                for key in _remove
            ]
            if _remove:
                gc.collect()
            self._set_self_byte_size(skip_purge=True)

    ###
    # Thread functions
    ###
    @staticmethod
    def _purge_thread_func(weak_self) -> None:
        """
        Thread function which will run in the background and purge expired keys.
        Uses a weak reference to allow garbage collection of the parent object.
        """
        while True:
            self = weak_self()
            if self is None or self._stop_purge:
                # Parent object was garbage collected or stop requested
                return

            # Read values before releasing reference
            sleep_time = self._auto_purge_seconds
            del self  # Release strong reference before sleeping

            time.sleep(sleep_time)

            # Re-acquire reference after sleep
            self = weak_self()
            if self is None or self._stop_purge:
                return

            try:
                self._purge_expired()
            except Exception as err:
                # Parent may have been collected during purge
                logger.debug("Purge thread exiting due to exception: %s", err)
                return

    ###
    # Memory size functions
    ###
    def get_byte_size(self, skip_purge: bool = False) -> int:
        """Get self size in bytes"""
        with self._lock:
            if not skip_purge:
                self._purge_expired()
                byte_size = get_deep_byte_size(self)
                self._self_byte_size = byte_size  # May as well!

            return self._self_byte_size

    def change_byte_size(self, max_size_bytes: int) -> None:
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

    def _set_self_byte_size(self, skip_purge: bool = False) -> None:
        """Calculate and set the new internal cache size"""
        self._self_byte_size = self.get_byte_size(skip_purge)

    def _shrink_to_fit_byte_size(self) -> None:
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
    def change_max_items(self, max_items: int) -> None:
        """
        Set new max item length and trim as required

        :param max_items: (int) optional: Max length of cache, `None` to disable max-length
        """
        with self._lock:
            self._max_items = max_items
            if self._max_items:
                while self._max_items < self.__len__():
                    self.delete_oldest_item()

    def delete_oldest_item(self) -> None:
        """
        Remove the oldest item in the cache, which is the HEAD of the OrderedDict
        """
        with self._lock:
            self._purge_expired()
            _keys = list(super().__iter__())
            if _keys:
                self.__delitem__(_keys[0])
            else:
                raise KeyError("EmptyCache")
