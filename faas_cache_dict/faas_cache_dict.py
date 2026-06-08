import gc
import logging
import sys
import time
import weakref
from collections import OrderedDict
from contextlib import contextmanager
from threading import RLock, Thread
from typing import Any, Callable, Iterable

from .exceptions import DataTooLarge
from .size_utils import get_deep_byte_size, user_input_byte_size_to_bytes
from .utils import _assert, _is_int, _is_number

__all__ = ["FaaSCacheDict"]

logger = logging.getLogger(__name__)

# Sentinel marking "no key is protected from eviction" - distinct from any real
# key, including None.
_UNSET = object()


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
            _is_number(default_ttl) or (default_ttl is None),
            "Invalid TTL config",
        )
        if default_ttl is not None:
            _assert(default_ttl >= 0, "TTL must be >=0")
        self.default_ttl = default_ttl

        # CACHE MEMORY SIZE
        _assert(
            isinstance(max_size_bytes, str)
            or _is_int(max_size_bytes)
            or (max_size_bytes is None),
            "Invalid byte size",
        )
        self._max_size_user = max_size_bytes
        self._max_size_bytes = None
        if self._max_size_user is not None:
            # Validate like change_byte_size: only None disables the limit; any
            # other value must convert to a positive byte count or raise.
            self._max_size_bytes = user_input_byte_size_to_bytes(self._max_size_user)
        self._self_byte_size = 0
        # Guards against __sizeof__ re-purging while we deep-measure the cache:
        # objsize calls sys.getsizeof(self) -> __sizeof__, which would otherwise
        # recurse back into _purge_expired during a size computation.
        self._suppress_sizeof_purge = False

        # CACHE LENGTH
        _assert(
            _is_int(max_items) or (max_items is None), "Invalid max items limit"
        )
        if max_items is not None:
            _assert(max_items > 0, "Max items limit must >0")
        self._max_items = max_items

        # Lifecycle callables
        self.on_delete_callable = on_delete_callable

        self._lock = RLock()
        # Reentrant-lock nesting depth and a buffer of (key, value) pairs whose
        # on_delete hooks must fire AFTER the outermost lock release (see _locked).
        self._lock_depth = 0
        self._pending_on_delete = []
        super().__init__()
        # Seed the running byte total with the empty-cache overhead, so the
        # incremental size (maintained per insert/delete) includes it; then load
        # any initial items and resync to an exact measure.
        self._set_self_byte_size(skip_purge=True)
        self.update(*args, **kwargs)
        self._set_self_byte_size(skip_purge=True)

        # Thread to purge expired data
        self._stop_purge = False
        self._purge_thread = Thread(
            target=FaaSCacheDict._purge_thread_func, args=(weakref.ref(self),)
        )
        self._purge_thread.daemon = True
        self._purge_thread.start()

    @contextmanager
    def _locked(self):
        """
        Acquire the reentrant lock; on the OUTERMOST release, fire any buffered
        on_delete callbacks.

        Removals that happen deep inside an eviction or expiry sweep buffer their
        (key, value) pairs rather than calling the hook directly, so the hook
        always runs with the lock released - preventing a deadlock if the hook
        touches the cache from another thread.
        """
        self._lock.acquire()
        self._lock_depth += 1
        try:
            yield
        finally:
            self._lock_depth -= 1
            pending = None
            if self._lock_depth == 0 and self._pending_on_delete:
                pending = self._pending_on_delete
                self._pending_on_delete = []
            self._lock.release()
            if pending:
                for key, value in pending:
                    try:
                        self.on_delete_callable(key, value)
                    except Exception as err:
                        logger.warning(
                            "on_delete_callable raised exception: %s",
                            err,
                            exc_info=True,
                        )

    def __getitem__(self, key: Any) -> Any:
        with self._locked():
            if self.is_expired(key):
                self.__delitem__(key)
                raise KeyError(key)
            value_with_expiry = super().__getitem__(key)
            super().move_to_end(key)
            return value_with_expiry[1]

    def __setitem__(
        self, key: Any, value: Any, expire_at: float | int | None = None
    ) -> None:
        with self._locked():
            expire = None
            if expire_at is not None:
                _assert(
                    _is_number(expire_at),
                    "Invalid expiry timestamp",
                )
                expire = expire_at
            elif self.default_ttl is not None:
                expire = time.time() + self.default_ttl

            new_entry_size = self._entry_byte_size(key, expire, value)

            if self._max_size_bytes and new_entry_size > self._max_size_bytes:
                # Cheap up-front reject for an item that cannot fit even on its
                # own, so we don't evict existing items to make room for something
                # that can never fit. The byte-size enforcement below is
                # authoritative for the item-plus-overhead case.
                raise DataTooLarge

            key_present = key in super().keys()

            if (
                self._max_items is not None
                and not key_present
                and super().__len__() >= self._max_items
            ):
                # Only when at capacity: reclaim expired items first so we don't
                # evict valid items unnecessarily, then evict the LRU until there
                # is room for the new key. Below capacity we touch nothing.
                self._purge_expired()
                while super().__len__() >= self._max_items:
                    self._evict_oldest()

            if key_present:
                # Refreshing an existing key: drop its old contribution first.
                old_expire, old_value = super().__getitem__(key)
                self._account_for_removed_entry(key, old_expire, old_value)

            super().__setitem__(key, (expire, value))
            super().move_to_end(key)
            self._self_byte_size += new_entry_size

            if self._max_size_bytes and self._self_byte_size > self._max_size_bytes:
                self._shrink_to_fit_byte_size(protected_key=key)

    def __delitem__(
        self,
        key: Any,
        is_terminal: bool = True,
        ignore_missing: bool = False,
    ) -> None:
        with self._locked():
            try:
                expire, value = super().__getitem__(key)
            except KeyError as err:
                if not ignore_missing:
                    raise err
            else:
                if self.on_delete_callable and is_terminal:
                    # Buffer the hook; _locked fires it after the outermost lock
                    # release, so it never runs while the lock is held.
                    self._pending_on_delete.append((key, value))
                # Maintain the running byte total incrementally rather than
                # rescanning the whole cache (see _entry_byte_size / get_byte_size).
                self._account_for_removed_entry(key, expire, value)
                super().__delitem__(key)

    def __iter__(self) -> Iterable[Any]:
        """Yield non-expired keys. Purges expired items before iterating."""
        with self._locked():
            self._purge_expired()
            keys = list(super().__iter__())

        for key in keys:
            if self.is_expired(key) is False:
                yield key

    def __contains__(self, key: Any) -> bool:
        with self._locked():
            self._purge_expired()
            return True if key in super().keys() else False

    def __len__(self) -> int:
        with self._locked():
            self._purge_expired()
            return super().__len__()

    def __repr__(self) -> str:
        with self._locked():
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
        with self._locked():
            self._purge_expired()
            inst_dict = vars(self).copy()
            for k in vars(OrderedDict()):
                inst_dict.pop(k, None)

            inst_dict.pop("_lock")
            inst_dict.pop("_purge_thread")
            inst_dict.pop("_stop_purge", None)
            inst_dict.pop("_lock_depth", None)
            inst_dict.pop("_pending_on_delete", None)

            # Store items separately to restore with original expiry times
            inst_dict["_pickled_items"] = list(super().items())

            return self.__class__, (), inst_dict or None, None, None

    def __setstate__(self, new_state: dict) -> None:
        """
        This allows the FaasCache object to be correctly un-pickled.

        A fresh RLock is installed. The purge thread is intentionally NOT started
        here: the unpickler reconstructs the object via __class__() (see
        __reduce__), so __init__ has already started exactly one purge thread for
        it. Starting another would orphan the first (it would keep running until
        the object is collected) - the bug this avoids.
        """
        # Extract pickled items before updating __dict__
        pickled_items = new_state.pop("_pickled_items", [])

        new_state["_lock"] = RLock()
        new_state["_stop_purge"] = False
        new_state["_lock_depth"] = 0
        new_state["_pending_on_delete"] = []
        self.__dict__.update(new_state)

        # Restore items directly to preserve original expiry times
        for key, value in pickled_items:
            super().__setitem__(key, value)

        # Recalculate byte size after restoring items
        self._set_self_byte_size(skip_purge=True)

    def __sizeof__(self) -> int:
        with self._locked():
            if not self._suppress_sizeof_purge:
                self._purge_expired()
            return super().__sizeof__()

    def __reversed__(self) -> Iterable[Any]:
        with self._locked():
            self._purge_expired()
            keys = list(super().__reversed__())

        for key in keys:
            if self.is_expired(key) is False:
                yield key

    def __eq__(self, other: Any) -> bool:
        if not hasattr(other, "items"):
            return False
        with self._locked():
            self._purge_expired()
            self_items = [(k, v[1]) for (k, v) in super().items()]
        # Mirror OrderedDict: order-sensitive against another OrderedDict,
        # order-insensitive (plain mapping equality) against everything else.
        if isinstance(other, OrderedDict):
            return self_items == list(other.items())
        return dict(self_items) == dict(other.items())

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
        with self._locked():
            try:
                return self[key]
            except KeyError:
                return default

    def keys(self) -> list[Any]:
        with self._locked():
            self._purge_expired()
            return list(super().keys())

    def items(self) -> list[tuple[Any, Any]]:
        with self._locked():
            self._purge_expired()
            return [(k, v[1]) for (k, v) in super().items()]

    def values(self) -> list[Any]:
        with self._locked():
            self._purge_expired()
            return [v[1] for v in super().values()]

    def pop(self, key: Any, default: Any = _UNSET) -> Any:
        with self._locked():
            self._purge_expired()
            if key not in super().keys():
                # Match dict.pop: raise when the key is absent and no default was
                # supplied; return the default otherwise.
                if default is _UNSET:
                    raise KeyError(key)
                return default
            expire, value = super().__getitem__(key)
            if self.on_delete_callable:
                # Buffer the hook; _locked fires it after the lock is released.
                self._pending_on_delete.append((key, value))
            self._account_for_removed_entry(key, expire, value)
            super().__delitem__(key)
            return value

    def popitem(self, last: bool = True) -> tuple[Any, Any]:
        with self._locked():
            self._purge_expired()
            if not super().__len__():
                raise KeyError("EmptyCache")
            k = list(super().keys())[-1 if last else 0]
            expire, value = super().__getitem__(k)
            if self.on_delete_callable:
                # Buffer the hook; _locked fires it after the lock is released.
                self._pending_on_delete.append((k, value))
            self._account_for_removed_entry(k, expire, value)
            super().__delitem__(k)
            return k, value

    def clear(self) -> None:
        return self.purge()

    def purge(self) -> None:
        """Delete all data in the cache, can't just call clear as it needs to call on_delete_callable"""
        with self._locked():
            for key in list(super().__iter__()):
                self.__delitem__(key, ignore_missing=True)
            # Resync the (now empty) cache size to an exact measure.
            self._set_self_byte_size(skip_purge=True)

    def stop_purge_thread(self) -> None:
        """Stop the background purge thread and release resources."""
        self._stop_purge = True

    def close(self) -> None:
        """Stop the background purge thread and release resources."""
        self.stop_purge_thread()

    def copy(self):
        raise NotImplementedError(
            "FaaSCacheDict cannot be copied; construct a new instance and add "
            "items individually instead."
        )

    def __copy__(self):
        # copy.copy() consults __copy__ before falling back to the pickle
        # reduce path, so this keeps copy.copy() consistent with .copy()
        # (unsupported) while leaving pickling - a supported feature - intact.
        return self.copy()

    def __deepcopy__(self, memo: Any):
        return self.copy()

    def setdefault(self, key: Any, default: Any = None) -> Any:
        """
        If key is in the cache and not expired, return its value.
        Otherwise, set key to default and return default.
        """
        with self._locked():
            try:
                return self[key]
            except KeyError:
                self[key] = default
                return default

    @classmethod
    def fromkeys(cls, iterable, value=None):
        """Not implemented - use constructor with explicit TTL and constraints instead."""
        raise NotImplementedError(
            "fromkeys is not supported. Use FaaSCacheDict() constructor and add items individually."
        )

    def move_to_end(self, key: Any, last: bool = True) -> None:
        """Move an existing key to either end of the cache. Raises KeyError if expired or missing."""
        with self._locked():
            if self.is_expired(key):
                self.__delitem__(key)
                raise KeyError(key)
            super().move_to_end(key, last=last)

    ###
    # TTL functions
    ###
    def get_ttl(self, key: Any, now: float | int | None = None) -> float | None:
        """Return remaining delta TTL for a key from now, or None if no TTL is set.

        Raises KeyError if the key doesn't exist or has expired.
        """
        if now is None:
            now = time.time()

        with self._locked():
            if self.is_expired(key, now=now):
                self.__delitem__(key)
                raise KeyError(key)
            expire, _value = super().__getitem__(key)
            if expire is None:
                return None
            return expire - now

    def set_ttl(
        self, key, ttl: float | int | None, now: float | int | None = None
    ) -> None:
        """Set TTL for the given key, this will be set ttl seconds ahead of now. Pass None to remove expiry."""
        if now is None:
            now = time.time()

        _assert(ttl is None or _is_number(ttl), "Invalid TTL")
        if ttl is not None:
            _assert(ttl >= 0, "TTL must be non-negative")

        with self._locked():
            if self.is_expired(key):
                self.__delitem__(key)
                raise KeyError(key)
            # Get value without affecting LRU order
            value = super().__getitem__(key)[1]
            if ttl is None:  # No expiry
                super().__setitem__(key, (None, value))
            else:
                super().__setitem__(key, (now + ttl, value))

    def expire_at(self, key: Any, timestamp: float | int) -> None:
        """Set the key expire absolute timestamp (epoch seconds - ie `time.time()`)"""
        _assert(_is_number(timestamp), "Invalid expiry timestamp")
        with self._locked():
            if self.is_expired(key):
                self.__delitem__(key)
                raise KeyError(key)
            # Get value without affecting LRU order
            value = super().__getitem__(key)[1]
            super().__setitem__(key, (timestamp, value))

    def is_expired(self, key: Any, now: float | int | None = None) -> bool | None:
        """
        Check if key has expired, and return it if so.

        Note: A historic key may have expired and have since been
        deleted in which case this will return `None` as its state is unknown.
        """
        if now is None:
            now = time.time()

        with self._locked():
            try:
                expire, _value = super().__getitem__(key)
            except KeyError:
                return None  # unknown

            if expire is not None:
                if expire < now:
                    return True

            return False

    def _purge_expired(self) -> bool:
        """
        Prune all expired keys. Returns True if any were removed.

        __delitem__ keeps the byte total current per item, so there is no full
        rescan here. gc.collect() is intentionally left to the background purge
        thread - calling it on every synchronous purge (len(), keys(), inserts at
        capacity, ...) was a large and needless cost.
        """
        with self._locked():
            _remove = [
                key for key in list(super().__iter__()) if self.is_expired(key)
            ]
            for key in _remove:
                self.__delitem__(key, ignore_missing=True)
            return bool(_remove)

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
                # The background thread owns gc.collect(): synchronous purges
                # skip it (it is far too costly to run on every cache operation).
                if self._purge_expired():
                    gc.collect()
                # Re-measure and enforce exactly each cycle so a value mutated in
                # place after insertion is caught (and the running total resynced)
                # within a purge interval. Only byte-limited caches need this, so
                # others stay scan-free.
                if self._max_size_bytes:
                    self._shrink_to_fit_byte_size()
            except Exception as err:
                # Parent may have been collected during purge
                logger.debug("Purge thread exiting due to exception: %s", err)
                return

    ###
    # Memory size functions
    ###
    def get_byte_size(self, skip_purge: bool = False) -> int:
        """
        Get self size in bytes.

        Always recomputes and caches the deep byte size. ``skip_purge=True`` only
        skips the expiry purge beforehand (used from within a purge to avoid
        re-entrancy); it is not a shortcut that returns a stale cached value.
        """
        with self._locked():
            if not skip_purge:
                self._purge_expired()
            # Suppress __sizeof__'s own purge while objsize traverses us, so the
            # measurement neither re-purges (skip_purge=False) nor purges at all
            # (skip_purge=True).
            self._suppress_sizeof_purge = True
            try:
                # Exclude the pending-callback buffer: it transiently holds
                # already-removed entries (awaiting their hook) that must not be
                # counted toward the cache's size during enforcement.
                self._self_byte_size = get_deep_byte_size(
                    self, exclude=[self._pending_on_delete]
                )
            finally:
                self._suppress_sizeof_purge = False
            return self._self_byte_size

    def change_byte_size(self, max_size_bytes: int | str | None) -> None:
        """
        Set new max byte size and delete objects if required

        :param max_size_bytes: (int|str) optional: Max byte size of cache (1024 or '1K'), `None` to disable
        """
        _assert(
            isinstance(max_size_bytes, str)
            or _is_int(max_size_bytes)
            or (max_size_bytes is None),
            "Invalid byte size",
        )
        if max_size_bytes is not None:
            # Validate by converting - will raise if invalid
            converted = user_input_byte_size_to_bytes(max_size_bytes)
            _assert(converted > 0, "Byte size must be >0")
        with self._locked():
            self._max_size_user = max_size_bytes
            self._max_size_bytes = None
            if self._max_size_user is not None:
                self._max_size_bytes = user_input_byte_size_to_bytes(
                    self._max_size_user
                )
            self._shrink_to_fit_byte_size()

    def _set_self_byte_size(self, skip_purge: bool = False) -> None:
        """Calculate and set the new internal cache size"""
        self._self_byte_size = self.get_byte_size(skip_purge)

    def _entry_byte_size(
        self, key: Any, expire: float | int | None, value: Any
    ) -> int:
        """
        Deep byte size of a single stored entry: its key plus the (expire, value)
        tuple it is wrapped in.

        Used to maintain the running byte total incrementally on insert and
        delete, so those paths cost O(one entry) rather than re-scanning the whole
        cache. The running total can over-count objects shared between entries
        (objsize dedups; a per-entry sum does not), which is conservative for a
        size limit; get_byte_size() does an exact full measure and resyncs it.
        """
        return get_deep_byte_size(key) + get_deep_byte_size((expire, value))

    def _account_for_removed_entry(
        self, key: Any, expire: float | int | None, value: Any
    ) -> None:
        """
        Decrement the running byte total for an entry being removed or replaced,
        clamped at zero.

        The clamp guards against drift: a value mutated in place after insertion
        has a different size here (at removal) than it did when added, which could
        otherwise drive the total negative and silently disable byte-limit
        enforcement. The background thread resyncs the total to an exact measure
        each cycle, so any accumulated drift self-heals within a purge interval.
        """
        self._self_byte_size -= self._entry_byte_size(key, expire, value)
        if self._self_byte_size < 0:
            self._self_byte_size = 0

    def _shrink_to_fit_byte_size(self, protected_key: Any = _UNSET) -> None:
        """
        As required delete the oldest LRU items in the cache dict until size criteria is met.

        When called after inserting ``protected_key``, that key is never silently
        evicted to make itself fit: if it cannot fit even on its own (once cache
        overhead is accounted for), it is removed and ``DataTooLarge`` is raised
        rather than discarding it without warning.
        """
        with self._locked():
            self._purge_expired()
            if self._max_size_bytes:
                # Enforce on an EXACT measure (re-measured each step) so the limit
                # is a hard ceiling and objects shared between entries are not
                # double-counted into spurious evictions. This is O(n) per step,
                # but callers only reach here when the cheap running total reports
                # the cache is at its byte limit - inserts well under it never
                # enforce. get_byte_size also resyncs the running total to exact.
                # Stop once empty: the residual is fixed structural overhead that
                # eviction cannot reduce, so we must not loop forever on a tiny limit.
                while (
                    self.get_byte_size(skip_purge=True) > self._max_size_bytes
                    and super().__len__()
                ):
                    oldest_key = next(iter(super().__iter__()))
                    if oldest_key == protected_key:
                        # Everything older has been evicted and only the
                        # just-inserted item remains, yet we are still over
                        # budget: it does not fit. Remove it (is_terminal=False so
                        # the delete hook does not fire - it was never really
                        # cached) and signal failure instead of dropping it silently.
                        self.__delitem__(protected_key, is_terminal=False)
                        raise DataTooLarge
                    self._evict_oldest()

    ###
    # LRU functions
    ###
    def change_max_items(self, max_items: int | None) -> None:
        """
        Set new max item length and trim as required

        :param max_items: (int) optional: Max length of cache, `None` to disable max-length
        """
        _assert(
            _is_int(max_items) or (max_items is None), "Invalid max items limit"
        )
        if max_items is not None:
            _assert(max_items > 0, "Max items limit must be >0")
        with self._locked():
            self._max_items = max_items
            if self._max_items is not None:
                # Purge once up front, then evict heads directly (without
                # re-purging each iteration) until within the new limit.
                self._purge_expired()
                while self._max_items < super().__len__():
                    self._evict_oldest()

    def _evict_oldest(self) -> None:
        """
        Remove the LRU (head) item without purging first.

        The cheap eviction primitive for loops that have already purged. Callers
        wanting expired items reclaimed must purge beforehand. Raises
        KeyError('EmptyCache') if the cache is empty.
        """
        oldest_key = next(iter(super().__iter__()), _UNSET)
        if oldest_key is _UNSET:
            raise KeyError("EmptyCache")
        self.__delitem__(oldest_key)

    def delete_oldest_item(self) -> None:
        """
        Remove the oldest item in the cache, which is the HEAD of the OrderedDict
        """
        with self._locked():
            self._purge_expired()
            self._evict_oldest()
