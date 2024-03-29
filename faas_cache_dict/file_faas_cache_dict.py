import os
import pickle
from threading import RLock

from filelock import FileLock, Timeout

from .faas_cache_dict import FaaSCacheDict

FILE_FAAS_CACHE_ROOT_PATH = os.environ.get("FILE_BACKED_FAAS_CACHE_ROOT_PATH")
_FILE_FAAS_PICKLE_FLAG = False


def _do_pickle_file_load(file_):
    """Given an opened file object attempt to unpickle"""
    global _FILE_FAAS_PICKLE_FLAG
    _FILE_FAAS_PICKLE_FLAG = True
    loaded = pickle.load(file_)
    _FILE_FAAS_PICKLE_FLAG = False
    loaded._lock = RLock()
    return loaded


class FileBackedFaaSCache(FaaSCacheDict):
    """
    ALPHA CODE: DO NOT USE.

    An implementation of a FaaSCacheDict which can resurrect its state from disk
    if for whatever reason it is dropped from memory (eg. app restart)

    This uses the built in Python pickling functionality and thus such should support
    most complex data structures.

    Usage:
        FileBackedFaaSCache.init(key_name='key')
    """

    file_path = None
    lock_path = None
    old_path = None
    file_lock = None

    @classmethod
    def init(cls, key_name, *args, root_path=FILE_FAAS_CACHE_ROOT_PATH, **kwargs):
        """
        The main callable for FileBackedFaaSCache and the function that should be
        called to create the cache.

        Tries to open an existing saved state if it exists else create a new
        object and save it
        """
        cls.file_path = cls.file_path_from_key_name(key_name, root_path=root_path)
        cls.lock_path = f"{cls.file_path}.lock"
        cls.old_path = f"{cls.file_path}.old"
        cls.file_lock = FileLock(cls.lock_path, timeout=-1)

        try:
            with cls.file_lock:
                with open(cls.file_path, "rb") as f:
                    return _do_pickle_file_load(f)
        except (EOFError, pickle.UnpicklingError):
            # This almost certainly means the pickled file did not finish fully writing
            # likely due to a wonky exit. Try and find an old version if exists.
            try:
                with cls.file_lock:
                    with open(cls.old_path, "rb") as f:
                        return _do_pickle_file_load(f)
            except (EOFError, FileNotFoundError):
                raise FileNotFoundError
        except FileNotFoundError:
            obj = FileBackedFaaSCache(*args, **kwargs)
            obj._self_to_disk()
            return obj

    @staticmethod
    def file_path_from_key_name(key_name, root_path):
        if not key_name.endswith(".faas"):
            key_name = f"{key_name}.faas"

        if root_path is None:
            return key_name

        if not root_path.endswith("/"):
            root_path = f"{root_path}/"

        return f"{root_path}{key_name}"

    def _self_to_disk(self):
        """Save self pickled state to disk"""
        if _FILE_FAAS_PICKLE_FLAG:
            return

        if not self.file_path:
            return
        try:
            with self.file_lock:
                try:
                    # We keep the old file in case we get a write error due to bad exit
                    os.remove(self.old_path)
                except FileNotFoundError:
                    pass

                try:
                    os.rename(self.file_path, self.old_path)
                except FileNotFoundError:
                    pass

                with open(self.file_path, "wb") as f:
                    real_lock = self._lock
                    self._lock = DummyLock()
                    pickle.dump(self, f, protocol=5)
                    self._lock = real_lock
        except Timeout:
            os.remove(self.lock_path)  # Assume because of old bad shutdown
            self._self_to_disk()

    def __setitem__(self, key, value, *args, **kwargs):
        """
        Set item and save new state to disk

        Has some custom logic for handling rehydrating a pickled object
        """
        if _FILE_FAAS_PICKLE_FLAG:
            expire, value = value
            r = super().__setitem__(key, value, *args, override_ttl=expire, **kwargs)
        else:
            r = super().__setitem__(key, value, *args, **kwargs)
        self._self_to_disk()

        return r

    def __delitem__(self, key):
        super().__delitem__(key)
        self._self_to_disk()

    def _purge_expired(self):
        super()._purge_expired()
        self._self_to_disk()

    def change_byte_size(self, max_size_bytes):
        super().change_byte_size(max_size_bytes)
        self._self_to_disk()


class DummyLock:
    """
    A dummy placeholder "lock" to use whilst saving to enable pickling
    """

    def __init__(self):
        pass

    def __enter__(self):
        return True

    def __exit__(self, exc_type, exc_val, exc_tb):
        return True
