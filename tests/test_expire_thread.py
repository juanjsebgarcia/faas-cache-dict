import gc
import time

import objsize

from faas_cache_dict import FaaSCacheDict


def test_purge_thread_alive():
    faas = FaaSCacheDict()
    time.sleep(1)
    assert faas._purge_thread.is_alive


def test_purge_thread_working():
    faas = FaaSCacheDict(default_ttl=2)
    for i in range(1000):
        faas[i] = i * 10
    time.sleep(1)
    assert faas._purge_thread.is_alive
    prev_size = objsize.get_deep_size(faas)
    assert prev_size > 40000
    time.sleep(faas._auto_purge_seconds + 2)
    gc.collect()
    assert objsize.get_deep_size(faas) < prev_size / 1.25
