import sys

__version__ = '0.0.5'


if sys.version_info < (3, 7):
    raise SystemError('Python 3.7 or newer required.')

from faas_cache_dict.faas_cache_dict import FaaSCacheDict  # noqa
