import sys

__version__ = '0.2.4'


if sys.version_info < (3, 8):
    raise SystemError('Python 3.8 or newer required.')

from faas_cache_dict.faas_cache_dict import FaaSCacheDict  # noqa
from faas_cache_dict.file_faas_cache_dict import FileBackedFaaSCache  # noqa
