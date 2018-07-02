from __future__ import absolute_import, print_function, division

# Patch old versions of dask before importing anything else
from . import _patch
from . import config
del _patch, config

from .core import YarnCluster

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
