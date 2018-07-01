from __future__ import absolute_import, print_function, division

# Patch old versions of dask before importing anything else
from . import _patch
del _patch

from .core import YarnCluster, make_specification
