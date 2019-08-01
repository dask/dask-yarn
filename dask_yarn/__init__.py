# Load configuration
from . import config
del config

from .core import YarnCluster

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
