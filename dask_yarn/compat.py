from __future__ import absolute_import, division, print_function

import sys

PY2 = sys.version_info.major == 2

if PY2:
    from urlparse import urlparse
    from backports import weakref
else:
    import weakref  # noqa
    from urllib.parse import urlparse  # noqa
