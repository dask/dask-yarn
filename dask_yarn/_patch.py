from __future__ import absolute_import, division, print_function

import os

import dask.config

if not hasattr(dask.config, 'PATH'):
    # Old versions of dask would try to write the configuration files to the
    # same directory, no matter the value of `DASK_CONFIG`. For these we wrap
    # `dask.config.ensure_file`.
    PATH = os.environ.get('DASK_CONFIG')
    if PATH is None:
        PATH = os.path.join(os.path.expanduser('~'), '.config', 'dask')

    _ensure_file = dask.config.ensure_file

    def ensure_file(source, destination=PATH, comment=True):
        try:
            return _ensure_file(source, destination=destination, comment=comment)
        except OSError:
            pass

    dask.config.ensure_file = ensure_file
