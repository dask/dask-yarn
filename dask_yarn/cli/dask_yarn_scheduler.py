from __future__ import print_function, division, absolute_import

import sys

import skein
from distributed import Scheduler
from distributed.utils import ignoring
from distributed.cli.utils import install_signal_handlers, uri_from_host_port
from distributed.proctitle import (enable_proctitle_on_children,
                                   enable_proctitle_on_current)
from tornado.ioloop import IOLoop


if sys.version_info.major > 2:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


def main():
    app_client = skein.ApplicationClient.from_current()

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if sys.platform.startswith('linux'):
        import resource   # module fails importing on Windows
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    addr = uri_from_host_port('', None, 0)

    loop = IOLoop.current()

    services = {}
    bokeh = False
    with ignoring(ImportError):
        from distributed.bokeh.scheduler import BokehScheduler
        services[('bokeh', 0)] = (BokehScheduler, {})
        bokeh = True

    scheduler = Scheduler(loop=loop, services=services)
    scheduler.start(addr)

    install_signal_handlers(loop)

    app_client.kv['dask.scheduler'] = scheduler.address.encode()

    if bokeh:
        bokeh_port = scheduler.services['bokeh'].port
        bokeh_host = urlparse(scheduler.address).hostname
        bokeh_address = 'http://%s:%d' % (bokeh_host, bokeh_port)

        app_client.kv['dask.dashboard'] = bokeh_address.encode()

    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()


if __name__ == '__main__':
    main()
