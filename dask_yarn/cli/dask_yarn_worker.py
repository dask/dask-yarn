from __future__ import print_function, division, absolute_import

import argparse

import skein
from distributed import Nanny
from distributed.utils import ignoring
from distributed.cli.utils import install_signal_handlers
from distributed.proctitle import (enable_proctitle_on_children,
                                   enable_proctitle_on_current)
from tornado.ioloop import IOLoop, TimeoutError
from tornado import gen


def main(args=None):
    parser = argparse.ArgumentParser(prog="dask-yarn-worker",
                                     description="Start a dask worker on YARN")
    parser.add_argument("nthreads", type=int, help="Number of threads")
    parser.add_argument("--memory_limit", help="Memory limit", default="auto")
    kwargs = vars(parser.parse_args(args=args))
    start_worker(**kwargs)


def start_worker(nthreads, memory_limit="auto"):
    enable_proctitle_on_current()
    enable_proctitle_on_children()

    services = {}
    with ignoring(ImportError):
        from distributed.bokeh.worker import BokehWorker
        services[('bokeh', 0)] = (BokehWorker, {})

    app_client = skein.ApplicationClient.from_current()

    scheduler = app_client.kv.wait('dask.scheduler')

    loop = IOLoop.current()

    nanny = Nanny(scheduler, ncores=nthreads, services=services, loop=loop,
                  memory_limit=memory_limit, worker_port=0)

    @gen.coroutine
    def close(signalnum):
        nanny._close(timeout=2)

    install_signal_handlers(loop, cleanup=close)

    @gen.coroutine
    def run():
        yield nanny._start(None)
        while nanny.status != 'closed':
            yield gen.sleep(0.2)

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass


if __name__ == '__main__':
    main()
