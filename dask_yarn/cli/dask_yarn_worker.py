from __future__ import print_function, division, absolute_import

import argparse

import skein
from distributed import Nanny
from distributed.cli.utils import install_signal_handlers
from distributed.proctitle import (enable_proctitle_on_children,
                                   enable_proctitle_on_current)
from tornado.ioloop import IOLoop, TimeoutError
from tornado import gen


def main(args=None):
    parser = argparse.ArgumentParser(prog="dask-yarn-worker",
                                     description="Start a dask worker on YARN")
    parser.add_argument("--nthreads", type=int, help="Number of threads")
    parser.add_argument("--memory_limit", help="Memory limit")
    kwargs = vars(parser.parse_args(args=args))
    start_worker(**kwargs)


def start_worker(nthreads=None, memory_limit=None):
    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if memory_limit is None:
        memory_limit = int(skein.properties.container_resources.memory * 1e6)
    if nthreads is None:
        nthreads = skein.properties.container_resources.vcores

    app_client = skein.ApplicationClient.from_current()

    scheduler = app_client.kv.wait('dask.scheduler').decode()

    loop = IOLoop.current()

    worker = Nanny(scheduler, ncores=nthreads, loop=loop,
                   memory_limit=memory_limit, worker_port=0)

    @gen.coroutine
    def close(signalnum):
        worker._close(timeout=2)

    install_signal_handlers(loop, cleanup=close)

    @gen.coroutine
    def run():
        yield worker._start(None)
        while worker.status != 'closed':
            yield gen.sleep(0.2)

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass


if __name__ == '__main__':
    main()
