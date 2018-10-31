from __future__ import print_function, division, absolute_import

import argparse
import sys

import skein
from tornado import gen
from tornado.ioloop import IOLoop, TimeoutError
from distributed import Scheduler, Nanny
from distributed.utils import ignoring
from distributed.cli.utils import install_signal_handlers, uri_from_host_port
from distributed.proctitle import (enable_proctitle_on_children,
                                   enable_proctitle_on_current)

if sys.version_info.major > 2:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

from . import __version__


class _Formatter(argparse.HelpFormatter):
    """Format with a fixed argument width, due to bug in argparse measuring
    argument widths"""
    @property
    def _action_max_length(self):
        return 16

    @_action_max_length.setter
    def _action_max_length(self, value):
        pass


class _VersionAction(argparse.Action):
    def __init__(self, option_strings, version=None, dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS, help="Show version then exit"):
        super(_VersionAction, self).__init__(option_strings=option_strings,
                                             dest=dest, default=default,
                                             nargs=0, help=help)
        self.version = version

    def __call__(self, parser, namespace, values, option_string=None):
        print(self.version % {'prog': parser.prog})
        sys.exit(0)


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def add_help(parser):
    parser.add_argument("--help", "-h", action='help',
                        help="Show this help message then exit")


def arg(*args, **kwargs):
    return (args, kwargs)


def subcommand(subparsers, name, help, *args):
    def _(func):
        parser = subparsers.add_parser(name,
                                       help=help,
                                       formatter_class=_Formatter,
                                       description=help,
                                       add_help=False)
        parser.set_defaults(func=func)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        add_help(parser)
        func.parser = parser
        return func
    return _


def node(subs, name, help):
    @subcommand(subs, name, help)
    def f():
        fail(f.parser.format_usage())
    f.subs = f.parser.add_subparsers(metavar='command', dest='command')
    f.subs.required = True
    return f


entry = argparse.ArgumentParser(prog="dask-yarn",
                                description="Deploy Dask on Apache YARN",
                                formatter_class=_Formatter,
                                add_help=False)
add_help(entry)
entry.add_argument("--version", action=_VersionAction,
                   version='%(prog)s ' + __version__,
                   help="Show version then exit")
entry.set_defaults(func=lambda: fail(entry.format_usage()))
entry_subs = entry.add_subparsers(metavar='command', dest='command')
entry_subs.required = True


@subcommand(entry_subs,
            'scheduler', 'Start the dask scheduler')
def scheduler():
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


@subcommand(entry_subs,
            'worker', 'Start a dask worker',
            arg("--nthreads", type=int, help="Number of threads"),
            arg("--memory_limit", help="Memory limit"))
def worker(nthreads=None, memory_limit=None):
    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if memory_limit is None:
        memory_limit = int(skein.properties.container_resources.memory * 2**20)
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


def main(args=None):
    kwargs = vars(entry.parse_args(args=args))
    kwargs.pop('command', None)  # Drop unnecessary `command` arg
    func = kwargs.pop('func')
    func(**kwargs)
    sys.exit(0)


if __name__ == '__main__':
    main()
