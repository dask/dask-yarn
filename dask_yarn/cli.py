import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from urllib.parse import urlparse

import skein
from skein.utils import format_table, humanize_timedelta
from tornado.ioloop import IOLoop, TimeoutError
from distributed import Scheduler, Nanny
from distributed.cli.utils import install_signal_handlers
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)

from . import __version__
from .core import _make_submit_specification, YarnCluster, _get_skein_client


class _Formatter(argparse.HelpFormatter):
    """Format with a fixed argument width, due to bug in argparse measuring
    argument widths"""

    @property
    def _action_max_length(self):
        return 16

    @_action_max_length.setter
    def _action_max_length(self, value):
        pass

    def _format_args(self, action, default_metavar):
        """Format remainder arguments nicer"""
        get_metavar = self._metavar_formatter(action, default_metavar)
        if action.nargs == argparse.REMAINDER:
            return "[%s...]" % get_metavar(1)
        return super(_Formatter, self)._format_args(action, default_metavar)


class _VersionAction(argparse.Action):
    def __init__(
        self,
        option_strings,
        version=None,
        dest=argparse.SUPPRESS,
        default=argparse.SUPPRESS,
        help="Show version then exit",
    ):
        super(_VersionAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help,
        )
        self.version = version

    def __call__(self, parser, namespace, values, option_string=None):
        print(self.version % {"prog": parser.prog})
        sys.exit(0)


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def add_help(parser):
    parser.add_argument(
        "--help", "-h", action="help", help="Show this help message then exit"
    )


def arg(*args, **kwargs):
    return (args, kwargs)


def subcommand(subparsers, name, help, *args):
    def _(func):
        parser = subparsers.add_parser(
            name,
            help=help,
            formatter_class=_Formatter,
            description=help,
            add_help=False,
        )
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

    f.subs = f.parser.add_subparsers(metavar="command", dest="command")
    f.subs.required = True
    return f


entry = argparse.ArgumentParser(
    prog="dask-yarn",
    description="Deploy Dask on Apache YARN",
    formatter_class=_Formatter,
    add_help=False,
)
add_help(entry)
entry.add_argument(
    "--version",
    action=_VersionAction,
    version="%(prog)s " + __version__,
    help="Show version then exit",
)
entry.set_defaults(func=lambda: fail(entry.format_usage()))
entry_subs = entry.add_subparsers(metavar="command", dest="command")
entry_subs.required = True


def _parse_env(service, env):
    out = {}
    if env is None:
        return out
    for item in env:
        elements = item.split("=")
        if len(elements) != 2:
            raise ValueError("Invalid parameter to --%s-env: %r" % (service, env))
        key, val = elements
        out[key.strip()] = val.strip()
    return out


# Exposed for testing
def _parse_submit_kwargs(**kwargs):
    if kwargs.get("worker_env") is not None:
        kwargs["worker_env"] = _parse_env("worker", kwargs["worker_env"])
    if kwargs.get("client_env") is not None:
        kwargs["client_env"] = _parse_env("client", kwargs["client_env"])
    if kwargs.get("tags") is not None:
        kwargs["tags"] = set(map(str.strip, kwargs["tags"].split(",")))
    if kwargs.get("worker_count") is not None:
        kwargs["n_workers"] = kwargs.pop("worker_count")
    return kwargs


@subcommand(
    entry_subs,
    "submit",
    "Submit a Dask application to a YARN cluster",
    arg("script", help="Path to a python script to run on the client"),
    arg(
        "args",
        nargs=argparse.REMAINDER,
        help="Any additional arguments to forward to `script`",
    ),
    arg("--name", help="The application name"),
    arg("--queue", help="The queue to deploy to"),
    arg(
        "--user",
        help=(
            "The user to submit the application on behalf of. Default "
            "is the current user - submitting as a different user "
            "requires proxy-user permissions."
        ),
    ),
    arg(
        "--tags",
        help=(
            "A comma-separated list of strings to use as " "tags for this application."
        ),
    ),
    arg(
        "--environment",
        help=(
            "Path to the Python environment to use. See the docs "
            "for more information"
        ),
    ),
    arg(
        "--deploy-mode",
        help=(
            "Either 'remote' (default) or 'local'. If 'remote', the "
            "scheduler and client will be deployed in a YARN "
            "container. If 'local', they will be run locally."
        ),
    ),
    arg("--worker-count", type=int, help="The number of workers to initially start."),
    arg(
        "--worker-vcores",
        type=int,
        help="The number of virtual cores to allocate per worker.",
    ),
    arg(
        "--worker-memory",
        type=str,
        help=(
            "The amount of memory to allocate per worker. Accepts a "
            "unit suffix (e.g. '2 GiB' or '4096 MiB'). Will be "
            "rounded up to the nearest MiB."
        ),
    ),
    arg(
        "--worker-restarts",
        type=int,
        help=(
            "The maximum number of worker restarts to allow before "
            "failing the application. Default is unlimited."
        ),
    ),
    arg(
        "--worker-env",
        type=str,
        action="append",
        help=(
            "Environment variables to set on the workers. Pass a "
            "key-value pair like ``--worker-env key=val``. May "
            "be used more than once."
        ),
    ),
    arg(
        "--client-vcores",
        type=int,
        help="The number of virtual cores to allocate for the client.",
    ),
    arg(
        "--client-memory",
        type=str,
        help=(
            "The amount of memory to allocate for the client. "
            "Accepts a unit suffix (e.g. '2 GiB' or '4096 MiB'). "
            "Will be rounded up to the nearest MiB."
        ),
    ),
    arg(
        "--client-env",
        type=str,
        action="append",
        help=(
            "Environment variables to set on the client. Pass a "
            "key-value pair like ``--client-env key=val``. May "
            "be used more than once."
        ),
    ),
    arg(
        "--scheduler-vcores",
        type=int,
        help="The number of virtual cores to allocate for the scheduler.",
    ),
    arg(
        "--scheduler-memory",
        type=str,
        help=(
            "The amount of memory to allocate for the scheduler. "
            "Accepts a unit suffix (e.g. '2 GiB' or '4096 MiB'). "
            "Will be rounded up to the nearest MiB."
        ),
    ),
    arg(
        "--temporary-security-credentials",
        action="store_true",
        help=(
            "Instead of using a consistent set of TLS credentials "
            "for all clusters, create a fresh set just for this "
            "application."
        ),
    ),
)
def submit(script, args=None, temporary_security_credentials=False, **kwargs):
    kwargs = _parse_submit_kwargs(**kwargs)
    args = args or []
    spec = _make_submit_specification(script, args=args, **kwargs)

    if temporary_security_credentials:
        security = skein.Security.new_credentials()
    else:
        security = None

    skein_client = _get_skein_client(security=security)

    if "dask.scheduler" in spec.services:
        # deploy_mode == 'remote'
        app_id = skein_client.submit(spec)
        print(app_id)
    else:
        # deploy_mode == 'local'
        if not os.path.exists(script):
            raise ValueError("%r doesn't exist locally" % script)

        with maybe_tempdir(
            temporary_security_credentials
        ) as security_dir, YarnCluster.from_specification(
            spec, skein_client=skein_client
        ) as cluster:
            env = dict(os.environ)
            env.update(
                {
                    "DASK_APPLICATION_ID": cluster.app_id,
                    "DASK_APPMASTER_ADDRESS": cluster.application_client.address,
                }
            )
            if temporary_security_credentials:
                security.to_directory(security_dir)
                env["DASK_SECURITY_CREDENTIALS"] = security_dir

            retcode = subprocess.call([sys.executable, script] + args, env=env)

            if retcode == 0:
                cluster.shutdown("SUCCEEDED")
            else:
                cluster.shutdown(
                    "FAILED",
                    "Exception in submitted dask application, "
                    "see logs for more details",
                )
                sys.exit(retcode)


@contextmanager
def maybe_tempdir(create=False):
    """Contextmanager for consistent syntax for maybe creating a tempdir"""
    if create:
        try:
            path = tempfile.mkdtemp()
            yield path
        finally:
            shutil.rmtree(path)
    else:
        yield None


app_id = arg("app_id", help="The application id", metavar="APP_ID")


@subcommand(
    entry_subs, "status", "Check the status of a submitted Dask application", app_id
)
def status(app_id):
    report = _get_skein_client().application_report(app_id)
    header = [
        "application_id",
        "name",
        "state",
        "status",
        "containers",
        "vcores",
        "memory",
        "runtime",
    ]
    data = [
        (
            report.id,
            report.name,
            report.state,
            report.final_status,
            report.usage.num_used_containers,
            report.usage.used_resources.vcores,
            report.usage.used_resources.memory,
            humanize_timedelta(report.runtime),
        )
    ]
    print(format_table(header, data))


@subcommand(entry_subs, "kill", "Kill a Dask application", app_id)
def kill(app_id):
    _get_skein_client().kill_application(app_id)


services = node(entry_subs, "services", "Manage Dask services")


@subcommand(services.subs, "scheduler", "Start a Dask scheduler process")
def scheduler():  # pragma: nocover
    app_client = skein.ApplicationClient.from_current()

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if sys.platform.startswith("linux"):
        import resource  # module fails importing on Windows

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    loop = IOLoop.current()
    scheduler = Scheduler(loop=loop, dashboard_address=("", 0))
    install_signal_handlers(loop)

    def post_addresses():
        # Set dask.dashboard before dask.scheduler since the YarnCluster object
        # waits on dask.scheduler only
        if "dashboard" in scheduler.services:
            bokeh_port = scheduler.services["dashboard"].port
            bokeh_host = urlparse(scheduler.address).hostname
            bokeh_address = "http://%s:%d" % (bokeh_host, bokeh_port)
            app_client.kv["dask.dashboard"] = bokeh_address.encode()
        app_client.kv["dask.scheduler"] = scheduler.address.encode()

    async def run():
        await scheduler
        await loop.run_in_executor(None, post_addresses)
        await scheduler.finished()

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass
    finally:
        scheduler.stop()


@subcommand(
    services.subs,
    "worker",
    "Start a Dask worker process",
    arg(
        "--nthreads",
        type=int,
        help=("Number of threads. Defaults to number of vcores in " "container"),
    ),
    arg(
        "--memory_limit",
        type=str,
        help=(
            "Maximum memory available to the worker. This can be an "
            "integer (in bytes), a string (like '5 GiB' or '500 "
            "MiB'), or 0 (no memory management). Defaults to the "
            "container memory limit."
        ),
    ),
)
def worker(nthreads=None, memory_limit=None):  # pragma: nocover
    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if memory_limit is None:
        memory_limit = int(skein.properties.container_resources.memory * 2 ** 20)
    if nthreads is None:
        nthreads = skein.properties.container_resources.vcores

    app_client = skein.ApplicationClient.from_current()

    scheduler = app_client.kv.wait("dask.scheduler").decode()

    loop = IOLoop.current()

    worker = Nanny(
        scheduler,
        loop=loop,
        memory_limit=memory_limit,
        worker_port=0,
        nthreads=nthreads,
        name=skein.properties.container_id,
    )

    async def cleanup():
        await worker.close(timeout=2)

    install_signal_handlers(loop, cleanup=cleanup)

    async def run():
        await worker
        await worker.finished()

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass


@subcommand(
    services.subs,
    "client",
    "Start a Dask client process",
    arg("script", help="Path to a Python script to run."),
    arg(
        "args",
        nargs=argparse.REMAINDER,
        help="Any additional arguments to forward to `script`",
    ),
)
def client(script, args=None):  # pragma: nocover
    app = skein.ApplicationClient.from_current()
    args = args or []

    if not os.path.exists(script):
        raise ValueError("%r doesn't exist" % script)

    retcode = subprocess.call([sys.executable, script] + args)

    if retcode == 0:
        app.shutdown("SUCCEEDED")
    else:
        print(
            "User submitted application %s failed with returncode "
            "%d, shutting down." % (script, retcode)
        )
        app.shutdown(
            "FAILED",
            "Exception in submitted dask application, " "see logs for more details",
        )


def main(args=None):
    kwargs = vars(entry.parse_args(args=args))
    kwargs.pop("command", None)  # Drop unnecessary `command` arg
    func = kwargs.pop("func")
    func(**kwargs)
    sys.exit(0)


if __name__ == "__main__":  # pragma: nocover
    main()
