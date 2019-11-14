import asyncio
import math
import os
import sys
import warnings
import weakref
from contextlib import contextmanager
from urllib.parse import urlparse

import dask
from distributed.core import rpc
from distributed.deploy.adaptive_core import AdaptiveCore
from distributed.scheduler import Scheduler
from distributed.utils import (
    format_bytes,
    log_errors,
    LoopRunner,
    format_dashboard_link,
    parse_timedelta,
)

import skein
from skein.utils import cached_property


_memory_error = """Memory specification for the `{0}` take string parameters
with units like "4 GiB" or "2048 MiB"

You provided:       {1}
Perhaps you meant: "{1} MiB"
"""

_one_MiB = 2 ** 20


_widget_status_template = """
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table style="text-align: right;">
    <tr><th>Workers</th> <td>%d</td></tr>
    <tr><th>Cores</th> <td>%d</td></tr>
    <tr><th>Memory</th> <td>%s</td></tr>
</table>
</div>
"""


async def cancel_task(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


class DaskYarnError(skein.SkeinError):
    """An error involving the dask-yarn skein Application"""

    pass


@contextmanager
def submit_and_handle_failures(skein_client, spec):
    app_id = skein_client.submit(spec)
    try:
        try:
            yield skein_client.connect(app_id, security=spec.master.security)
        except BaseException:
            # Kill the application on any failures
            skein_client.kill_application(app_id)
            raise
    except (skein.ConnectionError, skein.ApplicationNotRunningError):
        # If the error was an application error, raise appropriately
        raise DaskYarnError(
            (
                "Failed to start dask-yarn {app_id}\n"
                "See the application logs for more information:\n\n"
                "$ yarn logs -applicationId {app_id}"
            ).format(app_id=app_id)
        )


def parse_memory(memory, field):
    if isinstance(memory, str):
        memory = dask.utils.parse_bytes(memory)
    elif memory < _one_MiB:
        raise ValueError(_memory_error.format(field, memory))
    return int(math.ceil(memory / _one_MiB))


def lookup(kwargs, a, b):
    return kwargs[a] if kwargs.get(a) is not None else dask.config.get(b)


def _get_skein_client(skein_client=None, security=None):
    if skein_client is None:
        # Silence warning about credentials not being written yet
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return skein.Client(security=security)
    return skein_client


def _files_and_build_script(environment):
    parsed = urlparse(environment)
    scheme = parsed.scheme

    if scheme in {"conda", "venv", "python"}:
        path = environment[len(scheme) + 3 :]
        files = {}
    else:
        # Treat archived environments the same as venvs
        scheme = "venv"
        path = "environment"
        files = {"environment": environment}

    if scheme == "conda":
        setup = "conda activate %s" % path
        cli = "dask-yarn"
    elif scheme == "venv":
        setup = "source %s/bin/activate" % path
        cli = "dask-yarn"
    else:
        setup = ""
        cli = "%s -m dask_yarn.cli" % path

    def build_script(cmd):
        command = "%s %s" % (cli, cmd)
        return "\n".join([setup, command]) if setup else command

    return files, build_script


def _make_specification(**kwargs):
    """ Create specification to run Dask Cluster

    This creates a ``skein.ApplicationSpec`` to run a dask cluster with the
    scheduler in a YARN container. See the docstring for ``YarnCluster`` for
    more details.
    """
    if all(v is None for v in kwargs.values()) and dask.config.get(
        "yarn.specification"
    ):
        # No overrides and full specification in configuration
        spec = dask.config.get("yarn.specification")
        if isinstance(spec, dict):
            return skein.ApplicationSpec.from_dict(spec)
        return skein.ApplicationSpec.from_file(spec)

    deploy_mode = lookup(kwargs, "deploy_mode", "yarn.deploy-mode")
    if deploy_mode not in {"remote", "local"}:
        raise ValueError(
            "`deploy_mode` must be one of {'remote', 'local'}, " "got %r" % deploy_mode
        )

    name = lookup(kwargs, "name", "yarn.name")
    queue = lookup(kwargs, "queue", "yarn.queue")
    tags = lookup(kwargs, "tags", "yarn.tags")
    user = lookup(kwargs, "user", "yarn.user")

    environment = lookup(kwargs, "environment", "yarn.environment")
    if environment is None:
        msg = (
            "You must provide a path to a Python environment for the workers.\n"
            "This may be one of the following:\n"
            "- A conda environment archived with conda-pack\n"
            "- A virtual environment archived with venv-pack\n"
            "- A path to a conda environment, specified as conda://...\n"
            "- A path to a virtual environment, specified as venv://...\n"
            "- A path to a python binary to use, specified as python://...\n"
            "\n"
            "See http://yarn.dask.org/environments.html for more information."
        )
        raise ValueError(msg)

    n_workers = lookup(kwargs, "n_workers", "yarn.worker.count")
    worker_restarts = lookup(kwargs, "worker_restarts", "yarn.worker.restarts")
    worker_env = lookup(kwargs, "worker_env", "yarn.worker.env")
    worker_vcores = lookup(kwargs, "worker_vcores", "yarn.worker.vcores")
    worker_memory = parse_memory(
        lookup(kwargs, "worker_memory", "yarn.worker.memory"), "worker"
    )

    services = {}

    files, build_script = _files_and_build_script(environment)

    if deploy_mode == "remote":
        scheduler_vcores = lookup(kwargs, "scheduler_vcores", "yarn.scheduler.vcores")
        scheduler_memory = parse_memory(
            lookup(kwargs, "scheduler_memory", "yarn.scheduler.memory"), "scheduler"
        )

        services["dask.scheduler"] = skein.Service(
            instances=1,
            resources=skein.Resources(vcores=scheduler_vcores, memory=scheduler_memory),
            max_restarts=0,
            files=files,
            script=build_script("services scheduler"),
        )
        worker_depends = ["dask.scheduler"]
    else:
        worker_depends = None

    services["dask.worker"] = skein.Service(
        instances=n_workers,
        resources=skein.Resources(vcores=worker_vcores, memory=worker_memory),
        max_restarts=worker_restarts,
        depends=worker_depends,
        files=files,
        env=worker_env,
        script=build_script("services worker"),
    )

    spec = skein.ApplicationSpec(
        name=name, queue=queue, tags=tags, user=user, services=services
    )
    return spec


def _make_submit_specification(script, args=(), **kwargs):
    spec = _make_specification(**kwargs)

    environment = lookup(kwargs, "environment", "yarn.environment")
    files, build_script = _files_and_build_script(environment)

    if "dask.scheduler" in spec.services:
        # deploy_mode == 'remote'
        client_vcores = lookup(kwargs, "client_vcores", "yarn.client.vcores")
        client_memory = lookup(kwargs, "client_memory", "yarn.client.memory")
        client_env = lookup(kwargs, "client_env", "yarn.client.env")
        client_memory = parse_memory(client_memory, "client")

        script_name = os.path.basename(script)
        files[script_name] = script

        spec.services["dask.client"] = skein.Service(
            instances=1,
            resources=skein.Resources(vcores=client_vcores, memory=client_memory),
            max_restarts=0,
            depends=["dask.scheduler"],
            files=files,
            env=client_env,
            script=build_script(
                "services client %s %s" % (script_name, " ".join(args))
            ),
        )
    return spec


def _make_scheduler_kwargs(**kwargs):
    host = lookup(kwargs, "host", "yarn.host")
    port = lookup(kwargs, "port", "yarn.port")
    dashboard_address = lookup(kwargs, "dashboard_address", "yarn.dashboard-address")
    return {"host": host, "port": port, "dashboard_address": dashboard_address}


class YarnCluster(object):
    """Start a Dask cluster on YARN.

    You can define default values for this in Dask's ``yarn.yaml``
    configuration file. See http://docs.dask.org/en/latest/configuration.html
    for more information.

    Parameters
    ----------
    environment : str, optional
        The Python environment to use. Can be one of the following:

          - A path to an archived Python environment
          - A path to a conda environment, specified as `conda:///...`
          - A path to a virtual environment, specified as `venv:///...`
          - A path to a python executable, specifed as `python:///...`

        Note that if not an archive, the paths specified must be valid on all
        nodes in the cluster.
    n_workers : int, optional
        The number of workers to initially start.
    worker_vcores : int, optional
        The number of virtual cores to allocate per worker.
    worker_memory : str, optional
        The amount of memory to allocate per worker. Accepts a unit suffix
        (e.g. '2 GiB' or '4096 MiB'). Will be rounded up to the nearest MiB.
    worker_restarts : int, optional
        The maximum number of worker restarts to allow before failing the
        application. Default is unlimited.
    worker_env : dict, optional
        A mapping of environment variables to their values. These will be set
        in the worker containers before starting the dask workers.
    scheduler_vcores : int, optional
        The number of virtual cores to allocate per scheduler.
    scheduler_memory : str, optional
        The amount of memory to allocate to the scheduler. Accepts a unit
        suffix (e.g. '2 GiB' or '4096 MiB'). Will be rounded up to the nearest
        MiB.
    deploy_mode : {'remote', 'local'}, optional
        The deploy mode to use. If ``'remote'``, the scheduler will be deployed
        in a YARN container. If ``'local'``, the scheduler will run locally,
        which can be nice for debugging. Default is ``'remote'``.
    name : str, optional
        The application name.
    queue : str, optional
        The queue to deploy to.
    tags : sequence, optional
        A set of strings to use as tags for this application.
    user : str, optional
        The user to submit the application on behalf of. Default is the current
        user - submitting as a different user requires user permissions, see
        the YARN documentation for more information.
    host : str, optional
        Host address on which the scheduler will listen. Only used if
        ``deploy_mode='local'``. Defaults to ``'0.0.0.0'``.
    port : int, optional
        The port on which the scheduler will listen. Only used if
        ``deploy_mode='local'``. Defaults to ``0`` for a random port.
    dashboard_address : str
        Address on which to the dashboard server will listen. Only used if
        ``deploy_mode='local'``. Defaults to ':0' for a random port.
    skein_client : skein.Client, optional
        The ``skein.Client`` to use. If not provided, one will be started.
    asynchronous : bool, optional
        If true, starts the cluster in asynchronous mode, where it can be used
        in other async code.
    loop : IOLoop, optional
        The IOLoop instance to use. Defaults to the current loop in
        asynchronous mode, otherwise a background loop is started.

    Examples
    --------
    >>> cluster = YarnCluster(environment='my-env.tar.gz', ...)
    >>> cluster.scale(10)
    """

    def __init__(
        self,
        environment=None,
        n_workers=None,
        worker_vcores=None,
        worker_memory=None,
        worker_restarts=None,
        worker_env=None,
        scheduler_vcores=None,
        scheduler_memory=None,
        deploy_mode=None,
        name=None,
        queue=None,
        tags=None,
        user=None,
        host=None,
        port=None,
        dashboard_address=None,
        skein_client=None,
        asynchronous=False,
        loop=None,
    ):
        spec = _make_specification(
            environment=environment,
            n_workers=n_workers,
            worker_vcores=worker_vcores,
            worker_memory=worker_memory,
            worker_restarts=worker_restarts,
            worker_env=worker_env,
            scheduler_vcores=scheduler_vcores,
            scheduler_memory=scheduler_memory,
            deploy_mode=deploy_mode,
            name=name,
            queue=queue,
            tags=tags,
            user=user,
        )
        self._init_common(
            spec=spec,
            host=host,
            port=port,
            dashboard_address=dashboard_address,
            asynchronous=asynchronous,
            loop=loop,
            skein_client=skein_client,
        )

    @classmethod
    def from_specification(cls, spec, skein_client=None, asynchronous=False, loop=None):
        """Start a dask cluster from a skein specification.

        Parameters
        ----------
        spec : skein.ApplicationSpec, dict, or filename
            The application specification to use. Must define at least one
            service: ``'dask.worker'``. If no ``'dask.scheduler'`` service is
            defined, a scheduler will be started locally.
        skein_client : skein.Client, optional
            The ``skein.Client`` to use. If not provided, one will be started.
        asynchronous : bool, optional
            If true, starts the cluster in asynchronous mode, where it can be
            used in other async code.
        loop : IOLoop, optional
            The IOLoop instance to use. Defaults to the current loop in
            asynchronous mode, otherwise a background loop is started.
        """
        self = super(YarnCluster, cls).__new__(cls)
        if isinstance(spec, dict):
            spec = skein.ApplicationSpec.from_dict(spec)
        elif isinstance(spec, str):
            spec = skein.ApplicationSpec.from_file(spec)
        elif not isinstance(spec, skein.ApplicationSpec):
            raise TypeError(
                "spec must be an ApplicationSpec, dict, or path, "
                "got %r" % type(spec).__name__
            )
        if "dask.worker" not in spec.services:
            raise ValueError(
                "Provided Skein specification must include a 'dask.worker' service"
            )

        self._init_common(
            spec=spec, asynchronous=asynchronous, loop=loop, skein_client=skein_client
        )
        return self

    @classmethod
    def from_current(cls, asynchronous=False, loop=None):
        """Connect to an existing ``YarnCluster`` from inside the cluster.

        Parameters
        ----------
        asynchronous : bool, optional
            If true, starts the cluster in asynchronous mode, where it can be
            used in other async code.
        loop : IOLoop, optional
            The IOLoop instance to use. Defaults to the current loop in
            asynchronous mode, otherwise a background loop is started.

        Returns
        -------
        YarnCluster
        """
        self = super(YarnCluster, cls).__new__(cls)
        app_id = os.environ.get("DASK_APPLICATION_ID", None)
        app_address = os.environ.get("DASK_APPMASTER_ADDRESS", None)
        security_dir = os.environ.get("DASK_SECURITY_CREDENTIALS", None)
        if app_id is not None and app_address is not None:
            security = (
                None
                if security_dir is None
                else skein.Security.from_directory(security_dir)
            )
            app = skein.ApplicationClient(app_address, app_id, security=security)
        else:
            app = skein.ApplicationClient.from_current()

        self._init_common(application_client=app, asynchronous=asynchronous, loop=loop)
        return self

    @classmethod
    def from_application_id(
        cls, app_id, skein_client=None, asynchronous=False, loop=None
    ):
        """Connect to an existing ``YarnCluster`` with a given application id.

        Parameters
        ----------
        app_id : str
            The existing cluster's application id.
        skein_client : skein.Client
            The ``skein.Client`` to use. If not provided, one will be started.
        asynchronous : bool, optional
            If true, starts the cluster in asynchronous mode, where it can be
            used in other async code.
        loop : IOLoop, optional
            The IOLoop instance to use. Defaults to the current loop in
            asynchronous mode, otherwise a background loop is started.

        Returns
        -------
        YarnCluster
        """
        self = super(YarnCluster, cls).__new__(cls)
        skein_client = _get_skein_client(skein_client)
        app = skein_client.connect(app_id)

        self._init_common(
            application_client=app,
            asynchronous=asynchronous,
            loop=loop,
            skein_client=skein_client,
        )
        return self

    def _init_common(
        self,
        spec=None,
        application_client=None,
        host=None,
        port=None,
        dashboard_address=None,
        asynchronous=False,
        loop=None,
        skein_client=None,
    ):
        self.spec = spec
        self.application_client = application_client
        self._scheduler_kwargs = _make_scheduler_kwargs(
            host=host, port=port, dashboard_address=dashboard_address,
        )
        self._scheduler = None
        self.scheduler_info = {}
        self._requested = set()
        self.scheduler_comm = None
        self._watch_worker_status_task = None
        self._start_task = None
        self._stop_task = None
        self._finalizer = None
        self._adaptive = None
        self._adaptive_options = {}
        self._skein_client = skein_client
        self._asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self._loop_runner.start()

        if not self.asynchronous:
            self._sync(self._start_internal())

    def _start_cluster(self):
        """Start the cluster and initialize state"""

        skein_client = _get_skein_client(self._skein_client)

        if "dask.scheduler" not in self.spec.services:
            # deploy_mode == 'local'
            scheduler_address = self._scheduler.address
            for k in ["dashboard", "bokeh"]:
                if k in self._scheduler.services:
                    dashboard_port = self._scheduler.services[k].port
                    dashboard_host = urlparse(scheduler_address).hostname
                    dashboard_address = "http://%s:%d" % (
                        dashboard_host,
                        dashboard_port,
                    )
                    break
            else:
                dashboard_address = None

            with submit_and_handle_failures(skein_client, self.spec) as app:
                app.kv["dask.scheduler"] = scheduler_address.encode()
                if dashboard_address is not None:
                    app.kv["dask.dashboard"] = dashboard_address.encode()
        else:
            # deploy_mode == 'remote'
            with submit_and_handle_failures(skein_client, self.spec) as app:
                scheduler_address = app.kv.wait("dask.scheduler").decode()
                dashboard_address = app.kv.get("dask.dashboard")
                if dashboard_address is not None:
                    dashboard_address = dashboard_address.decode()

        # Ensure application gets cleaned up
        self._finalizer = weakref.finalize(self, app.shutdown)

        self.scheduler_address = scheduler_address
        self._dashboard_address = dashboard_address
        self.application_client = app

    def _connect_existing(self):
        spec = self.application_client.get_specification()
        if "dask.worker" not in spec.services:
            raise ValueError("%r is not a valid dask cluster" % self.app_id)

        scheduler_address = self.application_client.kv.wait("dask.scheduler").decode()
        dashboard_address = self.application_client.kv.get("dask.dashboard")
        if dashboard_address is not None:
            dashboard_address = dashboard_address.decode()

        self.spec = spec
        self.scheduler_address = scheduler_address
        self._dashboard_address = dashboard_address

    async def _start_internal(self):
        if self._start_task is None:
            self._start_task = asyncio.ensure_future(self._start_async())
        try:
            await self._start_task
        except BaseException:
            # On exception, cleanup
            await self._stop_internal()
            raise
        return self

    async def _start_async(self):
        if self.spec is not None:
            # Start a new cluster
            if "dask.scheduler" not in self.spec.services:
                self._scheduler = Scheduler(loop=self.loop, **self._scheduler_kwargs,)
                await self._scheduler
            else:
                self._scheduler = None
            await self.loop.run_in_executor(None, self._start_cluster)
        else:
            # Connect to an existing cluster
            await self.loop.run_in_executor(None, self._connect_existing)

        self.scheduler_comm = rpc(self.scheduler_address)
        comm = None
        try:
            comm = await self.scheduler_comm.live_comm()
            await comm.write({"op": "subscribe_worker_status"})
            self.scheduler_info = await comm.read()
            workers = self.scheduler_info.get("workers", {})
            self._requested.update(w["name"] for w in workers.values())
            self._watch_worker_status_task = asyncio.ensure_future(
                self._watch_worker_status(comm)
            )
        except Exception:
            if comm is not None:
                await comm.close()

    async def _stop_internal(self, status="SUCCEEDED", diagnostics=None):
        if self._stop_task is None:
            self._stop_task = asyncio.ensure_future(
                self._stop_async(status=status, diagnostics=diagnostics)
            )
        await self._stop_task

    async def _stop_async(self, status="SUCCEEDED", diagnostics=None):
        if self._start_task is not None:
            if not self._start_task.done():
                # We're still starting, cancel task
                await cancel_task(self._start_task)
            self._start_task = None

        if self._adaptive is not None:
            self._adaptive.stop()

        if self._watch_worker_status_task is not None:
            await cancel_task(self._watch_worker_status_task)
            self._watch_worker_status_task = None

        if self.scheduler_comm is not None:
            self.scheduler_comm.close_rpc()
            self.scheduler_comm = None

        await self.loop.run_in_executor(
            None, lambda: self._stop_sync(status=status, diagnostics=diagnostics)
        )

        if self._scheduler is not None:
            await self._scheduler.close()
            self._scheduler = None

    def _stop_sync(self, status="SUCCEEDED", diagnostics=None):
        if self._finalizer is not None and self._finalizer.peek() is not None:
            self.application_client.shutdown(status=status, diagnostics=diagnostics)
            self._finalizer.detach()  # don't run the finalizer later
        self._finalizer = None

    def __await__(self):
        return self.__aenter__().__await__()

    async def __aenter__(self):
        return await self._start_internal()

    async def __aexit__(self, typ, value, traceback):
        await self._stop_internal()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        return "YarnCluster<%s>" % self.app_id

    @property
    def loop(self):
        return self._loop_runner.loop

    @property
    def app_id(self):
        return self.application_client.id

    @property
    def asynchronous(self):
        return self._asynchronous

    def _sync(self, task):
        if self.asynchronous:
            return task
        future = asyncio.run_coroutine_threadsafe(task, self.loop.asyncio_loop)
        try:
            return future.result()
        except BaseException:
            future.cancel()
            raise

    @cached_property
    def dashboard_link(self):
        """Link to the dask dashboard. None if dashboard isn't running"""
        if self._dashboard_address is None:
            return None
        dashboard = urlparse(self._dashboard_address)
        return format_dashboard_link(dashboard.hostname, dashboard.port)

    def shutdown(self, status="SUCCEEDED", diagnostics=None):
        """Shutdown the application.

        Parameters
        ----------
        status : {'SUCCEEDED', 'FAILED', 'KILLED'}, optional
            The yarn application exit status.
        diagnostics : str, optional
            The application exit message, usually used for diagnosing failures.
            Can be seen in the YARN Web UI for completed applications under
            "diagnostics". If not provided, a default will be used.
        """
        if self.asynchronous:
            return self._stop_internal(status=status, diagnostics=diagnostics)
        if self.loop.asyncio_loop.is_running() and not sys.is_finalizing():
            self._sync(self._stop_internal(status=status, diagnostics=diagnostics))
        else:
            # Always run this!
            self._stop_sync(status=status, diagnostics=diagnostics)
        self._loop_runner.stop()

    def close(self, **kwargs):
        """Close this cluster. An alias for ``shutdown``.

        See Also
        --------
        shutdown
        """
        return self.shutdown(**kwargs)

    def __del__(self):
        if not hasattr(self, "_loop_runner"):
            return
        if self.asynchronous:
            # No del for async mode
            return
        self.close()

    @property
    def _observed(self):
        return {w["name"] for w in self.scheduler_info["workers"].values()}

    async def _workers(self):
        return await self.loop.run_in_executor(
            None,
            lambda: self.application_client.get_containers(services=["dask.worker"]),
        )

    def workers(self):
        """A list of all currently running worker containers."""
        return self._sync(self._workers())

    async def _scale_up(self, n):
        if n > len(self._requested):
            containers = await self.loop.run_in_executor(
                None, lambda: self.application_client.scale("dask.worker", n),
            )
            self._requested.update(c.id for c in containers)

    async def _scale_down(self, workers):
        self._requested.difference_update(workers)
        await self.scheduler_comm.retire_workers(names=list(workers))

        def _kill_containers():
            for c in workers:
                try:
                    self.application_client.kill_container(c)
                except ValueError:
                    pass

        await self.loop.run_in_executor(
            None, _kill_containers,
        )

    async def _scale(self, n):
        if self._adaptive is not None:
            self._adaptive.stop()
        if n >= len(self._requested):
            return await self._scale_up(n)
        else:
            n_to_delete = len(self._requested) - n
            pending = list(self._requested - self._observed)
            running = list(self._observed.difference(pending))
            to_close = pending[:n_to_delete]
            to_close.extend(running[: n_to_delete - len(to_close)])
            await self._scale_down(to_close)
            self._requested.difference_update(to_close)

    def scale(self, n):
        """Scale cluster to n workers.

        Parameters
        ----------
        n : int
            Target number of workers

        Examples
        --------
        >>> cluster.scale(10)  # scale cluster to ten workers
        """
        return self._sync(self._scale(n))

    def adapt(
        self,
        minimum=0,
        maximum=math.inf,
        interval="1s",
        wait_count=3,
        target_duration="5s",
        **kwargs
    ):
        """Turn on adaptivity

        This scales Dask clusters automatically based on scheduler activity.

        Parameters
        ----------
        minimum : int, optional
            Minimum number of workers. Defaults to ``0``.
        maximum : int, optional
            Maximum number of workers. Defaults to ``inf``.
        interval : timedelta or str, optional
            Time between worker add/remove recommendations.
        wait_count : int, optional
            Number of consecutive times that a worker should be suggested for
            removal before we remove it.
        target_duration : timedelta or str, optional
            Amount of time we want a computation to take. This affects how
            aggressively we scale up.
        **kwargs :
            Additional parameters to pass to
            ``distributed.Scheduler.workers_to_close``.

        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum=10)
        """
        if self._adaptive is not None:
            self._adaptive.stop()
        self._adaptive_options.update(
            minimum=minimum,
            maximum=maximum,
            interval=interval,
            wait_count=wait_count,
            target_duration=target_duration,
            **kwargs,
        )
        self._adaptive = Adaptive(self, **self._adaptive_options)

    async def _watch_worker_status(self, comm):
        # We don't want to hold on to a ref to self, otherwise this will
        # leave a dangling reference and prevent garbage collection.
        ref_self = weakref.ref(self)
        self = None
        try:
            while True:
                try:
                    msgs = await comm.read()
                except OSError:
                    break
                try:
                    self = ref_self()
                    if self is None:
                        break
                    for op, msg in msgs:
                        if op == "add":
                            workers = msg.pop("workers")
                            self.scheduler_info["workers"].update(workers)
                            self.scheduler_info.update(msg)
                            self._requested.update(w["name"] for w in workers.values())
                        elif op == "remove":
                            del self.scheduler_info["workers"][msg]
                            self._requested.discard(msg)
                    if hasattr(self, "_status_widget"):
                        self._status_widget.value = self._widget_status()
                finally:
                    self = None
        finally:
            await comm.close()

    def _widget_status(self):
        try:
            workers = self.scheduler_info["workers"]
        except KeyError:
            return None
        else:
            n_workers = len(workers)
            cores = sum(w["nthreads"] for w in workers.values())
            memory = sum(w["memory_limit"] for w in workers.values())

            return _widget_status_template % (n_workers, cores, format_bytes(memory))

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        if self.asynchronous:
            return None

        try:
            from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML, Accordion
        except ImportError:
            self._cached_widget = None
            return None

        layout = Layout(width="150px")

        title = HTML("<h2>YarnCluster</h2>")

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        request = IntText(0, description="Workers", layout=layout)
        scale = Button(description="Scale", layout=layout)

        minimum = IntText(0, description="Minimum", layout=layout)
        maximum = IntText(0, description="Maximum", layout=layout)
        adapt = Button(description="Adapt", layout=layout)

        accordion = Accordion(
            [HBox([request, scale]), HBox([minimum, maximum, adapt])],
            layout=Layout(min_width="500px"),
        )
        accordion.selected_index = None
        accordion.set_title(0, "Manual Scaling")
        accordion.set_title(1, "Adaptive Scaling")

        @adapt.on_click
        def adapt_cb(b):
            self.adapt(minimum=minimum.value, maximum=maximum.value)

        @scale.on_click
        def scale_cb(b):
            with log_errors():
                self.scale(request.value)

        app_id = HTML("<p><b>Application ID: </b>{0}</p>".format(self.app_id))

        elements = [title, HBox([status, accordion]), app_id]

        if self.dashboard_link is not None:
            link = HTML(
                '<p><b>Dashboard: </b><a href="{0}" target="_blank">{0}'
                "</a></p>\n".format(self.dashboard_link)
            )
            elements.append(link)

        self._cached_widget = box = VBox(elements)
        self._status_widget = status

        return box

    def _ipython_display_(self, **kwargs):
        widget = self._widget()
        if widget is not None:
            return widget._ipython_display_(**kwargs)
        else:
            from IPython.display import display

            data = {"text/plain": repr(self), "text/html": self._repr_html_()}
            display(data, raw=True)

    def _repr_html_(self):
        if self.dashboard_link is not None:
            dashboard = "<a href='{0}' target='_blank'>{0}</a>".format(
                self.dashboard_link
            )
        else:
            dashboard = "Not Available"
        return (
            "<div style='background-color: #f2f2f2; display: inline-block; "
            "padding: 10px; border: 1px solid #999999;'>\n"
            "  <h3>YarnCluster</h3>\n"
            "  <ul>\n"
            "    <li><b>Application ID: </b>{app_id}\n"
            "    <li><b>Dashboard: </b>{dashboard}\n"
            "  </ul>\n"
            "</div>\n"
        ).format(app_id=self.app_id, dashboard=dashboard)


class Adaptive(AdaptiveCore):
    def __init__(
        self,
        cluster=None,
        interval="1s",
        minimum=0,
        maximum=math.inf,
        wait_count=3,
        target_duration="5s",
        **kwargs
    ):
        self.cluster = cluster
        self.target_duration = parse_timedelta(target_duration)
        self._workers_to_close_kwargs = kwargs

        super().__init__(
            minimum=minimum, maximum=maximum, wait_count=wait_count, interval=interval
        )

    @property
    def requested(self):
        return self.cluster._requested

    plan = requested

    @property
    def observed(self):
        return self.cluster._observed

    async def target(self):
        return await self.cluster.scheduler_comm.adaptive_target(
            target_duration=self.target_duration
        )

    async def workers_to_close(self, target):
        return await self.cluster.scheduler_comm.workers_to_close(
            target=target, attribute="name", **self._workers_to_close_kwargs
        )

    async def scale_down(self, workers):
        await self.cluster._scale_down(workers)

    async def scale_up(self, n):
        await self.cluster._scale_up(n)

    @property
    def loop(self):
        return self.cluster.loop
