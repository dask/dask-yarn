from __future__ import absolute_import, print_function, division

import math
import os
import warnings
from contextlib import contextmanager
from distutils.version import LooseVersion

import dask
import distributed
from dask.distributed import get_client, LocalCluster
from distributed.utils import format_bytes, PeriodicCallback, log_errors

import skein
from skein.utils import cached_property

from .compat import weakref, urlparse


_memory_error = """Memory specification for the `{0}` take string parameters
with units like "4 GiB" or "2048 MiB"

You provided:       {1}
Perhaps you meant: "{1} MiB"
"""

_one_MiB = 2**20


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
            ("Failed to start dask-yarn {app_id}\n"
             "See the application logs for more information:\n\n"
             "$ yarn logs -applicationId {app_id}").format(app_id=app_id)
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
            warnings.simplefilter('ignore')
            return skein.Client(security=security)
    return skein_client


# exposed for testing only

def _files_and_build_script(environment):
    parsed = urlparse(environment)
    scheme = parsed.scheme

    if scheme in {'conda', 'venv', 'python'}:
        path = environment[len(scheme) + 3:]
        files = {}
    else:
        # Treat archived environments the same as venvs
        scheme = 'venv'
        path = 'environment'
        files = {'environment': environment}

    if scheme == 'conda':
        setup = 'conda activate %s' % path
        cli = 'dask-yarn'
    elif scheme == 'venv':
        setup = 'source %s/bin/activate' % path
        cli = 'dask-yarn'
    else:
        setup = ''
        cli = '%s -m dask_yarn.cli' % path

    def build_script(cmd):
        command = '%s %s' % (cli, cmd)
        return '\n'.join([setup, command]) if setup else command

    return files, build_script


def _make_specification(**kwargs):
    """ Create specification to run Dask Cluster

    This creates a ``skein.ApplicationSpec`` to run a dask cluster with the
    scheduler in a YARN container. See the docstring for ``YarnCluster`` for
    more details.
    """
    if (all(v is None for v in kwargs.values()) and
            dask.config.get('yarn.specification')):
        # No overrides and full specification in configuration
        spec = dask.config.get('yarn.specification')
        if isinstance(spec, dict):
            return skein.ApplicationSpec.from_dict(spec)
        return skein.ApplicationSpec.from_file(spec)

    deploy_mode = lookup(kwargs, 'deploy_mode', 'yarn.deploy-mode')
    if deploy_mode not in {'remote', 'local'}:
        raise ValueError("`deploy_mode` must be one of {'remote', 'local'}, "
                         "got %r" % deploy_mode)

    name = lookup(kwargs, 'name', 'yarn.name')
    queue = lookup(kwargs, 'queue', 'yarn.queue')
    tags = lookup(kwargs, 'tags', 'yarn.tags')
    user = lookup(kwargs, 'user', 'yarn.user')

    environment = lookup(kwargs, 'environment', 'yarn.environment')
    if environment is None:
        msg = ("You must provide a path to a Python environment for the workers.\n"
               "This may be one of the following:\n"
               "- A conda environment archived with conda-pack\n"
               "- A virtual environment archived with venv-pack\n"
               "- A path to a conda environment, specified as conda://...\n"
               "- A path to a virtual environment, specified as venv://...\n"
               "- A path to a python binary to use, specified as python://...\n"
               "\n"
               "See http://yarn.dask.org/environments.html for more information.")
        raise ValueError(msg)

    n_workers = lookup(kwargs, 'n_workers', 'yarn.worker.count')
    worker_restarts = lookup(kwargs, 'worker_restarts', 'yarn.worker.restarts')
    worker_env = lookup(kwargs, 'worker_env', 'yarn.worker.env')
    worker_vcores = lookup(kwargs, 'worker_vcores', 'yarn.worker.vcores')
    worker_memory = parse_memory(lookup(kwargs, 'worker_memory', 'yarn.worker.memory'),
                                 'worker')

    services = {}

    files, build_script = _files_and_build_script(environment)

    if deploy_mode == 'remote':
        scheduler_vcores = lookup(kwargs, 'scheduler_vcores',
                                  'yarn.scheduler.vcores')
        scheduler_memory = parse_memory(lookup(kwargs, 'scheduler_memory',
                                               'yarn.scheduler.memory'),
                                        'scheduler')

        services['dask.scheduler'] = skein.Service(
            instances=1,
            resources=skein.Resources(
                vcores=scheduler_vcores,
                memory=scheduler_memory
            ),
            max_restarts=0,
            files=files,
            script=build_script('services scheduler')
        )
        worker_depends = ['dask.scheduler']
    else:
        worker_depends = None

    services['dask.worker'] = skein.Service(
        instances=n_workers,
        resources=skein.Resources(
            vcores=worker_vcores,
            memory=worker_memory
        ),
        max_restarts=worker_restarts,
        depends=worker_depends,
        files=files,
        env=worker_env,
        script=build_script('services worker')
    )

    spec = skein.ApplicationSpec(name=name,
                                 queue=queue,
                                 tags=tags,
                                 user=user,
                                 services=services)
    return spec


def _make_submit_specification(script, args=(), **kwargs):
    spec = _make_specification(**kwargs)

    environment = lookup(kwargs, 'environment', 'yarn.environment')
    files, build_script = _files_and_build_script(environment)

    if 'dask.scheduler' in spec.services:
        # deploy_mode == 'remote'
        client_vcores = lookup(kwargs, 'client_vcores', 'yarn.client.vcores')
        client_memory = lookup(kwargs, 'client_memory', 'yarn.client.memory')
        client_env = lookup(kwargs, 'client_env', 'yarn.client.env')
        client_memory = parse_memory(client_memory, 'client')

        script_name = os.path.basename(script)
        files[script_name] = script

        spec.services['dask.client'] = skein.Service(
            instances=1,
            resources=skein.Resources(
                vcores=client_vcores,
                memory=client_memory
            ),
            max_restarts=0,
            depends=['dask.scheduler'],
            files=files,
            env=client_env,
            script=build_script('services client %s %s'
                                % (script_name, ' '.join(args)))
        )
    return spec


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
    skein_client : skein.Client, optional
        The ``skein.Client`` to use. If not provided, one will be started.

    Examples
    --------
    >>> cluster = YarnCluster(environment='my-env.tar.gz', ...)
    >>> cluster.scale(10)
    """
    def __init__(self,
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
                 skein_client=None):

        spec = _make_specification(environment=environment,
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
                                   user=user)

        self._start_cluster(spec, skein_client)

    @cached_property
    def dashboard_link(self):
        """Link to the dask dashboard. None if dashboard isn't running"""
        if self._dashboard_address is None:
            return None
        template = dask.config.get('distributed.dashboard.link')
        dashboard = urlparse(self._dashboard_address)
        params = dict(os.environ)
        params.update({'host': dashboard.hostname, 'port': dashboard.port})
        return template.format(**params)

    @classmethod
    def from_specification(cls, spec, skein_client=None):
        """Start a dask cluster from a skein specification.

        Parameters
        ----------
        spec : skein.ApplicationSpec, dict, or filename
            The application specification to use. Must define at least one
            service: ``'dask.worker'``. If no ``'dask.scheduler'`` service is
            defined, a scheduler will be started locally.
        skein_client : skein.Client, optional
            The ``skein.Client`` to use. If not provided, one will be started.
        """
        self = super(YarnCluster, cls).__new__(cls)
        if isinstance(spec, dict):
            spec = skein.ApplicationSpec.from_dict(spec)
        elif isinstance(spec, str):
            spec = skein.ApplicationSpec.from_file(spec)
        elif not isinstance(spec, skein.ApplicationSpec):
            raise TypeError("spec must be an ApplicationSpec, dict, or path, "
                            "got %r" % type(spec).__name__)
        self._start_cluster(spec, skein_client)
        return self

    def _start_cluster(self, spec, skein_client=None):
        """Start the cluster and initialize state"""

        if 'dask.worker' not in spec.services:
            raise ValueError("Provided Skein specification must include a "
                             "'dask.worker' service")

        skein_client = _get_skein_client(skein_client)

        if 'dask.scheduler' not in spec.services:
            # deploy_mode == 'local'
            if LooseVersion(distributed.__version__) >= '1.27.0':
                kwargs = {'dashboard_address': '0.0.0.0:0'}
            else:
                kwargs = {'diagnostics_port': ('', 0)}
            self._local_cluster = LocalCluster(n_workers=0,
                                               ip='0.0.0.0',
                                               scheduler_port=0,
                                               **kwargs)
            scheduler = self._local_cluster.scheduler

            scheduler_address = scheduler.address
            try:
                dashboard_port = scheduler.services['bokeh'].port
            except KeyError:
                dashboard_address = None
            else:
                dashboard_host = urlparse(scheduler_address).hostname
                dashboard_address = 'http://%s:%d' % (dashboard_host, dashboard_port)

            with submit_and_handle_failures(skein_client, spec) as app:
                app.kv['dask.scheduler'] = scheduler_address.encode()
                if dashboard_address is not None:
                    app.kv['dask.dashboard'] = dashboard_address.encode()
        else:
            # deploy_mode == 'remote'
            with submit_and_handle_failures(skein_client, spec) as app:
                scheduler_address = app.kv.wait('dask.scheduler').decode()
                dashboard_address = app.kv.get('dask.dashboard')
                if dashboard_address is not None:
                    dashboard_address = dashboard_address.decode()

        # Ensure application gets cleaned up
        self._finalizer = weakref.finalize(self, app.shutdown)

        self.scheduler_address = scheduler_address
        self._dashboard_address = dashboard_address
        self.app_id = app.id
        self.application_client = app

    @classmethod
    def from_current(cls):
        """Connect to an existing ``YarnCluster`` from inside the cluster.

        Returns
        -------
        YarnCluster
        """
        self = super(YarnCluster, cls).__new__(cls)
        app_id = os.environ.get('DASK_APPLICATION_ID', None)
        app_address = os.environ.get('DASK_APPMASTER_ADDRESS', None)
        security_dir = os.environ.get('DASK_SECURITY_CREDENTIALS', None)
        if app_id is not None and app_address is not None:
            security = (None if security_dir is None
                        else skein.Security.from_directory(security_dir))
            app = skein.ApplicationClient(app_address, app_id,
                                          security=security)
        else:
            app = skein.ApplicationClient.from_current()
        self._connect_existing(app)
        return self

    @classmethod
    def from_application_id(cls, app_id, skein_client=None):
        """Connect to an existing ``YarnCluster`` with a given application id.

        Parameters
        ----------
        app_id : str
            The existing cluster's application id.
        skein_client : skein.Client
            The ``skein.Client`` to use. If not provided, one will be started.

        Returns
        -------
        YarnCluster
        """
        self = super(YarnCluster, cls).__new__(cls)
        skein_client = _get_skein_client(skein_client)
        app = skein_client.connect(app_id)
        self._connect_existing(app)
        return self

    def _connect_existing(self, app):
        spec = app.get_specification()
        if 'dask.worker' not in spec.services:
            raise ValueError("%r is not a valid dask cluster" % app.id)

        scheduler_address = app.kv.wait('dask.scheduler').decode()
        dashboard_address = app.kv.get('dask.dashboard')
        if dashboard_address is not None:
            dashboard_address = dashboard_address.decode()

        self.app_id = app.id
        self.application_client = app
        self.scheduler_address = scheduler_address
        self._dashboard_address = dashboard_address
        self._finalizer = None

    def __repr__(self):
        return 'YarnCluster<%s>' % self.app_id

    def _dask_client(self):
        if hasattr(self, '_dask_client_ref'):
            client = self._dask_client_ref()
            if client is not None:
                return client
        client = get_client(address=self.scheduler_address)
        self._dask_client_ref = weakref.ref(client)
        return client

    def shutdown(self, status='SUCCEEDED', diagnostics=None):
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
        if self._finalizer is not None and self._finalizer.peek() is not None:
            self.application_client.shutdown(status=status, diagnostics=diagnostics)
            self._finalizer.detach()  # don't call shutdown later
            # Shutdown in local deploy_mode
            if hasattr(self, '_local_cluster'):
                self._local_cluster.close()
                del self._local_cluster

    def close(self, **kwargs):
        """Close this cluster. An alias for ``shutdown``.

        See Also
        --------
        shutdown
        """
        self.shutdown(**kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def workers(self):
        """A list of all currently running worker containers."""
        return self.application_client.get_containers(services=['dask.worker'])

    def scale_up(self, n, workers=None):
        """Ensure there are atleast n dask workers available for this cluster.

        No-op if ``n`` is less than the current number of workers.

        Examples
        --------
        >>> cluster.scale_up(20)  # ask for twenty workers
        """
        if workers is None:
            workers = self.workers()
        if n > len(workers):
            self.application_client.scale('dask.worker', n)

    def scale_down(self, workers):
        """Retire the selected workers.

        Parameters
        ----------
        workers: list
            List of addresses of workers to close.
        """
        self._dask_client().retire_workers(workers)

    def _select_workers_to_close(self, n):
        client = self._dask_client()
        worker_info = client.scheduler_info()['workers']
        # Sort workers by memory used
        workers = sorted((v['metrics']['memory'], k) for k, v in worker_info.items())
        # Return just the ips
        return [w[1] for w in workers[:n]]

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
        workers = self.workers()
        if n >= len(workers):
            return self.scale_up(n, workers=workers)
        else:
            n_to_delete = len(workers) - n
            # Before trying to close running workers, check if there are any
            # pending containers and kill those first.
            pending = [w for w in workers if w.state in ('waiting', 'requested')]

            for c in pending[:n_to_delete]:
                self.application_client.kill_container(c.id)
                n_to_delete -= 1

            if n_to_delete:
                to_close = self._select_workers_to_close(n_to_delete)
                self.scale_down(to_close)

    def _widget_status(self):
        client = self._dask_client()

        workers = client.scheduler_info()['workers']

        n_workers = len(workers)
        cores = sum(w['ncores'] for w in workers.values())
        memory = sum(w['memory_limit'] for w in workers.values())

        text = """
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
""" % (n_workers, cores, format_bytes(memory))
        return text

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML

        client = self._dask_client()

        layout = Layout(width='150px')

        title = HTML('<h2>YarnCluster</h2>')

        status = HTML(self._widget_status(), layout=Layout(min_width='150px'))

        request = IntText(0, description='Workers', layout=layout)
        scale = Button(description='Scale', layout=layout)

        @scale.on_click
        def scale_cb(b):
            with log_errors():
                self.scale(request.value)

        elements = [title,
                    HBox([status, request, scale])]

        if self.dashboard_link is not None:
            link = HTML('<p><b>Dashboard: </b><a href="%s" target="_blank">%s'
                        '</a></p>\n' % (self.dashboard_link, self.dashboard_link))
            elements.append(link)

        self._cached_widget = box = VBox(elements)

        def update():
            status.value = self._widget_status()

        pc = PeriodicCallback(update, 500, io_loop=client.loop)
        pc.start()

        return box

    def _ipython_display_(self, **kwargs):
        try:
            return self._widget()._ipython_display_(**kwargs)
        except ImportError:
            print(self)
