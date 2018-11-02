from __future__ import absolute_import, print_function, division

import math
import os
import sys

import dask
from dask.distributed import get_client
from distributed.utils import format_bytes, PeriodicCallback, log_errors

import skein

if sys.version_info.major == 2:
    from backports import weakref
else:
    import weakref


_memory_error = """Memory specification for the `{0}` take string parameters
with units like "4 GiB" or "2048 MiB"

You provided:       {1}
Perhaps you meant: "{1} MiB"
"""

_one_MiB = 2**20


def parse_memory(memory, field):
    if isinstance(memory, str):
        memory = dask.utils.parse_bytes(memory)
    elif memory < _one_MiB:
        raise ValueError(_memory_error.format(field, memory))
    return int(math.ceil(memory / _one_MiB))


def lookup(kwargs, a, b):
    return kwargs[a] if kwargs.get(a) is not None else dask.config.get(b)


# exposed for testing only

def _make_specification(**kwargs):
    """ Create specification to run Dask Cluster

    This creates a ``skein.ApplicationSpec`` to run a dask cluster with the
    scheduler in a YARN container. See the docstring for ``YarnCluster`` for
    more details.
    """
    if not kwargs and dask.config.get('yarn.specification'):
        # No overrides and full specification in configuration
        spec = dask.config.get('yarn.specification')
        if isinstance(spec, dict):
            return skein.ApplicationSpec.from_dict(spec)
        return skein.ApplicationSpec.from_file(spec)

    name = lookup(kwargs, 'name', 'yarn.name')
    queue = lookup(kwargs, 'queue', 'yarn.queue')
    tags = lookup(kwargs, 'tags', 'yarn.tags')

    environment = lookup(kwargs, 'environment', 'yarn.environment')

    n_workers = lookup(kwargs, 'n_workers', 'yarn.worker.count')
    worker_vcores = lookup(kwargs, 'worker_vcores', 'yarn.worker.vcores')
    worker_memory = lookup(kwargs, 'worker_memory', 'yarn.worker.memory')
    worker_restarts = lookup(kwargs, 'worker_restarts', 'yarn.worker.restarts')
    worker_env = lookup(kwargs, 'worker_env', 'yarn.worker.env')

    scheduler_vcores = lookup(kwargs, 'scheduler_vcores', 'yarn.scheduler.vcores')
    scheduler_memory = lookup(kwargs, 'scheduler_memory', 'yarn.scheduler.memory')

    if environment is None:
        msg = ("You must provide a path to an archived Python environment for "
               "the workers.\n"
               "This is commonly achieved through conda-pack or venv-pack.\n\n"
               "See http://yarn.dask.org/"
               "#distributing-python-environments for more information")
        raise ValueError(msg)

    scheduler_memory = parse_memory(scheduler_memory, 'scheduler')
    worker_memory = parse_memory(worker_memory, 'worker')

    scheduler = skein.Service(instances=1,
                              resources=skein.Resources(
                                  vcores=scheduler_vcores,
                                  memory=scheduler_memory
                              ),
                              max_restarts=0,
                              files={'environment': environment},
                              commands=['source environment/bin/activate',
                                        'dask-yarn services scheduler'])

    worker = skein.Service(instances=n_workers,
                           resources=skein.Resources(
                               vcores=worker_vcores,
                               memory=worker_memory
                           ),
                           max_restarts=worker_restarts,
                           depends=['dask.scheduler'],
                           files={'environment': environment},
                           env=worker_env,
                           commands=['source environment/bin/activate',
                                     'dask-yarn services worker'])

    spec = skein.ApplicationSpec(name=name,
                                 queue=queue,
                                 tags=tags,
                                 services={'dask.scheduler': scheduler,
                                           'dask.worker': worker})
    return spec


def _make_submit_specification(script, args=(), **kwargs):
    client_vcores = lookup(kwargs, 'client_vcores', 'yarn.client.vcores')
    client_memory = lookup(kwargs, 'client_memory', 'yarn.client.memory')
    client_env = lookup(kwargs, 'client_env', 'yarn.client.env')
    client_memory = parse_memory(client_memory, 'client')

    spec = _make_specification(**kwargs)
    environment = spec.services['dask.worker'].files['environment']

    script_name = os.path.basename(script)

    spec.services['dask.client'] = skein.Service(
        instances=1,
        resources=skein.Resources(
            vcores=client_vcores,
            memory=client_memory
        ),
        max_restarts=0,
        depends=['dask.scheduler'],
        files={'environment': environment,
               script_name: script},
        env=client_env,
        commands=[
            'source environment/bin/activate',
            'dask-yarn services client %s %s' % (script_name, ' '.join(args))
        ]
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
        Path to an archived Python environment (either ``tar.gz`` or ``zip``).
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
    name : str, optional
        The application name.
    queue : str, optional
        The queue to deploy to.
    tags : sequence, optional
        A set of strings to use as tags for this application.
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
                 name=None,
                 queue=None,
                 tags=None,
                 skein_client=None):

        spec = _make_specification(environment=environment,
                                   n_workers=n_workers,
                                   worker_vcores=worker_vcores,
                                   worker_memory=worker_memory,
                                   worker_restarts=worker_restarts,
                                   worker_env=worker_env,
                                   scheduler_vcores=scheduler_vcores,
                                   scheduler_memory=scheduler_memory,
                                   name=name,
                                   queue=queue,
                                   tags=tags)

        self._start_cluster(spec, skein_client)

    @classmethod
    def from_specification(cls, spec, skein_client=None):
        """Start a dask cluster from a skein specification.

        Parameters
        ----------
        spec : skein.ApplicationSpec, dict, or filename
            The application specification to use. Should define at least two
            services: ``'dask.scheduler'`` and ``'dask.worker'``.
        skein_client : skein.Client, optional
            The ``skein.Client`` to use. If not provided, one will be started.
        """
        self = super(YarnCluster, cls).__new__(cls)
        self._start_cluster(spec, skein_client)
        return self

    def _start_cluster(self, spec, skein_client=None):
        """Start the cluster and initialize state"""
        if skein_client is None:
            skein_client = skein.Client()

        app = skein_client.submit_and_connect(spec)
        try:
            scheduler_address = app.kv.wait('dask.scheduler').decode()
        except BaseException:
            # Failed to connect, kill the application and reraise
            skein_client.kill_application(app.id)
            raise

        # Ensure application gets cleaned up
        self._finalizer = weakref.finalize(self, app.shutdown)

        self.app_id = app.id
        self.application_client = app
        self.scheduler_address = scheduler_address

    @classmethod
    def from_current(cls):
        """Connect to an existing ``YarnCluster`` from inside the cluster.

        Returns
        -------
        YarnCluster
        """
        self = super(YarnCluster, cls).__new__(cls)
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
        if skein_client is None:
            skein_client = skein.Client()
        app = skein_client.connect(app_id)
        self._connect_existing(app)
        return self

    def _connect_existing(self, app):
        spec = app.get_specification()
        if 'dask.worker' not in spec.services:
            raise ValueError("%r is not a valid dask cluster" % app.id)

        scheduler_address = app.kv.wait('dask.scheduler').decode()

        self.app_id = app.id
        self.application_client = app
        self.scheduler_address = scheduler_address
        self._finalizer = None

    def __repr__(self):
        return 'YarnCluster<%r>' % self.scheduler_address

    def _dask_client(self):
        if hasattr(self, '_dask_client_ref'):
            client = self._dask_client_ref()
            if client is not None:
                return client
        client = get_client(address=self.scheduler_address)
        self._dask_client_ref = weakref.ref(client)
        return client

    def shutdown(self, status='SUCCEEDED'):
        """Shutdown the application.

        Parameters
        ----------
        status : {'SUCCEEDED', 'FAILED', 'KILLED'}, optional
            The yarn application exit status.
        """
        if self._finalizer is not None and self._finalizer.peek() is not None:
            self.application_client.shutdown(status=status)
            self._finalizer.detach()  # don't call shutdown later

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
            self.application_client.scale(service='dask.worker', instances=n)

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

        box = VBox([title,
                    HBox([status, request, scale])])

        self._cached_widget = box

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
