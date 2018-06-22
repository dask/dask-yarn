from __future__ import absolute_import, print_function, division

import weakref

from distributed import get_client
from distributed.utils import format_bytes, PeriodicCallback, log_errors

import skein


def make_remote_spec(environment, name='dask', queue='default', tags=None,
                     n_workers=0, worker_vcores=1, worker_memory=2048,
                     worker_max_restarts=-1, scheduler_vcores=1,
                     scheduler_memory=2048):
    """Create a ``skein.ApplicationSpec`` to run a dask cluster with the
    scheduler in a YARN container.

    Parameters
    ----------
    environment : str
        Path to an archived Conda environment (either ``tar.gz`` or ``zip``).
    name : str, optional
        The application name.
    queue : str, optional
        The queue to deploy to.
    tags : sequence, optional
        A set of strings to use as tags for this application.
    n_workers : int, optional
        The number of workers to initially start.
    worker_vcores : int, optional
        The number of virtual cores to allocate per worker.
    worker_memory : int, optional
        The ammount of memory in MB to allocate per worker.
    worker_max_restarts : int, optional
        The maximum number of worker restarts to allow before failing the
        application. Default is unlimited.
    scheduler_vcores : int, optional
        The number of virtual cores to allocate per scheduler.
    scheduler_memory : int, optional
        The ammount of memory in MB to allocate per scheduler.

    Returns
    -------
    spec : skein.ApplicationSpec
        The application specification.
    """
    scheduler = skein.Service(instances=1,
                              resources=skein.Resources(vcores=scheduler_vcores,
                                                        memory=scheduler_memory),
                              max_restarts=0,
                              env={'DASK_CONFIG': '.config'},
                              files={'environment': environment},
                              commands=['source environment/bin/activate',
                                        'dask-yarn-scheduler'])
    worker = skein.Service(instances=n_workers,
                           resources=skein.Resources(vcores=worker_vcores,
                                                     memory=worker_memory),
                           max_restarts=worker_max_restarts,
                           depends=['dask.scheduler'],
                           env={'DASK_CONFIG': '.config'},
                           files={'environment': environment},
                           commands=['source environment/bin/activate',
                                     ('dask-yarn-worker %d --memory_limit %dMB'
                                      % (worker_vcores, worker_memory))])

    spec = skein.ApplicationSpec(name=name,
                                 queue=queue,
                                 tags=tags,
                                 services={'dask.scheduler': scheduler,
                                           'dask.worker': worker})
    return spec


@skein.utils.with_finalizers
class YarnCluster(object):
    """Start a Dask cluster on YARN.

    Parameters
    ----------
    spec : skein.ApplicationSpec
        The application specification to use. Should define at least two
        services: ``'dask.scheduler'`` and ``'dask.worker'``.
    skein_client : skein.Client, optional
        The ``skein.Client`` to use. If not provided, one will be started.
    """
    def __init__(self, spec, skein_client=None):
        if skein_client is None:
            skein_client = skein.Client()

        app = skein_client.submit(spec)
        self._add_finalizer(lambda: app.kill())

        self.application_client = app.connect()

    @classmethod
    def from_current(cls):
        """Create a ``YarnCluster`` from within a running skein application.

        Returns
        -------
        cluster : YarnCluster
        """
        self = super(YarnCluster, cls).__new__(cls)
        self.application_client = skein.ApplicationClient.from_current()
        return self

    @classmethod
    def from_application_id(cls, app_id, skein_client=None):
        """Create a ``YarnCluster`` from an existing skein application.

        Parameters
        ----------
        app_id : str
            The application id.
        skein_client : skein.Client, optional
            The ``skein.Client`` to use. If not provided, one will be started.

        Returns
        -------
        cluster : YarnCluster
        """
        self = super(YarnCluster, cls).__new__(cls)

        if skein_client is None:
            skein_client = skein.Client()

        self.application_client = skein_client.connect(app_id)
        return self

    def wait_for_scheduler(self):
        """Wait for the scheduler to start"""
        self._scheduler_address = self.application_client.kv.wait('dask.scheduler')

    @property
    def scheduler_address(self):
        """The scheduler address.

        If the scheduler hasn't started, blocks until it has"""
        if not hasattr(self, '_scheduler_address'):
            self.wait_for_scheduler()
        return self._scheduler_address

    def __repr__(self):
        if hasattr(self, '_scheduler_address'):
            return 'YarnCluster<%r>' % self._scheduler_address
        return 'YarnCluster<"pending connection">'

    def _dask_client(self):
        if hasattr(self, '_dask_client_ref'):
            client = self._dask_client_ref()
            if client is not None:
                return client
        client = get_client(address=self.scheduler_address)
        self._dask_client_ref = weakref.ref(client)
        return client

    def close(self):
        """Close this cluster"""
        self._finalize()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def workers(self):
        """A list of all currently running worker containers."""
        return self.application_client.containers(services=['dask.worker'])

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
        workers = sorted((v['memory'], k) for k, v in worker_info.items())
        # Return just the ips
        return [w[1] for w in workers[:n]]

    def scale(self, n):
        """Scale cluster to n workers.

        Parameters
        ----------
        n : int
            Target number of workers

        Example
        -------
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
                self.application_client.kill(c.id)
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
