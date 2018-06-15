from __future__ import absolute_import, print_function, division


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
        self = super(YarnCluster, cls).__name__(cls)

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

    def close(self):
        """Close this cluster"""
        self._finalize()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
