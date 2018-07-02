Dask-Yarn
=========

Dask-Yarn deploys Dask on YARN clusters, such as are often found in traditional
Hadoop or Spark installations.  Dask-Yarn provides an easy interface to quickly
start, scale, and stop Dask clusters natively from Python.

.. code-block:: python

    from dask_yarn import YarnCluster
    from dask.distributed import Client

    # Create a cluster where each worker has two cores and eight GB of memory
    cluster = YarnCluster(environment='environment.tar.gz',
                          worker_vcores=2,
                          worker_memory="8GB")
    # Scale out to ten such workers
    cluster.scale(10)

    # Connect to the cluster
    client = Client(cluster)

Dask-yarn uses `Skein <https://jcrist.github.io/skein/>`__,
a Pythonic library to manage Yarn services.

Install
-------

Until an official release, ``dask-yarn`` can be installed from source:

.. code-block:: console

    pip install git+https://github.com/dask/dask-yarn.git


Distributing Python Environments
--------------------------------

We need to ensure that the libraries used on the Yarn cluster are the same as
what you are using locally.  We accomplish this by packaging up a conda
environment with `conda-pack <https://conda.github.io/conda-pack/>`__ and
shipping it to the Yarn cluster with Dask-yarn.

Dask-Yarn relies on Conda environments packaged with
`conda-pack <https://conda.github.io/conda-pack/>`__ to distribute
Python packages to the workers. These environments can contain any Python
packages you might need, but require ``dask-yarn`` (and its dependencies) at a
minimum.

.. code-block:: console

    $ conda create -n my-env pandas scikit-learn dask        # Create an environment
    $ pip install git+https://github.com/dask/dask-yarn.git  # Modify environment normally

    $ conda-pack -n my-env                                   # Package environment
    Collecting packages...
    Packing environment at '/home/username/miniconda/envs/my-env' to 'my-env.tar.gz'
    [########################################] | 100% Completed |  12.2s

You can now supply the path to ``my-env.tar.gz`` to the
``YarnCluster(environment=...)`` keyword.  You may want to verify that your
versions match with the following:

.. code-block:: python

   from dask_yarn import YarnCluster
   from dask.distributed import Client

   cluster = YarnCluster(environment='my-env.tar.gz')
   client = Client(cluster)
   client.get_versions(check=True)

Configuration
-------------

Specifying all parameters to the YarnCluster constructor every time can be
error prone, especially when sharing this workflow with new users.
Alternatively, you can provide defaults in a configuration file, traditionally
held in ``~/.config/dask/yarn.yaml`` or ``/etc/dask/yarn.yaml``.  Here is an
example:

.. code-block:: yaml

   # /home/username/.config/dask/yarn.yaml
   yarn:
     name: dask                 # Application name
     queue: default             # Yarn queue to deploy to

     environment: /path/to/my-env.tar.gz

     tags: []                   # List of strings to tag applications
     scheduler:                 # Specifications of scheduler container
       vcores: 1
       memory: 4GiB
     workers:                   # Specifications of worker containers
       vcores: 2
       memory: 8GiB
       instances: 0             # Number of default workers with which to start
       restarts: -1             # Allowed number of restarts, -1 for unlimited

Users can now create YarnClusters without specifying any additional
information.

.. code-block:: python

   from dask_yarn import YarnCluster

   cluster = YarnCluster()
   cluster.scale(20)

For more information on Dask configuration see the `Dask configuration
documentation <http://dask.pydata.org/en/latest/configuration.html>`_.


.. toctree::
    :hidden:

    api.rst
