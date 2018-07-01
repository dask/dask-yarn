Dask-Yarn
=========

Dask-Yarn deploys Dask on YARN clusters, such as are often found in traditional
Hadoop or Spark installations.  Dask-Yarn provides an easy interface to quickly
start, stop, and scale a Dask cluster from Python.

.. code-block:: python

    from dask_yarn import YarnCluster
    from dask.distributed import Client

    # Create a cluster with 4 workers, each with two cores and two GB of memory
    cluster = YarnCluster(environment='environment.tar.gz',
                          n_workers=4,
                          worker_vcores=2,
                          worker_memory="2GB")

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

    $ conda create -n my-env dask distributed                # Create an environment
    $ pip install git+https://github.com/dask/dask-yarn.git  # Modify normally

    $ conda-pack -n my-env
    Collecting packages...
    Packing environment at '/home/username/miniconda/envs/my-env' to 'my-env.tar.gz'
    [########################################] | 100% Completed |  12.2s

You can now supply the path to ``my-env.tar.gz`` to the
``YarnCluster(environment=...)`` keyword.  You may want to verify that your
versions match with the following:

.. code-block:: python

   from dask_yarn import YarnCluster
   from dask.distributed import Client

   cluster = YarnCluster(environment='dask-yarn.tar.gz')
   client = Client(cluster)
   client.get_versions(check=True)


.. toctree::
    :hidden:

    api.rst
