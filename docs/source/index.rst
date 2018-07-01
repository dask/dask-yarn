dask-yarn
=========

Dask Yarn deploys Dask on YARN clusters using `Skein
<https://jcrist.github.io/skein/>`__.  It provides a quick API for starting,
stopping, and scaling a Dask cluster from Python.


.. code-block:: python

    from dask_yarn import YarnCluster
    from dask.distributed import Client

    # Create a cluster with 4 workers, each with 2 virtual cores and 2 GB of
    # memory
    cluster = YarnCluster(environment='environment.tar.gz',
                          n_workers=4,
                          worker_vcores=2,
                          worker_memory="2GB")

    # Connect to the cluster
    client = Client(cluster)


Install
-------

Until an official release, ``dask-yarn`` can be installed from source:

.. code-block:: console

    pip install git+https://github.com/dask/dask-yarn.git


Distributing Python Environments
--------------------------------

By default, ``dask-yarn`` relies on Conda environments packaged with
`conda-pack <https://conda.github.io/conda-pack/>`__ as a means to distribute
python packages to the workers. These environments can contain any python
packages you might need, but require ``dask-yarn`` (and its dependencies) at a
minimum.

.. code-block:: console

    $ conda create -n dask-yarn dask distributed  # create an environment

    $ conda-pack -n dask-yarn  # package the environment
    Collecting packages...
    Packing environment at '/Users/jcrist/miniconda/envs/dask-yarn' to 'dask-yarn.tar.gz'
    [########################################] | 100% Completed |  12.2s


.. toctree::
    :hidden:

    api.rst
