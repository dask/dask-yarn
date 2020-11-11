Dask-Yarn
=========

Dask-Yarn deploys Dask on `YARN
<https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html>`__
clusters, such as are found in traditional Hadoop installations. Dask-Yarn
provides an easy interface to quickly start, scale, and stop Dask clusters
natively from Python.

.. code-block:: python

    from dask_yarn import YarnCluster
    from dask.distributed import Client

    # Create a cluster where each worker has two cores and eight GiB of memory
    cluster = YarnCluster(environment='environment.tar.gz',
                          worker_vcores=2,
                          worker_memory="8GiB")
    # Scale out to ten such workers
    cluster.scale(10)

    # Connect to the cluster
    client = Client(cluster)

Dask-Yarn uses `Skein <https://jcrist.github.io/skein/>`__,
a Pythonic library to create and deploy YARN applications.

Install
-------

Dask-Yarn is designed to only require installation on an edge node. To install,
use one of the following methods:

**Install with Conda:**

.. code-block:: console

    conda install -c conda-forge dask-yarn

**Install with Pip:**

.. code-block:: console

    pip install dask-yarn

**Install from Source:**

Dask-Yarn is `available on github <https://github.com/dask/dask-yarn>`_ and can
always be installed from source.

.. code-block:: console

    pip install git+https://github.com/dask/dask-yarn.git

.. toctree::
    :hidden:

    quickstart.rst
    environments.rst
    configuration.rst
    submit.rst
    aws-emr.rst
    gcp-dataproc.rst
    api.rst
    cli.rst
