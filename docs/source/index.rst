Dask-Yarn
=========

Dask-Yarn deploys Dask on `YARN
<https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html>`_
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
a Pythonic library to manage Yarn services.

Install
-------

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


Distributing Python Environments
--------------------------------

We need to ensure that the libraries used on the Yarn cluster are the same as
what you are using locally. By default, ``dask-yarn`` handles this by
distributing a packaged python environment to the Yarn cluster as part of the
applications. This is typically handled using

- conda-pack_ for Conda_ environments
- venv-pack_  for `virtual environments`_

These environments can contain any Python packages you might need, but require
``dask-yarn`` (and its dependencies) at a minimum.


Packing Conda Environments using Conda-Pack
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can package a conda environment using conda-pack_.

.. code-block:: console

    $ conda create -n my-env dask-yarn scikit-learn          # Create an environment

    $ conda activate my-env                                  # Activate the environment

    $ conda-pack                                             # Package environment
    Collecting packages...
    Packing environment at '/home/username/miniconda/envs/my-env' to 'my-env.tar.gz'
    [########################################] | 100% Completed |  12.2s


Packing Virtual Environments using Venv-Pack
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can package a virtual environment using venv-pack_. The virtual environment
can be created using either venv_ or virtualenv_. Note that the python linked
to in the virtual environment must exist and be accessible on every node in the
YARN cluster. If the environment was created with a different Python, you can
change the link path using the ``--python-prefix`` flag. For more information see
the `venv-pack documentation`_.

.. code-block:: console

    $ python -m venv my_env                     # Create an environment using venv
    $ python -m virtualenv my_env               # Or create an environment using virtualenv

    $ source my_env/bin/activate                # Activate the environment

    $ pip install dask-yarn scikit-learn        # Install some packages

    $ venv-pack                                 # Package environment
    Collecting packages...
    Packing environment at '/home/username/my-env' to 'my-env.tar.gz'
    [########################################] | 100% Completed |  8.3s


Specifying the Environment for the Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can now start a cluster with the packaged environment by passing the
path to the constructor, e.g. ``YarnCluster(environment='my-env.tar.gz',
...)``. After startup you may want to verify that your versions match with the
following:

.. code-block:: python

   from dask_yarn import YarnCluster
   from dask.distributed import Client

   cluster = YarnCluster(environment='my-env.tar.gz')
   client = Client(cluster)
   client.get_versions(check=True)  # check that versions match between all nodes

.. _conda-pack: https://conda.github.io/conda-pack/
.. _conda: http://conda.io/
.. _venv:
.. _virtual environments: https://docs.python.org/3/library/venv.html
.. _virtualenv: https://virtualenv.pypa.io/en/stable/
.. _venv-pack documentation:
.. _venv-pack: https://jcrist.github.io/venv-pack/


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

     worker:                   # Specifications of worker containers
       vcores: 2
       memory: 8GiB
       count: 0                 # Number of workers to start on initialization
       restarts: -1             # Allowed number of restarts, -1 for unlimited

Users can now create YarnClusters without specifying any additional
information.

.. code-block:: python

   from dask_yarn import YarnCluster

   cluster = YarnCluster()
   cluster.scale(20)

For more information on Dask configuration see the `Dask configuration
documentation <https://docs.dask.org/en/latest/configuration.html>`_.


.. toctree::
    :hidden:

    api.rst
    cli.rst
