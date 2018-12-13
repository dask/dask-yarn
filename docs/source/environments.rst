Managing Python Environments
============================

We need to ensure that the libraries used on the Yarn cluster are the same as
what you are using locally. There are a few ways to specify this:

- The path to an archived environment (either conda_ or virtual_ environments)
- The path to a Conda_ environment (as ``conda:///...``)
- The path to a `virtual environment`_ (as ``venv:///...``)
- The path to a python executable (as ``python:///...``)

Note that when not using an archive, the provided path must be valid on all
nodes in the cluster.

Using Archived Python Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most common way to use ``dask-yarn`` is to distribute an archived Python
environment throughout the YARN cluster as part of the application. Packaging
the environment for distribution is typically handled using

- conda-pack_ for Conda_ environments
- venv-pack_  for `virtual environments`_

These environments can contain any Python packages you might need, but require
``dask-yarn`` (and its dependencies) at a minimum.


Archiving Conda Environments Using Conda-Pack
---------------------------------------------

You can package a conda environment using conda-pack_.

.. code-block:: console

    $ conda create -n my-env dask-yarn scikit-learn          # Create an environment

    $ conda activate my-env                                  # Activate the environment

    $ conda-pack                                             # Package environment
    Collecting packages...
    Packing environment at '/home/username/miniconda/envs/my-env' to 'my-env.tar.gz'
    [########################################] | 100% Completed |  12.2s


Archiving Virtual Environments Using Venv-Pack
----------------------------------------------

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


Specifying the Archived Environment
-----------------------------------

You can now start a cluster with the packaged environment by passing the
path to the constructor, e.g. ``YarnCluster(environment='my-env.tar.gz',
...)``.

Note that if the environment is a local file, the archive will be automatically
uploaded to a temporary directory on HDFS before starting the application. If
you find yourself reusing the same environment multiple times, you may want to
upload the environment to HDFS once beforehand to avoid repeating this process
for each cluster (the environment is then specified as
``hdfs:///path/to/my-env.tar.gz``).

After startup you may want to verify that your versions match with the
following:

.. code-block:: python

   from dask_yarn import YarnCluster
   from dask.distributed import Client

   cluster = YarnCluster(environment='my-env.tar.gz')
   client = Client(cluster)
   client.get_versions(check=True)  # check that versions match between all nodes


Using Python Environments Local to Each Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alternatively, you can specify the path to a `conda environment`_, `virtual
environment`_, or Python executable that is already found on each node:

.. code-block:: python

   from dask_yarn import YarnCluster

   # Use a conda environment at /path/to/my/conda/env
   cluster = YarnCluster(environment='conda:///path/to/my/conda/env')

   # Use a virtual environment at /path/to/my/virtual/env
   cluster = YarnCluster(environment='venv:///path/to/my/virtual/env')

   # Use a Python executable at /path/to/my/python
   cluster = YarnCluster(environment='python:///path/to/my/python')

As before, these environments can have any Python packages, but must include
``dask-yarn`` (and its dependencies) at a minimum. It's also *very important*
that these environments are uniform across all nodes; mismatched environments
can lead to hard to diagnose issues. To check this, you can use the
``Client.get_versions`` method:

.. code-block:: python

   from dask.distributed import Client

   client = Client(cluster)
   client.get_versions(check=True)  # check that versions match between all nodes



.. _conda-pack: https://conda.github.io/conda-pack/
.. _conda environment: http://conda.io/
.. _conda: http://conda.io/
.. _venv:
.. _virtual:
.. _virtual environment:
.. _virtual environments: https://docs.python.org/3/library/venv.html
.. _virtualenv: https://virtualenv.pypa.io/en/stable/
.. _venv-pack documentation:
.. _venv-pack: https://jcrist.github.io/venv-pack/
