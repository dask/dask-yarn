Distributing Python Environments
================================

We need to ensure that the libraries used on the Yarn cluster are the same as
what you are using locally. By default, ``dask-yarn`` handles this by
distributing a packaged python environment to the Yarn cluster as part of the
applications. This is typically handled using

- conda-pack_ for Conda_ environments
- venv-pack_  for `virtual environments`_

These environments can contain any Python packages you might need, but require
``dask-yarn`` (and its dependencies) at a minimum.


Packing Conda Environments using Conda-Pack
-------------------------------------------

You can package a conda environment using conda-pack_.

.. code-block:: console

    $ conda create -n my-env dask-yarn scikit-learn          # Create an environment

    $ conda activate my-env                                  # Activate the environment

    $ conda-pack                                             # Package environment
    Collecting packages...
    Packing environment at '/home/username/miniconda/envs/my-env' to 'my-env.tar.gz'
    [########################################] | 100% Completed |  12.2s


Packing Virtual Environments using Venv-Pack
--------------------------------------------

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
------------------------------------------

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
