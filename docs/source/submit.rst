Submitting Applications
=======================

.. warning::

    The submission API is experimental and may change between versions


Sometimes you have Dask Application you want to deploy completely on YARN,
without having a corresponding process running on an edge node. This may come
up with production applications deployed automatically, or long running jobs
you don't want to consume edge node resources.

To handle these cases, ``dask-yarn`` provides a :doc:`cli` that can be used to
submit applications to be run on the YARN cluster asynchronously. There are
three commands that may be useful here:

- ``dask-yarn submit``: submit an application to the YARN cluster
- ``dask-yarn status``: check on the status of an application
- ``dask-yarn kill``: kill a running application


Submitting an Application
-------------------------

.. currentmodule:: dask_yarn

To prepare an application to be submitted using ``dask-yarn submit``, you need
to change the creation of your :class:`YarnCluster` from using the constructor
to using :func:`YarnCluster.from_current`.

.. code-block:: python

    # Replace this
    cluster = YarnCluster(...)

    # with this
    cluster = YarnCluster.from_current()

This is because the script won't be run until the cluster is already created -
at that point configuration passed to the :class:`YarnCluster` constructor
won't be useful. Cluster configuration is instead passed via the ``dask-yarn
submit`` CLI (note that `as before <quickstart.html>`__, the cluster can be
scaled dynamically after creation).

.. code-block:: bash

    # Submit `myscript.py` to run on a dask cluster with 8 workers,
    # each with 2 cores and 4 GiB
    $ dask-yarn submit \
      --environment my_env.tar.gz \
      --worker-count 8 \
      --worker-vcores 2 \
      --worker-memory 4GiB \
      myscript.py

    application_1538148161343_0051

This outputs a YARN Application ID, which can be used with other YARN tools.


Checking Application Status
---------------------------

Submitted application status can be checked using the YARN Web UI, or
programmatically using ``dask-yarn status``. This command takes one parameter -
the application id.

.. code-block:: bash

    $ dask-yarn status application_1538148161343_0051
    APPLICATION_ID                    NAME    STATE       STATUS       CONTAINERS    VCORES    MEMORY    RUNTIME
    application_1538148161343_0051    dask    RUNNING     UNDEFINED    9             17        33792     6m


Killing a Running Application
-----------------------------

Submitted applications normally run until completion. If you need to terminate
one before then, you can use the ``dask-yarn kill`` command. This command
takes one parameter - the application id.

.. code-block:: bash

    $ dask-yarn kill application_1538148161343_0051


Accessing the Application Logs
------------------------------

Application logs can be retrieved a few ways:

- The logs of running applications can be viewed using the `Skein Web UI
  <https://jcrist.github.io/skein/web-ui.html>`__ (``dask-yarn`` is built using
  `Skein <https://jcrist.github.io/skein/>`__).

- The logs of completed applications can be viewed using the ``yarn logs``
  command.

  .. code-block:: bash

    $ yarn logs -applicationId application_1538148161343_0051
