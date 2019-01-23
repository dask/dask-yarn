Configuration
=============

Specifying all parameters to the YarnCluster constructor every time may be
error prone, especially when sharing this workflow with new users.
Alternatively, you can provide defaults in a configuration file, traditionally
held in ``~/.config/dask/yarn.yaml`` or ``/etc/dask/yarn.yaml``.  Note that
this configuration is *optional*, and only changes the defaults when not
specified in the constructor.

**Example:**

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


Providing a Custom Skein Specification
--------------------------------------

.. currentmodule:: dask_yarn

Sometimes you'll need more control over the deployment than is provided by the
above configuration fields. In this case you can provide a custom `Skein
specification`_ to the ``yarn.specification`` field. If this field is present
in the configuration, it will be used as long as no parameters are passed to
the :class:`YarnCluster` constructor. Note that this is equivalent to calling
:func:`YarnCluster.from_specification` programatically.

The specification requires at least one Service_ named ``dask.worker`` which
describes how to start a single worker. If an additional service
``dask.scheduler`` is provided, this will be assumed to start the scheduler. If
``dask.scheduler`` isn't present, a scheduler will be started locally instead.

In the ``script`` section for each service, the appropriate ``dask-yarn``
:doc:`cli` command should be used:

- ``dask-yarn services worker`` to start the worker
- ``dask-yarn services scheduler`` to start the worker

Beyond that, you have full flexibility for how to define a specification. See
the Skein_ documentation for more information.  A few examples are provided
below:


Example: deploy-mode local with node_label restrictions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This specification is similar to that created automatically when
``deploy_mode='local'`` is specified (scheduler runs locally, only worker
service specified), except it adds `node_label restrictions`_ for the workers.
Here we restrict workers to run only on nodes labeled as GPU.

.. code-block:: yaml

   # /home/username/.config/dask/yarn.yaml
   yarn:
     specification:
       name: dask
       queue: myqueue

       services:
         dask.worker:
           # Restrict workers to GPU nodes only
           node_label: GPU
           # Don't start any workers initially
           instances: 0
           # Workers can infinite number of times
           max_restarts: -1
           # Restrict workers to 4 GiB and 2 cores each
           resources:
             memory: 4 GiB
             vcores: 2
           # Distribute this python environment to every worker node
           files:
             environment: /path/to/my/environment.tar.gz
           # The bash script to start the worker
           # Here we activate the environment, then start the worker
           script: |
             source environment/bin/activate
             dask-yarn services worker


Example: deploy-mode remote with custom setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This specification is similar to that created automatically when
``deploy_mode='remote'`` is specified (both scheduler and worker run inside
YARN containers), except it runs an initialization script before starting each
service.

.. code-block:: yaml

   # /home/username/.config/dask/yarn.yaml
   yarn:
     specification:
       name: dask
       queue: myqueue

       services:
         dask.scheduler:
           # Restrict scheduler to 2 GiB and 1 core
           resources:
             memory: 2 GiB
             vcores: 1
           # The bash script to start the scheduler.
           # Here we have dask-yarn already installed on the node,
           # and also run a custom script before starting the service
           script: |
             some-custom-initialization-script
             dask-yarn services worker

         dask.worker:
           # Don't start any workers initially
           instances: 0
           # Workers can infinite number of times
           max_restarts: -1
           # Workers should only be started after the scheduler starts
           depends:
             - dask.scheduler
           # Restrict workers to 4 GiB and 2 cores each
           resources:
             memory: 4 GiB
             vcores: 2
           # The bash script to start the worker.
           # Here we have dask-yarn already installed on the node,
           # and also run a custom script before starting the service
           script: |
             some-custom-initialization-script
             dask-yarn services worker

.. _Skein: https://jcrist.github.io/skein/
.. _Skein specification: https://jcrist.github.io/skein/specification.html
.. _Service: https://jcrist.github.io/skein/specification.html#service
.. _node_label restrictions: https://jcrist.github.io/skein/specification.html#node-label
