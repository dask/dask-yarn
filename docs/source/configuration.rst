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
