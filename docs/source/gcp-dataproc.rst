Deploying on Google Cloud Dataproc
==================================

`Dataproc`_ is `Google Cloud`_'s hosted service for creating Apache Hadoop and
Apache Spark clusters. Dataproc supports a series of open-source
`initialization actions`_ that allows installation of a wide range of open
source tools when creating a cluster. In particular, the following
instructions will guide you through creating a Dataproc cluster with Dask and
Dask-Yarn installed and configured for you. This tutorial is loosely adapted
from the `README`_ for the Dask initialization action.

What the Initialization Action is Doing
---------------------------------------
The initialization action `installation script`_ does several things:

- It accepts a metadata parameter for configuring your cluster to use Dask
  with either its standalone scheduler or with Dask-Yarn to utilize Yarn.

- For the ``yarn`` configuration, this script installs ``dask`` and
  ``dask-yarn`` on all machines and adds a baseline Skein config file. This
  file tells each machine where to locate the Dask-Yarn environment, as well
  as how many workers to use by default: 2. This way, you can get started with 
  ``dask-yarn`` by simply creating a ``YarnCluster`` object without providing
  any parameters. Dask relies on using Yarn to schedule its tasks.

- For the ``standalone`` configuration, this script installs ``dask`` and
  configures the cluster to use the Dask scheduler for managing Dask
  workloads.

- The Dataproc service itself provides support for web UIs such as Jupyter and
  the Dask web UIs. This will be explained in more detail below. 

Configuring your Dataproc Cluster
---------------------------------
There are several ways to `create a Dataproc cluster`_. This tutorial will
focus on using the `gcloud`_ SDK to do so.

First, you'll need to create a GCP Project. Please follow the instructions
`here`_ to do so.

Decide on a name for your Dataproc cluster. Then, pick a geographic `region`_
to place your cluster in, ideally one close to you. 

The following command will create a cluster for the dask-yarn configuration.

.. code-block:: console

    CLUSTER_NAME=<cluster-name>
    REGION=<region>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
      --region ${REGION} \
      --master-machine-type n1-standard-16 \
      --worker-machine-type n1-standard-16 \
      --image-version preview \
      --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh \
      --metadata dask-runtime=yarn \
      --optional-components JUPYTER \
      --enable-component-gateway

To break down this command:
  
- ``gcloud dataproc clusters create ${CLUSTER_NAME}`` uses the gcloud sdk to
  to create a Dataproc cluster.

- ``--region ${REGION}`` specifies the cluster region.

- ``--master-machine-type`` and ``worker-machine-type`` allow configuration of
  CPUs and RAM via different types of `machines`_.

- ``image-version preview`` specifies the `Dataproc image version`_. You'll
  use the latest preview image of Dataproc for the most up-to-date features.

- ``--initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh``
  specifies the initialization actions to install on the cluster. You can add
  as many as you'd like via a comma-separated list.

- ``--metadata dask-runtime=yarn`` specifies to configure your cluster with
  Dask configured for use with ``yarn``. 

- ``--optional-components JUPYTER`` configures the cluster with the Jupyter
  `optional component`_ to access Jupyter notebooks running on the cluster.
  Like initialization actions, you can add as many `optional components`_ as
  you'd like. These differ from initialization actions in that they come with
  first-class support from the Dataproc service, but there are less options
  available.
  
- ``--enable-component-gateway`` allows you to bypass needing an SSH tunnel
  for a certain predetermined list of web UIs on your cluster, such as Jupyter
  and the Yarn ApplicationMaster, by connecting directly through the Dataproc
  web console.

Connecting to your cluster
--------------------------

You can access your cluster several different ways. If you configured your 
cluster with a notebook service such as Jupyter or Zeppelin and enable 
component gateway (explained above), you can access these by navigating to
your `clusters`_ page, clicking on the name of your cluster and clicking on
the **Web Interfaces** tab to access your web UIs.

You can also ssh into your cluster. You can do this via the Dataproc web 
console: from the `clusters`_ page, click on your cluster name, then
**VM Instances** and click **SSH** next to the master node. 

Additionally, you can also use the gcloud sdk to SSH onto your cluster. First,
locate the zone that your cluster is in. This will be the region you specified
earlier but with a letter attached to it, such as ``us-central1-b``. 
To locate your cluster's zone, you can find this on the `clusters`_ page next
to your cluster. This was determined via Dataproc's `Auto Zone`_ feature, but
you can choose any zone to place your cluster by adding the ``--zone`` flag
when creating a new cluster.

.. code-block:: console

    gcloud compute ssh ${CLUSTER_NAME}-m --zone ${ZONE}

Once connected, either via a Jupyter notebook or via ssh, try running some
code. If your cluster is configured with Dask-Yarn:

.. code-block:: python
    
    from dask_yarn import YarnCluster
    from dask.distributed import Client
    import dask.array as da

    import numpy as np

    cluster = YarnCluster()
    client = Client(cluster)

    cluster.adapt() # Dynamically scale Dask resources

    x = da.sum(np.ones(5))
    x.compute()

If your cluster is configured with the standalone scheduler:

.. code-block:: python
   
    from dask.distributed import Client
    import dask.array as da

    import numpy as np

    client = Client("localhost:8786")

    x = da.sum(np.ones(5))
    x.compute()

Monitoring Dask Jobs
--------------------
You can monitor your Dask applications using Web UIs, depending on the runtime
you are using. 

For yarn mode, you can access the Skein Web UI via the YARN ResourceManager. 
To access the YARN ResourceManager, create your cluster with component
gateway enabled or create an `SSH tunnel`_. You can then access the Skein Web
UI by following these instructions.

For standalone mode, you can access the native Dask UI. Create an
`SSH tunnel`_ to access the Dask UI on port 8787.

Deleting your Dataproc Cluster
------------------------------
You can `delete`_ your cluster when you are done with it by running the
following command:

.. code-block:: console

    gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION}

Further Information
-------------------
Please refer to the Dataproc `documentation`_ for more information on using 
Dataproc.

.. _Dataproc: https://cloud.google.com/dataproc/
.. _Google Cloud: https://cloud.google.com/
.. _initialization actions: https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions
.. _README: https://github.com/GoogleCloudDataproc/initialization-actions/blob/master/dask/README.md
.. _installation script: https://github.com/GoogleCloudDataproc/initialization-actions/blob/master/dask/dask.sh
.. _create a Dataproc cluster: https://cloud.google.com/dataproc/docs/guides/create-cluster
.. _gcloud: https://cloud.google.com/sdk/docs/
.. _here: https://cloud.google.com/resource-manager/docs/creating-managing-projects
.. _region: https://cloud.google.com/compute/docs/regions-zones
.. _machines: https://cloud.google.com/compute/docs/machine-types#n1_standard_machine_types
.. _optional component: https://cloud.google.com/dataproc/docs/concepts/components/jupyter
.. _Dataproc image version: https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions#ubuntu_images
.. _component gateway: https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways
.. _Optional components: https://cloud.google.com/dataproc/docs/concepts/components/overview
.. _clusters: https://console.cloud.google.com/dataproc
.. _Auto Zone: https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/auto-zone
.. _SSH tunnel: https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces
.. _delete: https://cloud.google.com/dataproc/docs/guides/manage-cluster
.. _documentation: https://cloud.google.com/dataproc/docs