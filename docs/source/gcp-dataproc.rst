Deploying on Google Cloud Dataproc
==================================

`Dataproc`_ is `Google Cloud`_'s hosted Apache Hadoop and Apache Spark service.
Dataproc supports a series of `initialization actions`_ that allow you to
install a wide range of open source tools at cluster creation time. In
particular, an initialization action for adding Dask and Dask-Yarn will
configure these libraries and their dependencies on the cluster for you.

The following instructions will guide you through creating a Dataproc cluster
with Dask and/or Dask-Yarn configured for you. This tutorial is loosely adapted
from the `README`_ for the Dask initialization action.

What the Initialization Action is Doing
---------------------------------------
The initialization action `installation script`_ does several things:

- It accepts a metadata parameter for configuring your cluster to use Dask
  with either it's standalone scheduler or with Dask-Yarn to utilize Yarn.

- For a yarn configuration, this script installs ``dask`` and ``dask-yarn`` on
  all machines and adds a baseline Skein config file. This file tells each 
  machine where to locate the Dask-Yarn environment, as well as how many 
  workers to use by default: 2. This way, you can get started with 
  ``dask-yarn`` by simply creating a ``YarnCluster`` object without providing
  any parameters.

- For a "standalone" configuration, this script installs ``dask`` and
  configures the Dask scheduler service itself on the cluster.

- This initialization action does not provide extra accomodations for
  compatibility with Jupyter Notebooks or Dask UIs as the Dataproc service
  itself provides support for these. This will be explained in more detail
  below. 

Configuring your Dataproc Cluster
---------------------------------
There are several different ways to `create a Dataproc cluster`_. This tutorial
will focus on using the `gcloud`_ sdk to do so.

First, you'll need to create a GCP Project. Please follow the instructions
`here`_ before continuing.

Decide on a name for your Dataproc cluster. Then, pick a geographic `region`_
to place your cluster, ideally one closest to you. 

The following command will create a cluster for the dask-yarn configuration.

.. code-block:: console

    CLUSTER_NAME=<cluster-name>
    REGION=<region>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
      --region ${REGION} \
      --master-machine-type n1-standard-16 \
      --worker-machine-type n1-standard-16 \
      --image-version preview-ubuntu \
      --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh \
      --metadata dask-runtime=yarn \
      --optional-components JUPYTER \
      --enable-component-gateway

To break down what is happening here:
  
- ``gcloud dataproc clusters create ${CLUSTER_NAME}`` uses the gcloud SDK to
  to create a Dataproc cluster with the name you supplied.

- ``--region ${REGION}}`` tells the Dataproc service where to place the
  cluster.

- ``--master-machine-type`` and ``worker-machine-type`` denote what types of
  `machines`_ the cluster should use. Using ``n1-standard-16``s provide more
  computing power than the default.

- ``image-version preview-ubuntu`` specifies the `Dataproc image version`_. 
  Using the latest version of Dataproc is generally preferred when possible.
  In this example, you'll create an Ubuntu Dataproc 2.0 cluster, currently 
  in preview as of writing this.

- ``--initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh``
  can include a comma separated list of initialization actions to install on
  the cluster. You'll only be installing the ``dask`` initialization action
  in this tutorial, but you can combine as many of these initialization
  actions as you'd like. You can even use your own scripts!

- ``--metadata dask-runtime=yarn`` specifies to configure your cluster with
  either ``dask-yarn`` or the Dask scheduler service. The default is ``yarn``
  but we can add this parameter as an example anyway.

- ``--optional-components JUPYTER`` configures the cluster with the Jupyter
  `optional component`_ to access Jupyter notebooks running on the cluster.
  `Optional components`_ provide first-class support for various open source
  tools on your cluster. Like initialization actions, you can add any number
  of these to your cluster.
  
- ``--enable-component-gateway`` allows you to bypass needing an SSH tunnel
  for a certain predetermined list of web UIs on your cluster, such as Jupyter
  and the Yarn ApplicationMaster, directly in the Dataproc console. Navigate
  to your `clusters`_ page, click on the name of your cluster and click on the
  **Web Interfaces** tab.

Connecting to your cluster
--------------------------

You can access your cluster several different ways. One way is to utilize
Jupyter notebooks as explained above. You can also ssh into your cluster via
the gcloud sdk to access the Python environment. First locate the zone that 
your cluster is in. This will be the region you specified before but with a
letter attached to it, such as ``us-central1-b``. You can find this on the 
`clusters`_ page. 

.. code-block:: console

    gcloud compute ssh ${CLUSTER_NAME}-m --zone ${ZONE}

Additionally, you can also SSH into your cluster via the Dataproc web console.
From the `clusters`_ page, click on your cluster name, then **VM Instances**
and click **SSH** next to the master node. 

Once connected, either via a Jupyter notebook or via ssh, try running some
code. If your cluster is configured with Dask-Yarn:

.. code-block:: python
    
    # If running Dask-Yarn
    from dask_yarn import YarnCluster
    from dask.distributed import Client
    import dask.array as da

    import numpy as np

    cluster = YarnCluster()
    client = Client(cluster)

    cluster.adapt() # Dynamically scale Dask resources

    x = da.sum(np.ones(5))
    x.compute()

You can also manually configure the number of workers by 

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

For yarn mode, you can access the Skein Web UI via the YARN 
ResourceManager. To access the YARN ResourceManager, create your cluster with 
component gateway enabled or create an `SSH tunnel`_. You can then access the
Skein Web UI by following these instructions.

For standalone mode, you can access the native Dask UI. Create
an `SSH tunnel`_ to access the Dask UI on port 8787.

Deleting your Dataproc Cluster
------------------------------
You can `delete`_ your cluster when you are done with it by running the
following command:

.. code-block:: console

    gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION}

Further Information
-------------------
Please refer to the Dataproc `documentaiton`_ for more information on using 
Dataproc.

.. _Dataproc: https://cloud.google.com/dataproc/
.. _Google Cloud: https://cloud.google.com/
.. _README: https://github.com/GoogleCloudDataproc/initialization-actions/blob/master/dask/README.md
.. _installation script: https://github.com/GoogleCloudDataproc/initialization-actions/blob/master/dask/dask.sh
.. _create a Dataproc cluster: https://cloud.google.com/dataproc/docs/guides/create-cluster
.. _here: https://cloud.google.com/resource-manager/docs/creating-managing-projects
.. _region: https://cloud.google.com/compute/docs/regions-zones
.. _machines: https://cloud.google.com/compute/docs/machine-types#n1_standard_machine_types
.. _optional component: https://cloud.google.com/dataproc/docs/concepts/components/jupyter
.. _Dataproc image version: https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions#ubuntu_images
.. _component gateway: https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways
.. _Optional components: https://cloud.google.com/dataproc/docs/concepts/components/overview
.. _clusters: https://console.cloud.google.com/dataproc
.. _SSH tunnel: https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces
.. _delete: https://cloud.google.com/dataproc/docs/guides/manage-cluster
.. _documentation: https://cloud.google.com/dataproc/docs