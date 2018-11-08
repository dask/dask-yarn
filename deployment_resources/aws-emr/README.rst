Resources for Deploying on AWS Elastic MapReduce (EMR)
======================================================

This directory contains an *example* `bootstrap action
<https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html>`__
for deploying Dask on `AWS EMR
<https://docs.aws.amazon.com/emr/#lang/en_us>`__. The script does the following
things:

- Installs `miniconda <http://conda.pydata.org/miniconda.html>`__.

- Installs ``dask``, ``distributed``, ``dask-yarn``, `pyarrow
  <https://arrow.apache.org/docs/python/>`__, and `s3fs
  <http://s3fs.readthedocs.io/en/latest/>`__.  This list of packages can be
  extended using the ``--conda-packages`` flag.

- Packages the environment with `conda-pack
  <https://conda.github.io/conda-pack/>`__ for distribution to the workers.

- Optionally installs and starts a Jupyter Notebook Server running on port
  8888. This can be disabled with the ``--no-jupyter`` flag. The password for
  the notebook server can be set with the ``--password`` option, the default
  is ``dask-user``.

Example usage:

.. code-block:: bash

    bootstrap-dask --password mypassword --conda-packages scikit-learn numba

While usable directly as is, the bootstrap script is heavily commented, and
should hopefully provide you enough of an example to create your own custom
script should the need arise.
