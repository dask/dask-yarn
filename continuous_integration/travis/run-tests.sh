set -xe

source activate test-environment
cd dask-yarn
py.test dask_yarn --verbose
flake8 dask_yarn
