set -xe

source activate test-environment
py.test dask-yarn --verbose
flake8 dask-yarn
