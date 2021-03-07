#!/usr/bin/env bash
set -xe

conda config --set always_yes yes --set changeps1 no
conda update conda -n base

conda create -n test-environment \
    cryptography \
    dask \
    distributed \
    flake8 \
    nomkl \
    pytest \
    python=$1 \
    pyyaml \
    regex \
    ipywidgets

source activate test-environment

python -m pip install conda-pack skein pytest-asyncio black

cd ~/dask-yarn
python -m pip install -v --no-deps .

conda list

set +xe
