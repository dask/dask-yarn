#!/usr/bin/env bash
set -xe

conda config --set always_yes yes --set changeps1 no
conda update conda -n base

conda create -n test-environment -c conda-forge \
    cryptography \
    dask \
    distributed \
    dask-ctl \
    flake8 \
    nomkl \
    pytest \
    python=$1 \
    pyyaml \
    regex \
    conda-pack>=0.6 \
    skein>=0.8.1 \
    pytest-asyncio \
    black \
    ipywidgets

source activate test-environment

cd ~/dask-yarn
python -m pip install -v --no-deps .

conda list

set +xe
