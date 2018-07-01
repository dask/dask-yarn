#!/usr/bin/env bash
set -xe

conda config --set always_yes yes --set changeps1 no

conda create -n test-environment \
    cryptography \
    dask \
    distributed \
    flake8 \
    grpcio \
    maven \
    nomkl \
    pytest \
    python=$PYTHON \
    pyyaml

source activate test-environment

pip install grpcio-tools

cd ~/dask-yarn
pip install -v --no-deps .

conda list

set +xe
