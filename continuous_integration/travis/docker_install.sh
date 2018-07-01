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

conda activate test-environment

pip install grpcio-tools

pip install -v --no-deps .

conda list

set +xe
