#!/usr/bin/env bash
set -xe

conda config --set always_yes yes --set changeps1 no

conda create -n test-environment \
    cryptography \
    dask \
    distributed \
    flake8 \
    protobuf \
    grpcio \
    nomkl \
    pytest \
    python=$1 \
    pyyaml

source activate test-environment

pip install conda-pack 
pip install --no-deps grpcio-tools

if [[ $1 == '2.7' ]]; then
    pip install backports.weakref
fi

pip install --no-deps git+https://github.com/jcrist/skein

cd ~/dask-yarn
pip install -v --no-deps .

conda list

set +xe
