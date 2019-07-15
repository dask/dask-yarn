#!/usr/bin/env bash
set -xe

conda config --set always_yes yes --set changeps1 no

conda create -n test-environment \
    cryptography \
    dask \
    distributed \
    flake8 \
    nomkl \
    pytest \
    python=$1 \
    pyyaml

source activate test-environment

pip install conda-pack skein

if [[ $1 == '2.7' ]]; then
    pip install backports.weakref
fi

cd ~/dask-yarn
pip install -v --no-deps .

conda list

set +xe
