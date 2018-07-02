import os
import time
import sys

import conda_pack
import dask
from dask.distributed import Client
from distributed.utils_test import loop, inc
import pytest
import skein

from dask_yarn import YarnCluster, make_specification

loop = loop  # silence flake8 f811


@pytest.fixture(scope='module')
def conda_env():
    envpath = 'dask-yarn-py%d%d.tar.gz' % sys.version_info[:2]
    if not os.path.exists(envpath):
        conda_pack.pack(output=envpath, verbose=True)
    return envpath


@pytest.fixture(scope='module')
def spec(conda_env):
    spec = make_specification(environment=conda_env,
                              worker_memory='512 MB',
                              scheduler_memory='512 MB',
                              name='dask-yarn-tests')
    return spec


@pytest.fixture(scope='module')
def skein_client():
    with skein.Client() as client:
        yield client


def test_basic(skein_client, loop, spec):
    with YarnCluster(spec, skein_client=skein_client) as cluster:
        cluster.scale(2)
        with Client(cluster, loop=loop) as client:
            future = client.submit(inc, 10)
            assert future.result() == 11

            start = time.time()
            while len(client.scheduler_info()['workers']) < 2:
                time.sleep(0.1)
                assert time.time() < start + 5

            client.get_versions(check=True)


def test_yaml_file(skein_client, loop, spec, tmpdir):
    fn = os.path.join(str(tmpdir), 'spec.yaml')
    with open(fn, 'w') as f:
        f.write(spec.to_yaml())

    with YarnCluster(fn, skein_client=skein_client) as cluster:
        with Client(cluster, loop=loop):
            pass


def test_config_spec(skein_client, spec, loop):
    with dask.config.set({'yarn': {'specification': spec.to_dict()}}):
        with YarnCluster(skein_client=skein_client) as cluster:
            with Client(cluster, loop=loop):
                pass


def test_constructor_keyword_arguments(skein_client, loop, conda_env, spec):
    with YarnCluster(environment=conda_env, skein_client=skein_client) as cluster:
        with Client(cluster, loop=loop):
            pass

    with pytest.raises(ValueError) as info:
        with YarnCluster(spec, environment=conda_env, skein_client=skein_client):
            pass

    assert "keyword" in str(info.value)
    assert "not used" in str(info.value)


def test_configuration():
    config = {'yarn': {
        'environment': 'myenv.tar.gz',
        'queue': 'myqueue',
        'name': 'dask-yarn-tests',
        'tags': ['a', 'b', 'c'],
        'specification': None,
        'workers': {'memory': '1234MB', 'instances': 1, 'vcores': 1, 'restarts': -1},
        'scheduler': {'memory': '1234MB', 'vcores': 1}}
    }

    with dask.config.set(config):
        spec = make_specification()
        assert spec.name == 'dask-yarn-tests'
        assert spec.queue == 'myqueue'
        assert spec.tags == {'a', 'b', 'c'}
        assert spec.services['dask.worker'].resources.memory == 1234
        assert spec.services['dask.scheduler'].resources.memory == 1234


def test_make_specification_errors():
    with dask.config.set({'yarn.environment': None}):
        with pytest.raises(ValueError) as info:
            make_specification()

        assert 'conda-pack' in str(info.value)

    with pytest.raises(ValueError) as info:
        make_specification(environment='foo.tar.gz', worker_memory=1234)

        assert '1234 MB' in str(info.value)

    with pytest.raises(ValueError) as info:
        make_specification(environment='foo.tar.gz', scheduler_memory=1234)

        assert '1234 MB' in str(info.value)


def test_environment_relative_paths(conda_env):
    assert (make_specification(conda_env).to_dict() ==
            make_specification(os.path.relpath(conda_env)).to_dict())
