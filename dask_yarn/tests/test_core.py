import conda_pack

import dask
from dask_yarn import YarnCluster, make_specification
from dask.distributed import Client
from distributed.utils_test import loop, inc  # noqa: F811, F401
import time
import skein
import os

import pytest


@pytest.fixture(scope='module')
def env():
    env = conda_pack.CondaEnv.from_default()
    fn = os.path.abspath(env.name + '.tar.gz')
    if not os.path.exists(fn):
        print('Creating conda environment')
        env.pack(verbose=True)
    return fn


@pytest.fixture(scope='module')
def spec(env):
    spec = make_specification(env, worker_memory='1024 MB',
                              scheduler_memory='1024 MB')
    return spec


@pytest.fixture(scope='module')
def skein_client():
    skein.Client.start_global_daemon()

    with skein.Client() as client:
        yield client

    skein.Client.stop_global_daemon()


@pytest.fixture(scope='module')
def clean(skein_client):
    applications = skein_client.applications()
    for app in applications:
        skein_client.kill(app)


def test_basic(loop, spec, clean):  # noqa F811
    with YarnCluster(spec) as cluster:
        cluster.scale(2)
        with Client(cluster, loop=loop) as client:
            future = client.submit(inc, 10)
            assert future.result() == 11

            start = time.time()
            while len(client.scheduler_info()['workers']) < 2:
                time.sleep(0.1)
                assert time.time() < start + 5

            client.get_versions(check=True)


def test_yaml_file(loop, spec, clean, tmpdir):  # noqa F811
    fn = os.path.join(str(tmpdir), 'spec.yaml')
    with open(fn, 'w') as f:
        f.write(spec.to_yaml())

    with YarnCluster(fn) as cluster:
        with Client(cluster, loop=loop):
            pass


def test_config_spec(spec, loop):  # noqa F811
    with dask.config.set({'yarn': {'specification': spec.to_dict()}}):
        with YarnCluster() as cluster:
            with Client(cluster, loop=loop):
                pass


def test_config_env(loop, env):  # noqa F811
    config = {'yarn': {
        'environment': env,
        'queue': 'q-123',
        'name': 'dask-name-123',
        'tags': ['a', 'b', 'c'],
        'specification': None,
        'workers': {'memory': '1234MB', 'instances': 1, 'vcores': 1, 'restarts': -1},
        'scheduler': {'memory': '1234MB', 'vcores': 1}}
    }

    with dask.config.set(config):
        spec = make_specification()
        assert 'dask-name-123' in str(spec.to_dict())
        assert 'q-123' in str(spec.to_dict())
        with YarnCluster() as cluster:
            with Client(cluster, loop=loop) as client:
                client.submit(lambda: 0).result()  # wait for a worker

                info = client.scheduler_info()
                assert len(info['workers']) == 1
                [w] = info['workers'].values()
                assert w['memory_limit'] == 1234e6


def test_constructor_keyword_arguments(loop, env, spec):  # noqa F811
    with YarnCluster(environment=env) as cluster:
        with Client(cluster, loop=loop):
            pass

    with pytest.raises(ValueError) as info:
        with YarnCluster(spec, environment=env):
            pass

    assert "keyword" in str(info.value)
    assert "not used" in str(info.value)


def test_config_errors():
    with dask.config.set({'yarn.environment': None}):
        with pytest.raises(ValueError) as info:
            make_specification()

        assert all(word in str(info.value)
                   for word in
                   ['redeployable', 'conda-pack', 'dask-yarn.readthedocs.org'])

    with pytest.raises(ValueError) as info:
        make_specification(environment='foo.tar.gz', worker_memory=1234)

        assert '1234 MB' in str(info.value)

    with pytest.raises(ValueError) as info:
        make_specification(environment='foo.tar.gz', scheduler_memory=1234)

        assert '1234 MB' in str(info.value)


def test_relative_paths(env):
    assert (make_specification(env).to_dict() ==
            make_specification(os.path.relpath(env)).to_dict())
