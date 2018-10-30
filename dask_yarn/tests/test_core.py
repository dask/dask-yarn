import os
import time
import sys

import conda_pack
import dask
from dask.distributed import Client
from distributed.utils_test import loop, inc
import pytest
import skein

from dask_yarn import YarnCluster
from dask_yarn.core import _make_specification

loop = loop  # silence flake8 f811

APPNAME = 'dask-yarn-tests'


@pytest.fixture(scope='module')
def conda_env():
    envpath = 'dask-yarn-py%d%d.tar.gz' % sys.version_info[:2]
    if not os.path.exists(envpath):
        conda_pack.pack(output=envpath, verbose=True)
    return envpath


@pytest.fixture
def skein_client():
    with skein.Client() as client:
        yield client


def check_is_shutdown(client, app_id, status='SUCCEEDED'):
    timeleft = 5
    report = client.application_report(app_id)
    while report.state not in ('FINISHED', 'FAILED', 'KILLED'):
        time.sleep(0.1)
        timeleft -= 0.1
        if timeleft < 0:
            client.kill_application(app_id)
            assert False, "Application wasn't properly terminated"

    assert report.final_status == status


def test_basic(skein_client, conda_env, loop):
    with YarnCluster(environment=conda_env,
                     worker_memory='512 MiB',
                     scheduler_memory='512 MiB',
                     name=APPNAME,
                     skein_client=skein_client) as cluster:
        cluster.scale(2)
        with Client(cluster, loop=loop) as client:
            future = client.submit(inc, 10)
            assert future.result() == 11

            start = time.time()
            while len(client.scheduler_info()['workers']) < 2:
                time.sleep(0.1)
                assert time.time() < start + 5

            client.get_versions(check=True)

    check_is_shutdown(skein_client, cluster.app_id)


def test_from_specification(skein_client, conda_env, tmpdir, loop):
    spec = _make_specification(environment=conda_env,
                               worker_memory='512 MiB',
                               scheduler_memory='512 MiB',
                               name=APPNAME)
    fn = os.path.join(str(tmpdir), 'spec.yaml')
    with open(fn, 'w') as f:
        f.write(spec.to_yaml())

    with YarnCluster.from_specification(fn, skein_client=skein_client) as cluster:
        with Client(cluster, loop=loop):
            pass

    check_is_shutdown(skein_client, cluster.app_id)


def test_configuration():
    config = {'yarn': {
        'environment': 'myenv.tar.gz',
        'queue': 'myqueue',
        'name': 'dask-yarn-tests',
        'tags': ['a', 'b', 'c'],
        'specification': None,
        'worker': {'memory': '1234 MiB', 'count': 1, 'vcores': 1, 'restarts': -1,
                   'env': {'foo': 'bar'}},
        'scheduler': {'memory': '1234 MiB', 'vcores': 1}}
    }

    with dask.config.set(config):
        spec = _make_specification()
        assert spec.name == 'dask-yarn-tests'
        assert spec.queue == 'myqueue'
        assert spec.tags == {'a', 'b', 'c'}
        assert spec.services['dask.worker'].resources.memory == 1234
        assert spec.services['dask.worker'].env == {'foo': 'bar'}
        assert spec.services['dask.scheduler'].resources.memory == 1234


def test_configuration_full_specification(conda_env, tmpdir):
    spec = _make_specification(environment=conda_env,
                               worker_memory='512 MiB',
                               scheduler_memory='512 MiB',
                               name=APPNAME)
    fn = os.path.join(str(tmpdir), 'spec.yaml')
    with open(fn, 'w') as f:
        f.write(spec.to_yaml())

    # path to full specification
    with dask.config.set({'yarn': {'specification': fn}}):
        spec = _make_specification()
        assert spec == spec

    # full specification inlined
    with dask.config.set({'yarn': {'specification': spec.to_dict()}}):
        spec = _make_specification()
        assert spec == spec

    # when overrides specified, ignores full specification
    override = {'specification': spec.to_dict()}
    override.update(dask.config.get('yarn'))
    override['name'] = 'config-name'
    with dask.config.set({'yarn': override}):
        spec = _make_specification(environment='foo',
                                   name='test-name',
                                   n_workers=4)
        assert spec.services['dask.worker'].instances == 4
        assert spec.name == 'test-name'


def test_make_specification_errors():
    with dask.config.set({'yarn.environment': None}):
        with pytest.raises(ValueError) as info:
            _make_specification()

        assert 'conda-pack' in str(info.value)

    with pytest.raises(ValueError) as info:
        _make_specification(environment='foo.tar.gz', worker_memory=1234)

        assert '1234 MiB' in str(info.value)

    with pytest.raises(ValueError) as info:
        _make_specification(environment='foo.tar.gz', scheduler_memory=1234)

        assert '1234 MiB' in str(info.value)


def test_environment_relative_paths(conda_env):
    assert (_make_specification(environment=conda_env).to_dict() ==
            _make_specification(environment=os.path.relpath(conda_env)).to_dict())
