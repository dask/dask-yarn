from __future__ import division, print_function, absolute_import

import os
import shutil
import time

import dask
from dask.distributed import Client
from distributed.utils_test import inc
import pytest

from dask_yarn import YarnCluster
from dask_yarn.core import _make_specification, _make_submit_specification
from dask_yarn.tests.conftest import check_is_shutdown


def test_basic(skein_client, conda_env):
    with YarnCluster(environment=conda_env,
                     worker_memory='512 MiB',
                     scheduler_memory='512 MiB',
                     name='test-basic',
                     skein_client=skein_client) as cluster:
        # Smoketest repr
        repr(cluster)

        # Scale up
        cluster.scale(2)

        with Client(cluster) as client:
            future = client.submit(inc, 10)
            assert future.result() == 11
            client.get_versions(check=True)

        # Check that 2 workers exist
        start = time.time()
        while len(cluster.workers()) != 2:
            time.sleep(0.1)
            assert time.time() < start + 5, "timeout cluster.scale(2)"

        # Scale down
        cluster.scale(1)

        start = time.time()
        while len(cluster.workers()) != 1:
            time.sleep(0.1)
            assert time.time() < start + 5, "timeout cluster.scale(1)"

    check_is_shutdown(skein_client, cluster.app_id)


def test_from_specification(skein_client, conda_env, tmpdir):
    spec = _make_specification(environment=conda_env,
                               worker_memory='512 MiB',
                               scheduler_memory='512 MiB',
                               name='dask-yarn-test-from-specification')
    fn = os.path.join(str(tmpdir), 'spec.yaml')
    with open(fn, 'w') as f:
        f.write(spec.to_yaml())

    with YarnCluster.from_specification(fn, skein_client=skein_client) as cluster:
        with Client(cluster):
            pass

    check_is_shutdown(skein_client, cluster.app_id)


def test_from_application_id(skein_client, conda_env):
    with YarnCluster(environment=conda_env,
                     worker_memory='512 MiB',
                     scheduler_memory='512 MiB',
                     name='test-from-application-id',
                     skein_client=skein_client) as cluster:

        # Connect to the application with the application id
        cluster2 = YarnCluster.from_application_id(cluster.app_id, skein_client)

        cluster2.scale(1)

        start = time.time()
        while len(cluster2.workers()) != 1:
            time.sleep(0.1)
            assert time.time() < start + 5, "timeout cluster.scale(1)"

        del cluster2

        # Cluster is still running, finalizer not run in cluster2
        assert len(cluster.workers()) == 1

    check_is_shutdown(skein_client, cluster.app_id)


def test_from_current(skein_client, conda_env, monkeypatch, tmpdir):
    # Not running in a container
    with pytest.raises(ValueError) as exc:
        YarnCluster.from_current()
    assert str(exc.value) == "Not running inside a container"

    with YarnCluster(environment=conda_env,
                     worker_memory='512 MiB',
                     scheduler_memory='512 MiB',
                     name='test-from-current',
                     skein_client=skein_client) as cluster:

        # Patch environment so it looks like a container
        container_id = 'container_1526134340424_0012_01_000005'
        cont_dir = tmpdir.mkdir(container_id)
        shutil.copyfile(skein_client.security.cert_path,
                        str(cont_dir.join(".skein.crt")))
        shutil.copyfile(skein_client.security.key_path,
                        str(cont_dir.join(".skein.pem")))

        for key, val in [('SKEIN_APPLICATION_ID', cluster.app_id),
                         ('CONTAINER_ID', container_id),
                         ('SKEIN_APPMASTER_ADDRESS', cluster.application_client.address),
                         ('LOCAL_DIRS', str(tmpdir))]:
            monkeypatch.setenv(key, val)

        import skein.core
        monkeypatch.setattr(skein.core, 'properties', skein.core.Properties())

        cluster2 = YarnCluster.from_current()
        assert cluster2.app_id == cluster.app_id
        assert cluster2.scheduler_address == cluster.scheduler_address

        # Smoketest method
        cluster2.workers()

        finalizer = cluster2._finalizer
        assert finalizer is not None
        del cluster2

        # finalizer didn't run
        assert finalizer.peek() is not None
        # Manually run finalizer, would normally be called at exit
        finalizer()

        # Remove finalizer from cluster to prevent double-calling
        cluster._finalizer.detach()
        cluster._finalizer = None

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
                               name='test-configuration-full-specification')
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


def test_make_submit_specification():
    spec = _make_submit_specification('../script.py',
                                      environment='myenv.tar.gz',
                                      name='test-name',
                                      client_vcores=2,
                                      client_memory='2 GiB')

    client = spec.services['dask.client']
    scheduler = spec.services['dask.scheduler']
    assert client.files['environment'] == scheduler.files['environment']
    assert client.files['script.py'].source.startswith('file:///')
    assert client.resources.memory == 2048
    assert client.resources.vcores == 2

    config = {
        'yarn.name': 'dask-yarn-tests',
        'yarn.queue': 'myqueue',
        'yarn.environment': 'myenv.tar.gz',
        'yarn.client.memory': '1234 MiB',
        'yarn.client.vcores': 2,
        'yarn.client.env': {'foo': 'bar'},
    }

    with dask.config.set(config):
        spec = _make_submit_specification('script.py')
        assert spec.name == 'dask-yarn-tests'
        assert spec.queue == 'myqueue'
        assert spec.services['dask.client'].resources.memory == 1234
        assert spec.services['dask.client'].resources.vcores == 2
        assert spec.services['dask.client'].env == {'foo': 'bar'}


def test_environment_relative_paths(conda_env):
    assert (_make_specification(environment=conda_env).to_dict() ==
            _make_specification(environment=os.path.relpath(conda_env)).to_dict())
