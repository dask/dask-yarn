import conda_pack

import dask_yarn
from dask_yarn import YarnCluster
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
    spec = dask_yarn.make_remote_spec(env, worker_memory=1024, scheduler_memory=1024)
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
