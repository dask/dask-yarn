import conda_pack
# from dask_yarn import YarnCluster
from distributed.utils_test import loop, inc
import skein
import toolz
import os

import pytest


@pytest.fixture(scope='module')
def env():
    env = conda_pack.CondaEnv.from_default()
    fn = os.path.abspath(env.name + '.zip')
    if not os.path.exists(fn):
        print('Creating conda environment')
        env.pack(verbose=True)
    return fn


@pytest.fixture()
def scheduler_template(env):
    envname = os.path.basename(env).rsplit('.', 1)[0]
    scheduler_template = {
        'resources': {'vcores': 1, 'memory': 1024},
        'instances': 1,
        'files': {envname: {'source': env, 'type': 'archive'}},
        'commands': [
            "ls",
            "source " + envname + "/" + envname + "/bin/activate",
            "dask-yarn-scheduler"
        ]
    }
    return scheduler_template

@pytest.fixture()
def worker_template(env):
    envname = os.path.basename(env).rsplit('.', 1)[0]
    worker_template = {
        'resources': {'vcores': 1, 'memory': 1024},
        'files': {envname: {'source': env, 'type': 'archive'}},
        'commands': [
            "ls",
            "source " + envname + "/" + envname + "/bin/activate",
            "dask-yarn-worker --memory-limit 1024MB --nthreads 1"
        ]
    }
    return worker_template


@pytest.fixture(scope='module')
def skein_client():
    with skein.Client.temporary() as client:
        yield client


@pytest.fixture(scope='module')
def clean(skein_client):
    applications = skein_client.applications()
    for app in applications:
        skein_client.kill(app)


def test_basic(loop, scheduler_template, worker_template):
    with YarnCluster(n_workers=2,
                     worker_template=worker_template,
                     scheduler_template=scheduler_template) as cluster:
        with Client(cluster, loop=loop) as client:
            future = client.submit(inc, 10)
            assert future.result() == 11

            assert len(client.scheduler_info()['workers']) == 2

            client.get_versions(check=True)


def test_yarn_scheduler(skein_client, clean, scheduler_template,
                        worker_template):
    template = {
        'services': {
            'scheduler': scheduler_template,
            'workers': toolz.merge(worker_template, {'instances': 2}),
        },
        'name': 'my-job',
        'queue': 'default',
    }

    app = skein_client.submit(skein.Job.from_dict(template))
    ac = app.connect()
    print(ac.kv.wait('scheduler-address'))
    print(dict(ac.kv))
    import pdb; pdb.set_trace()
