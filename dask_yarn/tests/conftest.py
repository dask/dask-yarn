import os
import subprocess
import sys
import time
from contextlib import contextmanager

import skein
import pytest


@pytest.fixture(scope='session')
def conda_env():
    conda_pack = pytest.importorskip('conda_pack')
    envpath = 'dask-yarn-py%d%d.tar.gz' % sys.version_info[:2]
    if not os.path.exists(envpath):
        conda_pack.pack(output=envpath, verbose=True)
    return envpath


@pytest.fixture(scope='session')
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


@contextmanager
def ensure_shutdown(client, app_id, status=None):
    try:
        yield
    except Exception:
        client.kill_application(app_id)
        raise
    else:
        try:
            check_is_shutdown(client, app_id, status=status)
        except AssertionError:
            client.kill_application(app_id)
            raise


def wait_for_completion(client, app_id, timeout=30):
    while timeout:
        final_status = client.application_report(app_id).final_status
        if final_status != 'UNDEFINED':
            return final_status
        time.sleep(0.1)
        timeout -= 0.1
    else:
        assert False, "Application timed out"


def get_logs(app_id, tries=3):
    command = ["yarn", "logs", "-applicationId", app_id]
    for i in range(tries - 1):
        try:
            return subprocess.check_output(command).decode()
        except Exception:
            pass
    return subprocess.check_output(command).decode()
