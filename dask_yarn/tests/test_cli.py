from __future__ import division, print_function, absolute_import

import pytest

import dask_yarn
from dask_yarn.cli import main
from dask_yarn.tests.conftest import ensure_shutdown, wait_for_completion, get_logs


def run_command(command, error=False):
    with pytest.raises(SystemExit) as exc:
        main([arg for arg in command.split(' ') if arg])
    if error:
        assert exc.value.code != 0
    else:
        assert exc.value.code == 0


@pytest.mark.parametrize('command',
                         ['',
                          'scheduler',
                          'worker',
                          'submit',
                          'status',
                          'kill'])
def test_cli_help(command, capfd):
    run_command(command + ' -h')

    out, err = capfd.readouterr()
    assert not err
    assert 'usage: dask-yarn' in out


@pytest.mark.parametrize('group', [''])
def test_cli_call_command_group(group, capfd):
    run_command(group, error=True)

    out, err = capfd.readouterr()
    assert not out
    assert 'usage: dask-yarn' in err


def test_cli_version(capfd):
    run_command('--version')

    out, err = capfd.readouterr()
    assert not err
    assert dask_yarn.__version__ in out


GOOD_TEST_SCRIPT = """
import dask_yarn
from dask.distributed import Client

print("Connecting to cluster")
cluster = dask_yarn.YarnCluster.from_current()

print("Creating Dask Client")
client = Client(cluster)

print("Doing some computation")
assert client.submit(lambda x: x + 1, 1).result() == 2

print("Done!")
"""


BAD_TEST_SCRIPT = """
import dask_yarn
from dask.distributed import Client

print("Connecting to cluster")
cluster = dask_yarn.YarnCluster.from_current()

print("Creating Dask Client")
client = Client(cluster)

print("Intentionally Erroring")
assert False, "Failed!"
"""


scripts = {'good': GOOD_TEST_SCRIPT,
           'bad': BAD_TEST_SCRIPT}


@pytest.mark.parametrize('script_kind, final_status, searchtxt',
                         [('good', 'SUCCEEDED', 'Done!'),
                          ('bad', 'FAILED', 'Failed!')])
def test_cli_submit_and_status(script_kind, final_status, searchtxt,
                               tmpdir, conda_env, skein_client, capfd):
    script_path = str(tmpdir.join('script.py'))
    with open(script_path, 'w') as fil:
        fil.write(scripts[script_kind])

    run_command('submit '
                '--name test-cli-submit-and-status '
                '--environment %s '
                '--worker-count 1 '
                '--worker-memory 256MiB '
                '--worker-vcores 1 '
                '--scheduler-memory 256MiB '
                '--scheduler-vcores 1 '
                '--client-memory 128MiB '
                '--client-vcores 1 '
                '%s' % (conda_env, script_path))
    out, err = capfd.readouterr()
    # Logs go to err
    assert 'INFO' in err
    app_id = out.strip()
    assert '\n' not in app_id

    with ensure_shutdown(skein_client, app_id, status=final_status):
        # Wait for app to start
        skein_client.connect(app_id)

        # `dask-yarn status`
        run_command('status %s' % app_id)
        out, err = capfd.readouterr()
        assert 'INFO' in err
        assert len(out.splitlines()) == 2
        assert 'RUNNING' in out

        wait_for_completion(skein_client, app_id, timeout=60)

    logs = get_logs(app_id)
    assert searchtxt in logs


def test_cli_kill(tmpdir, conda_env, skein_client, capfd):
    script_path = tmpdir.join('script.py')
    with open(script_path, 'w') as fil:
        fil.write(GOOD_TEST_SCRIPT)

    run_command('submit '
                '--name test-cli-kill '
                '--environment %s '
                '--worker-count 1 '
                '--worker-memory 256MiB '
                '--worker-vcores 1 '
                '--scheduler-memory 256MiB '
                '--scheduler-vcores 1 '
                '--client-memory 128MiB '
                '--client-vcores 1 '
                '%s' % (conda_env, script_path))
    out, err = capfd.readouterr()
    # Logs go to err
    assert 'INFO' in err
    app_id = out.strip()
    assert '\n' not in app_id

    with ensure_shutdown(skein_client, app_id, status='KILLED'):
        # Wait for app to start
        skein_client.connect(app_id)

        # `dask-yarn kill`
        run_command('kill %s' % app_id)
        out, err = capfd.readouterr()
        assert 'INFO' in err
        assert not out

        wait_for_completion(skein_client, app_id, timeout=60)
