import asyncio
import os
import time

import dask
from dask.distributed import Client
from distributed.utils_test import inc
import pytest
import skein

from dask_yarn import YarnCluster
from dask_yarn.core import (
    _make_specification,
    _make_submit_specification,
    _make_scheduler_kwargs,
)
from dask_ctl.discovery import (
    list_discovery_methods,
    discover_cluster_names,
)
from .conftest import check_is_shutdown

try:
    import bokeh  # noqa

    bokeh_installed = True
except Exception:
    bokeh_installed = False


@pytest.mark.parametrize("deploy_mode", ["remote", "local"])
def test_basic(deploy_mode, skein_client, conda_env):
    with YarnCluster(
        environment=conda_env,
        deploy_mode=deploy_mode,
        worker_memory="512 MiB",
        scheduler_memory="512 MiB",
        name="test-basic",
        skein_client=skein_client,
        dashboard_address=":8787",
        port=8786,
        worker_options={"resources": {"FOO": "BAZ"}},
        worker_class="dask.distributed.Nanny",
    ) as cluster:
        # Smoketest repr

        if bokeh_installed:
            assert cluster.dashboard_link is not None
            if deploy_mode == "local":
                assert ":8787" in cluster.dashboard_link

        if deploy_mode == "local":
            assert ":8786" in cluster.scheduler_address

        # Scale up
        cluster.scale(2)

        with Client(cluster) as client:
            future = client.submit(inc, 10)
            assert future.result() == 11
            client.get_versions(check=True)
            resource_tags = client.run(lambda dask_worker: dask_worker.total_resources)
            assert {"FOO": "BAZ"} in list(resource_tags.values())

        # Check that 2 workers exist
        start = time.time()
        while len(cluster.workers()) != 2:
            time.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(2)"

        # Scale down
        cluster.scale(1)

        start = time.time()
        while len(cluster.workers()) != 1:
            time.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(1)"

    check_is_shutdown(skein_client, cluster.app_id)


def test_adapt(skein_client, conda_env):
    with YarnCluster(
        environment=conda_env,
        deploy_mode="local",
        worker_memory="256 MiB",
        name="test-adapt",
        skein_client=skein_client,
    ) as cluster:

        cluster.adapt()

        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11

        start = time.time()
        while cluster.workers():
            time.sleep(0.1)
            assert time.time() < start + 30, "auto-scaledown timeout"

    check_is_shutdown(skein_client, cluster.app_id)


def test_from_specification(skein_client, conda_env, tmpdir):
    spec = _make_specification(
        environment=conda_env,
        worker_memory="512 MiB",
        scheduler_memory="512 MiB",
        name="dask-yarn-test-from-specification",
    )
    fn = os.path.join(str(tmpdir), "spec.yaml")
    with open(fn, "w") as f:
        f.write(spec.to_yaml())

    with YarnCluster.from_specification(fn, skein_client=skein_client) as cluster:
        with Client(cluster):
            pass

    check_is_shutdown(skein_client, cluster.app_id)


def test_from_specification_errors():
    bad_spec = skein.ApplicationSpec.from_yaml(
        """
        name: bad_spec
        services:
          bad:
            resources:
              memory: 1 GiB
              vcores: 1
            script: exit 1
        """
    )
    with pytest.raises(ValueError):
        YarnCluster.from_specification(bad_spec)

    with pytest.raises(TypeError):
        YarnCluster.from_specification(object())


def test_from_application_id(skein_client, conda_env):
    with YarnCluster(
        environment=conda_env,
        worker_memory="512 MiB",
        scheduler_memory="512 MiB",
        name="test-from-application-id",
        skein_client=skein_client,
    ) as cluster:

        # Connect to the application with the application id
        cluster2 = YarnCluster.from_application_id(cluster.app_id, skein_client)

        cluster2.scale(1)

        start = time.time()
        while len(cluster2.workers()) != 1:
            time.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(1)"

        del cluster2

        # Cluster is still running, finalizer not run in cluster2
        assert len(cluster.workers()) == 1

    check_is_shutdown(skein_client, cluster.app_id)


def test_from_current(skein_client, conda_env, monkeypatch, tmpdir):
    # Not running in a container
    with pytest.raises(ValueError) as exc:
        YarnCluster.from_current()
    assert str(exc.value) == "Not running inside a container"

    with YarnCluster(
        environment=conda_env,
        worker_memory="512 MiB",
        scheduler_memory="512 MiB",
        name="test-from-current",
        skein_client=skein_client,
    ) as cluster:

        # Patch environment so it looks like a container
        container_id = "container_1526134340424_0012_01_000005"
        cont_dir = tmpdir.mkdir(container_id)
        with open(str(cont_dir.join(".skein.crt")), "wb") as fil:
            fil.write(skein_client.security._get_bytes("cert"))
        with open(str(cont_dir.join(".skein.pem")), "wb") as fil:
            fil.write(skein_client.security._get_bytes("key"))

        for key, val in [
            ("SKEIN_APPLICATION_ID", cluster.app_id),
            ("CONTAINER_ID", container_id),
            ("SKEIN_APPMASTER_ADDRESS", cluster.application_client.address),
            ("LOCAL_DIRS", str(tmpdir)),
        ]:
            monkeypatch.setenv(key, val)

        import skein.core

        monkeypatch.setattr(skein.core, "properties", skein.core.Properties())

        cluster2 = YarnCluster.from_current()
        assert cluster2.app_id == cluster.app_id
        assert cluster2.scheduler_address == cluster.scheduler_address

        # Smoketest method
        cluster2.scale(1)

        start = time.time()
        while len(cluster2.workers()) != 1:
            time.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(1)"

        del cluster2

        # Cluster is still running, finalizer not run in cluster2
        assert len(cluster.workers()) == 1

    check_is_shutdown(skein_client, cluster.app_id)


@pytest.mark.parametrize("deploy_mode", ["remote", "local"])
def test_configuration(deploy_mode):
    config = {
        "yarn": {
            "environment": "myenv.tar.gz",
            "queue": "myqueue",
            "name": "dask-yarn-tests",
            "user": "alice",
            "tags": ["a", "b", "c"],
            "specification": None,
            "deploy-mode": deploy_mode,
            "worker": {
                "memory": "1234 MiB",
                "count": 1,
                "vcores": 1,
                "restarts": -1,
                "gpus": 2,
                "env": {"foo": "bar"},
                "worker_class": "abc",
                "worker_options": {"FOO": "BAZ"},
            },
            "scheduler": {"memory": "1234 MiB", "vcores": 1, "gpus": 0},
            "host": "0.0.0.0",
            "port": 8786,
            "dashboard_address": ":8787",
        }
    }

    with dask.config.set(config):
        spec = _make_specification()
        assert spec.name == "dask-yarn-tests"
        assert spec.user == "alice"
        assert spec.queue == "myqueue"
        assert spec.tags == {"a", "b", "c", "dask-yarn"}  # dask-yarn is a default tag
        assert spec.services["dask.worker"].resources.memory == 1234
        assert spec.services["dask.worker"].resources.gpus == 2
        assert spec.services["dask.worker"].env == {
            "foo": "bar",
            "worker_class": "abc",
            "worker_options": "eyJGT08iOiAiQkFaIn0=",
        }

        if deploy_mode == "remote":
            assert spec.services["dask.scheduler"].resources.memory == 1234
            assert spec.services["dask.scheduler"].resources.gpus == 0
        else:
            assert "dask.scheduler" not in spec.services

        kwargs = _make_scheduler_kwargs()
        assert kwargs["host"] == "0.0.0.0"
        assert kwargs["port"] == 8786
        assert kwargs["dashboard_address"] == ":8787"


def test_configuration_full_specification(conda_env, tmpdir):
    spec = _make_specification(
        environment=conda_env,
        worker_memory="512 MiB",
        scheduler_memory="512 MiB",
        name="test-configuration-full-specification",
    )
    fn = os.path.join(str(tmpdir), "spec.yaml")
    with open(fn, "w") as f:
        f.write(spec.to_yaml())

    # path to full specification
    with dask.config.set({"yarn": {"specification": fn}}):
        spec = _make_specification()
        assert spec == spec

    # full specification inlined
    with dask.config.set({"yarn": {"specification": spec.to_dict()}}):
        spec = _make_specification()
        assert spec == spec

    # when overrides specified, ignores full specification
    override = {"specification": spec.to_dict()}
    override.update(dask.config.get("yarn"))
    override["name"] = "config-name"
    with dask.config.set({"yarn": override}):
        spec = _make_specification(environment="foo", name="test-name", n_workers=4)
        assert spec.services["dask.worker"].instances == 4
        assert spec.name == "test-name"


def test_make_specification_errors():
    with dask.config.set({"yarn.environment": None}):
        with pytest.raises(ValueError) as info:
            _make_specification()
        assert "conda-pack" in str(info.value)

    with pytest.raises(ValueError) as info:
        _make_specification(environment="foo.tar.gz", deploy_mode="unknown")
    assert "deploy_mode" in str(info.value)

    with pytest.raises(ValueError) as info:
        _make_specification(environment="foo.tar.gz", worker_memory=1234)
    assert "1234 MiB" in str(info.value)

    with pytest.raises(ValueError) as info:
        _make_specification(environment="foo.tar.gz", scheduler_memory=1234)
    assert "1234 MiB" in str(info.value)


@pytest.mark.parametrize(
    "env,path", [("conda://env-name", "env-name"), ("conda:///env/path", "/env/path")]
)
def test_environment_conda(env, path):
    spec = _make_submit_specification(
        "script.py", args=("foo", "bar"), deploy_mode="remote", environment=env
    )
    scheduler = spec.services["dask.scheduler"]
    assert not scheduler.files
    assert (
        scheduler.script == ("conda activate %s\ndask-yarn services scheduler") % path
    )
    worker = spec.services["dask.worker"]
    assert not worker.files
    assert worker.script == ("conda activate %s\ndask-yarn services worker") % path
    client = spec.services["dask.client"]
    assert set(client.files) == {"script.py"}
    assert (
        client.script
        == ("conda activate %s\ndask-yarn services client script.py foo bar") % path
    )


def test_environment_venv():
    env = "venv:///path/to/environment"
    path = "/path/to/environment"
    spec = _make_submit_specification(
        "script.py", args=("foo", "bar"), deploy_mode="remote", environment=env
    )
    scheduler = spec.services["dask.scheduler"]
    assert not scheduler.files
    assert (
        scheduler.script
        == ("source %s/bin/activate\ndask-yarn services scheduler") % path
    )
    worker = spec.services["dask.worker"]
    assert not worker.files
    assert worker.script == ("source %s/bin/activate\ndask-yarn services worker") % path
    client = spec.services["dask.client"]
    assert set(client.files) == {"script.py"}
    assert (
        client.script
        == ("source %s/bin/activate\ndask-yarn services client script.py foo bar")
        % path
    )


def test_environment_python_path():
    env = "python:///path/to/python"
    cmd = "/path/to/python -m dask_yarn.cli"
    spec = _make_submit_specification(
        "script.py", args=("foo", "bar"), deploy_mode="remote", environment=env
    )
    scheduler = spec.services["dask.scheduler"]
    assert not scheduler.files
    assert scheduler.script == "%s services scheduler" % cmd
    worker = spec.services["dask.worker"]
    assert not worker.files
    assert worker.script == "%s services worker" % cmd
    client = spec.services["dask.client"]
    assert set(client.files) == {"script.py"}
    assert client.script == "%s services client script.py foo bar" % cmd


def test_environment_archive():
    env = "env.tar.gz"
    spec = _make_submit_specification(
        "script.py", args=("foo", "bar"), deploy_mode="remote", environment=env
    )
    scheduler = spec.services["dask.scheduler"]
    assert set(scheduler.files) == {"environment"}
    assert scheduler.script == (
        "source environment/bin/activate\n"
        "environment/bin/python -m dask_yarn.cli services scheduler"
    )
    worker = spec.services["dask.worker"]
    assert set(worker.files) == {"environment"}
    assert worker.script == (
        "source environment/bin/activate\n"
        "environment/bin/python -m dask_yarn.cli services worker"
    )
    client = spec.services["dask.client"]
    assert set(client.files) == {"environment", "script.py"}
    assert client.script == (
        "source environment/bin/activate\n"
        "environment/bin/python -m dask_yarn.cli services client script.py foo bar"
    )


@pytest.mark.parametrize("deploy_mode", ["local", "remote"])
def test_make_submit_specification(deploy_mode):
    spec = _make_submit_specification(
        "../script.py",
        deploy_mode=deploy_mode,
        environment="myenv.tar.gz",
        name="test-name",
        user="alice",
        client_vcores=2,
        client_memory="2 GiB",
    )

    assert spec.name == "test-name"
    assert spec.user == "alice"
    if deploy_mode == "local":
        assert set(spec.services) == {"dask.worker"}
        assert "environment" in spec.services["dask.worker"].files
    else:
        client = spec.services["dask.client"]
        scheduler = spec.services["dask.scheduler"]
        assert client.files["environment"] == scheduler.files["environment"]
        assert client.files["script.py"].source.startswith("file:///")
        assert client.resources.memory == 2048
        assert client.resources.vcores == 2

    config = {
        "yarn.name": "dask-yarn-tests",
        "yarn.queue": "myqueue",
        "yarn.environment": "myenv.tar.gz",
        "yarn.client.memory": "1234 MiB",
        "yarn.client.vcores": 2,
        "yarn.client.env": {"foo": "bar"},
    }

    with dask.config.set(config):
        spec = _make_submit_specification("script.py", deploy_mode=deploy_mode)
        assert spec.name == "dask-yarn-tests"
        assert spec.queue == "myqueue"
        if deploy_mode == "remote":
            assert spec.services["dask.client"].resources.memory == 1234
            assert spec.services["dask.client"].resources.vcores == 2
            assert spec.services["dask.client"].env == {"foo": "bar"}


def test_environment_relative_paths():
    relpath = "path/to/foo.tar.gz"
    abspath = os.path.abspath(relpath)
    a = _make_specification(environment=relpath).to_dict()
    b = _make_specification(environment=abspath).to_dict()
    assert a == b


@pytest.mark.asyncio
@pytest.mark.parametrize("deploy_mode", ["remote", "local"])
async def test_basic_async(deploy_mode, skein_client, conda_env):
    async with YarnCluster(
        environment=conda_env,
        deploy_mode=deploy_mode,
        worker_memory="512 MiB",
        scheduler_memory="512 MiB",
        name="test-basic-async",
        skein_client=skein_client,
        asynchronous=True,
    ) as cluster:
        # Smoketest repr
        repr(cluster)

        if bokeh_installed:
            assert cluster.dashboard_link is not None

        # Scale up
        await cluster.scale(2)

        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(inc, 10)
            assert result == 11
            await client.get_versions(check=True)

        # Check that 2 workers exist
        start = time.time()
        while len(await cluster.workers()) != 2:
            await asyncio.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(2)"

        # Scale down
        await cluster.scale(1)

        start = time.time()
        while len(await cluster.workers()) != 1:
            await asyncio.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(1)"

    check_is_shutdown(skein_client, cluster.app_id)


def test_widget_and_html_reprs(skein_client, conda_env):
    pytest.importorskip("ipywidgets")

    with YarnCluster(
        environment=conda_env,
        deploy_mode="local",
        worker_memory="256 MiB",
        name="test-widget",
        skein_client=skein_client,
    ) as cluster:
        # Smoke test widget
        cluster._widget()

        # Test non-widget html repr
        assert cluster.app_id in cluster._repr_html_()

        assert "0" in cluster._widget_status()

        # Scale up and wait
        cluster.scale(1)
        start = time.time()
        while len(cluster._observed) != 1:
            time.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(1)"

        assert "1" in cluster._widget_status()

        # Scale down
        cluster.scale(1)

    check_is_shutdown(skein_client, cluster.app_id)


def test_logs(conda_env, skein_client):
    with YarnCluster(
        environment=conda_env,
        deploy_mode="local",
        worker_memory="256 MiB",
        name="test-widget",
        skein_client=skein_client,
    ) as cluster:
        cluster.scale(2)
        start = time.time()
        while len(cluster._observed) != 2:
            time.sleep(0.1)
            assert time.time() < start + 30, "timeout cluster.scale(2)"

        logs = cluster.logs()
        assert len(logs) == 3

        logs = cluster.logs(scheduler=True, workers=False)
        assert len(logs) == 1

        logs = cluster.logs(scheduler=False, workers=False)
        assert len(logs) == 0

        logs = cluster.logs(scheduler=False, workers=True)
        assert len(logs) == 2

    check_is_shutdown(skein_client, cluster.app_id)


@pytest.mark.asyncio
async def test_from_name(skein_client, conda_env):
    # Create cluster
    cluster = YarnCluster(
        environment=conda_env,
        deploy_mode="remote",
        worker_memory="512 MiB",
        scheduler_memory="512 MiB",
        name="test-basic",
        skein_client=skein_client,
        dashboard_address=":8787",
        port=8786,
        worker_options={"resources": {"FOO": "BAZ"}},
        worker_class="dask.distributed.Nanny",
    )
    cluster.scale(1)
    name = cluster.application_client.name

    # Check cluster listed in discovery
    discovery = "yarncluster"
    assert discovery in list_discovery_methods()
    clusters_names = [
        cluster async for cluster in discover_cluster_names(discovery=discovery)
    ]
    assert len(clusters_names) == 1
    discovered_name, discovered_class = clusters_names[0]
    assert discovered_name == name
    assert discovered_class == YarnCluster

    # Delete cluster manager
    del cluster

    # Recreate cluster manager from name
    cluster = await YarnCluster.from_name(name, asynchronous=True)
    assert "id" in cluster.scheduler_info

    # Ensure work can be run on cluster
    async with Client(cluster, asynchronous=True) as client:
        # Ensure that inter-worker communication works well
        futures = client.map(lambda x: x + 1, range(10))
        total = client.submit(sum, futures)
        assert (await total) == sum(map(lambda x: x + 1, range(10)))
        assert all((await client.has_what()).values())

    cluster.shutdown()