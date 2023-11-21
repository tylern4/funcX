import logging
import sys
import uuid

from globus_compute_endpoint.engines.globus_compute import GlobusComputeEngine


def platinfo():
    import platform
    import sys

    return platform.uname(), sys.version_info


def test_docker(tmp_path):
    gce = GlobusComputeEngine(
        address="127.0.0.1",
        label="GCE_TEST",
        docker_container_uri="funcx/kube-endpoint:main-3.10",
        container_cmd_options="-v /tmp:/tmp",
    )
    version_info = sys.version_info
    logging.warning(f"My version info : {version_info}")
    gce.start(endpoint_id=uuid.uuid4(), run_dir="/tmp")  # todo: change to tmp_path
    future = gce.executor.submit(platinfo, resource_specification={})
    logging.warning(f"Launch cmd : {gce.executor.launch_cmd}")
    result = future.result()
    assert result
    logging.warning(f"Got result : {result}")
