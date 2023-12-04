import logging
import sys
import uuid

from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GlobusComputeEngine
from globus_compute_sdk.serialize import ComputeSerializer
from tests.utils import double, ez_pack_function


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


def test_apptainer(tmp_path):
    gce = GlobusComputeEngine(
        address="127.0.0.1",
        label="GCE_TEST",
        apptainer_container_uri="APPTAINER_PATH",
        container_cmd_options="-",
    )
    version_info = sys.version_info
    logging.warning(f"My version info : {version_info}")
    gce.start(endpoint_id=uuid.uuid4(), run_dir="/tmp")  # todo: change to tmp_path
    future = gce.executor.submit(platinfo, resource_specification={})
    logging.warning(f"Launch cmd : {gce.executor.launch_cmd}")
    result = future.result(timeout=10)
    assert result
    logging.warning(f"Got result : {result}")


def test_singularity(tmp_path):
    gce = GlobusComputeEngine(
        address="127.0.0.1",
        max_workers=1,
        label="GCE_TEST",
        singularity_container_uri="/home/yadunand/kube-endpoint.py3.9.sif",
        container_cmd_options="",
    )
    version_info = sys.version_info
    logging.warning(f"My version info : {version_info}")
    gce.start(
        endpoint_id=uuid.uuid4(),
        run_dir="/home/yadunand/RUNDIR",
    )  # todo: change to tmp_path
    logging.warning(f"Launch cmd : {gce.executor.launch_cmd}")
    results_q = gce.results_passthrough

    # Compose task
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    future = gce.submit(task_id, task_message)
    logging.warning(f"Future. result : {future.result()}")
    for _i in range(5):
        res = results_q.get(timeout=5)
        logging.warning(f"Got : {res}")


if __name__ == "__main__":
    test_apptainer("/tmp")
    test_docker("/tmp")
    test_singularity("/tmp")
