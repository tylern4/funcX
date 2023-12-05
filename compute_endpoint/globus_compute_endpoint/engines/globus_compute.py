import logging
import os
import queue
import typing as t
import uuid
from concurrent.futures import Future

from globus_compute_common.messagepack.message_types import (
    EPStatusReport,
    TaskTransition,
)
from globus_compute_endpoint.engines.base import (
    GlobusComputeEngineBase,
    ReportingThread,
)
from globus_compute_endpoint.strategies import SimpleStrategy
from parsl.executors.high_throughput.executor import HighThroughputExecutor

logger = logging.getLogger(__name__)
DOCKER_CMD_TEMPLATE = "docker run {options} -v {rundir}:{rundir} -t {image} {command}"
APPTAINER_CMD_TEMPLATE = "apptainer run {options} {image} {command}"
SINGULARITY_CMD_TEMPLATE = "singularity run {options} {image} {command}"


class GlobusComputeEngine(GlobusComputeEngineBase):
    def __init__(
        self,
        *args,
        label: str = "GlobusComputeEngine",
        max_retries_on_system_failure: int = 0,
        strategy: t.Optional[SimpleStrategy] = SimpleStrategy(),
        executor: t.Optional[HighThroughputExecutor] = None,
        docker_container_uri: t.Optional[str] = None,
        apptainer_container_uri: t.Optional[str] = None,
        singularity_container_uri: t.Optional[str] = None,
        container_cmd_options: t.Optional[str] = None,
        **kwargs,
    ):
        self.run_dir = os.getcwd()
        self.label = label
        self._status_report_thread = ReportingThread(target=self.report_status, args=[])
        super().__init__(
            *args, max_retries_on_system_failure=max_retries_on_system_failure, **kwargs
        )
        self.strategy = strategy
        self.max_workers_per_node = 1
        self.docker_container_uri = docker_container_uri
        self.apptainer_container_uri = apptainer_container_uri
        self.singularity_container_uri = singularity_container_uri
        self.container_cmd_options = container_cmd_options
        if executor is None:
            executor = HighThroughputExecutor(  # type: ignore
                *args,
                label=label,
                **kwargs,
            )
        self.executor = executor

    def containerized_launch_cmd(self) -> str:
        """Recompose executor's launch_cmd to launch with containers

        Returns
        -------
        str launch_cmd
        """
        launch_cmd = self.executor.launch_cmd
        assert launch_cmd
        if self.docker_container_uri:
            launch_cmd = DOCKER_CMD_TEMPLATE.format(
                image=self.docker_container_uri,
                rundir=self.run_dir,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        elif self.apptainer_container_uri:
            launch_cmd = APPTAINER_CMD_TEMPLATE.format(
                image=self.apptainer_container_uri,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        elif self.singularity_container_uri:
            launch_cmd = SINGULARITY_CMD_TEMPLATE.format(
                image=self.singularity_container_uri,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        return launch_cmd

    def start(
        self,
        *args,
        endpoint_id: t.Optional[uuid.UUID] = None,
        run_dir: t.Optional[str] = None,
        results_passthrough: t.Optional[queue.Queue] = None,
        **kwargs,
    ):
        assert endpoint_id, "GCExecutor requires kwarg:endpoint_id at start"
        assert run_dir, "GCExecutor requires kwarg:run_dir at start"

        self.endpoint_id = endpoint_id
        self.run_dir = run_dir
        self.executor.run_dir = self.run_dir
        script_dir = os.path.join(self.run_dir, "submit_scripts")
        self.executor.provider.script_dir = script_dir
        self.executor.launch_cmd = self.containerized_launch_cmd()

        if (
            self.executor.provider.channel
            and not self.executor.provider.channel.script_dir
        ):
            self.executor.provider.channel.script_dir = script_dir

        os.makedirs(self.executor.provider.script_dir, exist_ok=True)
        if results_passthrough:
            # Only update the default queue in GCExecutorBase if
            # a queue is passed in
            self.results_passthrough = results_passthrough
        self.executor.start()
        if self.strategy:
            self.strategy.start(self)
        self._status_report_thread.start()

    def _submit(
        self,
        func: t.Callable,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> Future:
        return self.executor.submit(func, {}, *args, **kwargs)

    @property
    def provider(self):
        return self.executor.provider

    def get_connected_managers(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Returns
        -------
        List of dicts containing info for all connected managers
        """
        return self.executor.connected_managers()

    def get_total_managers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of managers
        """
        return len(managers)

    def get_total_active_managers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Number of managers that have capacity for new tasks
        """
        return sum(1 for m in managers if m["active"])

    def get_outstanding_breakdown(
        self, managers: t.Optional[t.List[t.Dict[str, t.Any]]] = None
    ) -> t.List[t.Tuple[str, int, bool]]:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]] | None
            List of dicts containing info for all connected managers

        Returns
        -------
        List of tuples of the form (component, # of tasks on component, active?)
        """
        if managers is None:
            managers = self.get_connected_managers()
        total_task_count = self.executor.outstanding
        breakdown = [(m["manager"], m["tasks"], m["active"]) for m in managers]
        total_count_managers = sum([m["tasks"] for m in managers])
        task_count_interchange = total_task_count - total_count_managers
        breakdown = [("interchange", task_count_interchange, True)] + breakdown
        return breakdown

    def get_total_tasks_outstanding(self) -> dict:
        """
        Returns
        -------
        Dict of type {str_task_type: count_tasks}
        """
        return {"RAW": self.executor.outstanding}

    def get_total_tasks_pending(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of pending tasks
        """
        outstanding = self.get_outstanding_breakdown(managers=managers)
        return outstanding[0][1]  # Queued in interchange

    def provider_status(self):
        status = []
        if self.provider:
            # ex.locks is a dict of block_id:job_id mappings
            job_ids = self.executor.blocks.values()
            status = self.provider.status(job_ids=job_ids)
        return status

    def get_total_live_workers(
        self, managers: t.Optional[t.List[t.Dict[str, t.Any]]] = None
    ) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of live workers
        """
        if managers is None:
            managers = self.get_connected_managers()
        return sum([mgr["worker_count"] for mgr in managers])

    def get_total_idle_workers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of workers that are not actively running tasks
        """
        idle_workers = 0
        for mgr in managers:
            workers = mgr["worker_count"]
            tasks = mgr["tasks"]
            idle_workers += max(0, workers - tasks)
        return idle_workers

    def scale_out(self, blocks: int):
        logger.info(f"Scaling out {blocks} blocks")
        return self.executor.scale_out(blocks=blocks)

    def scale_in(self, blocks: int):
        logger.info(f"Scaling in {blocks} blocks")
        return self.executor.scale_in(blocks=blocks)

    def _handle_task_exception(
        self,
        task_id: str,
        execution_begin: TaskTransition,
        exception: BaseException,
    ) -> bytes:
        result_bytes = b""
        retry_info = self._retry_table[task_id]
        if retry_info["retry_count"] < self.max_retries_on_system_failure:
            retry_info["retry_count"] += 1
            retry_info["exception_history"].append(exception)
            self.submit(task_id, retry_info["packed_task"])
        else:
            # This is a terminal state
            result_bytes = super()._handle_task_exception(
                task_id=task_id, execution_begin=execution_begin, exception=exception
            )

        return result_bytes

    @property
    def scaling_enabled(self) -> bool:
        """Indicates whether scaling is possible"""
        max_blocks = self.executor.provider.max_blocks
        return max_blocks > 0

    def get_status_report(self) -> EPStatusReport:
        """
        Returns
        -------
        Object containing info on the current status of the endpoint
        """
        managers = self.get_connected_managers()
        executor_status: t.Dict[str, t.Any] = {
            "task_id": -2,  # Deprecated
            "info": {
                "total_cores": 0,  # TODO
                "total_mem": 0,  # TODO
                "new_core_hrs": 0,  # TODO
                "total_core_hrs": 0,  # TODO
                "managers": self.get_total_managers(managers=managers),
                "active_managers": self.get_total_active_managers(managers=managers),
                "total_workers": self.get_total_live_workers(managers=managers),
                "idle_workers": self.get_total_idle_workers(managers=managers),
                "pending_tasks": self.get_total_tasks_pending(managers=managers),
                "outstanding_tasks": self.get_total_tasks_outstanding()["RAW"],
                "worker_mode": 0,  # Deprecated
                "scheduler_mode": 0,  # Deprecated
                "scaling_enabled": self.scaling_enabled,
                "mem_per_worker": self.executor.mem_per_worker,
                "cores_per_worker": self.executor.cores_per_worker,
                "prefetch_capacity": self.executor.prefetch_capacity,
                "max_blocks": self.executor.provider.max_blocks,
                "min_blocks": self.executor.provider.min_blocks,
                "max_workers_per_node": self.executor.max_workers,
                "nodes_per_block": self.executor.provider.nodes_per_block,
                "heartbeat_period": self.executor.heartbeat_period,
            },
        }
        task_status_deltas: t.Dict[str, t.List[TaskTransition]] = {}  # TODO
        return EPStatusReport(
            endpoint_id=self.endpoint_id,
            global_state=executor_status,
            task_statuses=task_status_deltas,
        )

    def shutdown(self, /, **kwargs) -> None:
        self._status_report_thread.stop()
        if self.strategy:
            self.strategy.close()
        self.executor.shutdown()
