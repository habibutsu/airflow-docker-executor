from __future__ import annotations
import os
import logging
import re2
import json
import datetime as dt
from enum import Enum
from slugify import slugify

from typing import TYPE_CHECKING, Any
from dataclasses import dataclass, is_dataclass, fields
from queue import Empty, Queue

from airflow.utils.hashlib_wrapper import md5
from airflow.configuration import conf, ENV_VAR_PREFIX
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstance import TaskInstanceState
from airflow.version import version as airflow_version

from docker import DockerClient
from docker.constants import DEFAULT_UNIX_SOCKET
from docker.models.containers import Container
from docker.models.services import Service
from docker.types import ContainerSpec, TaskTemplate, RestartPolicy
from docker import types as docker_types
from docker.utils import format_environment
# from airflow.utils.log import file_task_handler

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.models.taskinstance import TaskInstance

logger = logging.getLogger(__name__)

MAX_LABEL_LEN = 63

_DOCKER_TYPES = vars(docker_types)

class DockerTaskStatus(Enum):

    # https://docs.docker.com/engine/swarm/how-swarm-mode-works/swarm-task-states/

    new = "new" # The task was initialized.
    pending = "pending" # Resources for the task were allocated.
    assigned = "assigned" # Docker assigned the task to nodes.
    accepted = "accepted" # The task was accepted by a worker node. If a worker node rejects the task, the state changes to REJECTED.
    ready = "ready" # The worker node is ready to start the task
    preparing = "preparing" # Docker is preparing the task.
    starting = "starting" # Docker is starting the task.
    running = "running" # The task is executing.

    complete = "complete"  # The task exited without an error code.

    failed = "failed" # The task exited with an error code.
    shutdown = "shutdown" # Docker requested the task to shut down.
    rejected = "rejected" # The worker node rejected the task.
    orphaned = "orphaned" # The node was down for too long.
    remove = "remove"  # The task is not terminal but the associated service was removed or scaled down.

TASK_FAILED_STATES = [
    DockerTaskStatus.failed.value,
    DockerTaskStatus.shutdown.value,
    DockerTaskStatus.rejected.value,
    DockerTaskStatus.orphaned.value,
]


def make_safe_label_value(string: str) -> str:
    """
    Normalize a provided label to be of valid length and characters.

    Valid label values must be 63 characters or less and must be empty or begin and
    end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
    dots (.), and alphanumerics between.

    If the label value is greater than 63 chars once made safe, or differs in any
    way from the original value sent to this function, then we need to truncate to
    53 chars, and append it with a unique hash.
    """
    safe_label = re2.sub(r"^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$", "", string)

    if len(safe_label) > MAX_LABEL_LEN or string != safe_label:
        safe_hash = md5(string.encode()).hexdigest()[:9]
        safe_label = safe_label[: MAX_LABEL_LEN - len(safe_hash) - 1] + "-" + safe_hash

    return safe_label


def default_dumps(value):
    if isinstance(value, dict) and type(value).__name__ in _DOCKER_TYPES:
        result = dict(**{
            k: default_dumps(v)
            for k, v in iter(value.items())
        })
        result["__type__"] = type(value).__name__
        return result
    elif is_dataclass(value):
        # asdict - does not work, because returns generator for nested dicts
        result = {
            field.name: default_dumps(getattr(value, field.name))
            for field in fields(value)
        }
        return result
    return value


def object_hook(value):
    if isinstance(value, dict) and "__type__" in value:
        type_name = value.pop("__type__")
        cls = _DOCKER_TYPES[type_name]
        obj = cls.__new__(cls)  # keys is not mapped to kwargs, so we need to use __new__
        obj.update(**value)
        return obj
    return value


@dataclass
class DockerExecutorConfig:

    task: TaskTemplate
    auto_remove: bool = False
    auto_remove_delay: int = 60
    verbose: bool = False
    use_service: bool = True

    @classmethod
    def from_json(cls, value: str):
        return DockerExecutorConfig(**json.loads(value, object_hook=object_hook))

    def to_json(self):
        return json.dumps(self, default=default_dumps)

    def __str__(self) -> str:
        return self.to_json()


class DockerExecutor(BaseExecutor):

    supports_pickling: bool = False
    is_local: bool = False
    is_single_threaded: bool = False
    is_production: bool = False
    serve_logs: bool = False
    is_swarm: bool = False

    def __init__(self):
        super().__init__()
        self.client: DockerClient = None
        self.containers: dict[TaskInstanceKey, tuple[DockerExecutorConfig, Container]] = {}
        self.finished_containers: dict[TaskInstanceKey, tuple[DockerExecutorConfig, Container]] = {}

        self.services: dict[TaskInstanceKey, tuple[DockerExecutorConfig, Service]] = {}
        self.finished_services: dict[TaskInstanceKey, tuple[DockerExecutorConfig, Service]] = {}

        self.host =  conf.get('docker', 'host', fallback=DEFAULT_UNIX_SOCKET)
        self.tls_verify = conf.get('docker', 'tls_verify', fallback=None)
        self.cert_path = conf.get('docker', 'cert_path', fallback=None)
        self.task_queue: Queue[tuple[str, TaskInstanceKey, CommandType, DockerExecutorConfig]] = Queue()

    def is_swarm_available(self) -> bool:
        return self.client.info().get("Swarm", {}).get('ControlAvailable', False)

    def start(self):
        self.log.info("Starting the docker executor")

        environment = {
            'DOCKER_HOST': self.host,
        }
        if self.tls_verify:
            environment['DOCKER_TLS_VERIFY'] = self.tls_verify
        if self.cert_path:
            environment['DOCKER_CERT_PATH'] = self.cert_path

        self.client = DockerClient.from_env(environment=environment)

        self.is_swarm = self.is_swarm_available()
        if self.is_swarm:
            self.log.info("Swarm mode is enabled")

    def make_task_name(self, key: TaskInstanceKey, max_length=63) -> str:
        """
        name must be valid DNS name
        """
        hash_value = md5(
            "_".join([key.dag_id, key.run_id, key.task_id, str(key.try_number)]).encode()
        ).hexdigest()[:9]
        base_name = "_".join([
            key.dag_id,
            key.task_id,
        ])
        name = "-".join([
            slugify(base_name, lowercase=True)[:max_length - len(hash_value) - 1],
            hash_value
        ])
        return name

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: dict | None = None,
    ) -> None:
        docker_executor_config = executor_config.get("docker", None)

        if not docker_executor_config:
            self.log.error("There is no key 'docker' in executor_config for %s. Executor_config: %s", key, executor_config)
            self.fail(key=key, info="Invalid executor_config passed")
            return

        if isinstance(docker_executor_config, str):
            try:
                docker_executor_config = DockerExecutorConfig.from_json(docker_executor_config)
            except Exception as e:
                self.log.error("Invalid executor_config for %s. Executor_config: %s (error: %s)", key, executor_config, e)
                self.fail(key=key, info="Invalid executor_config passed")
                return

        self.log.info("Queuing task: %s", key)
        self.task_queue.put((queue, key, command, docker_executor_config))
        self.change_state(key, TaskInstanceState.QUEUED, info=self.job_id)

    def prepapre_task_template(self, key: TaskInstanceKey, command: CommandType, docker_executor_config: DockerExecutorConfig) -> TaskTemplate:
        if docker_executor_config.verbose:
            command = command + ["--verbose"]

        self.validate_airflow_tasks_run_command(command)
        # loop_command = ["bash", "-c", "trap exit INT TERM; while true; do echo hello world; sleep 30 & wait; done"]

        task_template: TaskTemplate = docker_executor_config.task
        container_spec: ContainerSpec = task_template["ContainerSpec"]
        task_template["ContainerSpec"]["Command"] = command
        # task_template["ContainerSpec"]["Command"] = loop_command

        labels: dict = container_spec.get("Labels", {})
        labels.update({
            "dag_id": make_safe_label_value(key.dag_id),
            "task_id": make_safe_label_value(key.task_id),
            "run_id": make_safe_label_value(key.run_id),
            "map_index": str(key.map_index),
            "try_number": str(key.try_number),
            "docker_executor": "true",
            "airflow-worker": make_safe_label_value(str(self.job_id)),
            "airflow_version": airflow_version.replace("+", "-"),
        })
        container_spec["Labels"] = labels

        env = {
            "AIRFLOW_CTX_COMMAND": " ".join(command),
            "AIRFLOW_CTX_DAG_ID": key.dag_id,
            "AIRFLOW_CTX_TASK_ID": key.task_id,
            "AIRFLOW_CTX_RUN_ID": key.run_id,
            "AIRFLOW_CTX_TRY_NUMBER": str(key.try_number),
        }
        for env_var in [
            os_environment for os_environment in os.environ if os_environment.startswith(ENV_VAR_PREFIX)
        ]:
            env[env_var] = os.environ[env_var]
        container_spec["Env"] = container_spec.get("Env", []) + format_environment(env)
        return task_template

    def run_task(self, queue, key: TaskInstanceKey, docker_executor_config: DockerExecutorConfig, task: TaskTemplate) -> None:
        name = self.make_task_name(key)

        client = self.client

        # ensure restart_policy="none"
        if task.get("RestartPolicy"):
            self.log.warning("RestartPolicy for task %s, will be overridden", key)

        task["RestartPolicy"] = RestartPolicy(
            condition="none",
        )

        if self.is_swarm and docker_executor_config.use_service:
            self.log.info("run service `%s` with spec=%s", name, task)
            # {'ID': 'j63abh6xu7xulcaioy8xzxsuc'}
            response: dict = client.api.create_service(
                task,
                name=name,
                labels=task["ContainerSpec"]["Labels"]
            )
            service = client.services.get(response["ID"])
            logger.info("service %s -> %s:%s", name, service, type(service))
            self.services[key] = (docker_executor_config, service)
        else:
            container_spec: ContainerSpec = task["ContainerSpec"]
            self.log.info("run container with spec=%s", container_spec)
            container = client.containers.run(
                name=name,
                image=container_spec["Image"] or task["Image"],
                command=container_spec["Command"],
                detach=True,
                auto_remove=False, # don't use auto_remove directly from config, because we need to check exit code
                environment=container_spec["Env"],
                network=task["Networks"][0]["Target"] if "Networks" in task else None,
                mounts=container_spec.get("Mounts"),
                labels=container_spec["Labels"]
            )
            self.containers[key] = (docker_executor_config, container)


    def sync(self) -> None:
        # if self.is_swarm:
        self.cleanup_finished_containers()
        self.cleanup_finished_services()

        while not self.task_queue.empty():
            queue, key, command, executor_config = self.task_queue.get()
            try:
                task_template = self.prepapre_task_template(key, command, executor_config)
                self.run_task(queue, key, executor_config, task_template)
            except Exception as e:
                self.log.error("Failed to execute task %s", key, exc_info=True)
                self.fail(key, info=str(e))
            finally:
                # self.change_state(key, TaskInstanceState.RUNNING, info=self.job_id)
                self.task_queue.task_done()

    def end(self) -> None:
        self.heartbeat()

    def cleanup_stuck_queued_tasks(self, tis: list[TaskInstance]) -> list[str]:
        readable_tis = []
        for ti in tis:
            readable_tis.append(repr(ti))

        return readable_tis

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        name = self.make_task_name(ti.key)
        container: Container = self.client.containers.get(name)
        if not container:
            return [], []
        logs: bytes =  container.logs()
        messages = [
            f"Log from Docker container with name: {name}",
        ]
        return messages, [logs.decode()]

    def cleanup_finished_containers(self):
        """
            Removing finished containers or stacks
        """
        keys = list(self.containers.keys())

        for key in keys:
            docker_executor_config, container = self.containers[key]
            container.reload()
            state = container.attrs['State']
            logger.info("container %s -> %s %s (ExitCode=%s)", key, container, container.status, state['ExitCode'])
            if container.status != 'running':
                if state['ExitCode'] == 0:
                    self.success(key)
                else:
                    self.fail(key, info=f"exit code: {state['ExitCode']}")

                if docker_executor_config.auto_remove:
                    self.finished_containers[key] = (docker_executor_config, container)
                del self.containers[key]

        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        keys = list(self.finished_containers.keys())
        for key in keys:
            docker_executor_config, container = self.finished_containers[key]
            container.reload()
            state = container.attrs['State']
            finished_at = dt.datetime.fromisoformat(state['FinishedAt'])

            if (now - finished_at).total_seconds() > docker_executor_config.auto_remove_delay:
                container.remove()
                del self.finished_containers[key]

    def cleanup_finished_services(self):
        keys = list(self.services.keys())

        for key in keys:
            docker_executor_config, service = self.services[key]
            service.reload()
            tasks = service.tasks()

            success_tasks = []
            failed_tasks = []
            for task in tasks:
                state = task["Status"]["State"]
                if state == DockerTaskStatus.complete.value:
                    success_tasks.append(task)
                elif state in TASK_FAILED_STATES:
                    failed_tasks.append(task)

            if len(success_tasks) + len(failed_tasks) == len(tasks):
                if len(success_tasks) == len(tasks):
                    self.success(key)
                elif failed_tasks:
                    errors = [
                        task['Status']['Err']
                        # f"{task['ID']} (exit code: {task['Status']['Err']})"
                        for task in failed_tasks
                    ]
                    self.fail(key, info=f"errors: {errors}")

                if docker_executor_config.auto_remove:
                    self.finished_services[key] = (docker_executor_config, service)

                del self.services[key]

        keys = list(self.finished_containers.keys())
        for key in keys:
            pass

    def remove_all(self):
        """
            Removing all containers or stacks
        """
        filters = {"label": [f"airflow-worker={self.job_id}"]}

        services: list[dict] = self.client.services.list(filters=filters)
        for service in services:
            pass

        containers: list[Container] = self.client.containers.list(all=True, filters=filters)
        for container in containers:
            container.remove()
