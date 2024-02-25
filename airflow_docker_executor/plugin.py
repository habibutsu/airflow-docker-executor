from functools import wraps

from airflow.plugins_manager import AirflowPlugin
from .docker_executor import DockerExecutor


class DockerExecutorPlugin(AirflowPlugin):
    name = "airflow-docker-executor"
    operators = []
    # Leave in for explicitness
    executors = [DockerExecutor]
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
