# Docker Executor


## Problems

- [ ] No logs from the container.

    Explanation: Airflow retrieves the hostname of a remote
    worker for reading logs. However, for the Docker executor, this is not
    possible because the container may have already stopped.
