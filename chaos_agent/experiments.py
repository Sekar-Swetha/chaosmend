"""
experiments.py – Chaos Agent

This module implements the actual failure injection logic.

WHY each experiment type?

  container_kill:
    Simulates a pod/service crash. In distributed systems,
    services crash all the time (OOM, bugs, deployments).
    We need to verify the system recovers automatically.

  latency_inject:
    Simulates slow networks or overloaded services.
    90% of production incidents involve latency, not hard crashes.
    Adding artificial delay lets us verify circuit breakers,
    timeouts, and retry logic actually work.

  memory_stress:
    Simulates a memory leak. The service gradually
    starts consuming more and more RAM until something
    breaks. Tests OOM kill + restart recovery.

  cpu_stress:
    Maxes out CPU cores inside the target container using
    stress-ng. Tests performance degradation, request
    timeouts, and CPU-throttle recovery.

HOW container control works:
    We use the Docker Python SDK. Since docker-compose gives
    every service a container name, we can find it by name.
    The chaos-agent container has /var/run/docker.sock mounted,
    giving it a direct channel to the Docker daemon.
"""
import logging
import subprocess
import threading
import time
from typing import Optional

import docker

logger = logging.getLogger(__name__)

_docker_client = None

def _get_docker_client():
    global _docker_client
    if _docker_client is None:
        try:
            _docker_client = docker.from_env()
        except Exception:
            try:
                _docker_client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
            except Exception as e:
                logger.warning("Docker client unavailable: %s", e)
    return _docker_client


def get_container(service_name: str):
    """Find a running container by service name."""
    client = _get_docker_client()
    if client is None:
        return None
    containers = client.containers.list()
    for c in containers:
        if service_name.lower() in c.name.lower():
            return c
    return None


def experiment_container_kill(target_service: str, duration_seconds: int = 30) -> dict:
    """
    Stop a target container for `duration_seconds`, then restart it.
    This is the most basic chaos experiment: "what if this service dies?"
    """
    container = get_container(target_service)
    if not container:
        return {"status": "error", "message": f"No container found for service '{target_service}'"}

    container_name = container.name
    logger.warning(f"CHAOS: Killing container '{container_name}' for {duration_seconds}s")

    def kill_and_restore():
        try:
            container.stop(timeout=5)
            logger.warning(f"CHAOS: Container '{container_name}' stopped.")
            time.sleep(duration_seconds)
            container.start()
            logger.info(f"CHAOS: Container '{container_name}' restarted after {duration_seconds}s.")
        except Exception as e:
            logger.error(f"CHAOS kill/restore failed: {e}")

    threading.Thread(target=kill_and_restore, daemon=True).start()
    return {
        "status": "started",
        "experiment": "container_kill",
        "target": container_name,
        "duration_seconds": duration_seconds,
    }


def experiment_latency_inject(target_service: str, latency_ms: int = 500, duration_seconds: int = 60) -> dict:
    """
    Adds artificial network latency to a container using Linux's
    `tc netem` (traffic control) inside the target container.

    tc netem = "network emulator" — part of the Linux kernel's
    queueing discipline system. Extremely accurate.

    Runs: tc qdisc add dev eth0 root netem delay 500ms
    After duration: tc qdisc del dev eth0 root
    """
    container = get_container(target_service)
    if not container:
        return {"status": "error", "message": f"No container found for '{target_service}'"}

    container_name = container.name
    logger.warning(f"CHAOS: Injecting {latency_ms}ms latency into '{container_name}' for {duration_seconds}s")

    def inject_and_remove():
        try:
            # Add delay
            container.exec_run(f"tc qdisc add dev eth0 root netem delay {latency_ms}ms", privileged=True)
            logger.warning(f"CHAOS: {latency_ms}ms latency active on '{container_name}'")
            time.sleep(duration_seconds)
            # Remove delay
            container.exec_run("tc qdisc del dev eth0 root", privileged=True)
            logger.info(f"CHAOS: Latency removed from '{container_name}'")
        except Exception as e:
            logger.error(f"CHAOS latency inject failed: {e}")
            # Best-effort cleanup
            try:
                container.exec_run("tc qdisc del dev eth0 root", privileged=True)
            except Exception:
                pass

    threading.Thread(target=inject_and_remove, daemon=True).start()
    return {
        "status": "started",
        "experiment": "latency_inject",
        "target": container_name,
        "latency_ms": latency_ms,
        "duration_seconds": duration_seconds,
    }


def experiment_memory_stress(target_service: str, duration_seconds: int = 60) -> dict:
    """
    Runs `stress-ng` inside the target container to consume RAM.
    Tests OOM kill behavior and memory pressure handling.
    NOTE: stress-ng must be installed in the target container.
    """
    container = get_container(target_service)
    if not container:
        return {"status": "error", "message": f"No container found for '{target_service}'"}

    container_name = container.name
    logger.warning(f"CHAOS: Memory stress on '{container_name}' for {duration_seconds}s")

    # background=True so the exec doesn't block
    def run_stress():
        try:
            container.exec_run(
                f"sh -c 'apt-get install -y stress-ng -qq && stress-ng --vm 1 --vm-bytes 256M --timeout {duration_seconds}s'",
                detach=True
            )
        except Exception as e:
            logger.error(f"CHAOS memory stress failed: {e}")

    threading.Thread(target=run_stress, daemon=True).start()
    return {
        "status": "started",
        "experiment": "memory_stress",
        "target": container_name,
        "duration_seconds": duration_seconds,
    }


def experiment_cpu_stress(target_service: str, duration_seconds: int = 60) -> dict:
    """
    Runs `stress-ng` inside the target container to max out all CPU cores.

    WHY: CPU exhaustion is a common production failure mode — a runaway
    process, a tight loop, or a crypto miner can peg CPU at 100%.
    This tests whether the system detects the degradation, whether
    request latency spikes, and whether the healing agent responds.

    Uses: stress-ng --cpu 0 (all cores) --cpu-load 100 --timeout Ns
    stress-ng is installed at runtime via apt if not present.
    """
    container = get_container(target_service)
    if not container:
        return {"status": "error", "message": f"No container found for '{target_service}'"}

    container_name = container.name
    logger.warning(f"CHAOS: CPU stress on '{container_name}' for {duration_seconds}s")

    def run_stress():
        try:
            container.exec_run(
                f"sh -c 'apt-get install -y stress-ng -qq && "
                f"stress-ng --cpu 0 --cpu-load 100 --timeout {duration_seconds}s'",
                detach=True,
            )
        except Exception as e:
            logger.error(f"CHAOS cpu stress failed: {e}")

    threading.Thread(target=run_stress, daemon=True).start()
    return {
        "status": "started",
        "experiment": "cpu_stress",
        "target": container_name,
        "duration_seconds": duration_seconds,
    }
