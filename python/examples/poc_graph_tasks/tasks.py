"""Trivial task functions for the declarative-pipelines POC.

These TaskFunctions run inside the Ray task image (imported by
michelangelo.uniflow.core.run_task), not in the workflow. They exist to test
graph topology at the minimum per-task cost: sleep, then exit. One of them
fails on purpose so failure semantics (skip propagation, run outcome) can be
exercised end to end.

The declarative interpreter dispatches them through the unmodified upstream
`core/ray` factory; this module is plain user code, the same thing any
pipeline author ships in their task image.
"""

import time

import michelangelo.uniflow.core as uniflow
from michelangelo.uniflow.plugins.ray import RayTask

# Minimum-cost specs: the task image's decorator config is what run_task
# falls back to; the workflow-side job_specs reach the pod as overrides.
_TASK_CONFIG = RayTask(
    head_cpu=1,
    head_memory="2Gi",
    worker_cpu=1,
    worker_memory="2Gi",
    worker_instances=1,
)


@uniflow.task(config=_TASK_CONFIG)
def sleep_task(seconds=5):
    """Sleep, then exit. Returns a small dict for output-reference tests.

    `received`/`received_type` echo the raw argument as the task pod saw it,
    before any coercion. Stage 2 uses them to prove numeric parameters survive
    the manifest jsonpb round trip as numbers (POC DoD item 4).
    """
    received = seconds
    received_type = type(seconds).__name__
    seconds = int(seconds)
    time.sleep(seconds)
    return {
        "slept": seconds,
        "received": received,
        "received_type": received_type,
        "status": "done",
    }


@uniflow.task(config=_TASK_CONFIG)
def echo_task(value=""):
    """Return its argument. Used to prove reference resolution end to end."""
    return {"echo": str(value)}


@uniflow.task(config=_TASK_CONFIG)
def fail_task(seconds=2, message="intentional failure"):
    """Sleep, then raise. Used to prove failure semantics in the run view."""
    seconds = int(seconds)
    time.sleep(seconds)
    raise RuntimeError(str(message))


# --- callable-override POC -------------------------------------------------
# Two interchangeable evaluators. The author YAML names one through
# `entry_point: "{{ params.evaluator_fn }}"`; a run swaps it by parameter.
# Same dotted-string contract locally (virtualenv) and on the cluster (image).


@uniflow.task(config=_TASK_CONFIG)
def default_eval(model=""):
    """Platform default evaluator: a fixed score."""
    return {"evaluator": "default_eval", "model": str(model), "score": 0.5}


@uniflow.task(config=_TASK_CONFIG)
def custom_eval(model=""):
    """User-provided evaluator: scores by model name length."""
    return {"evaluator": "custom_eval", "model": str(model), "score": len(str(model)) / 100}
