# Declarative pipelines POC: local-run and custom task function

This POC shows two things from the declarative pipelines RFC
(michelangelo-ai/enhancements #24):

1. A task graph written in YAML runs locally, in your virtualenv, with no
   cluster.
2. A task names its function as a dotted string. You swap the function per
   run with a parameter, and no YAML edit.

The same YAML runs on the cluster. There the dotted string resolves inside
the task image. That path needs a sandbox and is not part of this branch.

## Files

| File | What |
|---|---|
| `runner.py` | Local coordinator. Walks the graph in level order, gates on `dependsOn`, resolves `{{ }}` references, runs each task in-process. About 150 lines. |
| `tasks.py` | Example task functions: `echo_task`, `fail_task`, `default_eval`, `custom_eval`. Plain `@uniflow.task` functions. |
| `callable_override.yaml` | Three tasks. `evaluator` takes its function from `params.evaluator_fn`. |
| `callable_failure.yaml` | `train` fails on purpose. Shows skip and `when: FAILED` gates. |

## Prerequisites

- Python 3.11 and Poetry.
- This repository cloned, branch `poc/uber-local-run`.

## Setup

Run every command from the `python/` directory.

```bash
cd python
poetry env use python3.11
poetry install --no-root -E dev
export PYTHONPATH=.
```

## Run

Default evaluator:

```bash
poetry run python -m examples.poc_graph_tasks.runner examples/poc_graph_tasks/callable_override.yaml
```

Expected tail:

```
[evaluator] RUN examples.poc_graph_tasks.tasks.default_eval args=['baseline'] kwargs={}
[evaluator] SUCCEEDED -> {"evaluator": "default_eval", "model": "baseline", "score": 0.5}
[report] SUCCEEDED -> {"echo": "default_eval scored 0.5"}
run: SUCCEEDED {"train": "SUCCEEDED", "evaluator": "SUCCEEDED", "report": "SUCCEEDED"}
```

Custom evaluator, chosen per run:

```bash
poetry run python -m examples.poc_graph_tasks.runner examples/poc_graph_tasks/callable_override.yaml \
  --param evaluator_fn=examples.poc_graph_tasks.tasks.custom_eval \
  --param model_name=candidate-xgb
```

Expected tail:

```
[evaluator] RUN examples.poc_graph_tasks.tasks.custom_eval args=['candidate-xgb'] kwargs={}
[evaluator] SUCCEEDED -> {"evaluator": "custom_eval", "model": "candidate-xgb", "score": 0.13}
run: SUCCEEDED {"train": "SUCCEEDED", "evaluator": "SUCCEEDED", "report": "SUCCEEDED"}
```

Failure and gates:

```bash
poetry run python -m examples.poc_graph_tasks.runner examples/poc_graph_tasks/callable_failure.yaml
```

Expected tail (exit code 1):

```
[train] FAILED
[evaluator] SKIPPED (gate)
[on_failure] SUCCEEDED -> {"echo": "train failed"}
run: FAILED {"train": "FAILED", "evaluator": "SKIPPED", "on_failure": "SUCCEEDED"}
```

Each run writes `result.json` per task under `~/uf_storage/local-<timestamp>/`.

The first task takes a few seconds. `RayTask.pre_run` starts a local Ray
instance. This is the same hook the task runs on the cluster.

## Use your own function

1. Write a function in any module on `PYTHONPATH`:

   ```python
   import michelangelo.uniflow.core as uniflow
   from michelangelo.uniflow.plugins.ray import RayTask

   @uniflow.task(config=RayTask(head_cpu=1, head_memory="2Gi"))
   def my_eval(model=""):
       return {"evaluator": "my_eval", "model": model, "score": 0.9}
   ```

2. Run the same YAML with your dotted path:

   ```bash
   poetry run python -m examples.poc_graph_tasks.runner examples/poc_graph_tasks/callable_override.yaml \
     --param evaluator_fn=my_team.my_module.my_eval
   ```

A plain function without the decorator also works. It then runs without
`pre_run` and `post_run`.

`--param NAME=VALUE` parses `VALUE` as JSON when it parses, else as a string.

## The YAML

```yaml
tasks:
  evaluator:
    taskFunction: core/ray
    dependsOn: [{task: train}]
    config:
      entry_point: "{{ params.evaluator_fn }}"      # or a literal dotted path
      args: ["{{ tasks.train.output.echo }}"]
```

- `entry_point` is a dotted string. The process that runs the task imports
  it: your virtualenv here, the task image on the cluster.
- `{{ params.X }}` reads a parameter. `{{ tasks.X.output.FIELD }}` reads an
  upstream result. A whole-string reference keeps its JSON type.
- `dependsOn[].when` is `SUCCEEDED` (default), `FAILED`, or `COMPLETED`.
  A skipped task satisfies no gate.

## Not covered on this branch

- The cluster path (`ma pipeline apply` and `run`, PipelineRun parameter
  override). Proven on the fork branch `poc/callable-local-run` against a
  k3d sandbox. Results are in that branch's `python/poc/README.md`.
- `jobSpecs` are ignored locally. Tasks run with local resources.
- Retry and task cache.
- Create-time validation against the task type schema. The runner trusts
  the YAML.
