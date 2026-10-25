"""Local runner for a declarative author YAML (callable-override POC).

Replaces the Starlark interpreter's coordinator with an in-process walk, the
way `python my_workflow.py local-run` replaces Cadence for a Python workflow.
Everything below the task boundary is reused: `entry_point` is a dotted
string, resolved with the same `import_attribute` the task pod uses, and the
task writes the same `result.json` shape through `_uf_result_url`.

    python -m examples.poc_graph_tasks.runner poc/author_yamls/callable_override.yaml \
        --param evaluator_fn=examples.poc_graph_tasks.tasks.custom_eval

Same rules as python/engine.star: levels in topological order, sorted names
inside a level, gates on `when` (UNSPECIFIED -> SUCCEEDED, COMPLETED ->
SUCCEEDED or FAILED), a skipped task satisfies no gate, references resolved
in a single non-recursive pass.
"""

import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path

import yaml

from michelangelo.uniflow.core.utils import import_attribute

SUCCEEDED, FAILED, SKIPPED = "SUCCEEDED", "FAILED", "SKIPPED"


def levels(tasks):
    deps = {n: {d["task"] for d in t.get("dependsOn", [])} for n, t in tasks.items()}
    out, done = [], set()
    while len(done) < len(tasks):
        level = sorted(n for n in tasks if n not in done and deps[n] <= done)
        if not level:
            raise SystemExit(f"cycle among {sorted(set(tasks) - done)}")
        out.append(level)
        done |= set(level)
    return out


def ancestors(tasks):
    anc = {}
    for level in levels(tasks):
        for n in level:
            direct = {d["task"] for d in tasks[n].get("dependsOn", [])}
            anc[n] = direct | set().union(*(anc[d] for d in direct)) if direct else set()
    return anc


def lookup(body, name, params, outputs, anc, t0):
    parts = body.split(".")
    if len(parts) == 2 and parts[0] == "params":
        if parts[1] not in params:
            raise KeyError(f"task {name} references unknown parameter {parts[1]!r}")
        return params[parts[1]]
    if parts[:2] == ["pipeline", "execution_time"]:
        return t0
    if parts[0] == "tasks" and len(parts) in (3, 4) and parts[2] == "output":
        dep = parts[1]
        if dep not in anc[name]:
            raise KeyError(f"task {name} references non-dependency task {dep!r}")
        out = outputs.get(dep)
        if out is None:
            raise KeyError(f"task {name} references dependency {dep!r} which produced no output")
        if len(parts) == 3:
            return out
        if not isinstance(out, dict) or parts[3] not in out:
            raise KeyError(f"task {name} references missing output field {parts[3]!r} of {dep!r}")
        return out[parts[3]]
    raise KeyError(f"task {name} has an unparseable reference {body!r}")


def resolve(value, *ctx):
    if isinstance(value, dict):
        return {k: resolve(v, *ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, *ctx) for v in value]
    if not isinstance(value, str):
        return value
    s = value.strip()
    if s.startswith("{{") and s.endswith("}}") and s.find("{{", 2) == -1:
        return lookup(s[2:-2].strip(), *ctx)  # whole-string ref keeps its native type
    out, i = "", 0
    while True:
        start = value.find("{{", i)
        if start == -1:
            return out + value[i:]
        end = value.find("}}", start)
        if end == -1:
            return out + value[start:]
        v = lookup(value[start + 2 : end].strip(), *ctx)
        out += value[i:start] + ("" if v is None else str(v))
        i = end + 2


def gate_ok(dep, states):
    when = dep.get("when", "SUCCEEDED")
    state = states.get(dep["task"])
    return state == when or (when == "COMPLETED" and state in (SUCCEEDED, FAILED))


def run_task(name, task, args, kwargs, run_dir):
    fn = import_attribute(task["config"]["entry_point"])
    result_url = str(run_dir / name / "result.json")
    os.makedirs(run_dir / name, exist_ok=True)
    if type(fn).__name__ == "TaskFunction":
        fn(*args, **kwargs, _uf_result_url=result_url)
    else:  # plain callable: no pre_run/post_run, same output contract
        with open(result_url, "w") as f:
            json.dump(fn(*args, **kwargs), f)
    with open(result_url) as f:
        return json.load(f)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("yaml")
    p.add_argument("--param", action="append", default=[], metavar="NAME=VALUE")
    ns = p.parse_args(argv)

    spec = yaml.safe_load(open(ns.yaml))["spec"]["manifest"]["content"]
    tasks = spec["tasks"]
    params = {k: v.get("default") for k, v in (spec.get("parameters") or {}).items()}
    for kv in ns.param:
        k, _, v = kv.partition("=")
        try:
            v = json.loads(v)  # D6: JSON if it parses, else string
        except ValueError:
            pass
        params[k] = v

    os.environ.setdefault("UF_LOCAL_RUN", "1")
    run_dir = Path(os.path.expanduser("~/uf_storage")) / f"local-{int(time.time())}"
    os.environ["UF_STORAGE_URL"] = str(run_dir)
    plan, anc, t0 = levels(tasks), ancestors(tasks), time.time()

    print(f"plan: {len(tasks)} tasks, {len(plan)} levels -> {run_dir}")
    for i, level in enumerate(plan):
        print(f"  level {i}: {', '.join(level)}")
    print(f"params: {json.dumps(params)}")

    states, outputs = {}, {}
    for level in plan:
        # ponytail: sequential inside a level; RayTask.pre_run calls ray.init,
        # which does not tolerate concurrent init in one process.
        for name in level:
            task = tasks[name]
            if not all(gate_ok(d, states) for d in task.get("dependsOn", [])):
                states[name], outputs[name] = SKIPPED, None
                print(f"[{name}] SKIPPED (gate)")
                continue
            cfg = task.get("config", {})
            try:
                ctx = (name, params, outputs, anc, t0)
                args = resolve(cfg.get("args", []), *ctx)
                kwargs = resolve(cfg.get("kwargs", {}), *ctx)
                entry = resolve(cfg["entry_point"], *ctx)
                print(f"[{name}] RUN {entry} args={args} kwargs={kwargs}")
                outputs[name] = run_task(name, {**task, "config": {**cfg, "entry_point": entry}}, args, kwargs, run_dir)
                states[name] = SUCCEEDED
                print(f"[{name}] SUCCEEDED -> {json.dumps(outputs[name])}")
            except Exception:
                traceback.print_exc()
                states[name], outputs[name] = FAILED, None
                print(f"[{name}] FAILED")

    print("run:", FAILED if FAILED in states.values() else SUCCEEDED, json.dumps(states))
    return 1 if FAILED in states.values() else 0


if __name__ == "__main__":
    sys.exit(main())
