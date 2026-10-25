"""Trivial task functions for the declarative-pipelines POC.

These run inside the Ray task image, not in the workflow. They exist to test
graph topology at the minimum per-task cost: sleep, then exit. See
python/poc/README.md.
"""
