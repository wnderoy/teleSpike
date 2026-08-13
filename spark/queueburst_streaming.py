"""queueburst_streaming.py — STUB (Spark streaming job).

Planned: read kafka:telegram, tokenize + window-count words, run the QueueBurst
engine per (channel, word) via applyInPandasWithState, and push alerts to
console + kafka:alerts + output/alerts.

Note: "mapGroupsWithState" does not exist in the PySpark 3.5.1 Python API; its
replacement is applyInPandasWithState.
"""
