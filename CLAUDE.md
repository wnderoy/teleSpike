# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

teleSpike is a real-time spike detector: Telegram-style messages → **real Kafka** → **real PySpark** → alerts when a word's activity bursts. The math is **QueueBurst** (bucket-and-drain, from TwitterMonitor, SIGMOD 2010).

**Current state is a deliberate reset.** Only `src/queueburst.py` (the spike engine) has real code — and it is intentionally tiny and print-heavy for debugging. `src/tokenize.py`, `src/baseline.py`, `src/produce_synthetic.py`, `src/scan_telegram.py`, and `spark/queueburst_streaming.py` are **docstring-only stubs** by explicit user instruction ("init the other files we need in the future... but dont write any code in them yet"). Do not implement them unless the user asks. The canonical spec for all future work is the plan file:

**Plan: `/home/wnder/.claude/plans/yo-lets-plan-a-polymorphic-origami.md`** — read it before any implementation work. It locks the algorithm, stack, message schema, data layout, and phase order (P0–P7).

## Commands

No build step, no lint config, no test suite yet (no `pytest` installed, not even a git repo). The only runnable thing:

```bash
python3 src/queueburst.py   # runs the engine demo: prints every bucket step, fires 2 of 3 scenarios
```

Infra (Docker, runs on the VM — Docker is **not installed on this machine**):

```bash
docker compose up -d
# create topics (KRaft, no ZooKeeper):
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic telegram --partitions 3
# spark-submit runs from inside the jupyter container in client mode against spark://spark-master:7077
```

## Architecture

```
  source ───JSON───► kafka ──subscribe──► py spark ──alertJson──► alerter
 (synthetic gen. /   (broker,             tokenize → count →      (console + notebook,
  telethon scraper)   topic: telegram)    QueueBurst detect       /output/alerts)
                                                 ▲
  saved history ────► baseline job ─────────────┘
  (JSONL on disk)     (batch, re-run        baselineArrival per (channel, word, hour),
                      to update)            broadcast into py spark
```

- **Message schema (every source):** `{channel, sender, text, ts}`.
- **Key for detection:** `(channel, word)` — per-channel spikes aren't masked by an overall baseline.
- **Baseline** is time-of-day-aware: per `(channel, word, hour-of-day)`, floored at 0.2/min so rare words still drain. It comes from a batch job over saved history (`data/history/*.jsonl`), broadcast into the stream; it updates when the batch job re-runs, not live.
- **Window:** 15 s tumbling (configurable); watermark ≈ 2 windows; UTC event-time clock kept separate from the worker-clock drain.
- **Layout:** pure units in `src/`, the `spark-submit` entrypoint in `spark/`, notebooks in `notebooks/`, saved history in `data/history/`, injected-burst ground truth in `data/bursts/*.jsonl`, Parquet outputs in `output/{alerts,baseline,wordcounts}/`.
- **State TTL** (≈5 steps ProcessingTimeTimeout) evicts quiet keys that have fully drained.

## QueueBurst — the algorithm (the core domain)

Each word (per channel) gets a **bucket with a drain**. Every time step:

```
drainRate     = r × baselineArrival            # r = 2.0: empty at twice the word's normal rate
currentLevel  = max(0, currentLevel + arrivals − drainRate × stepLength)
```

- **currentLevel** is the water height, in units of *backlog above the drain line*. Ordinary noise can never accumulate: you must out-scream your own baseline by > 2× before the level rises.
- **threshold = 7**: fire when `currentLevel ≥ threshold`. The 7 is a probability bound, not a volume: `P(level ≥ threshold) = (1/2)^threshold ≈ 0.8%` false-alarm, identical for loud and quiet words. Not "7 messages out of thousands."
- **`inAlert` latch**: while `level ≥ threshold` the bucket latches; a spike is reported only on the **rising edge** (`not in_alert and level >= threshold`). This makes it fire once per burst and re-fire later — no manual reset.
- Both knobs (`r`, `threshold`) are tuned offline in the replay harness (`notebooks/03_replay_tuning.ipynb`, future work); defaults live as `DEFAULT_R = 2.0`, `DEFAULT_THRESHOLD = 7.0` in `src/queueburst.py`.

### Engine API (`src/queueburst.py`, the one real module)

`update_step(state, arrivals, step_minutes, baselineArrival, r, threshold)` → `(currentLevel, inAlert, triggered)`. It **prints every step** (`arrivals / drain / level / threshold / quiet|ALERT|SPIKE`) — that verbosity is by design, keep it.

Gotcha (a real past bug): `update_step` returns a **3-tuple** (the step report), but the **state** passed back in is a **2-tuple** `(level, in_alert)`. `_run` unpacks all three and re-stores only the two.

## Version lockstep — the #1 correctness lever

The 3.5.1 / 3.5.1 / spark-3.5.1 triple is non-negotiable; mixing versions throws incompatible-class errors:

| Component | Version |
|---|---|
| Spark | `apache/spark:3.5.1` |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` (passed to `--packages` at submit) |
| Jupyter | `jupyter/pyspark-notebook:spark-3.5.1` (bundled PySpark must match) |
| Kafka broker | `apache/kafka:3.7.2` KRaft single-node (no ZooKeeper) |

**PySpark API note:** `mapGroupsWithState` does **not** exist in the PySpark 3.5.1 Python API; its replacement is `applyInPandasWithState` (`pyspark.sql.streaming.state`). Recorded in the streaming stub so it isn't rediscovered the hard way.

## Working conventions

- **parts-bin modes are active** (`.claude/modes.json`: `interrogate` + `blank`). This means: ask clarifying questions before building each unit, and build pure logic as small, isolated, side-effect-free units with `@requires`/`@ensures` contracts and one runnable `assert` self-check. An explicit instruction always wins over a mode.
- **Keep it simple.** The user's standing theme: the plan looks straightforward, over-building is a real risk. Write the basics, not the full architecture, unless asked.
- The `interrogate` mode means don't assume requirements — when a choice matters (window size, baseline fallback, scraper coverage), the user has opinions and wants to be asked.
