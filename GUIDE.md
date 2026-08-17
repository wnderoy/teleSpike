# teleSpike — Implementation Guide

**From a blank VM to a working spike detector.** This guide walks a software engineer through building teleSpike stage by stage. Each stage ends with a **checkpoint** — a concrete, verifiable result. Stop at every checkpoint, confirm it, then continue. Do not skip ahead.

The final product: Telegram-style messages flow through **real Kafka** → **real PySpark** → a **QueueBurst** detector fires alerts when a word's activity bursts. Everything is driven from a **Jupyter notebook** (the control plane) on top of a Docker Compose stack.

- **The math** is QueueBurst (bucket-and-drain, TwitterMonitor SIGMOD 2010) — the core domain, explained at [Stage 4](#stage-4-run-the-engine-in-the-notebook).
- **The plan file** (`/home/wnder/.claude/plans/yo-lets-plan-a-polymorphic-origami.md`) is the canonical spec. When this guide and the plan disagree, the plan wins — flag the discrepancy.
- **Sibling doc:** `CLAUDE.md` (why the repo is stubbed the way it is, the version-lockstep rule, engine gotchas).

---

## Prerequisites

- A Linux VM with **≥ 15 GB RAM** and internet access (Docker pulls images, `spark-submit --packages` downloads jars).
- A **Telegram account** (only needed for the final scraper stages; not blocking).
- This repo cloned onto the VM.

## The stack (pinned — this is non-negotiable)

| Component | Version |
|---|---|
| Kafka broker | `apache/kafka:3.7.2` (KRaft, no ZooKeeper) |
| Spark master/worker | `apache/spark:3.5.1` |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` |
| Jupyter | `jupyter/pyspark-notebook:spark-3.5.1` |

Mixing any of these versions throws `NoSuchMethodError` / `ClassNotFound` that looks like your code is wrong. It isn't. **Keep the 3.5.1 / 3.5.1 / spark-3.5.1 triple identical.**

## Where you work

Two surfaces, used differently:

- **VM shell** — Docker commands (`docker compose …`), long-running processes, file editing.
- **Jupyter notebook** (browser at `http://<vm>:8888`) — the control plane: Spark sessions, `spark-submit`, reading outputs. The repo is mounted inside the container at `/home/repo`, so notebook cells can `%cd /home/repo` and `import src.*`. Run shell commands from a cell with a `!` prefix.

## The one mental model you need

**A word (per channel) gets a bucket with a drain.** The drain empties at *twice* the word's normal arrival rate. Mentions pour in faster than the drain empties, the water rises, and once it crosses the 7-line, that's a spike.

Two clocks must never be confused:
- **Event-time** — when a message's `ts` says it happened. Drives the 15 s tumbling window.
- **Worker-clock** — the machine's real clock. Drives the drain when a stream goes idle (a quiet gap must *drain the bucket*, not leave it artificially full).

## Stage map

| Stage | Plan phase | You build / verify |
|---|---|---|
| [1](#stage-1-vm-prep) | P0 | Docker + compose + repo clone |
| [2](#stage-2-kafka-up-and-topics) | P0 | Kafka (KRaft) healthy, `telegram`/`alerts` topics |
| [3](#stage-3-spark--jupyter-and-the-version-lockstep) | P0 | Spark cluster + Jupyter, version lockstep confirmed |
| [4](#stage-4-run-the-engine-in-the-notebook) | P1 | QueueBurst engine runs, math understood |
| [5](#stage-5-tokenizer-and-baseline-units) | P1 | `tokenize.py`, `baseline.py` pure units |
| [6](#stage-6-synthetic-producer--ground-truth) | P2 | Producer writes injected-burst ground truth |
| [7](#stage-7-producer-to-kafka) | P2 | JSON messages visible on `telegram` |
| [8](#stage-8-streaming-part-1-ingest--tokenize--count) | P3 | Kafka → window counts on console |
| [9](#stage-9-streaming-part-2-queueburst-detection) | P3 | Alerts fire, dedupe, Parquet sink |
| [10](#stage-10-live-demo) | P3 | Burst → alert in the notebook, live |
| [11](#stage-11-baseline-batch-job) | P4 | Per-hour baselines → Parquet |
| [12](#stage-12-baseline-into-the-stream) | P4 | Stream uses real baselines + floor |
| [13](#stage-13-replay-harness) | P5 | Offline replay, precision/recall |
| [14](#stage-14-tuning-sweep) | P5 | threshold/r sweep → `config/params.yaml` |
| [15](#stage-15-telegram-credentials--telethon) | P6 | API creds, Telethon installed, session |
| [16](#stage-16-the-scraper) | P6 | History JSONL for real channels, incremental |
| [17](#stage-17-end-to-end--real-history-warm-up) | P7 | Real baselines, full pipeline, live alerts |

---

## Stage 1 — VM prep

**Goal:** Docker + Compose v2 installed, repo cloned, `.env` created.

On the VM shell:

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"      # then log out and back in
git clone <your-repo-url> teleSpike
cd teleSpike
cp .env.example .env
docker compose version
```

**Checkpoint**

```bash
docker run --rm hello-world        # runs WITHOUT sudo (usermod worked)
docker compose version             # prints something like v2.x
```

**If it breaks**

- `hello-world` needs sudo → log out/in (or `newgrp docker`) to pick up the group.
- `docker.io` too old on your distro → use the official Docker apt repo instead; you specifically need `docker compose` v2, not the legacy `docker-compose`.
- No git repo exists yet locally → `git init` and add a remote, or clone a freshly pushed copy.

---

## Stage 2 — Kafka up and topics

**Goal:** Kafka running as a KRaft single node (broker + controller in one process, no ZooKeeper), `telegram` and `alerts` topics exist.

```bash
docker compose up -d
docker compose ps            # wait until kafka is 'healthy'
docker compose logs kafka | tail -n 20
```

Create the topics (KRaft has no ZooKeeper, so this is the one command form that works):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic telegram --partitions 3
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic alerts --partitions 3
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --list
```

**Checkpoint:** `docker compose ps` shows `kafka ... healthy`; `--list` shows `telegram` and `alerts`.

**Smoke test** (two terminals). Terminal A — producer:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic telegram
```

Type a line, e.g. `{"channel":"dev","sender":"me","text":"hello spike","ts":0}`.

Terminal B — consumer:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic telegram --from-beginning
```

**Checkpoint:** lines typed in A appear in B.

**If it breaks**

- Kafka never becomes healthy → the compose healthcheck uses `kafka-broker-api-versions.sh` (authoritative readiness, not process-alive). Check `docker compose logs kafka` for a first-run format error; the volume `kafka_data` may be in a bad state — `docker compose down -v` and re-up.
- You skip topic creation → it still works because `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` is set (default 3 partitions), but create them explicitly anyway.

---

## Stage 3 — Spark + Jupyter and the version lockstep

**Goal:** Spark master/worker cluster up, Jupyter reachable, and the **connector jar loads** — the single most common failure point in this project.

Bring up the rest of the stack (Jupyter depends on kafka + master being healthy, so give it a moment):

```bash
docker compose up -d
docker compose ps
```

Open `http://<vm>:8888` (no token — set for the VM demo), Spark UI at `http://<vm>:8080`.

**Create `notebooks/00_setup.ipynb`.** The compose env pre-sets `PYSPARK_SUBMIT_ARGS` (master, driver memory, and the `--packages` connector) inside the Jupyter container, so a notebook cell can build a session against the cluster without repeating flags:

```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("00_sanity") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

print("PySpark", pyspark.__version__)   # 3.5.1
print("Spark ", spark.version)          # 3.5.1
print(spark.conf.get("spark.jars.packages"))  # the kafka connector
```

Then prove the connector is actually loadable by asking Spark to *describe* the Kafka source (no data needed):

```python
spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "telegram") \
    .load().printSchema()
```

**Checkpoint:** both version lines print `3.5.1`; `printSchema()` succeeds (if the connector were missing/wrong you'd get `NoSuchMethodError` or `ClassNotFound` right here); the Spark UI shows one master + one worker.

**If it breaks**

- `NoSuchMethodError` / `ClassNotFound` → version mismatch. Check `spark.conf.get("spark.jars.packages")` resolves to `...spark-sql-kafka-0-10_2.12:3.5.1`, and that the images are the `spark-3.5.1` tags.
- Packages fail to download at submit → the VM needs internet; for offline fallback, pre-download the connector jar and pass `--jars` instead of `--packages`.
- Two ways to launch — don't confuse them: a **notebook cell** inherits `PYSPARK_SUBMIT_ARGS`; a raw **`spark-submit`** on the shell does **not**, so always pass `--master` and `--packages` explicitly there (you'll do this in Stages 8–12).

---

## Stage 4 — Run the engine in the notebook

**Goal:** the spike engine runs, you understand its math well enough to explain it.

The engine already exists: `src/queueburst.py`. Run its demo:

```bash
python3 src/queueburst.py
```

or, from a notebook cell with the repo mounted at `/home/repo`:

```python
%cd /home/repo
!python3 src/queueburst.py
```

**Read the output until it makes sense.** Each line prints the transition for one window:

```
arrivals=12  drain=10.00  level= 8.00  threshold=7.0  ->  SPIKE!
```

**The math, in words** (this is the whole product, get comfortable with it):

```
drainRate     = r × baselineArrival          # r = 2.0: empty at 2× the word's normal rate
currentLevel  = max(0, level + arrivals − drainRate × stepMinutes)
```

- `baselineArrival` — the word's usual loudness (messages/min), from history. Later stages measure it; for now the demo hard-codes 5/min.
- The `max(0, …)` floor means the bucket never goes negative — a quiet word just sits at 0.
- Ordinary noise can **never** stack up: arrivals must beat the drain by > 2× for the level to rise at all.
- **threshold = 7** is a *probability bound, not a volume*. `P(level ≥ 7) = (1/2)⁷ ≈ 0.8%` — the chance normal traffic fills the bucket to 7 is under 1 in 100, and this number is identical for a word that usually sees 1 msg/min and one that sees 1000. That's why one threshold serves quiet and loud words alike.
- **Rising edge only.** While the level is ≥ 7 the bucket *latches* (`inAlert`); a spike is reported only when it *first crosses* 7. So a burst fires once, and a later burst can fire again — no manual reset. Watch the demo's third scenario: `SPIKE!` on step 1, then `ALERT` (already latched) for several steps.

**Engine gotcha to remember** (a real past bug): `update_step` returns a **3-tuple** `(level, inAlert, triggered)` — the *report*. The **state** you feed back in is a **2-tuple** `(level, inAlert)`. Keep the three and the two straight or you'll unpack a float as a tuple.

**Checkpoint:** the demo prints three scenarios: ordinary stays `quiet`, the 2.4× surge `SPIKE!`s at step 4, the loud burst fires at step 1. You can predict the level after each step by hand.

---

## Stage 5 — Tokenizer and baseline units

**Goal:** two small pure functions with contracts, per the repo's `@requires`/`@ensures` + assert self-check convention.

**`src/tokenize.py`** — split a message into tokens. Keep it primitive:

```python
import re

def tokenize(text):
    """@requires text is a str
       @ensures result is a list of non-empty lowercased alphanumeric tokens"""
    return re.findall(r"[a-z0-9']+", (text or "").lower())
```

Optionally drop a short stopword list. Add a runnable assert at the bottom (`assert tokenize("Hello, WORLD 123!") == ["hello", "world", "123"]`).

**`src/baseline.py`** — estimate a word's usual rate from history rows `(channel, word, ts)`:

```python
def estimate_baselines(rows, floor=0.2):
    """@requires rows is iterable of (channel, word, ts)
       @ensures result maps (channel, word, hour) -> rate/min >= floor"""
    # bucket by minute -> per-minute counts -> mean per (channel, word, hour)
```

The output is a map the stream will broadcast: `{(channel, word, hour_of_day): msgs_per_min}`, **floored at 0.2/min** so a rare word still gets a working drain (no zero-drain / divide-by-zero). Time-of-day awareness is the whole point — a small night-time spike compares against the low *night* baseline, and daytime's universal elevation is absorbed into the higher *day* baseline.

**Checkpoint:** `python3 -c "import src.tokenize, src.baseline"` runs clean; the assert self-check passes; `estimate_baselines` on a tiny hand-written fixture returns the expected per-hour means with the floor applied.

**If it breaks** — nothing external here; this stage is pure Python and must run without Kafka/Spark. Keep it that way.

---

## Stage 6 — Synthetic producer (ground truth)

**Goal:** `src/produce_synthetic.py` deterministically generates Telegram-like traffic **with a known injected burst**, saved as replay ground truth.

Design (deterministic by design — you must be able to re-run the exact same traffic):

- `random.Random(seed)` for everything, so a given seed reproduces the same sequence.
- Per time step, each word's arrivals are drawn from a Poisson distribution around its baseline.
- One chosen word gets a burst: for `--burst-dur-steps` steps its arrivals are multiplied by `--burst-mult`.
- Every emitted message is JSON `{channel, sender, text, ts}` — the same schema every source must produce.
- Write the **exact injected sequence** to `data/bursts/<run>.jsonl`, tagging burst messages with a ground-truth marker `gt: {word, mult}`. That file is the replay harness's labeled answer key.

CLI shape:

```bash
python3 src/produce_synthetic.py \
  --seed 42 --duration-min 60 --baseline-per-min 5 \
  --channels dev,sports --vocab news,launch,fire,report,price \
  --burst-word launch --burst-mult 40 --burst-dur-steps 3 --burst-start-step 20 \
  --out data/bursts/demo_s42.jsonl
```

**Checkpoint:** `data/bursts/demo_s42.jsonl` exists; re-running with the same seed produces a byte-identical file; the burst word's count visibly jumps inside the burst window (e.g. `grep` the burst tag, or a quick notebook count).

**If it breaks** — Poisson arrivals can land on 0 for quiet words; that's correct. If the burst is invisible, check `--burst-mult` is large enough to exceed 2× the *burst word's own* baseline (that's what makes it a spike).

---

## Stage 7 — Producer to Kafka

**Goal:** the same synthetic traffic arrives on the `telegram` topic.

Add a `--send` mode to the producer that publishes each message to Kafka via `confluent-kafka==2.4.0` (`pip install` it in the Jupyter container — it's not preinstalled). Bootstrap servers: `kafka:29092` inside the compose network.

```bash
docker compose exec jupyter pip install confluent-kafka==2.4.0
docker compose exec jupyter python3 src/produce_synthetic.py --seed 42 ... --send
```

In a notebook cell, watch them land:

```python
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "telegram").load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp") \
  .writeStream.format("console").outputMode("append").start().awaitTermination()
```

**Checkpoint:** the notebook's console sink prints the JSON messages as they're produced; `ts` is present and ascending.

**If it breaks**

- No messages → confirm the producer connects (`kafka:29092`, not `localhost:9092` — the host port is for outside the network).
- Consumer sees messages but not the *burst* → remember the burst is a *rate*, not a constant; check the windowed count (next stage) rather than raw lines.

---

## Stage 8 — Streaming part 1: ingest → tokenize → count

**Goal:** the first slice of `spark/queueburst_streaming.py`: read Kafka, parse JSON, tokenize, and window-count words. No detection yet.

In the streaming job, build the DataFrame:

```python
schema = StructType([
    StructField("channel", StringType()),
    StructField("sender", StringType()),
    StructField("text",    StringType()),
    StructField("ts",      LongType()),   # unix epoch seconds, UTC
])
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "telegram") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("m")).select("m.*")
ts = parsed.withColumn("event_ts", to_timestamp(from_unixtime(col("ts"))))
```

Tokenize as a pure UDF (reuse `src/tokenize.tokenize`), `explode` into `(channel, word)` rows, then a **15 s tumbling window** count:

```python
counts = ts.select("channel", explode(udf(tokenize)(col("text"))).alias("word"), col("event_ts")) \
    .withWatermark("event_ts", "30 seconds") \
    .groupBy("channel", "word", window("event_ts", "15 seconds").alias("window")) \
    .count()
```

For this stage, sink `counts` to console (`append` mode) so you can *see* windows.

Run from the Jupyter container shell (explicit flags — `PYSPARK_SUBMIT_ARGS` does not apply to raw `spark-submit`):

```bash
docker compose exec jupyter spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/queueburst_streaming.py
```

Keep the producer running with a burst (`--burst-mult 40`).

**Checkpoint:** the console sink shows per-window counts; the burst word's count is clearly elevated for the 3 burst windows, then returns to baseline. The watermark (~2 windows = 30 s) means late stragglers are counted, not dropped.

**If it breaks**

- Empty windows for a low-rate word are normal (15 s is short); look at the *burst* word, not the chatter.
- `window(...)` on `event_ts` requires the column to be a timestamp — this is why you convert `ts` with `from_unixtime` first.
- **Two-clocks rule:** the *window* runs on event-time (`ts`); the *drain* (next stage) runs on the worker clock. Never derive the drain from the window clock.

---

## Stage 9 — Streaming part 2: QueueBurst detection

**Goal:** per-`(channel, word)` buckets, alerts fired on the rising edge, deduped, written to console + Parquet.

Extend the streaming job to run the engine as **grouped state**. PySpark 3.5.1's Python API has **no `mapGroupsWithState`** — the replacement is `applyInPandasWithState` (confirmed, recorded in the stub). Its shape:

```python
from pyspark.sql.streaming.state import GroupStateTimeout, GroupState

def detect(new_df):   # called per (channel, word) group per micro-batch
    ...               # build a pandas row per group: read state, run update_step, latch, emit on rising edge
    return output_df  # alert rows: {channel, word, window_start, window_end, currentLevel, alert_id}

counts.groupBy("channel", "word") \
    .applyInPandasWithState(detect, outputStructType, stateStructType,
                            outputMode="Append", timeoutConf=GroupStateTimeout.ProcessingTimeTimeout)
```

Concretely the function needs to:
1. **Load state** — `(currentLevel, inAlert)`, or a fresh bucket on first sighting.
2. **Drain across idle batches** — with `ProcessingTimeTimeout`, the function is still invoked (with empty new data) so idle keys drain by elapsed real time instead of staying artificially full.
3. **Update** — call the exact same `update_step` from `src/queueburst.py` (reuse, don't rewrite — this is what keeps offline replay faithful to live behavior).
4. **Fire on the rising edge** — emit an alert row only when `not inAlert and level ≥ threshold`; the bucket latches so one burst = one alert.
5. **TTL the state** — `state.setTimeoutDuration(...)` for ~5 windows; quiet keys that have fully drained get evicted, bounding the state map to *distinct words*, not message volume.

Alert row → three sinks: **stdout**, **`output/alerts` Parquet** (append), and optionally `kafka:alerts`. Dedupe is at-least-once + app-level on `(channel, word, window_start, currentLevel)` — exactly-once end-to-end is overkill for alerts.

Keying by `(channel, word)` means a loud group and a quiet one each get their own baseline comparison — a spike in a small channel is never masked by an overall average.

**Checkpoint:** with the producer running a burst, the stream prints an alert **once** per burst (check the latch), and `output/alerts` contains a matching Parquet row with the injected burst's window. Alert `currentLevel` at fire time is ≥ 7.

**If it breaks**

- `applyInPandasWithState` is the fiddliest API in the project. If the exact signature fights you, run `help(df.applyInPandasWithState)` and `help(GroupState)` in the notebook and match the installed PySpark — don't guess.
- Alert fires repeatedly on the same burst → the latch isn't stored back into state. Remember: report is a 3-tuple, state is a 2-tuple.

---

## Stage 10 — Live demo

**Goal:** the full loop *watching you*: burst in → alert in the notebook, live.

In `notebooks/02_live_demo.ipynb`, run the producer in a background cell (or `subprocess.Popen`), start the stream, and poll the `output/alerts` Parquet every few seconds:

```python
spark.read.parquet("/home/repo/output/alerts").orderBy("window_start").show()
```

**Checkpoint:** within one window + processing latency of the injected burst, the alert appears in the notebook. Kill the producer; a few seconds later the level drains and the bucket is ready to re-fire on the next burst.

**If it breaks** — alerts lag far behind the burst → check the watermark isn't holding late data back, and that the stream's event-time clock isn't stuck waiting for `ts` values in the future (all times UTC).

---

## Stage 11 — Baseline batch job

**Goal:** a batch job turns saved history into per-hour baselines, written to `output/baseline` Parquet.

Build a small batch entrypoint (a notebook cell or `spark/` script) that:
1. Reads `data/history/*.jsonl` (or, for now, synthetic history you generate with the producer's `--out`).
2. Tokenizes, floors `ts` to minute, groups `(channel, word, hour, minute)`.
3. Computes `baselineArrival = mean(count per minute)` per `(channel, word, hour)`, floored at 0.2/min.
4. Writes `output/baseline` as Parquet and shows the **top-baseline table** in `notebooks/01_baseline.ipynb`.

**Checkpoint:** `output/baseline` exists; the notebook table shows sensible per-hour rates (e.g. a burst-heavy word has a higher daytime rate); no word's rate is below the 0.2 floor.

**If it breaks** — if you only have burst-injected history, the burst word's baseline is inflated (it *is* its own noise). That's expected at this stage; real history (Stage 16) fixes it. The floor keeps every word drainable regardless.

---

## Stage 12 — Baseline into the stream

**Goal:** the streaming job uses real baselines instead of a hard-coded default.

- Load `output/baseline` Parquet → `collectAsMap()` → `spark.sparkContext.broadcast(...)`.
- In the `detect` function, look up the baseline by `(channel, word, hour_of_day)` **using the window's UTC hour**; if missing, fall back to a configurable default baseline (so the pipeline runs before any history exists).
- `drainRate = r × baselineArrival` now varies per word per hour — the bucket drains *with* the word's own rhythm.

**Checkpoint:** restart the stream; a word with history drains at its real rate (a quiet word's bucket drains slowly, a loud word's fast), and a burst still fires. A rare word with near-zero baseline gets the 0.2 floor, so its drain is non-zero.

**If it breaks** — baseline lookup must use the **event-time** hour of the window, not the worker's current hour, or nighttime traffic gets judged against the daytime baseline and nothing ever fires.

---

## Stage 13 — Replay harness

**Goal:** measure the detector against known labels — *no Kafka, no Spark streaming*, just the same pure engine over the ground-truth file.

`replay.py` (driven from `notebooks/03_replay_tuning.ipynb`):
1. Read `data/bursts/<run>.jsonl`, split by `(channel, word)`.
2. Walk the traffic step by step through `update_step` (the *same* function the stream uses — that's what makes offline results match live behavior).
3. Record the alert steps; the injected burst windows (`gt` tags) are the labels.
4. Report **precision / recall** — did it fire in the burst windows, and did it *not* fire anywhere else?

**Checkpoint:** on a clean injected run you see recall 1.0 (every injected burst caught) and precision near 1.0 (few or no false alerts). A false alert on the ordinary (non-burst) words is the knob telling you the threshold is too low.

**If it breaks** — recall < 1.0 on an injected burst usually means the burst didn't exceed 2× *that word's* baseline (raise `--burst-mult`), or `--burst-dur-steps` was shorter than the time to stack 7 units.

---

## Stage 14 — Tuning sweep

**Goal:** pick the `(r, threshold)` pair that best separates bursts from noise, and persist it.

Extend `replay.py` to sweep `threshold ∈ {5..9}` × `r ∈ {1.5, 2, 3}` and produce a precision/recall table per pair. Choose the pair with the best F1 (or the one with recall 1.0 and the fewest false alerts — your call), then write it to `config/params.yaml`:

```yaml
surgeMultiplier: 2.0      # r
threshold: 7.0            # fire when currentLevel >= threshold
windowSec: 15
baselineFloor: 0.2
```

**Checkpoint:** the sweep table renders in the notebook; `config/params.yaml` holds the chosen pair; re-running the replay with those params reproduces the table's numbers for that pair.

---

## Stage 15 — Telegram credentials + Telethon

**Goal:** the scraper's prerequisites. This is the only stage that needs *you* to visit a website.

1. Go to **my.telegram.org** → *API development tools* → create an app. You get an `api_id` (integer) and `api_hash` (string). Store them — they're secrets.
2. Fill `.env` (already templated):
   ```
   TELEGRAM_API_ID=1234567
   TELEGRAM_API_HASH=abcdef...
   TELEGRAM_PHONE=+1...
   TELEGRAM_SESSION=data/history/telegram
   ```
3. Install Telethon in the Jupyter container (the pinned version matters — PyPI's API moves):
   ```bash
   docker compose exec jupyter pip install telethon==1.36.0
   ```
4. First run performs an **interactive login** (phone → SMS code → optional 2FA password) and saves a session file to `data/history/telegram.session`.

**Checkpoint:** `docker compose exec jupyter python -c "import telethon; print(telethon.__version__)"` prints `1.36.0`; the `.session` file exists after login; a `!` notebook cell can list one public channel's entities.

**If it breaks**

- Login is interactive → run it with `!` in the notebook or in the container shell with a TTY (`docker compose exec -it jupyter python ...`); it needs to prompt for the code.
- Telegram rate-limits new accounts that hammer APIs → start with 2–3 small public groups, not 50.
- The login prompt can't appear in a plain background cell → do this step interactively.

---

## Stage 16 — The scraper

**Goal:** `src/scan_telegram.py` pulls real public-group history into `data/history/<channel>.jsonl`, resuming incrementally.

Implementation sketch (Telethon):

```python
async def scan(client, channel_name, out_path, offset):
    async for msg in client.iter_messages(channel_name, reverse=False):
        if msg.id <= offset: break                 # already seen
        if msg.message:                            # text-only, skip media
            append({channel, sender, text, ts})    # sender = msg.sender_id or username
    save_meta(offset=max_seen_id)                  # .meta.json for next run
```

Notes that matter:
- Write one JSON line per message, `{channel, sender, text, ts}` — **identical schema** to the synthetic producer, so the baseline batch job and replay harness are source-agnostic.
- The `.meta.json` offset + message-id dedupe make re-runs **idempotent** (append only new messages). Re-scraping is safe.
- Saved history is the **baseline warm-up** input (Stage 17) and doubles as real replay data.
- Caveat to keep in mind (from the plan): Telethon reliably pulls the *recent* history of public groups, but coverage isn't guaranteed for every channel — the baseline floor absorbs gaps.

**Checkpoint:** a real channel produces `data/history/<channel>.jsonl` with valid JSON lines and ascending `ts`; re-running the scraper appends nothing (offset advanced) — verify by line count.

**If it breaks**

- Only very recent messages come back → `iter_messages` limits or the account's permissions; that's expected, not a bug — recent history is enough to warm baselines.
- Media messages (`msg.message` empty) → skip them; text is all the detector needs.

---

## Stage 17 — End-to-end, real history warm-up

**Goal:** the whole product with real baselines, live alerts, and every output present.

1. **Warm baselines from real history** (`notebooks/04_history_warmup.ipynb`): run the Stage 11 batch job over `data/history/*.jsonl` (real) → refreshed `output/baseline`.
2. **Restart the stream** so it picks up the new broadcast baseline.
3. **Live demo** (`notebooks/02_live_demo.ipynb`): run real traffic (keep the scraper going) or synthetic bursts on top of real baselines; watch alerts land in the notebook.
4. **Final inventory** — every deliverable present and accountable:

| Artifact | Where |
|---|---|
| Messages | `kafka:telegram` |
| Ground truth (synthetic) | `data/bursts/*.jsonl` |
| Real history | `data/history/*.jsonl` |
| Baselines | `output/baseline/` |
| Word counts | `output/wordcounts/` |
| Alerts | stdout + `output/alerts/` + `kafka:alerts` |
| Tuned params | `config/params.yaml` |

**Checkpoint:** a burst (real or synthetic) fires an alert in the notebook within one window of happening; quiet periods produce no alerts; the baseline table reflects the time of day you're running; re-running anything (scraper, baseline, replay) is idempotent.

---

## Troubleshooting quick-reference

- **`NoSuchMethodError` / `ClassNotFound`** → version lockstep broken. All four components must be the pinned triple. This is the #1 failure mode.
- **Notebook session ≠ shell session** → `PYSPARK_SUBMIT_ARGS` applies to notebook cells; raw `spark-submit` needs explicit `--master`/`--packages`.
- **`--packages` needs internet at submit** → offline fallback: pre-download the connector jar and use `--jars`.
- **Kafka "healthy" but topics fail** → confirm `kafka:29092` (internal) vs `localhost:9092` (host).
- **Nothing ever fires** → the burst isn't exceeding 2× *its own* baseline (raise `--burst-mult`), or the baseline lookup is using the worker clock's hour instead of the event-time hour.
- **Burst fires every window** → the `inAlert` latch isn't being written back to state.
- **State map growing unbounded** → state TTL (~5 windows, `ProcessingTimeTimeout`) isn't configured or idle keys never drain.
- **Two clocks, one rule:** windows run on event-time, the drain runs on worker-time. Never mix them.
