"""queueburst.py — the spike detector engine (QueueBurst, TwitterMonitor SIGMOD 2010).

A word (per channel) gets a bucket with a drain at r x its baseline arrival
rate. Mentions pour in faster than the drain empties the bucket, so the water
level rises; once it crosses `threshold`, that is a spike.

Kept intentionally small: one transition function that PRINTS every step (so
you can watch the level move) plus a demo main. The tokenizer, baseline
estimation, Kafka/Spark plumbing, scraper and mock-data generator come later —
stubs for them live in src/.
"""

DEFAULT_R = 2.0          # surge multiplier: drain = r * baselineArrival
DEFAULT_THRESHOLD = 7.0  # fire when currentLevel >= threshold (0.5**7 ~ 0.8% false-alarm)


def update_step(state, arrivals, step_minutes, baselineArrival,
                r=DEFAULT_R, threshold=DEFAULT_THRESHOLD):
    """Advance the bucket one step (one window) and report a spike.

    state:           (currentLevel, inAlert) or None on the first sighting.
    arrivals:        mentions of the word inside this window (>= 0).
    step_minutes:    window length in minutes.
    baselineArrival: usual msgs/min for this word (from history, later).

    Prints each step's numbers so the math is visible while debugging.

    @requires arrivals >= 0 and step_minutes > 0 and threshold >= 0
    @ensures result is (currentLevel, inAlert, triggered)
    """
    if state is None:
        level, in_alert = 0.0, False
    else:
        level, in_alert = state

    drain_rate = r * baselineArrival
    level = max(0.0, level + arrivals - drain_rate * step_minutes)
    triggered = (not in_alert) and level >= threshold
    in_alert = level >= threshold

    print(f"  arrivals={arrivals:4.0f}  drain={drain_rate * step_minutes:5.2f}  "
          f"level={level:5.2f}  threshold={threshold:4.1f}  ->  "
          f"{'SPIKE!' if triggered else 'ALERT' if in_alert else 'quiet'}")
    return level, in_alert, triggered


def _run(steps, baseline_arrival, label):
    """Walk one labelled scenario, printing every step.

    @requires all(a >= 0 for a, _ in steps)
    @ensures no return value
    """
    print(f"\n== {label}  (baseline={baseline_arrival}/min, "
          f"drain=r*base={DEFAULT_R * baseline_arrival}/min, "
          f"threshold={DEFAULT_THRESHOLD}) ==")
    state = None
    for i, (arrivals, note) in enumerate(steps, start=1):
        print(f"step {i:2d} | {note:22s}", end="")
        level, in_alert, triggered = update_step(state, arrivals, step_minutes=1.0,
                                                 baselineArrival=baseline_arrival)
        state = (level, in_alert)  # 3-tuple is the step report; the bucket is the 2-tuple
        if triggered:
            print(f"  ***** SPIKE DETECTED at step {i} *****")


def main():
    """Demo: ordinary jitter stays quiet, a mild surge stacks up, a loud burst fires instantly."""
    _run([(5, "ordinary"),
          (6, "ordinary"),
          (4, "ordinary"),
          (5, "ordinary"),
          (7, "ordinary"),
          (5, "ordinary"),
          (6, "ordinary")], 5.0, "ordinary traffic")

    _run([(12, "mild surge"),
          (12, "mild surge"),
          (12, "mild surge"),
          (12, "mild surge"),
          (12, "mild surge")], 5.0, "sustained 2.4x surge (stacks)")

    _run([(60, "loud burst"),
          (55, "loud burst"),
          (50, "loud burst"),
          (6, "ordinary"),
          (5, "ordinary"),
          (4, "ordinary"),
          (5, "ordinary")], 5.0, "loud burst (fires instantly)")


if __name__ == "__main__":
    main()
