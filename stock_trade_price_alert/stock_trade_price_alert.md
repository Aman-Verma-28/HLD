# HLD: Stock Price Tracking & Alert Notification System

> **Context:** Uber SDE-2 Bar Raiser — 60 min HLD round.
> **Problem:** Ingest stock ticks from exchanges; let users subscribe to alerts ("notify me when AAPL moves >5% vs price 1 hour ago / 1 minute ago / 1 day ago / 1 week ago"); deliver push notifications.
> **Hot challenges the interviewer wants to see:**
> 1. Where & how to store historical prices.
> 2. How to keep comparing the live price against last-1m / 1h / 1d / 1w prices.
> 3. How to shard when ticks are wildly unequal across stocks (AAPL ≫ tiny-cap).
> 4. How to avoid duplicate notifications for the same alert.

This doc follows the 7-phase framework from `answering_framework.txt`. Each phase has the verbatim things I'd say + the diagrams I'd draw on the whiteboard.

---

## Phase 1 — Requirements Clarification (5 min)

### What I'd say first

> "Before I draw anything, let me confirm scope. 'Stock alert system' could mean a lot of things — let me propose a scope and you can adjust."

### Functional requirements (in scope)

1. **Tick ingestion** — consume real-time price ticks from N exchanges (NYSE, NASDAQ, etc.) over their feed protocols. Assume ~10K listed stocks total.
2. **Alert subscription CRUD** — a user can create / list / delete alert rules. An alert rule = `(stock_symbol, threshold_pct, window, direction)` where:
   - `window ∈ {1min, 1hour, 1day, 1week}` — "compared to the price `window` ago"
   - `direction ∈ {up, down, both}`
   - `threshold_pct` e.g., 5%
3. **Alert evaluation** — every tick, evaluate any matching alert rules and fire if threshold crossed.
4. **Notification delivery** — push (APNs/FCM), with email/SMS as future. Latency target: tick → device < 5s p99.
5. **Historical price queries** — read-only API for charts (`GET prices?symbol=AAPL&from=...&to=...&granularity=1m`).
6. **Dedup** — never fire the same logical alert twice for the same triggering window.

### Out of scope (call out explicitly)

- Trading / order placement.
- User auth & onboarding (assume an `auth-service` exists, give us a `user_id`).
- Portfolio / P&L.
- News / sentiment-based alerts.
- Charting UI.

### Non-functional requirements

| Property        | Target                                                                     |
| --------------- | -------------------------------------------------------------------------- |
| Scale           | ~10K stocks, ~500M registered users, ~100M with ≥1 active alert            |
| Tick volume     | ~1M ticks/sec average, ~3M/sec peak (market open/close)                    |
| Alert latency   | < 5 s tick → device (p99); < 1 s for evaluation                            |
| Availability    | 99.99% — missed alerts are user-visible failures                           |
| Consistency     | Eventual is fine for historical reads. Alert evaluation must be at-least-once + dedup. |
| Durability      | Zero tick loss for raw stream (compliance/audit may need it)               |
| Data retention  | Raw 7 days; 1-min rollups 1 yr; 1-day rollups forever                      |

### Things I'd ask the interviewer

1. "Should I support custom price-level alerts (e.g., 'AAPL > $200') in addition to %-based windowed ones, or just the windowed ones?" — *Assume just windowed for now.*
2. "Do alerts re-arm? If AAPL stays >5% up for an hour, fire once or every minute?" — *Critical for dedup design. Assume fire once per window-bucket, then hysteresis re-arm.*
3. "Are we the source of truth for historical prices, or just consumers?" — *Assume consumers; the exchange feed is truth.*
4. "Push only, or also email/SMS in v1?" — *Push only for v1, design extensibly.*

### Visible scratchpad I leave on the board

```
SCOPE                       SCALE
- Ingest ticks              - 10K stocks
- Alert CRUD                - 1M ticks/sec (3M peak)
- Evaluate per tick         - 100M users w/ alerts
- Push notif (APNs/FCM)     - p99 < 5s tick→device
- Historical query          - 99.99% availability
                            - At-least-once + dedup
OUT: trading, auth, charts, news
```

---

## Phase 2 — Back-of-Envelope Estimation (3 min)

### Tick ingestion QPS

```
10,000 stocks × ~100 ticks/sec avg = 1,000,000 ticks/sec average
Peak (open/close): × 3 = ~3M ticks/sec
```

### Tick storage

```
Per tick wire format: {ts:8, symbol_id:4, price:8, volume:8} = 28B raw, ~50B with framing
1M ticks/sec × 86,400 s = 86B ticks/day
86B × 50B = ~4.3 TB/day raw
×7 days hot retention = ~30 TB hot
```

### Rollup storage (1-min OHLCV per symbol)

```
10K symbols × 1440 min/day × ~40B = ~600 MB/day → ~220 GB/yr
1-day rollups forever: 10K × 365 × 40B = 150 MB/yr — trivial
```

### Alert subscriptions

```
100M users × 5 alerts avg = 500M alert rows
Each row ~200B → ~100 GB. Single sharded SQL/Cassandra cluster.
```

### Alert evaluation fan-out

```
On a tick for AAPL: how many alert rules to evaluate?
AAPL has the most subscribers — say 10M alerts on AAPL.
At 100 ticks/sec on AAPL, naive = 1B evaluations/sec just for AAPL.
=> MUST aggregate ticks before evaluating. Don't evaluate per raw tick.
```

This last number is the most important one — it directly drives the architecture: **we cannot evaluate alerts per-raw-tick; we must aggregate to a stream of "price changed enough to matter" events first.**

### Notification QPS

```
If 0.1% of alerts fire per minute on average (very rough):
500M × 0.001 / 60 = ~8K notifs/sec average, bursty 10× during volatility = 80K/sec.
APNs/FCM handle this with batching.
```

### What these numbers buy us

| Number              | Design implication                                         |
| ------------------- | ---------------------------------------------------------- |
| 1M–3M ticks/sec     | Kafka, partitioned. Stream processing (Flink/Kafka Streams). |
| 4 TB/day raw        | Tiered storage: hot (Redis + Druid), cold (S3/Parquet).    |
| 1B evals/sec naive  | Pre-aggregate ticks → only evaluate on % change events.    |
| 10M alerts on AAPL  | Alert index sharded by symbol; hot symbol = sub-shards.    |

---

## Phase 3 — API Design (3 min)

### External (user-facing)

```http
POST /v1/alerts
Body: {
  "stock_symbol": "AAPL",
  "threshold_pct": 5.0,
  "window": "1hour",        // 1min | 1hour | 1day | 1week
  "direction": "both",      // up | down | both
  "channels": ["push"]
}
→ 201 { "alert_id": "uuid", "status": "active" }

GET /v1/alerts?cursor=...   // cursor-based pagination
DELETE /v1/alerts/{alert_id}
PATCH  /v1/alerts/{alert_id}   // pause/resume

GET /v1/prices/{symbol}?from=...&to=...&granularity=1m
→ [{ts, open, high, low, close, volume}, ...]
```

Auth header `Authorization: Bearer <jwt>` resolved at gateway → `user_id`.

### Internal (between services)

- **Tick ingest:** Kafka topic `stock.ticks.raw`, key = `symbol`, value = `(ts, price, volume, exchange)`.
- **Aggregated change events:** Kafka topic `stock.price.windowed` — emitted only when a window's % change crosses any "interesting" threshold band, key = `symbol`.
- **Alert triggers:** Kafka topic `alerts.triggered`, key = `alert_id`, value = `(user_id, alert_id, symbol, window, pct_change, ts, dedup_key)`.

### Design decisions to call out

- **Cursor pagination** on `GET /alerts` — stable when alerts are added concurrently.
- **Idempotency-Key header** on `POST /alerts` — safe retries.
- **Rate limiting** at gateway (we'll see big users with thousands of alerts).
- **Historical query is read-only** → can hit a read replica / Druid directly, no path through the write services.

---

## Phase 4 — High-Level Design (8 min)

### Strategy: start simple, then evolve

I'll draw v0 → v1 → final, each step solving a problem the previous one creates.

#### v0 — naive

```
Exchange ──→ Ingest ──→ DB ──→ Evaluator ──→ Push
                                    ↑
                              User Alerts
```

Problem: 1M ticks/sec hitting a DB; evaluator scans all alerts per tick. Dies immediately.

#### v1 — Kafka + stream processor

```
Exchange ──→ Ingest ──→ Kafka(ticks) ──→ Stream Proc ──→ TSDB
                                              ↓
                                        Alert Evaluator
                                              ↓
                                         Notifier
```

Better, but: (a) hot partitions on AAPL, (b) evaluator still does too much, (c) no dedup.

### Final architecture

```
┌──────────────┐
│  Exchanges   │  (NYSE, NASDAQ, ...)
└──────┬───────┘
       │  raw feed (FIX/ITCH/WebSocket)
       ▼
┌──────────────┐
│ Feed Adapter │  one per exchange. Normalizes to (ts, symbol, price, vol).
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────────┐
│ Kafka: stock.ticks.raw                   │  partitioned by symbol
│   (hot symbols split into sub-partitions │  retention 7d, RF=3
│    — see Deep Dive #1)                   │
└────┬─────────────────────────────┬───────┘
     │                             │
     ▼                             ▼
┌─────────────────┐        ┌──────────────────────┐
│ Window Aggregat │        │ Persistence Worker   │
│ (Flink job)     │        │ (Flink/Spark)        │
│ - keeps state:  │        │ writes:              │
│   1m / 1h / 1d  │        │   - raw → S3/Parquet │
│   / 1w prices   │        │   - 1m rollup → Druid│
│ - emits %-chg   │        │   - 1h/1d rollup ↑   │
│   events when   │        └──────────┬───────────┘
│   crossing a    │                   │
│   coarse band   │                   ▼
└────────┬────────┘            ┌────────────────┐
         │                     │ Time-Series DB │
         │                     │ (Druid /       │
         ▼                     │  TimescaleDB)  │
┌──────────────────────┐       └────────┬───────┘
│ Kafka:               │                │
│ stock.price.windowed │                │
│ (symbol, window,     │                ▼
│  pct_change, ts)     │       ┌────────────────┐
└──────────┬───────────┘       │ Historical API │  (read-only)
           │                   └────────────────┘
           ▼
┌──────────────────────┐       ┌────────────────────┐
│ Alert Evaluator      │◄──────│ Alert Index (Redis │
│ (sharded by symbol)  │       │ symbol → list of   │
│ - lookup matching    │       │ alert_ids + thresh)│
│   alerts             │       └────────▲───────────┘
│ - decide fire/skip   │                │
│ - emit dedup_key     │                │  invalidate on CDC
└──────────┬───────────┘                │
           │                  ┌─────────┴────────────┐
           ▼                  │ Alert Subscription   │
┌──────────────────────┐      │ Service (REST)       │
│ Kafka:               │      │   ↕                  │
│ alerts.triggered     │      │ Postgres (alerts)    │
└──────────┬───────────┘      │   ↓ CDC              │
           │                  └──────────────────────┘
           ▼
┌──────────────────────┐      ┌────────────────────┐
│ Notification Service │◄─────│ Dedup Cache (Redis │
│ - check dedup        │      │   SET dedup_key   │
│ - resolve user prefs │      │   TTL = window)    │
│ - dispatch           │      └────────────────────┘
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐      ┌────────────────────┐
│  APNs / FCM / SES    │      │ Notif Log (Cass.)  │
└──────────────────────┘      │ user_id PK, ts CK  │
                              └────────────────────┘
```

### Why each box exists (verbal)

- **Feed Adapter, one per exchange** — exchange protocols differ; isolate the parsing.
- **Kafka** — buffers the 1M/sec ingest spike, decouples producers from consumers, gives us replay for failures and recomputation.
- **Window Aggregator (Flink)** — the trick. Instead of every alert evaluator looking up "what was AAPL's price 1 hour ago?", a single stateful job per symbol computes that once. It only emits a downstream event when the % change crosses a coarse band (every 0.5% say) — this collapses 1B naive evals/sec into something tractable.
- **Persistence Worker** — separate consumer of the same topic, writes to the time-series store and cold storage. Read path and write path don't share fate.
- **Time-Series DB (Druid / TimescaleDB)** — handles range scans by `(symbol, ts)` natively, has built-in rollups.
- **Alert Evaluator** — stateless workers, sharded by symbol. Loads its symbol's alert list from Redis, evaluates the windowed event against each. State is just `last-fired-bucket per alert_id`.
- **Alert Index in Redis** — `symbol → [(alert_id, threshold, window, direction, last_bucket)]`. Hot read path; rebuilt from Postgres via CDC (Debezium).
- **Alert Subscription Service + Postgres** — boring CRUD on user alerts. Postgres because the data is small (~100 GB), we want transactions, and the access pattern is by `user_id` and by `alert_id`.
- **Dedup Cache (Redis)** — `SET sent:{alert_id}:{bucket} 1 EX <window-seconds> NX`. NX returns false if already sent → drop.
- **Notification Service** — applies user prefs (channels, quiet hours), dispatches to provider, retries with backoff.
- **Notif Log (Cassandra)** — append-only, partitioned by `user_id`, used for user-facing "alert history" UI and for audit.

### Walkthroughs

**Tick → notification:**
1. NYSE pushes AAPL @ $182.30 → Feed Adapter → Kafka `stock.ticks.raw` (key=AAPL).
2. Window Aggregator consumes, updates state: 1m-ago = $180.0, 1h-ago = $173.5, etc. Computes %-changes. Crosses a coarse band → emits `(AAPL, 1hour, +5.07%, ts)` to `stock.price.windowed`.
3. Alert Evaluator (shard owning AAPL) reads. Looks up Redis `alerts:AAPL` → finds 10M alerts; filters to ones with `window=1hour, threshold_pct ≤ 5.07, direction in {up,both}`. (More on how to make this fast in Deep Dive #2.)
4. For each match, computes `dedup_key = sha(alert_id, window_bucket(ts, 1h))`. Tries `SET NX` in Redis. Survivors go to `alerts.triggered`.
5. Notification Service consumes, looks up user prefs, calls APNs/FCM, writes Notif Log.

**Alert subscription change:**
1. User → API GW → Alert Service → `INSERT INTO alerts ...`.
2. Debezium CDC publishes the row to Kafka `alerts.cdc`.
3. A small "Index Builder" service consumes, updates Redis `alerts:AAPL` and `alerts_by_user:{user_id}`.
4. Evaluators see the new alert on their next read (Redis is the source of truth for the hot path).

---

## Phase 5 — Database Design (5 min)

### Storage decisions, summarized

| Data                       | Store                  | Why                                             |
| -------------------------- | ---------------------- | ----------------------------------------------- |
| Raw ticks (last 7 d)       | Kafka + S3 Parquet     | Stream + cheap cold replay                      |
| 1-min rollups (1 yr)       | Druid / TimescaleDB    | Range scans by (symbol, ts), built-in rollups   |
| 1-day rollups (forever)    | S3 Parquet (Athena)    | Tiny, cold, queryable                           |
| Latest price + window refs | Redis                  | Hot path, sub-ms reads                          |
| User alerts                | Postgres (sharded)     | Transactions, low volume, query by user_id      |
| Alert index (symbol→rules) | Redis                  | Hot read on every windowed event                |
| Notification log           | Cassandra              | Append-heavy, partition by user_id              |
| Dedup keys                 | Redis (TTL)            | Fast SET NX, auto-expiry                        |

### Schemas

**Postgres — `alerts`**
```sql
CREATE TABLE alerts (
  alert_id      UUID PRIMARY KEY,
  user_id       BIGINT NOT NULL,
  symbol        VARCHAR(10) NOT NULL,
  threshold_pct NUMERIC(5,2) NOT NULL,
  window        VARCHAR(8) NOT NULL,   -- '1min','1hour','1day','1week'
  direction     VARCHAR(4) NOT NULL,   -- 'up','down','both'
  channels      JSONB NOT NULL,
  status        VARCHAR(8) NOT NULL,   -- 'active','paused'
  created_at    TIMESTAMPTZ NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON alerts (user_id, status);
CREATE INDEX ON alerts (symbol, status);  -- index rebuilds use this
```
Sharded by `user_id` (consistent hashing). The `(symbol, status)` index is per-shard; full symbol→alert list is materialized in Redis.

**Redis — alert index**
```
KEY:   alerts:{symbol}                          (HASH or compressed structure)
VAL:   alert_id → (user_id, threshold_pct, window, direction)
```
For very hot symbols (AAPL): split into `alerts:AAPL:0..N` by hash(alert_id) % N so the evaluator shards can fan out.

**Redis — dedup**
```
KEY:   sent:{alert_id}:{window_bucket}
TTL:   window_seconds (60 / 3600 / 86400 / 604800)
SET .. NX EX ttl
```

**Druid (time-series rollups)**
```
datasource: stock_prices_1m
dimensions: symbol
metrics:    open, high, low, close, volume   (segment granularity = 1h, query granularity = 1m)
partition:  by day, sharded by symbol hash
```

**Cassandra — notification log**
```
CREATE TABLE notif_log (
  user_id     BIGINT,
  ts          TIMESTAMP,
  notif_id    UUID,
  alert_id    UUID,
  symbol      TEXT,
  pct_change  DECIMAL,
  status      TEXT,
  PRIMARY KEY ((user_id), ts, notif_id)
) WITH CLUSTERING ORDER BY (ts DESC);
```
Partition by `user_id` → all of one user's notifications on one node, ordered by recency.

### Access patterns sanity-check

| Query                                       | Path                                   |
| ------------------------------------------- | -------------------------------------- |
| List my alerts                              | Postgres shard by user_id              |
| Evaluator: alerts on AAPL                   | Redis `alerts:AAPL`                    |
| Has this alert already fired this bucket?   | Redis `SET NX` on dedup key            |
| 1-min OHLC for AAPL last 24h                | Druid range scan                       |
| Last week of AAPL                           | Druid (1-min) or S3 Parquet (1-h)      |
| Show user's notification history            | Cassandra by user_id                   |

No cross-shard joins, no scans.

### Sharding strategy

- **Tick stream (Kafka):** partition by `symbol` → 1024 partitions baseline. Hot symbols (AAPL, TSLA, SPY, top ~100) get K sub-partitions (`AAPL:0..K-1`); the producer round-robins across them. Consumers pre-aggregate per sub-partition then a downstream Flink `keyBy(symbol)` operator merges. **(See Deep Dive #1.)**
- **Alerts (Postgres):** by `user_id`, consistent hash, 64 shards. Symbol→alerts materialization runs out of CDC.
- **Notification log (Cassandra):** by `user_id`. RF=3.
- **Druid:** segments by day + symbol-hash; replication via deep storage on S3.

---

## Phase 6 — Deep Dives (15 min)

The interviewer asked about four things explicitly. I'd budget Deep Dive #1 (hot partition) and #3 (dedup) as the longest, since those are the spicy ones.

### Deep Dive #1 — Hot stocks & uneven tick distribution

> "AAPL gets 100× the ticks of a small-cap. If we partition Kafka by symbol, the AAPL partition's consumer dies while small-cap consumers idle. How do we fix it?"

#### Approaches

**A. Hash-partition by symbol (baseline).**
Pros: locality, easy stateful aggregation (all AAPL ticks on one consumer → one piece of state).
Cons: AAPL partition is 100× hotter. Consumer can't keep up; lag grows.

**B. Round-robin partitioning (no key).**
Pros: perfect load balance.
Cons: AAPL ticks scattered across all consumers — can't maintain per-symbol windowed state on any single consumer. Would need an expensive shuffle.

**C. Two-tier: split hot symbols into sub-partitions, regular ones single-partition (RECOMMENDED).**

```
Producer logic:
  if symbol in HOT_SET:                 // dynamically maintained, e.g., top-200 by volume
      subkey = (symbol, hash(tick) % K)
      partition = hash(subkey) % P
  else:
      partition = hash(symbol) % P

Consumer (Flink):
  Stage 1: keyBy(symbol, sub_id) → partial windowed aggregation
              (each sub-aggregator sees 1/K of AAPL's ticks; computes partial sums)
  Stage 2: keyBy(symbol) → merge K partials into the symbol's true state
              (this stage's traffic is K× smaller than raw ticks because pre-aggregated)
```

**Why this works:** Stage 1 distributes tick CPU evenly. Stage 2's input is N pre-aggregates per second per symbol (small), so even AAPL's stage-2 task is fine.

**D. Hot-symbol detection is dynamic.**
A monitoring job watches per-partition lag and per-symbol tick rate. When a symbol exceeds threshold, it gets added to the `HOT_SET` config (stored in ZooKeeper / etcd / control-plane). Producers reload config every few seconds. Old data in the original partition is drained over the rollover window.

**E. Trade-offs to call out**
- Stage-2 is now a join across the K pre-aggs — added latency (tens of ms).
- Out-of-order ticks across sub-partitions → use Flink event-time + watermarks, not processing time.
- Memory: stage-1 keeps K× state for hot symbols, but each is 1/K size — net the same.

#### Recommendation

> "Hybrid two-tier with dynamic hot-symbol detection. Top-100 symbols get K=16 sub-partitions; everything else hash-partitioned by symbol. Stage 1 pre-aggregates, Stage 2 merges. Hot list is updated by a control-plane job watching partition lag, with a 60-second cooldown to avoid flapping."

### Deep Dive #2 — Maintaining 1m / 1h / 1d / 1w comparisons

> "How do you, on every tick, know what the price was exactly 1 hour ago, 1 day ago, 1 week ago — without scanning history?"

#### Naive: query the time-series DB on every tick

For AAPL @ 100 ticks/sec, 4 windows × 100 = 400 DB lookups/sec on AAPL alone. Across 10K symbols at avg load = 400K lookups/sec on the TSDB. Possible but expensive, and adds latency. The TSDB also wasn't built for that read pattern.

#### Right approach: stateful stream operator with tiered ring buffers

The Window Aggregator (Flink job) holds, **per symbol**, this state:

```
struct SymbolState {
    last_price: f64
    last_ts:    ts

    // ring buffer of recent prices, one slot per "tick of resolution"
    minute_ring: RingBuffer<(ts, price)>   // resolution 1s, capacity 60   → covers 1 min
    hour_ring:   RingBuffer<(ts, price)>   // resolution 1m, capacity 60   → covers 1 hr
    day_ring:    RingBuffer<(ts, price)>   // resolution 1h, capacity 24   → covers 1 day
    week_ring:   RingBuffer<(ts, price)>   // resolution 1d, capacity 7    → covers 1 week

    // last band emitted per window (for change-detection — see below)
    last_band:   {1m: int, 1h: int, 1d: int, 1w: int}
}
```

**Total memory:** ~ (60 + 60 + 24 + 7) × 16B = ~2.5 KB per symbol. 10K symbols = ~25 MB. Tiny — trivially fits in a single Flink task's heap; even per shard.

**On each tick:**
```
1. Update last_price, last_ts.
2. If ts crossed the slot boundary in any ring, append (ts, price) and drop the oldest.
3. For each window in {1m, 1h, 1d, 1w}:
     ref_price = ring.tail()       // oldest still in window
     pct = (last_price - ref_price) / ref_price * 100
     band = floor(pct / BAND_SIZE)        // BAND_SIZE = 0.5%
     if band != last_band[window]:
        emit (symbol, window, pct, ts) to stock.price.windowed
        last_band[window] = band
```

**Why bands?** The downstream evaluator only needs to react when % change *crosses* an interesting threshold. If AAPL is hovering at +5.01%, +5.02%, +5.03% — that's all the same alert decision. Coarse-banding by 0.5% (configurable) drops downstream traffic by ~100×.

**Why the ring buffer rather than per-tick history?** We never need the price 17 minutes ago — only "the price 1 minute ago", "1 hour ago", etc. So we keep one snapshot per relevant resolution. The 1-min ring's slot at index `k` is the close price of second `(now_second - k)`.

**Bootstrapping (job restart):** Flink checkpoints state to S3 every 30s. On restart, reload state, then replay from Kafka with `auto.offset.reset = earliest checkpoint`. For the 1-week ring on a fresh deploy, we backfill from the Druid 1-min rollups (~10K symbols × 7 days × 24 × 60 = 100M rows, takes a few minutes).

**Edge cases to mention:**
- After-hours / weekends: ring slots are still allocated by wall clock, but no ticks → ref_price stale. State holds the last actual price, which is correct semantically ("compared to the last known price 1 week ago").
- Symbol with sparse trading (small caps, ~1 tick/min): rings still work; some slots will be `None`, fall back to the most recent prior slot.
- Stock splits / dividends → corporate-action service publishes adjustment events; Flink applies a multiplier to the rings on receipt.

#### Recommendation

> "Stateful Flink job, per-symbol tiered ring buffers (25 MB total), emit only on cross-band changes. This is what makes the system tractable — naive per-tick TSDB lookup would not fit the latency or cost budget."

### Deep Dive #3 — Dedup: never fire the same alert twice

> "User has 'AAPL +5% in 1 hour'. Stock spikes 5.1%. We get five ticks in a row that all cross threshold. We must fire once. Stream replay during recovery — must still fire once. Multiple evaluator workers — still once."

#### Sources of duplicates

1. **Threshold flapping** — 5.0% → 5.1% → 4.9% → 5.0% in five ticks. All "crossings" of 5.0%.
2. **Stream replay** — Flink restarts, replays from last checkpoint, re-emits windowed events.
3. **At-least-once delivery** — Kafka consumer retries, evaluator emits a trigger twice.
4. **Multiple evaluator workers** for one symbol shard — partition rebalance during deploy.
5. **The window persists** — alert is "5% in 1h"; once it's true at 10:00:00, it's still true at 10:00:01. We don't want to fire every second.

#### Approaches

**A. Idempotency key with TTL = window**
```
dedup_key = sha256(alert_id || window_bucket(ts, window))
window_bucket(ts, '1hour') = floor(ts / 3600)   # tumbling buckets
```
Notification service does `SET NX EX <window_seconds>` on Redis with this key. If it returns 0, drop. **Solves 1, 2, 3.**

**B. Cooldown / re-arm hysteresis**
For (5):
> "Once an alert fires for `(alert_id, window)`, don't fire again until the price has come back inside the threshold band, then re-crossed it."

Implemented by storing a small `armed: bool` per alert in Redis:
```
last_state:{alert_id} = { "fired_bucket": 8765, "armed": false }
```
- On windowed event: if `pct_change ≥ threshold` and `armed`: fire, set `armed=false`, record bucket.
- If `pct_change < threshold * 0.8` (hysteresis margin): set `armed=true`.

Hysteresis margin (e.g., 80% of threshold) prevents immediate re-arm flapping.

**C. Tumbling-bucket dedup (simpler alternative)**
> "An alert fires at most once per `window`-sized tumbling bucket of wall-clock time."

If `window=1hour`, alert fires at most once between 10:00–11:00. This is what idempotency key (A) gives us if `bucket = floor(ts/3600)`. Slightly less precise than (B) but much simpler — no `armed` state.

**D. Exactly-once Kafka semantics on the alert pipeline**
For the evaluator → notification hop, use Kafka transactions (`enable.idempotence=true`, `isolation.level=read_committed`). Combined with a transactional consumer in the notification service, we get exactly-once *within* the pipeline. This is belt-and-braces: dedup key still protects against pre-pipeline causes (1, 5).

#### Recommendation

> "Combine (A) idempotency key + (C) tumbling-bucket cap as the floor — dead simple, covers replay & flapping. Layer (B) hysteresis on top for nicer UX (no spamming when a stock hovers right at threshold). Use Kafka transactions on the trigger→notify hop for an extra safety net."

This way the user gets at most one notification per `(alert_id, hour-bucket)`, and even within that bucket the price had to drop below `0.8 × threshold` and come back — no jitter alerts.

### Deep Dive #4 — Storing historical prices

> "Where do prices live? How do we serve charts and the rolling-window references?"

#### Tiered storage

```
┌────────────┐   <100ms hot reads, last-1h ticks
│   Redis    │   stream key per symbol, MAXLEN ~3600
└────────────┘
┌────────────┐   1-min OHLCV, last 1 yr, range scans
│   Druid    │   segments partitioned by day + symbol
└────────────┘
┌────────────┐   1-h, 1-day rollups forever, ad-hoc analytics
│ S3 Parquet │   queried via Athena/Trino
└────────────┘
```

#### Why three tiers, not one

- **Druid alone** could handle everything but cost: storing 7 days of *raw* (4 TB/day) in Druid is wasteful when nothing reads raw older than a few minutes.
- **S3 alone** can't serve sub-second chart queries.
- **Redis alone** is RAM-bound — can't hold a year of 1-min rollups.

#### Rollup pipeline

A separate Flink job consumes `stock.ticks.raw` and emits 1-second OHLCV → Druid (real-time ingestion). A daily Spark job rolls 1-min → 1-hour and 1-day, lands as Parquet on S3.

#### Why time-series DB and not just Postgres?

Time-series DBs (Druid, TimescaleDB, ClickHouse) are designed for the (timestamp, dimension) range-scan pattern, with column-store compression that gets us 10–20× over row-store. They also have built-in rollup tables. Postgres works at small scale; falls apart by 100M rows per symbol.

#### What about the "1 week ago" reference price?

Note: the Window Aggregator's ring buffer (Deep Dive #2) is the source for live evaluation. The TSDB is *not* on the hot path — it's only for charts and bootstrap. This keeps the alert-evaluation path independent from analytical queries.

### Deep Dive #5 — Failure handling (brief)

| Failure                          | Mitigation                                                                   |
| -------------------------------- | ---------------------------------------------------------------------------- |
| Feed adapter crashes             | Multiple adapters per exchange, leader election. Exchange retains feed.      |
| Kafka broker dies                | RF=3, min.insync.replicas=2.                                                 |
| Flink job crashes                | Checkpoint to S3 every 30s, restart from last checkpoint. Rings rebuilt fast. |
| Redis (alert index) dies         | Replica failover. Worst case rebuild from Postgres CDC (~minutes).           |
| Postgres primary fails           | Synchronous replica, automated failover (Patroni / RDS multi-AZ).            |
| APNs/FCM down                    | Notification Service queues, retries with backoff, dedup-keys make retry safe.|
| Whole region down                | Active-active across 2 regions; Kafka MirrorMaker, Postgres logical repl.   |

---

## Phase 7 — Wrap-Up (3 min)

### Recap (30 sec)

> "We took 1M tick/sec from exchanges through Kafka with two-tier partitioning to handle hot stocks like AAPL. A Flink job per-symbol keeps tiered ring buffers for 1m/1h/1d/1w reference prices and emits change events only when a coarse % band is crossed. Stateless evaluators sharded by symbol look up alerts in Redis and dedup via tumbling-bucket idempotency keys plus hysteresis. Triggered alerts go through a notification service to APNs/FCM. Historical data is tiered: Redis (last hour raw) → Druid (1-min, 1-yr) → S3 Parquet (forever). The four critical decisions were: pre-aggregate before evaluating, ring-buffer windowed state, sub-partition hot symbols, and dedup with hysteresis."

### Bottlenecks I'd flag

1. **Window Aggregator stage-2 merge for AAPL** — single keyed task per hot symbol; if AAPL needs >1 core, we'd need to further parallelize via partial aggregation trees.
2. **Notification provider rate limits** — APNs/FCM batch APIs cap at a few thousand devices per call. During market-wide events (circuit breaker), batching + sharded dispatchers needed.
3. **Redis alert index for AAPL** — 10M alert rules under one key won't fit. Sub-shard `alerts:AAPL:0..N` and route by `hash(alert_id) % N` at the evaluator.
4. **Cold start of ring buffers** after a deploy — backfill from Druid takes minutes; during that window evaluations may miss. We'd hold off emitting until rings are warm (mark state ready).

### Future improvements

- **Volume- and volatility-based alerts** ("notify me when AAPL volume spikes 3× normal").
- **Composite alerts** ("AAPL +5% AND SPY <-1%") — needs a small DSL and per-user stateful evaluator.
- **ML-based anomaly alerts** — flag unusual moves rather than fixed %.
- **Per-user quiet hours / digest mode** — batch alerts overnight, send a summary at market open.
- **Multi-asset** — extend to crypto (24/7 markets, different feed semantics) and FX.
- **Compliance / replay UI** — let an oncall replay any alert decision from raw ticks, leveraging the 7-day Kafka retention + Flink checkpoint.

### Curveball answers I'd be ready for

- **"What if traffic 10×?"** — Hot-symbol set grows; bump K (sub-partitions) per hot symbol; horizontally scale Flink stage-1; nothing else fundamentally changes.
- **"What if a region goes down?"** — Active-active deploy. Alert evaluation is symbol-sharded per region with Kafka MirrorMaker; dedup keys are global (Redis Enterprise active-active CRDT or shared Redis cluster with regional caches and global TTL). Worst case: a few seconds of dual-fire risk during failover; dedup TTL absorbs it.
- **"How do you migrate from a legacy system?"** — Dual-write ticks into both pipelines; shadow-evaluate alerts in the new one and compare results without sending; flip user buckets gradually behind a feature flag.

---

## Interview-flow cheat sheet

| Min       | Phase             | What I do                                                            | What I say                                              |
| --------- | ----------------- | -------------------------------------------------------------------- | ------------------------------------------------------- |
| 0–5       | Requirements      | Write the SCOPE/SCALE box on the board. Ask the 4 questions.         | "Let me confirm scope first…"                           |
| 5–8       | Estimation        | Walk through tick QPS, storage, eval fan-out, notif QPS.             | "1B naive evals/sec — that drives the design."          |
| 8–11      | API               | Sketch external + internal APIs.                                     | "Cursor pagination, idempotency-key on POST."           |
| 11–19     | High-Level Design | Draw v0 → v1 → final, narrating why each box exists.                 | "Each component solves a problem the previous step had."|
| 19–24     | DB Design         | Pick stores per data, justify, sketch schemas, sharding.             | "TSDB is off the hot eval path."                        |
| 24–39     | Deep Dive #1      | Hot partition: hybrid two-tier with sub-partitions + Flink stage-1/2.| "Top-100 symbols get K=16 sub-shards."                  |
| 39–46     | Deep Dive #2      | Tiered ring buffers in Flink state, coarse-band emit.                | "25 MB per shard for all 10K symbols."                  |
| 46–53     | Deep Dive #3      | Dedup: idempotency key + tumbling bucket + hysteresis + Kafka tx.    | "Window-sized TTL on the dedup key."                    |
| 53–57     | Deep Dive #4      | Tiered storage: Redis → Druid → S3 Parquet.                          | "TSDB is not on the alert path."                        |
| 57–60     | Wrap-Up           | Recap, bottlenecks, future, take curveball.                          | "Main bottleneck is the stage-2 merge for AAPL…"        |

### Things to keep saying out loud

- "I'm choosing X over Y because…" — every decision is a trade-off.
- "Our estimation said Z, so we need…" — link numbers to design.
- "The alternative would be…, but it loses on…" — show breadth.
- "Let me come back to that in the deep dive" — manage time.

### Common mistakes to avoid for *this* problem

- **Don't evaluate alerts per raw tick** — it's the trap. Pre-aggregate.
- **Don't query the TSDB for "1 hour ago" on the hot path** — keep it in stream state.
- **Don't shard alerts purely by user_id** — evaluators need symbol → alerts. Shard the index by symbol; storage by user_id.
- **Don't forget hysteresis** — easy to design dedup that still spams a hovering price.
- **Don't put raw ticks in Postgres / Cassandra** — they aren't column stores; you'll regret it at 1 TB.

---

*That's the answer end-to-end. Total time spent: ~58 min, with 2 min slack for the curveball question.*
