# Stock Price Tracking & Alert Notification System — Interview Script

> **Source problem:** Design a system that ingests real-time stock prices, lets users subscribe to per-ticker alerts (percent move over a window OR static threshold), and pushes notifications when alerts fire. Variants: % move vs last minute / hour / week. Must avoid duplicate notifications, handle skew (AAPL/TSLA traded far more than small caps), and store historical prices efficiently.
>
> **Time budget (45 min):** 5 reqs · 3 estimation · 4 entities · 5 APIs · 12 HLD · 13 deep dives · 3 wrap-up.

---

## Opening line (say this verbatim)

> "Before I jump in, let me make sure I have the problem framed right: we're building a system that ingests real-time price ticks from one or more exchanges, lets users register alerts on individual tickers — either a percentage move over a sliding window like 1-minute / 1-hour / 1-week, or a static price threshold — and pushes a notification within a second or two when the alert fires. Two things I want to flag up front as the hard parts: tick volume is heavily skewed (AAPL gets 1000x the ticks of a small cap, so naive per-ticker sharding won't work), and we have to be exactly-once on notifications — sending the same alert twice is worse than sending it late. I'll come back to both of these in deep dives. Let me start with requirements."

**How to say it:** Slow, calm. The two flags at the end signal "I see the senior-level traps" — that's the whole point of saying them in the opener. Don't rush past them.

---

## 1. Requirements Clarification (3–5 min)

### Functional Requirements (what to say)

1. Users can **subscribe to a ticker** and create an alert with one of two rules: (a) **percentage move** of ≥ X% over a sliding window W (W ∈ {1m, 5m, 1h, 1d, 1w}), or (b) **static threshold** (price crosses above/below P).
2. The system **ingests real-time price ticks** from exchange feeds (treat as a Kafka-style stream), normalizes them, and persists raw + aggregated history.
3. When an alert fires, the system delivers a **notification within ~1 second** through the user's preferred channels (push, email, SMS).
4. Users can **list, edit, pause, and delete** their alerts. Alerts can be one-shot (auto-disable after firing) or recurring (re-arm after a cooldown).
5. The system **never sends duplicate notifications** for the same alert event — exactly-once at the user's perception layer.
6. Users can **query historical prices** for a ticker (last day, week, month, year) for display in the app.

### Clarifying questions to ask the interviewer (ask 2–3, listen)

- "Are we ingesting from a single exchange feed (e.g., NYSE direct) or aggregating multiple? That changes the ingestion fan-in story." (Assume: single normalized feed for now.)
- "For percent-move alerts, is the reference point the **start of the window** or a **rolling baseline** (e.g., 1h ago vs the min/max in the last 1h)? It affects the aggregate we precompute." (Assume: price N units of time ago, i.e., point-in-time reference.)
- "Do alerts need to survive a market close — i.e., does 'last 1 hour' mean wall-clock or trading-hours? And do we evaluate during pre/post market?" (Assume: trading hours only, wall-clock window inside that.)

**How to say it:** Ask the questions, then **state your assumptions out loud** and write them in a corner of the whiteboard. This is what differentiates a senior signal — you're not paralyzed by ambiguity, you make a choice and move.

### Non-Functional Requirements (what to say)

- **Scale:** ~10K tickers globally. Peak **~1M ticks/sec** during market open across all tickers (NYSE alone does ~500K msgs/sec at peak, plus NASDAQ — round to 1M for headroom). ~100M registered users, 10M DAU, ~5 alerts/active user → **~50M active alerts**.
- **Latency:** **end-to-end < 1s** from tick arrival at the gateway to notification dispatched. Of that budget: ingestion < 50ms, aggregate update < 100ms, alert evaluation < 200ms, notification push < 500ms.
- **Availability:** **99.95%** for the read path (price lookups). Notification path can tolerate brief unavailability since we'll buffer in Kafka — but we cannot **lose** alert events. So: highly available ingestion, durable buffer, eventually-consistent fan-out.
- **Consistency:** **Eventual** for displayed prices and history. **Stronger** for alert state (we cannot fire the same alert twice — needs idempotency / dedup).
- **Domain-specific #1 — Skew:** Tick distribution is wildly non-uniform. AAPL/TSLA/SPY may take 30–40% of total volume, while 8000 small caps share <5%. **Sharding by ticker alone produces hot partitions** — has to be addressed in the design, not retrofitted.
- **Domain-specific #2 — Exactly-once notification:** Users will rage-uninstall on duplicates. We need an **idempotency story end-to-end**: dedup keys on the alert event, idempotent notification dispatch, and at-least-once Kafka with dedup at the consumer.

**How to say it:** When you read out the scale numbers, tie each one to a design constraint immediately ("1M ticks/sec means we cannot evaluate alerts inline on the ingestion path — it has to be async via Kafka"). The interviewer is checking whether you know *why* the numbers matter, not whether you can recite them.

---

## 2. Back-of-the-Envelope Estimation (2–3 min)

### Traffic (QPS)

- **Tick ingestion (write):** ~1M ticks/sec at peak, sustained ~200K ticks/sec during regular trading hours.
- **Alert evaluation (internal):** every tick on a watched ticker triggers evaluation of all alerts on that ticker. Worst case: AAPL has, say, 5M alerts subscribed × 10K ticks/sec for AAPL alone = 5×10¹⁰ evaluations/sec. **This is the number that breaks naive designs** — call it out and say we'll fix it with (a) batching by ticker (group ticks into 100ms windows) and (b) only re-evaluating when the *aggregate window* updates, not every tick.
- **User-facing reads:** 10M DAU × 50 price views/day → ~6K price-lookup QPS, very cacheable.
- **Alert CRUD:** trivial — ~1K QPS, ignore.

### Storage

- **Raw ticks:** 1M ticks/sec × ~6.5 hours/trading day × ~50 bytes/tick (ticker, price, volume, ts, exch) ≈ **1.2 TB/day raw**. We don't need raw forever — keep 7 days hot, then downsample.
- **Aggregates (1-min OHLCV bars):** 10K tickers × 390 minutes/day × ~80 bytes ≈ **300 MB/day**. Keep these for years — cheap.
- **Alerts:** 50M alerts × ~200 bytes ≈ **10 GB**. Fits comfortably in PSQL.
- **Notifications log:** 50M alerts × ~1% fire-rate/day = 500K notifications/day × ~300 bytes ≈ **150 MB/day**.

### Domain-specific number — fan-out per tick on hot stocks

- AAPL alone: ~50K alerts subscribed (rough — popular ticker). If AAPL ticks 10K times/sec, naive design would do 500M alert evaluations/sec **for one stock**. Solution: evaluate against *aggregates*, not raw ticks. Window aggregates update once per second per window-size, not 10K times/sec. → drops evaluation rate by ~4 orders of magnitude.

**How to say it:** Do the math out loud and *write it on the board*. Then point at the 5×10¹⁰ number and say "this is why we shift evaluation off the tick path and onto the aggregate update path." That single sentence is the architecture's organizing insight — same role the 2.5M location-update number played in the Uber design.

---

## 3. Core Entities (3–5 min)

Bottom-up, 7 entities:

1. **Stock / Ticker** — `ticker` (PK, e.g., AAPL), `name`, `exchange`, `currency`, `is_active`.
2. **PriceTick** — `ticker`, `price`, `volume`, `timestamp_ns`, `exchange_seq` (for ordering). Append-only, immutable.
3. **PriceAggregate** — `ticker`, `window_size` (1m/5m/1h/1d/1w), `bucket_start`, `open`, `high`, `low`, `close`, `volume`. Rolling OHLCV bars.
4. **User** — `user_id`, `email`, `phone`, `notification_prefs` (which channels, quiet hours).
5. **Alert** — `alert_id`, `user_id`, `ticker`, `type` (`PERCENT` | `STATIC`), `threshold`, `window_size` (only for PERCENT), `direction` (UP/DOWN/EITHER), `status` (ACTIVE/PAUSED/FIRED), `cooldown_seconds`, `created_at`.
6. **AlertEvaluationState** — `alert_id`, `last_evaluated_at`, `last_fired_at`, `last_baseline_price`, `version`. Critical for dedup (see deep dive 3).
7. **Notification** — `notification_id` (idempotency key = `{alert_id}:{fired_event_seq}`), `user_id`, `alert_id`, `channel`, `status`, `sent_at`.

**How to say it:** Read the names, *don't* read the fields. The interviewer can see them. Just call out the two non-obvious ones: PriceAggregate (so we don't evaluate on raw ticks) and AlertEvaluationState (so dedup works). Those are senior signals.

---

## 4. API Design (3–5 min)

6 APIs:

1. **Create Alert** — internal alert-management API.
   - `POST /v1/alerts` body `{ticker, type, threshold, window_size?, direction, cooldown_seconds}` → `{alert_id, status}`. Auth via JWT.

2. **List / Update / Delete Alerts** — standard CRUD.
   - `GET /v1/alerts?cursor=...` cursor pagination. `PATCH /v1/alerts/{id}`. `DELETE /v1/alerts/{id}`. Soft-delete (status=DELETED) so historical notifications still resolve.

3. **Get Current Price** — read path, heavily cached.
   - `GET /v1/stocks/{ticker}/price` → `{ticker, price, ts, change_1d_pct}`. Served from Redis, ~1ms p50.

4. **Get Historical Prices** — chart data.
   - `GET /v1/stocks/{ticker}/history?window=1d&resolution=1m` → `{bars: [{t, o, h, l, c, v}, ...]}`. Reads from aggregate store; resolution must be ≥ stored bar size.

5. **Stream Real-time Prices** — WebSocket push for app charts.
   - `WS /v1/stream` subscribe `{action: "subscribe", tickers: ["AAPL"]}`. Server pushes `{ticker, price, ts}` per tick or per 100ms throttled batch (depends on UI tier).

6. **Internal: Ingest Tick** — exchange feed → ingestion service.
   - Not a public API. Exchange adapter publishes directly to Kafka topic `ticks.raw` partitioned by `hash(ticker)` (with hot-key handling — see deep dive 1). Schema: `{ticker, price, volume, ts_exch, ts_ingest, seq}`.

**How to say it:** "I'll skip read/write of static threshold vs percent — the schema already encodes that on the alert. Let me move to the high-level architecture, where the interesting decisions live."

---

## 5. High Level Design (10–12 min)

### Services (call out 7)

1. **Ingestion Service** — terminates exchange feeds (FIX/multicast/WebSocket), normalizes, publishes to Kafka `ticks.raw`. Stateless, horizontally scaled per exchange feed.
2. **Aggregation Service** — Kafka consumer on `ticks.raw`. Maintains rolling 1m / 5m / 1h / 1d / 1w OHLCV per ticker. Writes finalized bars to Cassandra, exposes current in-flight bar in Redis.
3. **Alert Management Service** — CRUD on alerts. Backed by PostgreSQL.
4. **Alert Evaluation Service** — Kafka consumer on `aggregates.updated`. For each updated aggregate, looks up all alerts on that ticker for that window, evaluates condition, publishes alert events to `alerts.fired`.
5. **Notification Service** — Kafka consumer on `alerts.fired`. Performs idempotent dispatch via channel adapters (FCM/APNs, SES, Twilio).
6. **Subscription / WebSocket Service** — fan-out service for app clients streaming live prices. Stateful (holds open WS connections); subscribes to a sharded subset of Kafka `ticks.raw`.
7. **Price Read Service** — fronts `GET /price` and `/history`. Reads Redis (current) + Cassandra (history). Heavily cached.

### Database layer

- **PostgreSQL** — `users`, `alerts`, `alert_evaluation_state`, `notifications` index. **Why:** ACID, relational queries (list a user's alerts, find all alerts on a ticker), and we need transactional updates on `alert_evaluation_state` for dedup. **Why not:** Cassandra here would be wrong — we need read-your-writes on alert state.
- **Cassandra (or ScyllaDB)** — `price_ticks_raw` (TTL 7 days), `price_aggregates` (long-term OHLCV bars). **Why:** wide-column, time-series-friendly, write-optimized (1M+ writes/sec across cluster), partition key = `(ticker, day)` keeps queries fast. **Why not:** TimescaleDB is also valid and arguably more ergonomic for OHLCV — call this out as a real trade-off the interviewer might want to discuss. Cassandra wins on raw write throughput at our scale; Timescale wins on query ergonomics for analyst tooling.
- **Redis (cluster mode)** — current price per ticker (`SET price:AAPL <json>`), in-flight aggregates (`HASH agg:AAPL:1m:<bucket> {o,h,l,c,v}`), alert dedup keys (`SETNX dedup:{alert_id}:{event_seq}`). **Why:** sub-ms reads, atomic ops for dedup. TTL on dedup keys = cooldown_seconds + buffer.
- **S3 (cold storage)** — Parquet rollups of raw ticks older than 7 days, partitioned by `date/ticker`. Queried via Athena/Presto for compliance/analyst use cases. Not on the live path.

### Cache layer

- **Redis: latest price + in-flight aggregate cache.** Sits between the Aggregation Service (writer) and the Price Read Service / Alert Evaluation Service / WebSocket Service (readers). Write-through from Aggregation Service when an aggregate updates; readers go to Redis first, fall back to Cassandra for finalized historical bars.
- **CDN edge cache for `/history` queries on popular tickers.** Cache key = `(ticker, window, resolution)`, TTL = 30s during market hours, 1h after close. Cuts the read tier load by 80%+ for the AAPL-like tickers everyone is staring at.

### Queue layer

- **Kafka — `ticks.raw` topic.** Sits between Ingestion Service (producer) and Aggregation Service + WebSocket Service (consumers). **Fan-out pattern** — multiple consumer groups read the same stream for different purposes. Partitioned by `ticker` *with hot-key splitting* (see deep dive 1). Retention 24h — it's a buffer, not a store.
- **Kafka — `aggregates.updated` topic.** Aggregation Service produces, Alert Evaluation Service consumes. Decouples the heavy fan-out (one aggregate update → potentially thousands of alerts) from the ingestion path.
- **Kafka — `alerts.fired` topic.** Alert Evaluation Service produces, Notification Service consumes. Durable buffer means we can survive a notification-channel outage (Twilio down, FCM down) without losing alerts.

### Architecture sketch (draw this)

```
                         ┌─────────────────────┐
   Exchange feeds ──▶   │  Ingestion Service  │── Kafka ticks.raw ──┐
   (NYSE, NASDAQ)       └─────────────────────┘                     │
                                                                     │
                ┌────────────────────────────────────────────────────┤
                │                                                    │
                ▼                                                    ▼
       ┌────────────────┐                                  ┌───────────────────┐
       │  Aggregation   │── writes ──▶  Redis (in-flight)  │ WebSocket / Sub   │── push ──▶ Mobile/Web
       │   Service      │── writes ──▶  Cassandra (final)  │   Service         │
       └────────────────┘                                  └───────────────────┘
                │
                │ Kafka aggregates.updated
                ▼
       ┌────────────────┐         ┌──────────────────────┐
       │ Alert Eval     │── reads │ PostgreSQL alerts +  │
       │ Service        │────────▶│ alert_eval_state     │
       └────────────────┘         └──────────────────────┘
                │
                │ Kafka alerts.fired (idempotency key per event)
                ▼
       ┌────────────────┐         ┌────────────────────┐
       │ Notification   │── SETNX │ Redis dedup        │
       │ Service        │────────▶│ dedup:{alert}:{seq}│
       └────────────────┘         └────────────────────┘
                │
                ├──▶ FCM / APNs (push)
                ├──▶ SES (email)
                └──▶ Twilio (SMS)


   Price Read Service ──▶ Redis ──▶ Cassandra (cold) ──▶ S3 (very cold)
```

**How to say it:** Draw it left-to-right in the order the data flows: feeds → ingest → ticks topic → aggregator → aggregates topic → evaluator → fired topic → notifier → channels. Talk while you draw. The Read Service is a side branch — draw it last and say "this is the user-facing read path; everything above the line is the streaming write path."

---

## 6. Deep Dives & Trade-offs (12–15 min)

> Pick 4. Go in this order — each builds on the previous one.

### Deep Dive 1 — Hot-key skew in tick ingestion (sharding strategy)

1. **What & where:** Kafka `ticks.raw` topic, partition strategy. Naively partitioning by `hash(ticker)` gives even partition count but wildly uneven load — one partition gets AAPL (300K ticks/sec), another gets a random small cap (5 ticks/sec). The hot partition becomes the bottleneck for the entire downstream Aggregation Service.
2. **Interactions:** Ingestion Service is the producer; Aggregation Service consumer group reads partitions. Whichever Aggregation pod owns the AAPL partition saturates CPU while peers idle.
3. **Why critical:** Without fixing this, no amount of horizontal scaling helps — adding more Kafka partitions doesn't help if they're empty. This is the single biggest source of operational pain in market-data systems.
4. **Other approaches & trade-offs:**
   - **(a) Per-ticker partition:** simple, but you can't have more consumers than partitions, so AAPL stays bottlenecked on one consumer.
   - **(b) Hash(ticker) into N partitions:** uniform partition *count*, non-uniform partition *load* — same hot-partition problem.
   - **(c) Sub-key splitting:** for a known set of hot tickers, partition by `(ticker, hash(seq) % K)` where K depends on the ticker's volume tier. AAPL gets K=16 sub-partitions; small caps get K=1. Aggregation Service merges sub-partition state for hot tickers in a final reduce step.
   - **(d) Two-tier pipeline:** all ticks → first Kafka topic partitioned by ticker → per-hot-ticker dedicated topic with K partitions → second-stage Aggregation. More moving parts but cleanly isolates hot tickers.
5. **Recommendation:** **(c) sub-key splitting with a dynamic hot-list.** Maintain a `hot_tickers` config (refreshed every 5 min from a moving-average of tick rate per ticker) — for tickers in that list, partition by `(ticker, seq % K_ticker)`. Aggregation Service reduces sub-partition aggregates per minute boundary. This handles the 80/20 cleanly: 50 hot tickers get fan-out, 9950 cold tickers stay simple. The dynamic refresh prevents ossification when, e.g., a small cap pops on earnings news.

**How to say it:** "Partition by ticker is the obvious answer and it's wrong. Here's why..." Then walk through (a) → (b) → (c). Drawing the partition bar chart with one giant AAPL bar and 9999 tiny ones is worth 30 seconds of explanation.

### Deep Dive 2 — Sliding-window aggregate computation

1. **What & where:** Aggregation Service maintains rolling OHLCV per (ticker, window_size). Lives between Kafka `ticks.raw` and Redis/Cassandra/Kafka-`aggregates.updated`.
2. **Interactions:** Reads ticks from Kafka, updates in-memory state, periodically flushes (a) to Redis as the in-flight bucket and (b) to Cassandra + Kafka `aggregates.updated` when a bucket finalizes.
3. **Why critical:** This is what lets us evaluate alerts on **aggregates** instead of **raw ticks**. Without it, alert evaluation is 10⁹+ QPS (broken). With it, evaluation runs once per window-update boundary — drops alert-eval QPS by 4 orders of magnitude.
4. **Other approaches & trade-offs:**
   - **(a) Recompute window on every read** (no precomputation): trivial code, impossible at scale — 1M alerts each scanning 1h of ticks per evaluation = catastrophic.
   - **(b) Tumbling windows only:** simple state machine (one bucket at a time per window-size), but the "1h ago" reference for percent-move alerts is *aligned to clock*, not "exactly 1h ago from now" — accuracy suffers.
   - **(c) True sliding windows:** maintain a deque of ticks per window; evict from head, append at tail. Accurate but state size grows with tick rate × window — for AAPL × 1h that's tens of millions of ticks in memory.
   - **(d) Sliding windows over pre-aggregated buckets:** maintain 1-minute tumbling buckets (small), and compute "last 1h" as the rollup of the most recent 60 buckets. State size = 60 floats per ticker per metric, regardless of tick rate.
5. **Recommendation:** **(d) hierarchical pre-aggregation.** Store 1m OHLCV as the ground truth. Build 5m / 1h / 1d / 1w on demand by rolling up 1m buckets. Memory is bounded, accuracy is "rounded to nearest minute" which is fine for percent-move alerts, and Cassandra storage is trivial (300 MB/day for finalized bars). For static-threshold alerts we evaluate on the raw tick stream — they don't need windows, they need "did price cross P this tick?" — so route those alerts through a separate, smaller code path that reads raw ticks directly from `ticks.raw`.

**How to say it:** This is where you draw two boxes on the board: "PERCENT alerts → aggregate path" and "STATIC alerts → raw-tick path". The split is cheap and the senior insight is recognizing that the two alert types have fundamentally different latency/accuracy trade-offs.

### Deep Dive 3 — Exactly-once notification (dedup architecture)

1. **What & where:** Notification Service, consuming Kafka `alerts.fired`. Kafka is at-least-once by default; on consumer rebalance or retry, the same `alerts.fired` event will be re-delivered. We must dispatch to FCM/SES/Twilio exactly once *as observed by the user*.
2. **Interactions:** Notification Service reads event → checks Redis `SETNX dedup:{alert_id}:{event_seq} <ts> EX <cooldown_seconds + 60>` → if SETNX succeeds, dispatch; if fails, it's a duplicate and we drop it.
3. **Why critical:** Duplicate notifications are the single most common reason users uninstall a stock alert app. Worse, our own retry logic (Kafka consumer rebalance, channel-adapter timeout retry) is the largest source of duplicates — we have to design this in, not bolt it on.
4. **Other approaches & trade-offs:**
   - **(a) DB unique constraint on (alert_id, event_seq):** correct but adds a synchronous PSQL write to the dispatch hot path. At 500K notifications/day this is fine, but at peak burst (a market crash where every alert fires within a minute) we'd hit DB saturation.
   - **(b) Kafka exactly-once semantics (transactional producer + read-committed consumer):** works between Kafka topics, but doesn't extend across the boundary into FCM/SES — once we call `fcm.send()`, the side effect is uncontrolled.
   - **(c) Redis SETNX with TTL:** atomic, sub-ms, decoupled from PSQL. Trade-off: Redis is not durable in the same way — a Redis failover could lose recent dedup keys and cause duplicates during the failover window. Mitigation: AOF persistence + replicas with sync replication.
   - **(d) Idempotency keys in the channel adapter** (FCM supports `collapse_key`, Twilio supports per-message dedup): provider-dependent, inconsistent across channels.
5. **Recommendation:** **(c) Redis SETNX as the primary dedup, with PSQL `notifications` table as the durable audit log.** SETNX gates the dispatch; after successful dispatch we async-write the `notifications` row for compliance/audit. This gives us the latency of Redis (~1ms) with durable audit (~async, lossless via Kafka if Redis is unavailable). The `event_seq` itself is generated deterministically — `event_seq = hash(alert_id, aggregate_bucket_start)` — so even if the Alert Evaluation Service produces the same event twice (e.g., on its own consumer retry), they collide on the same dedup key.

**How to say it:** Walk the failure cases: "What if Notification Service crashes after dispatch but before commit? Kafka redelivers, Redis SETNX fails (key exists), we drop. What if Redis fails over and loses the key? FCM's own collapse_key is a second line of defense for push, and we accept that SMS may have a tiny duplicate window — call out the limit honestly." Honesty about the failure modes is a strong senior signal.

### Deep Dive 4 — Alert evaluation fan-out at scale

1. **What & where:** Alert Evaluation Service, consuming Kafka `aggregates.updated`. For each aggregate update on (ticker, window), it must find all matching alerts and evaluate.
2. **Interactions:** Reads PostgreSQL for alerts on a given ticker, evaluates each, produces `alerts.fired` on match.
3. **Why critical:** AAPL has tens of thousands of alerts. When AAPL's 1-minute bucket finalizes, we need to evaluate all of them within the latency budget. Hitting PSQL per evaluation is impossible (50K reads/sec per ticker × 50 hot tickers = 2.5M PSQL reads/sec, breaks PSQL).
4. **Other approaches & trade-offs:**
   - **(a) Per-evaluation PSQL lookup:** correct, simplest, breaks at scale.
   - **(b) In-memory alert index per ticker** in Alert Evaluation Service, hydrated on startup, kept fresh via Postgres logical-replication / Debezium: fast, but consistency lag matters (a brand-new alert might miss its first evaluation if replication lag > 1 sec).
   - **(c) Materialized alert cache in Redis** keyed by ticker: `HASH alerts:by_ticker:{ticker}` → list of alert IDs and rules. Refreshed on write via the Alert Management Service's transactional outbox.
   - **(d) Sharded Alert Evaluation Service** where each pod owns a subset of tickers: aligns with the Kafka partition assignment of `aggregates.updated` (consumer-group co-partitioning), so the in-memory alert cache only needs to hold the alerts for *its* tickers. Memory bounded.
5. **Recommendation:** **(d) co-partitioned shards with in-memory alert cache, kept fresh via transactional outbox + Kafka.** When a user creates/edits/deletes an alert, the Alert Management Service writes the alert to PSQL and an outbox event to Kafka `alerts.changed` in the same DB transaction. Each Alert Evaluation Service pod consumes `alerts.changed` filtered to its owned tickers and updates its in-memory map. Steady-state evaluation is pure in-memory — no DB hit, no Redis hit, microseconds per alert. The trade-off is a few hundred milliseconds of staleness for new alerts (the outbox → Kafka → eval-service hop), which is acceptable since alerts don't need to fire on the *first* tick after creation — they need to fire reliably forever after.

**How to say it:** "I want my alert evaluation to be a tight in-memory loop, not a database query. The outbox pattern is how I keep the in-memory state correct without losing the durability of PSQL." That sentence is the deep dive in one line — say it after walking through the trade-offs, as the punchline.

---

## 7. Wrap-Up (2–3 min)

### Summary of the design (2 points)

- The architecture is a **streaming pipeline** with three stages: ingest → aggregate → evaluate-and-notify. Each stage is decoupled by Kafka, so any single stage can scale, fail, or be redeployed independently. The key shift from a naive design is moving alert evaluation **off the raw-tick path** and **onto the aggregate-update path**, which drops the evaluation rate by ~10⁴x.
- The user-facing read path (current price, history, WebSocket stream) is a separate, heavily-cached side branch fed by the same aggregation stage — readers never touch the write path, and the CDN absorbs the 80/20 of "everyone is staring at AAPL" load.

### Summary of trade-offs (2 points)

- We chose **Cassandra over TimescaleDB** for tick + aggregate storage on raw write throughput at our scale; the cost is more clunky analyst queries — we'd give analysts an Athena-on-S3 path on cold data instead.
- We chose **Redis SETNX for notification dedup** over a PSQL unique constraint for latency, accepting a tiny duplicate-window during Redis failover; we mitigate with AOF + sync replicas + provider-side idempotency where available.

### Summary of performance optimizations (2 points)

- **Hierarchical pre-aggregation** (1m base bars, rolled up on demand for 5m/1h/1d/1w) keeps Aggregation Service memory bounded regardless of tick rate, and makes percent-move evaluation a constant-time lookup.
- **Co-partitioned in-memory alert cache** in the Alert Evaluation Service, kept fresh by a transactional outbox, makes alert evaluation a pure in-memory operation — no DB or Redis hit on the hot path.

### Extensibility (2 points)

- **Adding new alert types** (volume-spike, multi-leg, options-implied-vol, news-correlated) drops in cleanly: add a new evaluator that subscribes to whichever stream it needs (`aggregates.updated`, a news Kafka topic, etc.) and produces to the same `alerts.fired` topic — the entire downstream notification pipeline doesn't change.
- **Adding new notification channels** (Slack, Discord webhook, brokerage auto-trade trigger) is a new consumer of `alerts.fired` with its own dedup namespace. Same exactly-once story applies.

### Follow-up questions to ask the interviewer (3–4)

1. **"Are alerts strictly user-defined, or do you also want algorithmically-suggested alerts (e.g., 'this stock is unusually volatile right now')?"** — The latter introduces an ML scoring stage upstream of alert evaluation, which is a different design.
2. **"What's the SLA on alerts firing during a major outage of the exchange feed itself? Do we replay from a backup feed, or do alerts simply not fire?"** — Multi-feed ingestion and feed-failover is a meaningful add-on.
3. **"Are we regulated for trading actions? If users can wire alerts to auto-trade, we cross into a stricter audit / replay / reconciliation regime."** — That changes Cassandra TTLs, S3 retention, and the durability bar on the entire pipeline.
4. **"Do we need the same architecture for crypto (24/7, no market close), and if so, how do you want me to revisit the trading-hours assumptions?"** — Crypto is a clean variant: drop the trading-hours filter, raise the cold-storage retention bar, otherwise same pipeline.

---

## Two sentences that earn the senior bar (memorize these)

1. *"The interesting number isn't the 1M ticks/sec — it's that AAPL alone has more alerts than a small cap has ticks per day, and that's why I move evaluation off the tick path and onto the aggregate-update path."*
2. *"Exactly-once delivery isn't a Kafka feature — it's a contract between Redis dedup, idempotent channel adapters, and a deterministic event_seq. I'll show you each link."*

Drop these into Deep Dive 1 and Deep Dive 3 respectively. They're the kind of sentences that make the interviewer write down "L5 signal."

---

## Cheat sheet — what to draw, in order

1. Box: feeds → Ingestion Service. Arrow → Kafka `ticks.raw`.
2. Two consumers off `ticks.raw`: Aggregation Service (write path) and WebSocket Service (read-push path).
3. Aggregation Service writes to Redis (in-flight) + Cassandra (finalized) + Kafka `aggregates.updated`.
4. Alert Evaluation Service consumes `aggregates.updated`, holds in-memory alert cache, produces `alerts.fired`.
5. Notification Service consumes `alerts.fired`, SETNX on Redis dedup, dispatches to FCM/SES/Twilio.
6. Side branch: Price Read Service → Redis → Cassandra → S3 (cold).
7. Annotate one partition of `ticks.raw` with "HOT — sub-key split for AAPL" to remind yourself to call out Deep Dive 1.

If you draw exactly this, you've shown the entire architecture. Everything else is words on top of these seven boxes.
