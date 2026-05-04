# Uber Driver Heatmap — HLD Interview Walkthrough (SDE2 / Bar-Raiser)
**Link:** https://claude.ai/chat/8646a987-8989-4c9d-b348-b91bdd18a5a3

> **Problem:** Build a heatmap of Uber drivers in a city for an internal analytics dashboard.
>
> Two views:
> 1.  **Near real-time** — heatmap of the city for the **past 20 minutes**.
> 2.  **Historical** — same heatmap **24 hours after the fact**, **bucketized by the hour**.
>
> **Input stream:** ~**500,000 TPS** of driver location pings.
>
> Source: LeetCode Uber L5A interview experience / Hello Interview canonical.

This document is structured as the **seven-phase answering framework** from algomaster.io. Each section has:
- What I'd actually **say out loud** in the room (in quotes / "You:" blocks).
- The **diagrams** I'd draw on the whiteboard.
- The **trade-offs** and **why** I'd make each call.
- **Uber-flavor:** H3, Kafka, Flink, Pinot, M3 — these are the tools Uber actually runs in production, and naming them signals you've read their eng blog. (Don't fake it; only mention what you can defend.)

Total time budget: **60 min** (typical Uber bar-raiser).

| Phase | Duration | Cumulative |
|---|---|---|
| 1. Requirements | 5–7 min | 7 |
| 2. Estimation | 3–5 min | 12 |
| 3. API Design | 3–5 min | 17 |
| 4. High-Level Design | 8–10 min | 27 |
| 5. Data / Storage Design | 5–7 min | 34 |
| 6. Deep Dives | 18–20 min | 54 |
| 7. Wrap-Up | 4–6 min | 60 |

---

## Phase 1 — Requirements Clarification (5–7 min)

> **Goal:** Don't start drawing until you know *what* "heatmap" means here. The whole architecture pivots on three questions: what's a "driver count" (distinct drivers vs pings?), what's the cell granularity (H3 res?), and how fresh is "real-time" (sub-30s vs sub-5min?).

### What I'd say

> "Before I design anything, let me pin down scope. I want to make sure I'm building the right thing.
>
> **On functionality:**
> 1. Just to confirm — this is a **read-only internal analytics dashboard**, right? Analysts viewing a map of where drivers are, not a system that *acts* on the data (no dispatch, no surge pricing). Correct?
> 2. By 'driver heatmap' I'm picturing: the city is broken into geographic cells (I'd use **H3 hexagons**, since Uber open-sourced H3 for exactly this), and each cell shows a **count of drivers** colored by density. Is that the mental model?
> 3. **Distinct drivers per cell**, not raw pings, right? A driver pings every ~4 seconds, so over 20 minutes one driver would emit ~300 pings — we don't want that double-counted.
> 4. Any filtering dimensions analysts will want? **Driver status** (online / idle / en-route / on-trip)? Vehicle type (UberX / UberBlack)?
> 5. Out-of-scope check: I'm assuming **rider demand**, **surge map**, **ETA**, and **forecasted heatmaps** are all out of scope. Just current/past driver supply. Yes?
>
> **On scale and SLAs:**
> 6. Input is **500K TPS** of pings — confirmed. How many cities? Globally, Uber has ~10K cities, but a single dashboard view is one city. Should the system serve all cities concurrently, or one-city-at-a-time per query?
> 7. Concurrent dashboard users? My guess: **low hundreds** of internal analysts, polling every 30s.
> 8. **'Near real-time'** — what's acceptable end-to-end staleness from a driver's ping to it being visible on the dashboard? I'll target **< 30 seconds**.
> 9. **24-hour historical view** — can the data show up *24 hours later*, or do I need it to be queryable at any t+24h? I'm assuming the latter, with **hour granularity**.
> 10. **Retention** for historical hourly data — analysts will want to look back how far? 90 days? A year?
> 11. **Consistency** — eventual consistency is fine, right? Off by a few drivers in a cell is acceptable; missing whole minutes is not.
> 12. **Availability** — ingestion is the SLA-critical path (we cannot drop pings). The dashboard itself can tolerate a few minutes of read unavailability. Agreed?"

### Likely answers from the interviewer (assumed defaults)

| Question | Assumed answer |
|---|---|
| Scope | Read-only dashboard; H3 cells; distinct driver count |
| Filters | At minimum, by **driver status** |
| Out-of-scope | Surge, demand-side, forecasting, dispatch |
| Cities | Multi-city, one city per view |
| Staleness (real-time) | **< 30 seconds** end-to-end |
| Historical | Queryable at any time after t+24h, hourly buckets |
| Retention | **90 days** hot, 1 year cold |
| Consistency | Eventual; approximate distinct counts OK |
| Availability | Ingest **99.99%**, dashboard 99.9% |

### Requirements summary I'd write on the board

```
FUNCTIONAL
  F1. Real-time heatmap: distinct drivers / H3 cell, last 20 min sliding
  F2. Historical heatmap: distinct drivers / H3 cell / hour-bucket, 90d retention
  F3. Filter by driver status (online | idle | on_trip)
  F4. Per-city query (one city per dashboard view)

NON-FUNCTIONAL
  NF1. Ingest 500K TPS of location pings, no drops (99.99% durable)
  NF2. End-to-end freshness < 30s (real-time view)
  NF3. Dashboard p99 query < 1s
  NF4. Eventual consistency / approximate counts acceptable
  NF5. Cost-sensitive: 90d hot + 1y cold, tiered storage
```

### Common mistakes I'm avoiding

- ❌ Designing a "track every driver" system — that's a dispatch problem, not a heatmap.
- ❌ Treating raw pings as the unit — it's **distinct drivers per cell**.
- ❌ Skipping the filter question — `status` matters because the aggregate has to be sliced multiple ways.

---

## Phase 2 — Back-of-the-Envelope Estimation (3–5 min)

> **Goal:** Translate "500K TPS" into the four numbers that will actually drive design: ingest bandwidth, raw storage, hot aggregate size, query QPS.

### What I'd say

> "Let me size this so my architecture decisions have grounding."

### Numbers

```
INGEST
  TPS                        = 500,000 pings/s
  Ping size on the wire     ≈ 100 bytes  (driver_id 16B + lat/lng 16B
                                         + timestamp 8B + status 4B
                                         + protobuf overhead)
  Ingest bandwidth          = 500K * 100B = 50 MB/s  ≈ 400 Mb/s

PER-DAY VOLUME
  Pings/day                 = 500K * 86,400 ≈ 4.3 * 10^10  (~43 billion)
  Raw bytes/day             = 50 MB/s * 86,400 ≈ 4.3 TB/day
  Per-year raw              ≈ 1.5 PB  →  must tier to S3 / GCS

DRIVERS (the actual unit we care about)
  Active drivers globally   ≈ 5 million peak
  Per city peak (e.g. NYC)  ≈ 50–100K
  Driver ping cadence       ≈ 1 ping / 4s  →  500K TPS / (1/4) = 2M concurrent drivers ✓ sanity
  Distinct drivers in 20min sliding window per cell — small,
    bounded by drivers in city. ≪ raw ping count.

GEO-CELLS
  H3 resolution 8           ≈ 0.74 km² hexagons
  Cells per major city      ≈ 5K–20K
  Cells globally (10K cities) ≈ 10M

HOT AGGREGATE (real-time, Redis)
  Bucket: 1-min tumble (we'll sum 20 of them at query time)
  Active cells / city / min ≈ a few thousand (most cells empty)
  Per minute, globally       ≈ 10M cells × few-byte counter = ~80 MB
  20-min hot window          ≈ 1.5 GB → fits comfortably in Redis

WARM AGGREGATE (Pinot / Druid)
  Hourly rows = (city, h3_cell, hour, status) → distinct_count
  Active hourly rows / day   ≈ 24 * 10M cells * 4 statuses ≈ 1B rows/day
    (sparse: most cells empty, real ≈ 100M/day)
  90 days @ 100M/day, ~50B/row ≈ 450 GB hot in Pinot — easy.

READS
  Concurrent analysts        ≈ 200
  Refresh                    ≈ every 30s
  Query QPS                  ≈ 200 / 30 ≈ 7 QPS  ← read traffic is trivial
```

### What these numbers tell me

| Number | Implication for the design |
|---|---|
| 500K TPS ingest | Need a partitioned log (**Kafka, ~200 partitions**); single DB is impossible |
| Distinct-drivers, not pings | Stream processor must dedupe → **Flink with keyed state** |
| 1.5 GB hot aggregate | **Redis** fits the real-time path easily |
| 7 read QPS | Serving layer is *not* the bottleneck — don't over-engineer it |
| 1.5 PB / year raw | Raw events must go to **S3 (Parquet)**; only aggregates in OLAP |
| 30s freshness SLA | Stream pipeline; batch alone won't meet it |

> "So the read side is trivially small — 7 QPS. The whole problem is the **write/aggregate path**. That's what I'm going to spend most of the design on."

---

## Phase 3 — API Design (3–5 min)

> **Goal:** Two ingestion contracts and two query contracts. Keep it minimal.

### Ingestion (internal, from existing Driver Location Service)

We don't take HTTP from drivers directly — Uber already has a Driver Location Service. We **subscribe to its Kafka topic**.

```
Kafka topic:  driver.location.pings.v1   (200 partitions, RF=3)

ProtoBuf schema:
  message DriverPing {
    string  driver_id   = 1;   // UUID
    double  lat         = 2;
    double  lng         = 3;
    int64   ts_ms       = 4;   // epoch ms, device clock
    enum    Status      = 5;   // ONLINE | IDLE | EN_ROUTE | ON_TRIP | OFFLINE
    string  city_id     = 6;   // resolved by upstream service
    string  vehicle_type= 7;
  }
```

### Query API (dashboard → backend)

```
GET /v1/heatmap/realtime
  ?city_id=sfo
  &status=online,idle              // optional filter
  &resolution=8                    // H3 res
  &bbox=37.7,-122.5,37.8,-122.4    // optional viewport clipping

200 OK
{
  "city_id":     "sfo",
  "as_of":       "2026-04-29T18:42:30Z",
  "window":      "20m",
  "resolution":  8,
  "cells": [
    {"h3": "8a2a1072b59ffff", "driver_count": 142},
    {"h3": "8a2a1072b5bffff", "driver_count":  87},
    ...
  ]
}
```

```
GET /v1/heatmap/historical
  ?city_id=sfo
  &start=2026-04-28T00:00:00Z
  &end=2026-04-28T23:59:59Z
  &granularity=hour
  &status=online
  &resolution=8

200 OK
{
  "city_id":     "sfo",
  "granularity": "hour",
  "series": [
    {
      "hour": "2026-04-28T00:00:00Z",
      "cells": [{"h3":"…","driver_count":89}, ...]
    },
    ...
  ]
}
```

### Decisions to call out (briefly)

- **Cursor pagination** if a city has > 50K cells in result; otherwise single-shot.
- **Rate limit** at the API gateway — internal but still abusable by a runaway dashboard.
- **No `POST` from clients** — write path is internal Kafka only.
- **Bounding-box** filter so the dashboard only fetches the visible viewport, not the whole city.
- **`as_of`** in the response so the UI can warn analysts if the speed layer is lagging.

---

## Phase 4 — High-Level Design (8–10 min)

> **Goal:** Draw the architecture incrementally. This problem is the textbook **Lambda Architecture**: one stream path for fresh data, one batch path for accurate, durable historicals. State that explicitly.

### What I'd say

> "Two SLAs, two paths. Real-time wants < 30s freshness — that's a streaming job. Historical wants accurate, replayable hourly aggregates over 90 days — that's a batch job over a durable raw event store. This is exactly the shape Lambda Architecture is for, and Uber's analytics platform (M3 / Pinot / Flink) is built around it."

### Step 1 — the simplest thing that could possibly work

```
[Driver app] → [Location Service] → [Kafka] → [Aggregator] → [DB] → [Dashboard API] → [UI]
```

This breaks at 500K TPS in the aggregator and the DB. So we evolve.

### Step 2 — split into two paths (Lambda)

```
                                        ┌────────────────────────────┐
                                        │  SPEED LAYER (real-time)   │
                                        │  Flink stream job          │
                                        │  → 1-min tumbling windows  │
                                        │  → Redis hot store         │
                                        └────────────┬───────────────┘
                                                     │
[Driver Loc Svc] ──▶ Kafka ──▶ branch ──┤
                     (driver.                        │
                      location.                      │
                      pings)                         ▼
                                        ┌────────────────────────────┐
                                        │  BATCH LAYER (historical)  │
                                        │  Kafka Connect → S3 Parquet│
                                        │  → Spark hourly job        │
                                        │  → Pinot OLAP              │
                                        └────────────────────────────┘
```

### Step 3 — full architecture with serving layer

```
                              ┌──────────────────┐
                              │  Driver mobile   │
                              │  app (already    │
                              │  exists)         │
                              └────────┬─────────┘
                                       │ ping every ~4s
                                       ▼
                              ┌──────────────────┐
                              │ Driver Location  │   (existing Uber service —
                              │ Service (gRPC)   │    we're a downstream consumer)
                              └────────┬─────────┘
                                       │ produce
                                       ▼
                       ┌─────────────────────────────────┐
                       │           KAFKA                 │
                       │  topic: driver.location.pings   │
                       │  200 partitions, RF=3, 7d retn  │
                       │  partition key = driver_id      │
                       └────┬───────────────────────┬────┘
                            │ consume                │ tee (Kafka Connect)
                            ▼                        ▼
       ┌───────────────────────────────┐    ┌─────────────────────────┐
       │       SPEED LAYER             │    │     BATCH LAYER         │
       │   Apache Flink (AthenaX)      │    │                         │
       │                               │    │  Kafka Connect S3 Sink  │
       │  1. parse ping                │    │  → S3 Parquet           │
       │  2. compute h3_cell @ res 8   │    │    /year=YYYY/          │
       │  3. keyBy(driver_id)          │    │    /month=MM/           │
       │  4. last-cell state per       │    │    /day=DD/             │
       │     driver (TTL 60s)          │    │    /hour=HH/            │
       │  5. emit (cell, status, +1/-1)│    │    /city=...            │
       │     on transitions            │    │                         │
       │  6. keyBy(city, cell, status) │    │      ┌─────────────┐    │
       │  7. tumbling 1-min count      │    │      │ Spark job   │    │
       │     using HLL for distinct    │    │      │ (hourly,    │    │
       │                               │    │      │  on +24h    │    │
       │              │ sink            │    │      │  schedule)  │    │
       └──────────────┼────────────────┘    │      └──────┬──────┘    │
                      ▼                     │             ▼           │
              ┌──────────────┐              │      ┌─────────────┐    │
              │    REDIS     │              │      │   PINOT     │    │
              │  (cluster)   │              │      │  (OLAP)     │    │
              │              │              │      │ partitioned │    │
              │ rt:{city}:   │              │      │ by day      │    │
              │   {minute}   │              │      └──────┬──────┘    │
              │   → hash     │              └─────────────┼───────────┘
              │     {h3}→cnt │                            │
              │ TTL 25m      │                            │
              └──────┬───────┘                            │
                     │                                    │
                     └────────────┬───────────────────────┘
                                  ▼
                       ┌────────────────────┐
                       │  Heatmap API svc   │   (Go, stateless,
                       │  - /realtime  →    │    behind ALB)
                       │      query Redis   │
                       │  - /historical →   │
                       │      query Pinot   │
                       │  + per-query cache │
                       │    (60s TTL)       │
                       └─────────┬──────────┘
                                 ▼
                       ┌────────────────────┐
                       │  Internal          │
                       │  Dashboard         │
                       │  (React +          │
                       │   Deck.gl HexLayer)│
                       └────────────────────┘
```

### Walk-through: a driver ping's journey

> "Let me trace one ping through the system end-to-end."

1. Driver app pings Driver Location Service.
2. DLS produces to `driver.location.pings`, partitioned by `driver_id` (so a driver's events are in order on a single partition).
3. **Speed path:** Flink consumes, computes the H3 cell, looks up the driver's previous cell from keyed state, and emits a `(cell→ -1, new_cell→ +1)` transition only if the cell changed. A 1-min tumbling window aggregates these transitions per `(city, cell, status)` and writes to Redis under `rt:{city}:{minute_bucket}` as a hash `{h3 → count}` with a 25-min TTL.
4. **Batch path:** Kafka Connect dumps raw pings to S3 as hourly Parquet files. A Spark job runs at `hour + 30min` (after the hour closes and any straggler pings settle), reads that hour's partition, computes per `(city, cell, status, hour)` distinct driver count, and upserts into Pinot.
5. **Real-time read:** Dashboard hits `/realtime?city=sfo`. API service reads the last 20 one-minute hashes from Redis, sums per cell, returns. p99 < 200ms.
6. **Historical read:** Dashboard hits `/historical?city=sfo&start=...&end=...`. API service issues a Pinot SQL query, returns hourly series. p99 < 1s for typical queries.

### Why each component is here (anchored to the numbers)

| Component | Justification (point back to estimation) |
|---|---|
| **Kafka, 200 partitions** | 500K TPS / 200 = 2.5K TPS/partition — well under broker limits |
| **Flink** (not Kafka Streams) | Need exactly-once with RocksDB state for keyed `last_cell` per driver; Flink's checkpointing model is the proven choice at Uber |
| **Redis hot store** | 1.5 GB hot footprint; sub-ms reads; 7 read QPS is laughable for Redis |
| **S3 + Parquet for raw** | 1.5 PB/year — only object storage is economical; columnar + hive partitioning for batch scans |
| **Pinot for hourly OLAP** | Column store, sub-second slice/dice on (city, hour, h3, status); Uber's standard for real-time analytics |
| **Stateless API tier** | 7 QPS — one or two instances behind ALB is enough; horizontal scale-out is trivial if needed |
| **CDN** for the dashboard JS bundle, not for data (responses are per-user, low cache hit) |

### Things I'd explicitly **not** include and would defend

- ❌ **No GraphQL** — overkill for two endpoints.
- ❌ **No WebSocket push** — 30s polling is fine for 200 analysts; WebSocket adds infra without changing the user experience.
- ❌ **No Cassandra for raw pings** — we don't query raw pings; S3 + Parquet is cheaper and serves the batch job.
- ❌ **No separate ML / feature store** — out of scope per Phase 1.

---

## Phase 5 — Data / Storage Design (5–7 min)

> **Goal:** Justify each store, then specify schemas focused on the keys that drive query and write patterns.

### Store-by-store

| Store | Purpose | Why this choice |
|---|---|---|
| **Kafka** | Durable ingest log + replay source | Battle-tested at Uber's scale; 7-day retention covers any batch reprocessing |
| **Redis Cluster** | Real-time aggregate hot store, last 20 min | In-memory, sub-ms reads, native hash type, TTL semantics |
| **S3 (Parquet)** | Raw event archive + batch source | Cheap, queryable from Spark/Trino, Hive-partitioned |
| **Apache Pinot** | Historical hourly OLAP | Sub-second analytical slice/dice; Uber's house OLAP engine |
| **Postgres** (small) | Metadata: cities, H3 cell→neighborhood, user/auth | Tiny, relational, 1 master + replica |

### Schemas

**Kafka topic `driver.location.pings`** — see Phase 3 (protobuf).

**Redis hot store**

```
KEY:    rt:{city_id}:{status}:{minute_epoch}
TYPE:   HASH       field = h3_index   value = distinct_driver_count
TTL:    1500s (25 minutes — covers the 20-min query window with margin)

Example:
  HSET rt:sfo:online:28394820  8a2a1072b59ffff  142
  HSET rt:sfo:online:28394820  8a2a1072b5bffff   87

Real-time query for city=sfo, status=online, last 20 min:
  for m in (now_minute - 19 .. now_minute):
    HGETALL rt:sfo:online:{m}
  merge by h3 and sum counts → response
```

**S3 raw event lake**

```
s3://uber-driver-location/raw/
  year=2026/month=04/day=29/hour=18/city=sfo/part-00000.parquet

Schema (Parquet):
  driver_id      string
  lat            double
  lng            double
  ts_ms          long
  status         string
  city_id        string
  vehicle_type   string
  h3_res8        string    -- precomputed in Flink before writing
```

**Pinot historical aggregate table**

```
Table:  driver_heatmap_hourly
  hour_epoch      LONG       (timestamp column, hour granularity)
  city_id         STRING     (dimension, replica grouping)
  h3_cell         STRING     (dimension)
  status          STRING     (dimension)
  vehicle_type    STRING     (dimension)
  distinct_drivers INT        (metric — produced by HLL.estimate or COUNT(DISTINCT))
  total_pings     LONG       (metric — sanity / sampling check)

Partitioning:  by day_of_year
Retention:     90 days hot in Pinot, 1 year in S3 cold archive
Replica grouping: city_id  (so a city's data co-locates on a node)
```

### Sharding / partitioning strategy

| Layer | Key | Rationale |
|---|---|---|
| Kafka | `driver_id` hash | Keeps a driver's events ordered → Flink's keyed `last_cell` state is correct |
| Flink | `(city_id, h3_cell, status)` after dedup | Aggregates land on one task per cell — hot cells concentrate but counts are tiny |
| Redis | `{city_id}` hash-tag | Co-locates a city's minute keys on one Redis slot → multi-key fetch in one round trip |
| Pinot | `city_id` replica group + `day` partition | Per-city queries hit one replica group; time-range queries prune by day |

### Why I'm using **distinct count, not raw counts**, and how

> "A driver pings every ~4 seconds. Over 20 minutes that's ~300 pings. We don't want a driver who's stationary in one cell to count as 300; the analyst wants 'how many drivers'."

Two options I'd present:

| Approach | How it works | Trade-off |
|---|---|---|
| **A. Stateful 'last cell' (exact)** | Flink keyBy `driver_id`, keep `(last_cell, expiry)`. On each ping, if cell changed, emit `(old_cell, -1) (new_cell, +1)`. Aggregate signed deltas per cell per minute. | Exact count. State size = active drivers (~5M × ~64B = 320MB) → fits in RocksDB. **My pick.** |
| **B. HyperLogLog per cell-minute** | KeyBy `(cell, minute)`, add `driver_id` to HLL sketch. Estimate cardinality on read. | Approximate (~1% error), but state-free per driver. Useful as a sanity backup. |

I'd go with **A for the speed layer** because the exact answer fits, and use **HLL in Pinot** at the hourly granularity for the batch layer (cheaper to maintain, and analysts care less about exact counts in 24h-old data).

---

## Phase 6 — Deep Dives (18–20 min)

> **Goal:** This is where staff-level evaluation happens. Pick 3–4 topics; for each: state the problem, present 2–3 approaches, trade-offs, recommendation. The interviewer will pick more.

I expect a bar-raiser to drill on:

1. **Stateful distinct-driver counting in the Flink job** (correctness)
2. **Sliding 20-min real-time window without recomputing on every read** (latency / cost)
3. **Late and out-of-order pings** (correctness)
4. **Hot cells / hot partitions** (skew)
5. **Failure recovery — speed layer crash, Kafka outage, Pinot down** (availability)
6. **Cost & retention tiering** (sr-eng instinct)

I'll prepare all six. In the interview I'll lead with the first two and let them steer.

---

### Deep Dive 1 — Distinct drivers per cell, in stream, exactly

**Problem.** A driver emits ~300 pings per 20 minutes. If we just `COUNT(*)` we 300x-overcount. We need distinct drivers per `(city, cell, status, minute)`.

**Approaches.**

**(a) Naive — `COUNT(DISTINCT driver_id)` in window**
- Group by `(city, cell, status, minute)`, set semantics for `driver_id` per group.
- State = number of distinct drivers per group per minute.
- Globally: ~10M cell-minute groups × small set ≈ tens of GB of Flink state. Workable but heavy.

**(b) HyperLogLog sketches per group**
- HLL sketch with ~1% error: ~16KB per group at standard precision; smaller variants exist.
- State drops 100x. Mergeable across windows (sum 20 minute-sketches).
- Trade-off: approximate, can't enumerate drivers if asked.

**(c) Stateful "last cell per driver" + transition deltas (my pick)**
- KeyBy `driver_id`. Flink keeps `(last_cell, last_status, expiry)` in RocksDB-backed state with TTL = 60s.
- On each ping: lookup state. If `(cell, status)` unchanged → drop the event (idempotent). If changed → emit `(old_cell, old_status, -1)` and `(new_cell, new_status, +1)`. If state expired → emit `(new_cell, +1)` (treat as appearance).
- Second stage keyBy `(city, cell, status)`, **sum the signed deltas in 1-min tumbling windows**, sink the running count.
- State: O(active drivers) ≈ 5M × ~80B ≈ 400MB across the cluster. Easy.

**Trade-offs.**

| | Naive set | HLL | Transition deltas |
|---|---|---|---|
| Accuracy | exact | ~1% error | exact |
| State size | tens of GB | hundreds of MB | hundreds of MB |
| Read-time aggregation | trivial sum | merge sketches | trivial sum |
| Re-key complexity | low | low | **two stages, more code** |

**Recommendation.** Transition deltas. Exact, state fits in cluster, reads are simple sums. The two-stage Flink job is well-trodden Uber territory.

```
            Stage 1 (keyBy driver_id)            Stage 2 (keyBy city,cell,status)
     ─────────────────────────────────       ──────────────────────────────────
ping ──▶ │ state: last_cell, ttl=60s │  ──▶  │ 1-min tumble: sum(+/-1) │ ──▶ Redis
         │ emit (cell,±1) on change  │       │ output: (city,cell,status,minute,N) │
         └───────────────────────────┘       └─────────────────────────────────────┘
```

---

### Deep Dive 2 — Serving the 20-min sliding window cheaply

**Problem.** "Past 20 minutes" is a sliding window. Naive: maintain a 20-min sliding window in Flink and emit a full city snapshot every second. At 10K cells × 200 cities × 1Hz = 2M writes/s to Redis. Too much.

**Approaches.**

**(a) Sliding window in Flink, push snapshots**
- Flink emits the full 20-min state on every slide (e.g., every 30s).
- High write amplification on Redis; tight coupling between Flink slide and dashboard refresh.

**(b) Tumbling 1-min windows + read-time aggregation (my pick)**
- Flink writes one row per `(city, cell, status, minute)` per minute. ~10–50K writes/min globally.
- Real-time API reads the last 20 minutes' hashes from Redis on demand and sums them.
- Reads are 7 QPS; 20 HGETALLs each is fine. Use Redis pipelines / mget on hash-tagged keys to make it one round-trip per query.

**(c) Pre-aggregated 20-min sliding sketches**
- Maintain a single key per `(city, cell, status)` that's a "rolling 20-min count" updated on each minute boundary by adding the new minute and subtracting the expiring one.
- Looks elegant; requires a strict ordering guarantee on +/- operations and adds a bookkeeping job.

**Trade-offs.**

| | Push slides | Tumble + read-aggregate | Rolling counter |
|---|---|---|---|
| Write load on Redis | high | **low** | medium |
| Read latency | 1 GET | 20 HGETALLs (~few ms) | 1 GET |
| Memory | snapshot per slide | 20 minute-keys/cell | 1 key/cell |
| Operational simplicity | medium | **high** | low (state machine) |

**Recommendation.** **(b) Tumble + read-aggregate.** With 7 read QPS we're absolutely not read-bound; we should optimize for write simplicity and correctness. Each minute key is independently TTL'd, so expired data evicts itself.

```
Redis layout (for sfo, online):
  rt:sfo:online:28394801  →  HASH {h3 → count}     ← minute t-19
  rt:sfo:online:28394802  →  HASH {h3 → count}     ← minute t-18
  ...
  rt:sfo:online:28394820  →  HASH {h3 → count}     ← minute t (most recent)

Query path:
  pipeline:
    HGETALL rt:sfo:online:28394801
    HGETALL rt:sfo:online:28394802
    ... (20 keys, all on the same hash slot via {sfo} tag)
  → merge in API layer, sum per h3
```

---

### Deep Dive 3 — Late, out-of-order, and dropped pings

**Problem.** Mobile networks are flaky. A ping with `ts_ms` from 90s ago can arrive now. Worse, batches of pings can come in clumps when a phone reconnects.

**Approaches.**

- **Watermarks in Flink** with bounded lateness, e.g., 30s. Events later than the watermark go to a **side-output stream** logged to S3 for offline reprocessing.
- For the real-time view, lateness > 30s is **dropped from the real-time path** but still ends up in the batch path (S3) and therefore in the historical view. Eventual consistency.
- For the batch path, the hourly Spark job runs at `hour + 30min` to give stragglers time to land. Pings older than 24h are simply ignored (out of SLA scope).

**What I'd commit to in the interview.**

> "I'd configure Flink with event-time processing and a 30-second bounded-lateness watermark. Events later than 30s go to a side output and S3 — they never appear in the real-time view, but they do appear in the historical view because the batch layer reprocesses the raw S3 events. This means real-time and historical can briefly disagree for very-late drivers, which I'd document in the dashboard tooltip."

---

### Deep Dive 4 — Hot cells / partition skew

**Problem.** Times Square in Manhattan or downtown SFO at 5pm: one H3 cell holds 5K drivers. The Stage-2 Flink keyBy `(city, cell, status)` puts all 5K transitions on one task slot.

But: the *throughput* into one cell is bounded by *driver transitions*, not pings. A driver crosses cell boundaries at most a few times per minute. 5K drivers × 2 transitions/min = 10K events/min/cell. That's **167 events/sec** at the absolute hottest cell. Not a hot partition.

The actual risk:

- **Stage-1 hot key (driver_id):** uniform by hash. Not a problem.
- **Skewed cities:** US-EAST has more drivers than rural areas. Solved by giving Flink autoscaling on subtasks per source partition.
- **Holiday spikes (NYE):** doubled traffic. Pre-warm Flink and Kafka; alarms on consumer lag.

> "Hot keys aren't a real problem here because the bottleneck is *transitions* not pings. A driver crossing a cell boundary is rare relative to their ping rate."

---

### Deep Dive 5 — Failure scenarios

| What fails | Blast radius | Mitigation |
|---|---|---|
| **A Kafka broker** | One partition leader migrates | RF=3, min.insync.replicas=2; producers see <100ms blip |
| **Whole Kafka cluster** | Ingest pipeline halts | DLS already has its own buffer; we'd lose freshness, never durability. Multi-region active-passive Kafka for DR |
| **Flink job crash** | Speed layer freezes (real-time view stales) | Checkpointing every 60s to S3; restart resumes from checkpoint. Dashboard surfaces the lag via `as_of` timestamp |
| **Redis primary down** | Real-time view 503 | Sentinel + replica failover (~10s); dashboard auto-retries; historical path unaffected |
| **Pinot down** | Historical view 503 | Real-time path unaffected (different store entirely). Pinot read replicas help |
| **Spark hourly job failure** | Specific hour missing in historical | Job is idempotent on `(hour, city)` key; rerun. Alerting on job lag |
| **S3 outage** | Batch ingestion stalls; speed layer fine | Kafka holds 7 days; replay when S3 returns |

The **invariants** I'd state:

1. **Kafka is the source of truth.** All downstreams (Redis, Pinot, S3) are derived. Any of them can be rebuilt by replay.
2. **Real-time and historical paths fail independently.** No single failure breaks both.
3. **Drops are visible.** `as_of` lag and the speed-layer's own metrics surface staleness to the dashboard.

---

### Deep Dive 6 — Cost & tiering (interviewer-bait at Uber)

> "At 1.5 PB/year of raw events, cost dominates engineering. I'd tier explicitly."

| Tier | Store | Data | Retention | Cost shape |
|---|---|---|---|---|
| **Hot** | Redis | 1-min aggregates per `(city, cell, status)` | 25 min | RAM, ~few GB, cheap |
| **Warm** | Pinot | 1-hour aggregates | 90 days | SSD, ~hundreds of GB |
| **Cold (queryable)** | S3 + Trino/Athena | Hourly aggregates | 1 year | Object storage, cheap |
| **Frozen** | S3 Glacier | Raw pings (Parquet) | 3 years | <$1/TB-month |
| **Drop** | — | Raw pings older than 3y | — | — |

Two more cost levers I'd mention:

- **Coarsen H3 resolution for older data.** Last 7 days at res 8 (0.7 km²); older data downsampled to res 7 (5 km²) — analysts rarely zoom in on year-old data, and the row count drops 7×.
- **Sample low-density cells.** Cells with < 5 drivers/hour can be merged into a "rural" bucket per region; saves Pinot rows without affecting visualizations (those cells are barely visible anyway).

---

## Phase 7 — Wrap-Up (4–6 min)

> "Let me summarize what we've built, what I'd worry about, and what I'd do next."

### Summary

> "Lambda architecture. Driver Location Service produces to a 200-partition Kafka topic. A Flink job dedupes pings into cell transitions per driver, then aggregates into 1-minute tumbling windows per `(city, h3_cell, status)`, written to Redis with a 25-minute TTL. The dashboard's real-time view reads the last 20 minute-keys and sums them — sub-second, < 30s freshness, supports the 20-minute view.
>
> In parallel, raw pings are tee'd to S3 as hourly Parquet. A Spark job 30 minutes after each hour aggregates into Pinot, giving us the historical hourly view, queryable for 90 days hot, 1 year cold.
>
> Stateless Go API service serves both paths behind an internal load balancer. Internal-only, ~7 read QPS, trivial to scale."

### Key bottlenecks I'd flag

| Bottleneck | When it bites | Watch-fors |
|---|---|---|
| **Flink state size growth** | If active driver count grows 10× | Alarm on RocksDB size per task |
| **Kafka consumer lag** during NYE / Super Bowl | Spike traffic | Pre-warm, alert on lag > 60s |
| **Pinot ingestion throughput** | If we add fine-grained dimensions | Monitor segment merge time |
| **Redis memory if cells multiply** | H3 res bumped or new statuses | Aggregate by city × min × status footprint |
| **S3 small-file problem** | Many small Parquet files | Compaction job nightly |

### What I'd build next, in priority order

1. **Demand-side overlay** — heatmap of rider requests, then driver-supply / rider-demand differential. Same architecture, different topic.
2. **Anomaly detection on the stream** — alert ops when a city's online-driver count drops 30% in 5 min.
3. **Forecasted heatmap** — short-horizon prediction (15 min ahead) using the same hourly aggregates as features.
4. **Multi-region active-active** — currently one Flink + one Pinot cluster per region; cross-region failover via Kafka MirrorMaker.
5. **Driver privacy** — even internally, raw `driver_id` shouldn't leak to analytics dashboards. Hash with rotating salts; expose only counts.

### "What if 10×" curveball — prepared answers

| "What if traffic 10× to 5M TPS?" | Kafka partitions to 2000; Flink autoscale; Redis cluster reshard; Pinot more nodes. Architecture doesn't change, only knobs. |
| "What if a region goes down?" | Kafka MirrorMaker active-active; route DNS to surviving region; degraded freshness for ~minutes |
| "How would you migrate from a current system?" | Dual-write: produce the new Kafka topic alongside the old store. Backfill historicals from existing logs into Pinot. Cutover dashboard with feature flag |
| "Strong consistency?" | Out of scope per requirements; would require synchronous writes and kill 500K TPS. Push back on the requirement |

---

## Common mistakes I'm consciously avoiding

1. **Treating the read side as hard.** It's 7 QPS. The whole problem is the write/aggregate path.
2. **Picking Cassandra "because Uber".** Cassandra doesn't help here — we don't need wide-row queries on raw pings.
3. **One big OLAP table for everything.** Real-time and historical have different SLAs and different storage; mashing them into Pinot only would either blow cost (storing minute-level) or miss the freshness SLA.
4. **Forgetting to dedupe driver pings.** This is *the* correctness trap of the problem. State it early.
5. **No `as_of` timestamp on the API.** Without it, analysts can't tell when the speed layer is lagging.
6. **Designing surge / dispatch.** Out of scope; resist scope creep.
7. **Skipping Lambda explanation.** Naming the pattern signals the candidate has seen this shape before — it's expected at staff/SDE2 bar-raiser.

---

## Cheat sheet — what to draw on the whiteboard, in order

```
0:00  Requirements box (functional + non-functional bullet list)
0:07  Estimation table (TPS / storage / hot-set / read QPS)
0:12  API endpoints (just the two: /realtime, /historical)
0:17  Architecture diagram, build it in 3 steps:
        v1: client → server → DB
        v2: split speed / batch
        v3: full diagram with Kafka, Flink, Redis, S3, Spark, Pinot, API
0:27  Schemas: Kafka proto, Redis hash key, Pinot table
0:34  Deep dive 1: stateful distinct count (two-stage Flink diagram)
       Deep dive 2: tumbling vs sliding aggregation (Redis layout)
       Deep dive 3: lateness / watermarks
       (Whichever 2-3 the interviewer picks)
0:54  Wrap-up: bottlenecks list, what's next, what-if-10x answers
1:00  Done.
```

---

## Things to mention only if asked (don't volunteer)

- **CDC from Postgres for driver metadata** — only relevant if the interviewer asks how driver attributes (e.g., vehicle type) are joined.
- **Specific Flink operator parallelism numbers** — only if pressed.
- **Choice of HLL precision parameter** — only if pressed.
- **Exact Redis cluster topology** — only if asked about availability.
- **Auth / SSO on the dashboard** — only if asked about security.

---

## Final note: posture for the bar-raiser

The bar-raiser is a Staff engineer testing **judgment**, not knowledge of buzzwords. The signals they're scoring:

- Did the candidate **clarify requirements** before designing? (yes — 5–7 min in Phase 1)
- Did they **let numbers drive decisions**? ("7 read QPS" → don't over-engineer the API tier)
- Did they **name trade-offs explicitly**? (sliding vs tumbling, exact vs HLL, push vs pull)
- Did they **handle failure** as a first-class concern? (Phase 6.5 table)
- Did they **stay in scope**? (no surge, no dispatch, no ML)
- Could they **defend every component**? (mapping table in Phase 4)

When in doubt: **fewer components, clearer reasoning, explicit trade-offs**. That's what staff-level looks like.
