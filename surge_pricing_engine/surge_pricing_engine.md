# Design a Real-Time Surge Pricing Engine — Uber SDE-2 Bar Raiser

> **Prompt:** Ingest millions of driver GPS pings + rider requests/sec, compute supply-vs-demand per city zone in real time, emit a per-zone surge multiplier that refreshes every ~30 seconds.

> **Format:** rehearsal-ready interview script following the [7-phase answering framework](../answering_framework.txt). Read the **"What I'd actually say"** blocks out loud — they are the words to use. Diagrams are ASCII so they reproduce on any whiteboard / virtual canvas.

---

## How to open the interview (first 30 seconds)

> **What I'd actually say:**
> "Let me reframe this in my own words to make sure we're aligned. We're building the engine that sits between the driver-location stream and the pricing service — it ingests GPS pings and ride requests, computes supply vs. demand per geographic zone, and emits a multiplier (e.g. 1.4×) every ~30 seconds that the pricing service multiplies into the base fare quote. We're **not** designing the dispatch system, the pricing quote service, payments, or the ML-based forecasting model — just the streaming pipeline that produces the multiplier. Does that match what you have in mind, or should I adjust scope?"

This 30-second reframe is the single most important move in the interview. It separates you from candidates who panic-draw boxes.

---

# Phase 1 — Requirements (~5 min)

## Functional requirements (in scope)

| # | Requirement |
|---|---|
| F1 | Ingest driver GPS pings (lat, lng, status: `available`/`busy`/`offline`) at ~1 ping every 4 sec per active driver. |
| F2 | Ingest rider ride-request events (pickup lat/lng, timestamp) from the dispatch system's Kafka topic. |
| F3 | Map every event to a geographic **zone** (we'll use H3 hex resolution 8 — Uber's own library, ~0.74 km² cells). |
| F4 | Compute, per zone, **supply** = count of available drivers and **demand** = count of recent ride requests. |
| F5 | Emit a per-zone **surge multiplier** in `[1.0, 5.0]` that refreshes every 30 seconds. |
| F6 | Expose a low-latency read API: `GET /surge?lat&lng → {zone, multiplier}` for the pricing service. |
| F7 | Persist a historical archive of (zone, time, supply, demand, multiplier) for analytics, audits, and ML training. |

## Out of scope (state explicitly)

- The pricing-quote service itself (we just produce the multiplier).
- Ride dispatch / driver matching.
- Payments, receipts, driver payouts.
- ML forecasting of surge (deterministic computation only — predictive surge is v2).
- Rider/driver auth (handled upstream).

## Non-functional requirements

| NFR | Target | Why it matters |
|---|---|---|
| **Scale** | 5M concurrent drivers globally, ~1M GPS pings/sec average, ~2M peak. ~20M rides/day. | Sets ingestion architecture. |
| **Latency (read)** | p99 < 100ms for `GET /surge` (called on every fare quote). | Fare quote is on the user's critical path. |
| **Freshness** | Multiplier ≤ 30s stale. | Surge has to react to a sudden demand spike (concert ends, rain starts). |
| **Availability** | 99.99% on the **read** path (fare quotes can't fail). | Pricing outage = no rides = no revenue. |
| **Consistency** | **Eventual** is fine for multiplier reads. | A 15s delay on a multiplier change is acceptable; a fare-quote failure is not. |
| **Fail-safe** | If the engine is degraded, **default to 1.0×**, never higher. | Regulatory + reputational: Uber has been burned by surge-during-disasters PR. |
| **Cost** | Pipeline is one of the busiest at Uber — must be horizontally scalable. | Drives Kafka + Flink + Redis choice. |

> **Killer phrase to drop here:** "I'm explicitly choosing fail-safe-toward-1.0 over fail-safe-toward-current. If we lose state we'd rather under-charge for 30 seconds than over-charge anyone — that's a hard regulatory line at Uber after the 2017 NYC blizzard incident."

---

# Phase 2 — Back-of-Envelope Estimation (~3 min)

> Round aggressively, talk out loud, anchor every number to a later design decision.

## Driver GPS ingestion (the dominant workload)

```
Active drivers globally           ≈ 5,000,000
GPS ping interval                  ≈ 4 sec
Average pings/sec                  = 5M / 4   = 1.25M pings/sec
Peak pings/sec (rush + weekend)    ≈ 2× avg   ≈ 2.5M pings/sec
Bytes per ping (driver_id, lat, lng, ts, status, signature)
                                   ≈ 200 bytes
Ingestion bandwidth                ≈ 2.5M × 200B ≈ 500 MB/s ≈ 4 Gbps
```

**→ Design implication:** No single broker handles 2.5M pings/sec. We **must** partition Kafka and process with a parallel streaming engine (Flink). Synchronous DB writes are off the table.

## Ride requests

```
Rides per day globally              ≈ 20M
Average rides/sec                   = 20M / 86,400  ≈ 230 /sec
Peak (Friday 6pm, NYE)              ≈ 10× avg       ≈ 2,300 /sec
```

**→ Design implication:** Demand-side traffic is 1000× smaller than supply-side. We can process it cheaply.

## Zones

```
Resolution-8 H3 hexagons globally  ≈ 700M (mostly empty ocean / desert)
Active zones (with drivers OR riders in last 30s)
                                    ≈ 100,000
State per zone (multiplier, supply, demand, ts, smoothing fields)
                                    ≈ 100 bytes
Total hot state                     ≈ 100K × 100B = 10 MB
```

**→ Design implication:** The entire hot read state fits trivially in **a single Redis node's RAM** (we'll still shard for HA, but capacity is a non-issue).

## Read traffic on `GET /surge`

```
Fare quotes/sec (pricing service)   ≈ peak rides × ~5 quotes per ride attempt
                                    ≈ 2,300 × 5 ≈ 12K QPS peak
Bytes per response                  ≈ 100 bytes
Bandwidth                           ≈ negligible (~1 MB/s)
```

**→ Design implication:** Read path is laughably easy if we hit Redis. Only concern is availability.

## Historical archive

```
100K zones × 1 update / 30s × 50 bytes × 86,400s/day
≈ 100K × 2,880 × 50 = 14 GB / day
≈ 5 TB / year (compressed: ~1 TB)
```

**→ Design implication:** A single Cassandra cluster handles this easily; partition by zone_id, TTL old buckets at 90 days online + S3 cold tier.

## Numbers cheat sheet (memorize before walking in)

| Metric | Value |
|---|---|
| Peak GPS pings/sec | **2.5M** |
| Ingestion bandwidth | **500 MB/s** |
| Active zones | **100K** |
| Hot Redis state | **10 MB** |
| Read QPS (peak) | **12K** |
| Archive growth | **14 GB/day** |

---

# Phase 3 — API Design (~3 min)

> Define the contract at three boundaries: **ingest** (driver app → engine), **internal** (dispatch → engine), and **read** (pricing service → engine).

## 1. Driver location ingestion (gRPC, batched, internal-only)

```
POST /v1/location/batch          (gRPC stream, but shown as REST for clarity)
Headers: x-driver-id, x-auth-token
Body:
{
  "pings": [
    { "lat": 37.7749, "lng": -122.4194, "ts": 1735689600123, "status": "available" },
    ...                               // up to 10 pings batched per call
  ]
}
Response: 202 Accepted   (fire-and-forget, no body)
```

**Why batched:** at 1 ping every 4s, batching 10 pings every 40s reduces RPC overhead by 10× and saves driver-phone battery.

## 2. Ride request consumption (Kafka, not HTTP)

We don't expose an endpoint — the **dispatch service already publishes** `ride.request.created` events to Kafka. The surge engine just subscribes:

```
Topic: ride-requests
Schema:
{
  "request_id": "uuid",
  "rider_id": "uuid",
  "pickup_lat": 37.7749,
  "pickup_lng": -122.4194,
  "ts": 1735689600123,
  "estimated_distance_km": 4.2
}
```

> **What I'd actually say:** "I'd reuse the dispatch system's existing event stream rather than have riders double-publish. This keeps the surge engine a pure consumer — no coupling to the rider app's release cycle."

## 3. Get surge multiplier (read path — the hot one)

```
GET /v1/surge?lat=37.7749&lng=-122.4194
Response 200:
{
  "zone_id": "8a283082a677fff",         // H3 cell at resolution 8
  "multiplier": 1.4,
  "computed_at": 1735689600,
  "valid_until": 1735689630              // 30s window
}
```

## 4. Bulk lookup (for ETA / batch pricing)

```
POST /v1/surge/bulk
Body: { "locations": [{"lat":..,"lng":..}, ... up to 100] }
Response: [{ "zone_id":..,"multiplier":.. }, ...]
```

## 5. Things I'd mention but not design

- **Idempotency** on ingest is naturally fine — pings are dedup'd by `(driver_id, ts)`.
- **Rate limiting** on the read API at the gateway, not in the engine.
- **Auth** delegated to the API gateway (mTLS for internal callers).

---

# Phase 4 — High-Level Design (~10 min)

## Build it in 3 steps (don't draw the final picture yet)

### Step 1: Naive synchronous version (and why it dies)

```
 Driver app ──▶ HTTP server ──▶ Postgres (driver_locations table)
 Rider app  ──▶ HTTP server ──▶ Postgres (ride_requests table)
                              ▲
 Pricing svc ────  GET /surge ─┘  (does live aggregation query)
```

> **What I'd say:** "This is the dumbest design that could work. It dies immediately at our scale: 2.5M writes/sec to Postgres is impossible, and computing supply/demand on every read by querying the last 30s of GPS pings would be a full-table scan per request. We need (a) a streaming ingestion path and (b) a pre-computed read path."

### Step 2: Add Kafka + Flink for ingestion and aggregation

```
                                    ┌─ Flink: Supply Job ──┐
 Driver app ──▶ Ingest API ──▶ Kafka │   (window 30s,       │──┐
                                ├──▶│    key=zone_id)      │  │
                                │    └──────────────────────┘  │
                                │                              ▼
                                │                       (state: zone → supply)
                                │
                                │    ┌─ Flink: Demand Job ──┐
 Dispatch svc ───────────▶ Kafka │   │   (window 60s slide,  │──┐
                                └──▶│    key=zone_id)       │  │
                                     └──────────────────────┘  │
                                                               ▼
                                                        (state: zone → demand)
                                                               │
                                          ┌────────────────────┘
                                          ▼
                              ┌─ Flink: Multiplier Calc ──┐
                              │  (joins supply + demand,   │
                              │   applies formula, EMA)    │
                              └────────────────────────────┘
                                          │
                                          ▼
                                  Kafka topic `surge.updates`
```

This handles ingestion and computation. But the pricing service still needs a fast read endpoint.

### Step 3: Add the read path (Redis) and the historical sink

```
                          surge.updates  ─┬─▶ Redis writer ──▶ Redis (hot reads)
                                          │
                                          └─▶ Cassandra writer ─▶ Cassandra archive
```

Now `GET /surge` is just a Redis `HGET surge:zone:<h3_id>`.

## Final architecture (this is what you draw)

```
┌─────────────────────────── EDGE ──────────────────────────┐
│   Driver app          Rider app          Pricing svc      │
│       │                   │                    ▲          │
└───────┼───────────────────┼────────────────────┼──────────┘
        │                   │                    │
        ▼                   ▼                    │
   ┌─────────┐         ┌─────────┐               │
   │ Ingest  │         │Dispatch │               │
   │ Gateway │         │ Service │               │
   └────┬────┘         └────┬────┘               │
        │ gRPC              │ Kafka producer     │
        ▼                   ▼                    │
┌──────────────────────────────────────┐         │
│             KAFKA CLUSTER            │         │
│  ┌─────────────────┐ ┌───────────┐   │         │
│  │ driver.locations│ │ride.reqs  │   │         │
│  │  256 partitions │ │ 64 parts  │   │         │
│  │  (key: H3 prefix│ │(key:H3pre)│   │         │
│  └────────┬────────┘ └─────┬─────┘   │         │
└───────────┼────────────────┼─────────┘         │
            │                │                   │
            ▼                ▼                   │
   ┌────────────────────────────────┐            │
   │      FLINK STREAMING JOB       │            │
   │ ┌──────────┐  ┌──────────────┐ │            │
   │ │ Supply   │  │   Demand     │ │            │
   │ │ keyed by │  │  keyed by    │ │            │
   │ │ zone_id  │  │  zone_id     │ │            │
   │ │ tumble   │  │  sliding     │ │            │
   │ │  30s     │  │  60s/30s     │ │            │
   │ └─────┬────┘  └───────┬──────┘ │            │
   │       └──┬──── join ──┘        │            │
   │          ▼                     │            │
   │   ┌─────────────┐              │            │
   │   │  Multiplier │              │            │
   │   │   compute   │              │            │
   │   │  + EMA + clamp              │            │
   │   └──────┬──────┘              │            │
   │          │                     │            │
   │   RocksDB state backend +      │            │
   │   S3 checkpoints every 10s     │            │
   └──────────┼─────────────────────┘            │
              │                                  │
              ▼                                  │
       Kafka `surge.updates`                     │
              │                                  │
   ┌──────────┴───────────┬────────────────┐     │
   ▼                      ▼                ▼     │
┌────────┐          ┌──────────┐     ┌──────────┐│
│ Redis  │          │Cassandra │     │ Metrics  ││
│ writer │          │ writer   │     │  / OLAP  ││
└───┬────┘          └────┬─────┘     └──────────┘│
    ▼                    ▼                       │
┌────────┐         ┌─────────────┐               │
│ Redis  │         │ Cassandra   │               │
│cluster │         │  archive    │               │
│(sharded│         │ (90d hot,   │               │
│ by zone│         │  S3 cold)   │               │
│ TTL 90s│         └─────────────┘               │
└───┬────┘                                       │
    │                                            │
    └─── Surge Read API (HTTP) ──────────────────┘
                  ▲
                  │ GET /surge?lat&lng
```

## Golden-path data flow (walk through it out loud)

### Path A — Driver ping arrives

1. Driver app batches 10 pings, hits Ingest Gateway over gRPC.
2. Gateway authenticates via mTLS, normalizes to canonical schema.
3. Gateway computes the **H3 prefix at resolution 5** (~250 km², used as Kafka partition key) and produces to topic `driver.locations`.
4. Flink Supply job consumes, repartitions internally to **resolution 8** (the actual zone), updates per-zone `available_drivers` count in keyed state.

### Path B — Rider requests a ride

1. Dispatch service emits `ride.request.created` to Kafka.
2. Flink Demand job consumes, maps pickup point → H3 res-8 zone, increments demand counter in a 60s sliding window.

### Path C — Multiplier emission (every 30s tumbling)

1. Flink Multiplier-Calc job triggers per zone every 30s.
2. Reads supply & demand from keyed state.
3. Computes raw ratio: `r = demand / max(supply, 1)`.
4. Applies smoothing: `m_new = α · clamp(r, 1.0, 5.0) + (1-α) · m_old`, α = 0.3.
5. Emits `{zone_id, multiplier, supply, demand, ts}` to `surge.updates`.

### Path D — Pricing service quotes a fare

1. Pricing service calls `GET /surge?lat&lng`.
2. Surge Read API computes H3 res-8 of the point, does `HGET surge:zone:<h3>` from Redis.
3. **Cache miss or Redis down → return `{multiplier: 1.0}`.** Fail-safe, never overcharge.
4. Returns within 5–10ms p99.

### Path E — Archive

1. Separate consumer group on `surge.updates` writes to Cassandra `(zone_id, bucket_ts) → (mult, supply, demand)` for analytics, audit, ML training.

---

# Phase 5 — Database Design (~6 min)

> The interviewer cares more about *why each store* than about column-level schema. Lead with a per-store choice table.

## Per-store choice rationale

| Store | Tech | Why this one |
|---|---|---|
| Event bus | **Kafka** | High-throughput, partitioned, replicated, replayable. Industry default for this volume. |
| Stream state | **Flink + RocksDB** | Native exactly-once, keyed state, large state via local SSD, S3 checkpoints. |
| Hot read | **Redis (Cluster)** | Sub-ms reads, 10MB hot state fits trivially, sharded by zone for HA. |
| Historical | **Cassandra** | High write throughput, time-series natural fit, partition by zone, TTL old data. |
| Cold archive | **S3 (Parquet)** | Cheap, queryable via Presto/Athena for ML training. |

## Schemas

### Redis (hot read path)

```
Key:   surge:zone:<h3_resolution_8_index>
Value: HASH {
   multiplier:  1.4
   supply:      23
   demand:      45
   computed_at: 1735689600
}
TTL:   90 seconds   ← so a dead Flink job → keys expire → reads return 1.0 (fail-safe!)
```

The TTL is a **deliberate availability/safety choice** — it's not just hygiene. Call it out.

### Flink keyed state (RocksDB-backed)

```
Key: zone_id (H3 res-8 index)
Value: {
   supply_count, demand_count,
   smoothed_multiplier,
   last_emit_ts
}
```

### Cassandra (historical)

```sql
CREATE TABLE surge_history (
   zone_id      text,         -- partition key
   bucket_ts    timestamp,    -- clustering key, DESC
   multiplier   float,
   supply       int,
   demand       int,
   PRIMARY KEY (zone_id, bucket_ts)
) WITH CLUSTERING ORDER BY (bucket_ts DESC)
  AND default_time_to_live = 7776000;  -- 90 days
```

Partition by `zone_id`: most queries are "show me this zone over the last hour" (audit) or "show me all zones at this time" (which we serve from a separate `(bucket_ts, zone_id)` table — fan-out write).

## Sharding strategy

### Kafka `driver.locations` topic — the critical one

- **256 partitions** (room to scale Flink consumers; each Flink subtask reads ~10 partitions at peak).
- **Partition key = H3 resolution-5 prefix** (not driver_id, not res-8 zone).
  - Why not driver_id? Random distribution, but no geo-locality → every Flink task touches every zone's state → state gets scattered → bad cache locality.
  - Why not res-8? Too fine — 100K zones into 256 partitions is fine, but it makes hot-zone problem harder to mitigate.
  - **Res-5 prefix gives geo-locality** (same neighborhood goes to same partition) **with enough cardinality** (~50K res-5 cells) to balance load. Hot cells get sub-partitioned (deep dive).

### Redis Cluster

- Hash slots based on `zone_id` (H3 index has good entropy when hashed).
- Replicas: 1 replica per primary, AZ-aware placement.

### Cassandra

- Default token-based partitioning on `zone_id`. Hot zones are mitigated by the fact that *writes* are 1/30s per zone (not a hot key).

---

# Phase 6 — Deep Dives (~15 min)

> The interviewer will pick 2–3 of these. Be ready to go deep on any. Each deep dive follows the same structure: **problem → approaches → trade-offs → recommendation**.

## Deep Dive 1 — Hot zones / hot Kafka partitions (the #1 question)

### The problem
NYC Manhattan generates ~1000× more GPS pings than rural Iowa. If we partition by H3 res-5 prefix, the Manhattan partition becomes a hot partition: one Kafka broker melts, one Flink task lags, surge updates are stale exactly where they matter most.

### Approaches

**Approach 1: Just partition by driver_id**
- ✅ Perfectly even distribution.
- ❌ Lose geo-locality. Every Flink task touches every zone → state explodes (each task has to keep state for all 100K zones, not just its slice).
- ❌ Worse for downstream caching.

**Approach 2: Static sub-partitioning of known hot cells**
- Maintain a config map: "Manhattan res-5 cell → fan out to 16 sub-partitions via `(prefix, hash(driver_id) % 16)`".
- ✅ Hot cells get parallelism, cold cells stay localized.
- ❌ Config has to be maintained; new hot cells need ops intervention.

**Approach 3: Two-tier partitioning with virtual nodes**
- Each H3 res-5 cell maps to N virtual partitions (N=1 for cold, N=16 for hot).
- Hash `(prefix, driver_id) % N` selects the virtual partition; virtual partitions map to real Kafka partitions via consistent hashing.
- ✅ Adaptive.
- ❌ Complex; needs a config service that Flink also consults to know how to merge sub-partition state.

**Approach 4: Partition by zone, accept hot partitions, scale Kafka**
- Throw hardware at it.
- ✅ Simple.
- ❌ Wasteful — most partitions are at 1% utilization while one is at 100%.

### Trade-off summary

| Approach | Even load | Geo-locality | Ops complexity |
|---|---|---|---|
| 1 — by driver_id | ✅ | ❌ | Low |
| 2 — static sub-partition | ✅ | ✅ | Medium |
| 3 — virtual nodes | ✅✅ | ✅ | High |
| 4 — accept it | ❌ | ✅ | Low |

### Recommendation
**Approach 2 (static sub-partitioning of known hot cells).** Manhattan, SF Mission, downtown LA are not changing — we know which res-5 cells are hot. Bake them into a config that's hot-reloaded by both producers and Flink. If a new hot cell emerges, it shows up in a Grafana panel and ops adds it. Approach 3 is what you'd build if you had a year; Approach 2 is what ships.

## Deep Dive 2 — The multiplier formula (the fairness deep dive)

### The naive formula
```
multiplier = clamp( demand / supply, 1.0, 5.0 )
```

### Why naive breaks
- **Flapping:** demand=10, supply=8 → 1.25×. Next window demand=8, supply=10 → 1.0×. Riders see prices oscillate every 30s.
- **Tiny denominators:** supply=1, demand=2 → 2.0× even though that's just statistical noise.
- **Sudden cliffs:** zone sees one ride, jumps from 1.0× to 2.5× in one window.

### The real formula (what Uber actually does, roughly)

```
1.  raw_ratio   = demand_60s_window / max(supply_30s_window, MIN_SAMPLES)
2.  clamped     = clamp(raw_ratio, 1.0, 5.0)
3.  smoothed    = α · clamped + (1 - α) · prev_multiplier        # EMA, α = 0.3
4.  hysteresis: if |smoothed - prev| < 0.1, keep prev             # don't churn
5.  step quantize: round to nearest 0.1                           # 1.4× not 1.4327×
6.  if supply + demand < MIN_SAMPLES (=20), force 1.0×            # not enough data
```

### Trade-offs

| Knob | Effect of increasing | Effect of decreasing |
|---|---|---|
| `α` (EMA weight) | More responsive, more flapping | Smoother, but lags real spikes |
| Window size | Less noise, more lag | More noise, more responsive |
| Step quantization | Less driver/rider confusion | Less granular |
| MIN_SAMPLES | Skip more sparse zones | More noise from small zones |

### Recommendation
α = 0.3, 30s tumble for supply, 60s sliding/30s slide for demand, MIN_SAMPLES = 20, hysteresis = 0.1, quantize to 0.1. Run shadow A/B on these knobs offline against historical data.

> **Killer phrase:** "The smoothing isn't a nice-to-have, it's the difference between a price that feels reactive and a price that feels arbitrary. Arbitrary prices erode trust, and trust is the actual moat."

## Deep Dive 3 — Out-of-order events (event-time processing)

### The problem
A driver in a tunnel buffers 5 pings, then sends them when signal returns at t+90s. Naively appending these to the current window inflates supply for "now" using stale data.

### Approach: Flink event-time + watermarks

- Use the GPS ping's `ts` as event time (not arrival time).
- Watermark = `max(seen_ts) - 10s` (allow 10s of lateness).
- Late events past watermark → either drop or send to a side output for monitoring.
- Window assignment uses event time, so the buffered pings get assigned to their original 30s bucket which is already closed. They go to the late-output and update the historical record but **don't pollute the live multiplier** (which is the right behavior — we don't want to use 2-minute-old data in the live signal).

### Trade-off
- **Lower watermark lateness (e.g., 1s):** more accurate, more late events dropped.
- **Higher watermark lateness (e.g., 30s):** fewer drops but every emission lags by that amount → multiplier is 30s+ stale by definition.
- **Recommendation:** 10s lateness allowance, monitor late-event rate as a SLO (<0.1%).

## Deep Dive 4 — Failure handling (what happens when stuff breaks)

| Failure mode | Detection | Mitigation |
|---|---|---|
| **Flink job crashes** | Heartbeat to control plane | Restart from S3 checkpoint (every 10s), max 30s of recompute lag. State preserved. |
| **Kafka partition unavailable** | Producer retry exhaustion | Producers buffer locally + exponential backoff. Acceptable: ingestion lag, not loss (RF=3). |
| **Redis primary down** | Health check | Failover to replica (Redis Sentinel/Cluster auto). 1–2s blip during which reads → fall through to default 1.0×. |
| **Redis writer crashes** | Lag metric on `surge.updates` consumer group | New keys stop appearing. Existing keys expire at 90s TTL → reads start returning 1.0×. **This is the fail-safe by design.** |
| **Pricing service can't reach surge API** | Local circuit breaker | Pricing service caches last-known multiplier for 30s, then defaults to 1.0×. |
| **Schema change in `ride-requests` upstream** | Schema registry | Backward-compatible Avro/Protobuf required; Flink job continues with default for new fields. |
| **Bad code deploy emits crazy multipliers** | Alert: "any zone > 5.0×" | Auto-rollback. Cap is enforced both in Flink AND in Surge Read API (defense in depth). |

> **Killer phrase:** "The 90-second Redis TTL is doing double duty: it's a memory hygiene mechanism *and* a circuit breaker. If anything upstream stops emitting, the system silently degrades to 1.0× — which is exactly the safe default."

## Deep Dive 5 — H3 zoning and sparse zones

### Why H3 over alternatives

| Indexing | Even neighbors? | Hierarchical? | Used at Uber? |
|---|---|---|---|
| Geohash (rectangles) | ❌ (corners) | ✅ | Legacy |
| S2 (Google) | ⚠️ (12 corners on a sphere) | ✅ | Some teams |
| **H3 (hexagons, Uber's OSS)** | ✅ | ✅ | ✅ Native |

Hexagons have a single distance to all neighbors — important for "find nearby drivers" semantics that the dispatch system also uses, so we share the same primitive.

### Multi-resolution fallback for sparse zones

A res-8 cell with 2 GPS pings and 1 ride request in 30s is statistical noise. Approaches:

**Approach 1: Force 1.0× when below MIN_SAMPLES.**
- ✅ Safe.
- ❌ Misses real surge in low-density areas (small towns).

**Approach 2: Aggregate up to parent H3 cell when sparse.**
- If res-8 cell has < 20 samples, use res-7 (its parent, ~5× area).
- If res-7 also sparse, use res-6.
- ✅ Always uses a meaningful denominator.
- ❌ More complex serving logic — read API has to do up to 3 lookups.

**Recommendation:** Approach 2, but cache the resolved cell-id-at-which-this-point-has-surge alongside the multiplier so the read path is one Redis lookup (the writer figures out the right res once and writes a sentinel key at res-8 pointing to the parent's value).

## Deep Dive 6 — Cold start, fairness, and regulatory caps (bonus)

- **Cold start (newly active zone):** default 1.0× until enough samples. We're conservative on the upside.
- **Hard cap at 5.0×** enforced in two places (Flink + read API). Some jurisdictions (NYC after Sandy) cap surge during declared emergencies — this is a config flag the engine reads from a central feature service.
- **Anti-gaming:** drivers can't "go offline" en masse to fake supply collapse — we monitor sudden mass-status changes and dampen multiplier responsiveness during anomalies. (Mention but don't design.)

---

# Phase 7 — Wrap-Up (~3 min)

## Summary (30 seconds, hit these beats)

> "To recap: drivers and dispatch produce GPS pings and ride requests into Kafka, partitioned by H3 prefix to keep zone data co-located. Flink consumes both streams and maintains keyed state per zone, computing a smoothed multiplier every 30 seconds with EMA + hysteresis. Multipliers are pushed to Redis for sub-millisecond reads by the pricing service, with a 90-second TTL that acts as a circuit breaker — if the pipeline stalls, reads silently degrade to 1.0×, never higher. Cassandra holds the historical archive for analytics and ML training."

## Bottlenecks I'd watch

| Bottleneck | Symptom | Mitigation |
|---|---|---|
| Hot Kafka partition (Manhattan) | Consumer lag on one partition | Sub-partitioning config |
| Flink state size | RocksDB compaction stalls | Increase parallelism, prune zones with no activity |
| Redis memory under city growth | Memory pressure | Re-shard, scale up node sizes |
| Read API at peak fare-quote storm | p99 spikes | Add edge cache (5s TTL is acceptable) |

## Future improvements (what v2 looks like)

1. **Predictive surge:** ML model that forecasts demand 5 min ahead (concert ending, rain incoming) so multiplier reacts *before* shortage hits. Train on the Cassandra archive.
2. **Cross-zone smoothing:** if zone A is 2.0× and adjacent B is 1.0×, taper at the boundary to avoid drivers hovering on the line.
3. **Fairness constraints:** prevent surge in low-income areas during commute hours (regulatory pressure in some markets).
4. **Per-product multipliers:** UberX vs Black vs Pool can have different surge curves.
5. **Driver incentives, not rider surcharge:** in some markets, regulators allow paying drivers more without raising rider price — same engine, different output target.

## Curveball cheat sheet

| If they ask… | Say this |
|---|---|
| "How would you handle 10× the traffic?" | Kafka partitions are the linchpin — scale to 2048; Flink scales horizontally on key; Redis re-shard. The bottleneck would shift to ingest gateway, which is stateless, so just add nodes. |
| "What if the entire region goes down?" | Surge engine is per-region by design (drivers in NYC don't surge prices in Tokyo). Cross-region failover means routing rider/driver traffic to next-nearest region; surge starts cold there but defaults to 1.0× — fail-safe. |
| "How would you migrate from a synchronous monolith?" | Dual-write phase: monolith keeps computing surge, new pipeline computes in parallel, compare outputs offline for 2 weeks. Switch reads gradually behind a feature flag, per-city. |
| "Can you explain consistency?" | Eventual consistency on reads (90s TTL bounds staleness). Strong consistency on the **write order** within a zone (Flink keyed state guarantees per-key serialization). The system is **AP** — we choose availability over freshness because a stale 1.4× is fine but a 500 error is not. |
| "What about cold caches at startup?" | Flink replays from Kafka (last 5 min retained). Within ~60s of startup, all live zones have multipliers. Until then, reads default to 1.0× — fail-safe. |
| "How do you test this?" | Shadow pipeline in staging consuming production Kafka stream; offline replay against historical Cassandra; canary deploys to 1% of cities first; per-zone alerting on anomalous multiplier swings. |

---

# Cheat Sheet (memorize the night before)

## Numbers to walk in with
- **2.5M GPS pings/sec** peak ingestion
- **500 MB/s** ingestion bandwidth
- **100K active zones** globally
- **10 MB hot Redis state** (the punchline: it fits anywhere)
- **30s** multiplier refresh, **60s** demand window, **90s** Redis TTL
- **12K read QPS** at peak fare-quote storm
- **14 GB/day** archive growth

## Killer phrases to drop
- "I'm explicitly choosing fail-safe-toward-1.0× over fail-safe-toward-current — never overcharge on partial failure."
- "The 90-second Redis TTL is doing double duty: memory hygiene and circuit breaker."
- "Partition by H3 res-5, not driver_id, because we want geo-locality of state, not just even load."
- "EMA smoothing isn't a nice-to-have — flapping prices erode trust, and trust is the actual moat."
- "Sub-partition the known hot cells (Manhattan, SF Mission) statically, don't try to be clever with adaptive virtual nodes on day one."

## Tech-name-drops that work at Uber
- **H3** (Uber's own OSS for hex grids — using it shows you've read their engineering blog)
- **Flink** keyed state + RocksDB + S3 checkpoints
- **Kafka** partition key = H3 prefix
- **Cassandra** with `(zone_id, bucket_ts DESC)` clustering

## Time budget (45-min interview)
| Phase | Target end-time |
|---|---|
| 1. Requirements | 5 min |
| 2. Estimation | 8 min |
| 3. API | 11 min |
| 4. HLD | 21 min |
| 5. DB | 27 min |
| 6. Deep dives | 42 min |
| 7. Wrap-up | 45 min |

## Common mistakes to avoid (from the framework, applied here)
1. **Don't jump in.** Spend the full 5 min on requirements — surge is a fairness-loaded problem and you need to scope what they actually want.
2. **Don't over-engineer.** No need to mention service mesh, k8s operators, CQRS — none of it matters for this question.
3. **Don't under-engineer.** A single Postgres + sync writes is the wrong answer at 2.5M pings/sec.
4. **Don't forget trade-offs.** Every approach in every deep dive should have explicit pros/cons.
5. **Don't go silent.** Narrate every decision. "I'm choosing Flink over Spark Streaming because Flink's exactly-once on keyed state is mature."
6. **Don't get stuck on details.** PostgreSQL vs MySQL doesn't matter; H3 vs S2 does (because Uber).
7. **Don't draw nothing.** The 3-step incremental architecture is your best visual.
8. **Don't forget non-functionals.** Especially fail-safe-toward-1.0× — that's the regulatory/fairness signal that lands at Uber specifically.

---

> **Final mindset:** This is a streaming-systems question dressed up as a pricing question. Anchor every decision in the numbers from Phase 2, name Uber's own tech (H3, Flink, Kafka), and treat fairness/safety as first-class requirements — not afterthoughts. That's what gets a "strong hire" at the bar-raiser.
