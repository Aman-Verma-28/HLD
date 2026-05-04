# Design Uber / Ride-Sharing — HLD Interview Walkthrough (Uber SDE2, Bar Raiser)

> A script-style answer following the [algomaster.io 7-phase framework](https://algomaster.io/learn/system-design-interviews/answering-framework). Read top-to-bottom, rehearse out loud. Time targets are for a 60-min round; collapse by ~25% for 45 min.

---

## Opening line (15 sec)

> "Cool, Design Uber. Before I start drawing, I'll spend ~5 min scoping the problem with you, ~3 min on capacity math, ~5 min on APIs, then build the architecture incrementally. I'll save the bulk of the time for deep dives — I expect we'll spend it on dispatch, geospatial indexing, location-update scaling, and the ride state machine, since those are where the interesting trade-offs live. Stop me anytime."

This sets a contract with the interviewer, signals familiarity with the framework, and pre-commits to the deep-dive areas you've actually prepared.

---

## Phase 1 — Requirements (5 min)

**Strategy: propose a scope, don't ask blind questions.** Showing you already know what Uber is wins points.

### Functional requirements (proposed)

> "Let me propose the core flow and you can refine. I want to focus on:
> 1. **Rider requests a ride** — pickup + dropoff, sees fare estimate + ETA
> 2. **System matches the rider with a nearby driver** — exclusive assignment
> 3. **Driver accepts or declines** — on decline/timeout, re-dispatch
> 4. **Live tracking** — both sides see each other's location during the trip
> 5. **Ride lifecycle** — `requested → matched → en_route_to_pickup → in_trip → completed | cancelled`
> 6. **Fare + surge pricing**
>
> I'll treat the following as **out of scope** unless you want them in: payments/payouts, ratings, in-app chat, Pool/shared rides, driver onboarding, and trip-history analytics. OK?"

### Non-functional requirements

| Property | Target | Why this matters |
|---|---|---|
| **Scale** | 100M MAU, 10M DAU riders, 5M drivers (~1M concurrent online), 15M rides/day | Drives sharding + capacity choices |
| **Latency** | Match < 2s p99; location update e2e < 1s; map view < 500ms | Riders abandon slow apps; safety |
| **Availability** | 99.99% | Outages = lost revenue + safety risk |
| **Consistency — ride state** | **Strong** | A driver assigned to two riders is catastrophic |
| **Consistency — driver location** | **Eventual / last-write-wins** | Ephemeral; staleness of 1–2s is fine |
| **Geography** | Globally distributed, but rides are local | City-level partitioning is natural |

### Confirm before moving on
> "The one thing I want to lock in: **strong consistency on dispatch is a hard constraint** — no double-dispatch ever. That's going to drive several of my design choices. Sound right?"

---

## Phase 2 — Back-of-envelope estimation (3 min)

> "Quick math to anchor the rest of the design. I'll round aggressively."

### Traffic
- **Rides**: 10M DAU × 1.5 rides/day = **15M rides/day** → 175 ride requests/sec avg → **~500/sec at peak** (commute spike).
- **Driver location pings**: 1M concurrent online drivers × 1 ping every 4s = **250K writes/sec**. *This is our dominant workload, not ride requests.*
- **Map view (rider sees nearby cars before requesting)**: 1M concurrent map-watchers × refresh every 3s = **~330K reads/sec**.
- **GPS breadcrumbs during trips**: 1M concurrent rides × 1 ping/4s = 250K/sec — same order as driver pings.

### Storage
- **Trips**: 15M/day × ~1 KB = 15 GB/day → ~5.5 TB/year. Manageable with sharded Postgres.
- **Live driver locations**: 1M × ~100 B = **100 MB hot data — fits in RAM**. This is the unlock.
- **GPS breadcrumbs**: 350 GB/day → archive to **S3** after trip completes. Don't keep in OLTP.

### What the numbers tell us
| Number | Design implication |
|---|---|
| 250K location writes/sec | **In-memory geospatial store, not Postgres**. Postgres would die. |
| 100 MB hot location data | Fits per-shard in RAM — no disk needed |
| 1M concurrent rides + strong consistency | **Single-writer-per-driver pattern** required |
| Rides are local | **Shard everything by city_id** |
| 5.5 TB/year for trips | Sharded RDBMS, not a NoSQL move |

> "So the headline: location updates dominate write QPS, ride state needs strong consistency, and rides are geographically local — those three observations drive the rest."

---

## Phase 3 — API Design (4 min)

```
# ───── Rider → Backend ─────────────────────────────────────────
POST   /v1/rides
  body : { rider_id, pickup{lat,lng}, dropoff{lat,lng}, ride_type }
  resp : { ride_id, state:"searching", fare_estimate, eta_sec }

GET    /v1/rides/{ride_id}
  resp : { ride_id, state, driver?, driver_loc?, eta_sec? }

POST   /v1/rides/{ride_id}/cancel
  headers: Idempotency-Key

# ───── Driver → Backend ────────────────────────────────────────
POST   /v1/drivers/location              ← high frequency, fire-and-forget
  body : { driver_id, lat, lng, heading, ts }

POST   /v1/rides/{ride_id}/accept        Idempotency-Key
POST   /v1/rides/{ride_id}/decline
POST   /v1/rides/{ride_id}/start         (rider boarded)
POST   /v1/rides/{ride_id}/complete      Idempotency-Key

# ───── Realtime push (server → client) ─────────────────────────
WSS    /v1/realtime
  events: ride_offered, ride_state_change, driver_location, surge_update
```

### Things to call out explicitly

- **Idempotency-Key** on every state-changing write (`accept`, `complete`, `cancel`). Driver networks are flaky; we cannot create duplicate dispatches or double charges.
- **WebSockets / MQTT, not polling**, for realtime. 1M concurrent clients polling = self-inflicted DDoS.
- **Cursor-based pagination** for trip history (when in scope).
- **Rate limiting** at the gateway, especially on `POST /drivers/location` so a buggy app build can't take us down.
- The `POST /drivers/location` endpoint is **fire-and-forget over a persistent connection** — we don't ack each ping, just process latest-wins.

---

## Phase 4 — High-Level Design (8 min)

**Strategy: build it in 3 steps so the interviewer follows your reasoning, then show the final picture.**

### Step 1 — naive starting point

```
[ Rider/Driver Apps ] → [ Monolith API ] → [ Postgres ]
```

> "This works for a hackathon. Two immediate problems: (1) 250K location writes/sec destroys Postgres, (2) 'find drivers within 2 km' is an expensive spatial query at scale."

### Step 2 — split the location problem out

```
                       ┌── Trip Service     → Postgres (trips)
[Apps] → [Gateway] → ──┤
                       └── Location Service → Redis Geo / in-mem (live positions)
```

> "Now the hot path doesn't touch the durable store."

### Step 3 — add the dispatch pipeline

> "Matching needs to be async and partitioned for back-pressure + double-dispatch prevention. Kafka per city, dispatch workers consume."

### Final architecture

```
                   ┌─────────────┐    ┌─────────────┐
                   │  Rider App  │    │ Driver App  │
                   └──────┬──────┘    └──────┬──────┘
                          │   HTTPS / WSS    │
                          ▼                  ▼
                  ┌──────────────────────────────────┐
                  │  API Gateway / Edge LB           │
                  │  (TLS, auth, rate limit)         │
                  └──────┬─────────────┬─────────────┘
                         │             │
              ┌──────────┴──┐       ┌──┴──────────┐
              │  WebSocket  │       │   REST      │
              │  Gateway    │       │   Routers   │
              └──────┬──────┘       └──┬──────────┘
                     │                 │
       ┌─────────────┼─────────────────┼──────────────┐
       ▼             ▼                 ▼              ▼
 ┌──────────┐  ┌────────────┐   ┌──────────────┐  ┌──────────┐
 │  Trip    │  │  Location  │   │  Matching /  │  │ Pricing /│
 │  Service │  │  Service   │   │   Dispatch   │  │  Surge   │
 │ (state   │  │  (H3 idx,  │   │   Service    │  │  Service │
 │  machine)│  │   sharded) │   │  (per-city)  │  │          │
 └─────┬────┘  └─────┬──────┘   └──────┬───────┘  └────┬─────┘
       │             │                  │                │
       │      ┌──────┴───────┐          │         ┌──────┴──────┐
       │      │ In-mem H3    │          │         │ Surge KV    │
       │      │ shards +     │          │         │ per H3 cell │
       │      │ Redis Geo    │          │         │ (Redis)     │
       │      └──────────────┘          │         └─────────────┘
       │                                │
       ▼                                │
 ┌────────────┐                         │
 │ Postgres   │  ◄────  Kafka  ─────────┘
 │ trips +    │  (ride.requested,
 │ users      │   ride.state_changed,
 │ sharded by │   driver.location)
 │ city_id    │      │
 └────────────┘      ▼
                ┌───────────┐
                │    S3     │  GPS breadcrumbs (post-trip)
                └───────────┘
```

### Walkthrough — "Rider posts a ride" (the golden path)

> Trace it on the diagram with your finger:

1. Rider `POST /rides` → Gateway → **Trip Service**
2. Trip Service writes `Trip(state=REQUESTED)` to **Postgres**, sharded by `city_id`
3. Trip Service calls **Pricing** for fare estimate, returns `ride_id` + estimate to rider
4. Trip Service publishes `ride.requested` to **Kafka**, partition key = `city_id`
5. **Dispatch worker** (one consumer group per city) picks up the event:
   a. Calls **Location Service** → "top-K drivers within R km of pickup"
   b. Picks the best candidate (closest, score-weighted)
   c. **Atomic claim** via Postgres CAS: `UPDATE drivers SET state='OFFERED' WHERE driver_id=? AND state='ONLINE'` — rowcount must be 1, else try next candidate
   d. Pushes offer to driver via WebSocket Gateway
6. Driver `POST /accept` (with Idempotency-Key) → Trip Service transitions `REQUESTED → MATCHED` via versioned conditional update
7. Both rider + driver are subscribed to the ride channel, get realtime updates over WebSocket

### Walkthrough — "Driver pings location" (the high-volume path)

1. Driver app holds a persistent WebSocket to a **Location Service shard** (sticky routed by `driver_id`)
2. Shard updates its **in-memory H3 cell index** in O(1)
3. Async: every Nth ping or on-cell-change → write through to **Redis Geo** for cross-shard reads
4. Async: produce to Kafka topic `driver-locations` → consumer batches breadcrumbs to **S3**
5. If driver is on a trip: Matching/Trip service forwards latest location to rider's WebSocket

### Where to acknowledge hand-waving

> "I'm hand-waving how we handle celebrity H3 cells (downtown SF), how dispatch handles driver decline timeouts, and the exact state machine. I'd love to deep-dive on those next."

---

## Phase 5 — Database Design (6 min)

### Per-store choice (the SQL-vs-NoSQL section)

| Data class | Store | Rationale |
|---|---|---|
| Users (rider + driver profiles) | **PostgreSQL** | Structured, ACID for identity, low volume |
| Trips | **PostgreSQL, sharded by city_id** | Need txns for state transitions; rides are local to a city |
| Driver live location | **In-memory H3 index + Redis Geo** | 250K writes/sec, ephemeral, last-write-wins |
| GPS breadcrumbs (post-trip) | **S3** | Write-once, read-rare, 350 GB/day |
| Surge multiplier per H3 cell | **Redis** | Updated every ~5 min, hot reads |
| Ride events / audit log | **Kafka → BigQuery** | Streaming, analytics |

### Trip schema (Postgres)

```sql
CREATE TABLE trips (
  trip_id        UUID PRIMARY KEY,
  city_id        INT  NOT NULL,                     -- shard key
  rider_id       UUID NOT NULL,
  driver_id      UUID,
  state          TEXT NOT NULL,  -- requested|matched|en_route|in_trip|completed|cancelled
  state_version  INT  NOT NULL,                     -- optimistic locking
  pickup         GEOGRAPHY(POINT),
  dropoff        GEOGRAPHY(POINT),
  fare_estimate  NUMERIC,
  fare_final     NUMERIC,
  surge_mult     NUMERIC,
  created_at     TIMESTAMPTZ DEFAULT now(),
  updated_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON trips (rider_id,  created_at DESC);
CREATE INDEX ON trips (driver_id, created_at DESC);
```

**Why shard by `city_id`:**
- Rides are local — cross-city queries are rare
- Hot cities (NYC, SF, London) scale independently
- Failure blast radius = one city, not the world

### Driver state (Redis)

```
HASH driver:{driver_id}
   state    = "online" | "offered" | "on_trip" | "offline"
   offer_id = <ride_id>     (TTL 15s — auto-expire stuck offers)
   cell_h3  = <h3 index>

GEO live-drivers:{city_id}
   GEOADD live-drivers:nyc <lng> <lat> <driver_id>
   key TTL 30s — drop stale drivers
```

### Sanity check — every query has an answer

| Query | How served |
|---|---|
| Find drivers within 2 km of pickup | `kRing` over in-mem H3 index, fall back to `GEOSEARCH` Redis |
| Driver accepts ride | Postgres CAS: `UPDATE trips ... WHERE state='requested' AND state_version=?` |
| Atomically reserve a driver | Postgres CAS on `drivers.state` (and/or Redis `SET NX PX`) |
| Rider's trip history | Postgres index `(rider_id, created_at DESC)` |
| Driver's trip history | Postgres index `(driver_id, created_at DESC)` |
| Live driver locations on rider's map | Redis Geo + WebSocket push |

### Sharding callouts

- **Trips** → `city_id`. Cross-shard analytics? Stream to BigQuery via Kafka.
- **Users** → `user_id` hash (consistent hashing). Users move between cities, so don't pin them.
- **Locations** → `city_id` then sub-shard by H3 cell prefix.
- **Hot-city mitigation**: NYC gets multiple physical shards under the logical `city_id=NYC`; route by `(city_id, hash(driver_id) % N)`.

---

## Phase 6 — Deep Dives (15 min) — *the bar-raiser zone*

> "I'd lead with these four; happy to follow your interest."

### DD 1 — Geospatial Indexing: H3 vs Quadtree vs Geohash

**Problem.** Given pickup `(lat, lng)`, find drivers within R km in <100 ms, with 1M live drivers globally.

**The three contenders:**

| Index | How it works | Pros | Cons |
|---|---|---|---|
| **Geohash** | Encode lat/lng as base-32 string; common prefix ≈ nearby | Simple, lex-ordered range queries | Edge cases at meridian/poles; non-uniform cell sizes; "near but different prefix" problem |
| **Quadtree** | Recursive 4-way subdivision; dense areas split deeper | Adapts to density automatically | Tree mutations as drivers move; rebalancing cost |
| **H3 (Uber's own OSS lib)** | Hexagonal grid, 16 resolution levels, hierarchical | **`kRing` neighbor lookup is O(1)**; uniform neighbor distance (hexagons!); no tree mutation; resolution-tunable per area | Not perfectly hierarchical (small overlap between parent/child cells) |

**Recommendation: H3 at resolution 8 (~0.7 km² cells).**
- Driver location → compute H3 index in O(1)
- "Drivers near pickup" = `kRing(cell, 1 or 2)` → 7 or 19 cells, hashmap lookup each
- Total query: O(1) cells × O(drivers per cell) — bounded and fast
- **Naming H3 explicitly is a flex**: it's Uber's own open-source library; the interviewer will recognize you've done your homework.

**Hot cell mitigation.** Downtown SF at rush hour might have thousands of drivers in one cell. Two options:
1. **Sub-shard at finer resolution** (level 9, ~0.1 km²) for that area only
2. **Cap candidates per cell** at the dispatch query — we only need top-K nearest, not all

**Trade-off worth volunteering.** H3 cells are *not* perfectly hierarchical — a child cell isn't strictly contained in its parent (small geometric overlap). Fine for nearest-neighbor dispatch; for billing/surge zones you stitch city polygons separately.

### DD 2 — Dispatch & Double-Dispatch Prevention

**Problem.** ~1000 ride requests/sec, each must atomically reserve **exactly one** driver. Two riders claiming the same driver = catastrophic safety/UX failure.

**Three approaches:**

**A. Pessimistic — distributed lock (Redlock per driver_id)**
- Acquire lock before offering
- ❌ Adds latency, lock service is a SPOF, TTL handling is tricky, and the [Martin Kleppmann critique](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) of Redlock under GC pauses applies.

**B. Optimistic — single-row CAS**
```sql
UPDATE drivers
   SET state='OFFERED', offer_id=:ride_id, offer_expires=now()+'15s'
 WHERE driver_id=:id AND state='ONLINE';
-- if rowcount = 0, someone got there first → try next candidate
```
- ✅ Simple, no extra infra, leverages Postgres atomicity.

**C. Single-writer-per-driver via partitioned dispatch**
- Kafka topic `ride.requested` partitioned by **`city_id`** (or `driver_id` for fine-grained); only **one** consumer ever processes a given driver's offers at a time
- Eliminates the race **by design** — no two workers can be offering the same driver simultaneously

**My recommendation: B + C combined — defense in depth.**

> "Partitioning prevents 99% of races by design. CAS is the safety net for the 1% — partition rebalancing during deploys, consumer-group failover, cross-region requests. This is the 'killer phrase' for me: **Kafka partitioning + Postgres CAS, defense in depth against double-dispatch.**"

**Driver decline / timeout flow**
- Each offer carries a 15s TTL stored on the row
- If driver doesn't accept in 15s → dispatcher re-queues to next candidate
- Background sweeper releases stale offers (`offer_expires < now() AND state='OFFERED'` → set `state='ONLINE'`)

### DD 3 — Location Updates at 250K writes/sec

**Problem.** 1M drivers × 1 ping / 4s = 250K writes/sec. Postgres dies; even Redis at this rate single-node would struggle.

**Pipeline:**

1. **Persistent connection layer** — drivers connect via WebSocket / MQTT, not HTTP-per-ping. Saves TCP/TLS setup × 250K/sec.
2. **Sticky routing** at the gateway — hash on `driver_id`, all of a driver's pings land on the same Location Service shard.
3. **In-memory shard** — each shard holds:
   - `driver_id → (lat, lng, ts, h3_cell)` hashmap
   - `h3_cell → set<driver_id>` reverse index
   - Pure RAM updates: ~1 µs per write. A single shard handles 50K drivers comfortably.
4. **Cross-shard read replica** — every Nth update OR on cell change, write through to **Redis Geo**. Powers global "find nearest" queries that don't know which shard the driver is on.
5. **Async breadcrumb pipeline** — every update also produces to Kafka `driver-locations`. A consumer batches per-trip GPS traces and writes to S3 after trip completion.

**Backpressure.** If a driver's app pings >1 Hz (buggy build), gateway drops intermediate pings. Latest-wins.

**Failure handling.** A shard dies →
- Drivers reconnect via consistent hashing (with virtual nodes) to a new shard
- New shard has no state for them yet → state rebuilds within ~10s as drivers re-ping
- We **accept brief location staleness** rather than engineer HA replication for ephemeral data. This is a deliberate choice worth saying out loud.

### DD 4 — Ride Lifecycle State Machine & Consistency

**The states:**

```
                ┌───────────────┐
                │   REQUESTED   │
                └───────┬───────┘
                        │ matched
                        ▼
                ┌───────────────┐  cancelled
                │    MATCHED    │──────────┐
                └───────┬───────┘          │
                        │ driver arrived    │
                        ▼                  │
                ┌───────────────┐          │
                │    EN_ROUTE   │──────────┤
                └───────┬───────┘          │
                        │ rider boarded    │
                        ▼                  │
                ┌───────────────┐          │
                │    IN_TRIP    │          │
                └───────┬───────┘          │
                        │ dropoff          ▼
                        ▼          ┌───────────────┐
                ┌───────────────┐  │   CANCELLED   │
                │   COMPLETED   │  └───────────────┘
                └───────────────┘
```

**Consistency mechanism — versioned conditional updates:**

```sql
UPDATE trips
   SET state         = :new_state,
       state_version = state_version + 1,
       updated_at    = now()
 WHERE trip_id      = :id
   AND state        = :expected_state
   AND state_version = :expected_version;
-- rowcount must be 1; else conflict → return 409 to caller
```

Every transition emits `ride.state_changed` to Kafka — downstream services (notifications, billing, ETA, analytics) react eventually.

**Idempotency.** Every state-changing API takes an `Idempotency-Key`. The Trip Service stores `(trip_id, idempotency_key) → response` in Redis with 24h TTL. Replay returns cached response — driver retries on flaky network are safe.

**Why not Temporal / Cadence?** Worth mentioning to score points:
> "For SDE2 scope, Postgres + a version field is simpler and we own it end-to-end. Temporal becomes the right answer if state transitions span hours and need durable timers — e.g., scheduled rides that need to fire at a specific time. I'd flag it as a future-improvement, not a day-one choice."

### DD 5 (bonus, if time) — Surge Pricing

- Bucket each H3 cell at resolution 7 (~5 km²) — surge zones are bigger than dispatch cells.
- Recompute every 5 min: `surge = f(open_requests, available_drivers)` per zone.
- Cache in Redis: `surge:{h3_cell} → multiplier`.
- Pricing service joins `fare_estimate × surge` at request time.
- **Trade-off**: 5-min staleness vs. surge-chasing oscillation. Apply EWMA smoothing so prices don't flap.

---

## Phase 7 — Wrap-Up (3 min)

### Summary (memorize this)

> "To recap: API gateway fronts four core services — **Trip, Location, Matching, Pricing**. Trips live in **Postgres sharded by city_id**; live driver locations live in an **in-memory H3 index** with Redis Geo as the cross-shard read replica. Dispatch is **event-driven via Kafka, partitioned by city**, with **Postgres CAS** on driver state as defense-in-depth against double-dispatch. Realtime updates flow over **WebSockets**. The two non-obvious choices are (1) **H3 for the geo index**, because `kRing` neighbor lookup is O(1) and the hex grid gives uniform neighbor distances, and (2) **Kafka partitioning + row-level CAS** to eliminate dispatch races by both design and defense-in-depth."

### Bottlenecks to monitor

- **Hot H3 cells** in dense areas — sub-shard at finer resolution, cap candidates-per-cell
- **Dispatch worker lag** during surge — auto-scale on Kafka consumer-group lag metric
- **Postgres trip-state write contention** — vertical scale per shard, then split by ride age (active vs archived)
- **WebSocket gateway connections** — 1M concurrent is fine on modern infra but plan capacity carefully (~10K conns/instance)

### Future improvements

- **ML-based ETA** with road-graph routing (OSRM, in-house map service)
- **Demand prediction** to pre-position drivers
- **Pool / shared rides** — entirely different matching algorithm (graph optimization, not nearest-neighbor)
- **Multi-region active-active** — currently city-pinned; replicate trips cross-region for resilience
- **Temporal/Cadence** for scheduled rides

### Curveball prep

| Question | Your angle |
|---|---|
| "What if traffic 10×?" | More Kafka partitions, sub-shard hot H3 cells, scale dispatch consumers, add Postgres read replicas |
| "What if a region goes down?" | City-level failover; in-flight trips degrade to "find driver in adjacent city"; idempotency keys make client retries safe; trip writes need cross-region replication for true HA |
| "How do you migrate from a monolith?" | Strangler fig: extract **Location Service first** (highest QPS, easiest boundary), then Matching, then Trip. Dual-write during cutover, shadow-read to verify, then flip. |
| "What if Kafka is down?" | Trip Service falls back to direct gRPC to dispatch worker — degrades latency but preserves correctness. Or buffer in Trip Service's own outbox table and replay. |
| "How do you handle cross-region riders?" | Pin to home region; when traveling, write trips to nearest region with global async replication for history reads. |
| "What about safety / SOS?" | Out of scope today, but it'd be a separate critical-path service with its own SLA — never share infra with surge or analytics. |

---

## Cheat Sheet — memorize before the interview

**The 7 phases (time → cumulative for 60 min round)**
1. Requirements — 5–7 / 7
2. Estimation — 3–5 / 12
3. API Design — 3–5 / 17
4. High-Level Design — 8–10 / 27
5. Database Design — 5–7 / 34
6. Deep Dives — 18–20 / 54
7. Wrap-Up — 4–6 / 60

**Numbers to anchor on**
- 10M DAU riders, 1M concurrent online drivers
- **15M rides/day** → ~500 ride req/sec peak
- **250K driver-location writes/sec** ← the dominant workload
- Trip storage: 5.5 TB/year — sharded Postgres
- Live location: 100 MB hot — fits in RAM

**The 4 services**
Trip · Location · Matching · Pricing

**The 4 deep dives — be ready for any**
1. **H3 geospatial index** (Uber's OSS — name it explicitly)
2. **Kafka partitioning + Postgres CAS** for double-dispatch prevention
3. **In-memory sharded H3 + sticky routing** for 250K location writes/sec
4. **Versioned conditional updates** for the ride state machine

**Two killer phrases to drop**
- *"Kafka partitioned by city plus Postgres CAS — defense in depth against double-dispatch."*
- *"H3 with kRing because hex-cell neighbor lookup is O(1) and uniform — that's why Uber built it."*

**Common mistakes to actively avoid (from the framework)**
1. Jumping to solution before scoping
2. Over-engineering (don't add k8s, service mesh, CQRS for points)
3. Under-engineering (your design must meet your stated QPS)
4. Ignoring trade-offs (every choice has a cost — say it out loud)
5. Designing in silence (narrate everything)
6. Getting stuck on detail decisions (PostgreSQL vs MySQL — pick and move)
7. Not drawing — always draw
8. Forgetting NFRs — periodically check design against availability/latency targets

---

## How to practice this

1. **Whiteboard run** — set a 45-min timer, no notes, walk top-to-bottom out loud. The first run will feel terrible. That's the point.
2. **Time-box each phase strictly** — your biggest risk is over-spending on Phase 1 or HLD and arriving at deep dives with 5 min left.
3. **Record yourself** explaining DD1 and DD2 — these are where you score or lose the round.
4. **Memorize the cheat sheet** — the numbers and the killer phrases. The structure should be muscle memory so your brain is free to think about whatever curveball lands.
