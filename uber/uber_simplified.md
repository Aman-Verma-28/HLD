# Uber HLD — Interview Script (Ride-Sharing Application)

> **How to use this doc:** Each section has **What to say** (the substance) and **How to say it** (delivery cues — pacing, what to draw, where to invite the interviewer in). Stick to the time budgets in the framework — if you're over, cut the "nice-to-haves," not the deep dives.
>
> **Opening line (verbatim):** *"Cool — designing Uber. Before I jump in, I'd like to spend about 3–4 minutes on requirements so we're aligned, then walk through estimation, entities, APIs, a high-level architecture, and finally pick 3–4 components to go deep on. Does that flow work for you?"* — wait for a nod, then start.

---

## 1. Requirements Clarification (3–5 min)

### Functional Requirements

**What to say:**
1. A **rider** can request a ride from a pickup location to a destination, and see a fare estimate before confirming.
2. The system **matches** the rider with a nearby available driver within a few seconds.
3. A **driver** can accept or decline an incoming ride request; if declined or timed out, the system retries with the next-best driver.
4. Both rider and driver get **real-time location updates** of each other for the duration of the trip.
5. The ride goes through a **lifecycle state machine**: `REQUESTED → MATCHED → EN_ROUTE_TO_PICKUP → IN_TRIP → COMPLETED` (with `CANCELLED` as a terminal state from most states).
6. The system computes a **fare** at trip end, including base + distance + time + surge multiplier.

**Clarifying questions to ask:**
1. *"Are we scoping this to standard rides only, or do we also need to handle Pool/UberX Share, scheduled rides, and Uber Eats-style deliveries?"* — I'll assume **standard on-demand rides only**.
2. *"Do we own payments end-to-end, or do we just compute the fare and hand off to a separate payments platform?"* — I'll assume **fare computation + a thin payments integration**.
3. *"Are we designing for a single city, a country, or globally?"* — I'll assume **global, multi-region**, since that drives sharding and replication choices.

**How to say it:**
Lead with the rider's journey, not a feature list — interviewers like seeing the user flow. After listing the 6 FRs, pause and explicitly ask the 3 questions; **don't** assume answers silently. State your assumptions out loud after they answer ("Got it, so I'll exclude scheduled rides for this design — happy to revisit if time permits").

### Non-Functional Requirements

**What to say:**
1. **Scale:** ~100M DAU globally, ~10M active drivers, ~1M concurrent rides at peak. Drivers send location every ~4s — that's the dominant write workload.
2. **Latency:** Match a driver in **< 5s p99**. Location-update propagation to the rider's app **< 2s p99**. Fare estimate **< 200ms**.
3. **Availability:** **99.99%** for the ride-request and dispatch path — this is mission-critical; a 5-min outage means tens of millions of dollars and a PR incident.
4. **Consistency:**
   - **Strong consistency** for ride state (no double-dispatch — *one driver can have at most one active ride; one rider can have at most one active ride*).
   - **Eventual consistency** is fine for driver location data — being 1–2s stale is acceptable.
5. **Domain-specific NFR #1 — No double dispatch:** the matching service must guarantee a driver is offered to **exactly one** rider at a time. This is the single most important correctness property.
6. **Domain-specific NFR #2 — Geospatial query performance:** "find drivers within 2km of (lat, lng)" must be **< 100ms** even when the driver pool in that cell is in the thousands.

**How to say it:**
Call out **#5 (no double dispatch)** explicitly and say *"this is the property I'll come back to in the deep-dive section."* That plants a flag — interviewers love when you tee up your own deep dives. For availability, give a business-impact framing (cost of outage), not just a number — that signals senior thinking.

---

## 2. Estimation (2–3 min)

**What to say — show the math out loud:**

**Traffic / QPS:**
- 100M DAU, ~10% take a ride/day → **10M rides/day** → ~115 rides/sec average → **~1,000 rides/sec peak** (Friday/Saturday evening, 8–10x average).
- Driver location updates: 10M drivers × 1 update / 4s = **~2.5M location updates/sec**. **This dominates the write path** — it's roughly 2,500x the ride-request QPS.
- Map/nearby-driver reads from rider apps: ~1M concurrent riders watching the map, refreshing every 5s → **~200K reads/sec**.

**Storage:**
- Ride record ~1KB → 10M/day × 1KB = **10GB/day** → ~3.6TB/year of ride records. Cheap — single sharded PSQL handles it.
- Location data: **don't persist every update**. Store only the trip route (sampled every ~10s during a trip) → ~50KB per trip → 500GB/day of trip telemetry. Goes to an append-only store (Cassandra / S3 + Parquet for analytics).

**Domain-specific — bandwidth:**
- 2.5M location updates/sec × ~100 bytes/update = **250MB/sec ingest** (~2 Gbps). This is why we **don't put location updates through a normal REST + DB path** — that's the first deep-dive.

**How to say it:**
Write the numbers on the whiteboard as you say them — interviewers track them. End this section with: *"The headline number here is 2.5M writes/sec on locations vs. 1K rides/sec — that's a 2,500x asymmetry, and it shapes most of the architectural choices coming up."* That sentence alone earns senior points.

---

## 3. Core Entities (3–5 min, bottom-up)

**What to say:**

1. **User** — the base identity. Sub-types: Rider, Driver. `id, name, phone, email, role, rating`.
2. **Driver** — extends User. `driver_id, vehicle_id, status (OFFLINE | AVAILABLE | EN_ROUTE | IN_TRIP), current_location, current_h3_cell`.
3. **Vehicle** — `vehicle_id, driver_id, make, model, plate, type (UberX, UberXL, ...)`.
4. **Location** — `entity_id, lat, lng, heading, speed, timestamp`. Ephemeral; lives in Redis, not the OLTP DB.
5. **Ride** — the central entity. `ride_id, rider_id, driver_id (nullable), pickup, dropoff, state, fare, surge_multiplier, requested_at, completed_at`.
6. **Trip** — the realized path of a ride. `trip_id, ride_id, route (polyline), distance_m, duration_s`.
7. **Fare / Pricing** — `ride_id, base_fare, distance_fare, time_fare, surge_multiplier, total, currency`.
8. **Payment** — `payment_id, ride_id, method, status, amount`. Lives behind a payments service boundary.

**How to say it:**
Bottom-up means: start from the smallest atom (User, Vehicle), build up to the central object (Ride). Do **not** start with "Ride" — interviewers can tell you're working backwards from the API. Spend ~30 seconds per entity max. After the list, say: *"Ride is the central transactional entity; Location is high-volume but ephemeral — those two get different storage treatments, which I'll cover in the HLD section."*

---

## 4. API Design (3–5 min)

**What to say — list 7 APIs, two bullets each:**

1. **Request a ride** — initiates a new ride request and kicks off matching.
   - `POST /v1/rides` → body: `{ pickup: {lat, lng}, dropoff: {lat, lng}, ride_type }`. Response: `{ ride_id, state: "REQUESTED", fare_estimate, eta_to_pickup }`.

2. **Get ride status** — riders/drivers poll or subscribe to the ride state.
   - `GET /v1/rides/{ride_id}` → returns the full ride object including current state, assigned driver, and live ETA. WebSocket variant `WSS /v1/rides/{ride_id}/stream` for push updates.

3. **Update ride state (driver action)** — driver accepts/declines/starts/completes.
   - `PATCH /v1/rides/{ride_id}` body: `{ action: "ACCEPT" | "DECLINE" | "START_TRIP" | "COMPLETE_TRIP" }`. Response: updated ride. Server validates the state transition.

4. **Cancel a ride** — rider or driver cancels.
   - `POST /v1/rides/{ride_id}/cancel` body: `{ reason }`. Response: `{ ride_id, state: "CANCELLED", cancellation_fee }`.

5. **Driver location ingest** — high-frequency, separate path.
   - `POST /v1/drivers/{driver_id}/location` body: `{ lat, lng, heading, speed, ts }`. **Not** REST in practice — this is gRPC/UDP/WebSocket to a Location Ingestion Gateway, then onto Kafka. Returns 202 Accepted (fire-and-forget).

6. **Find nearby drivers** — used by rider's "show drivers around me" map and by the matching service.
   - `GET /v1/drivers/nearby?lat=&lng=&radius_m=2000&ride_type=UberX`. Response: `{ drivers: [{driver_id, location, eta_seconds}], next_cursor }`.

7. **Get fare estimate** — pre-confirmation pricing for the rider.
   - `POST /v1/pricing/estimate` body: `{ pickup, dropoff, ride_type }`. Response: `{ low, high, surge_multiplier, currency }`.

**How to say it:**
Group the APIs by actor: rider-facing (1, 2, 4, 7), driver-facing (3, 5), and shared (6). After listing, **flag the asymmetry** out loud: *"Notice that #5 — driver location — looks like one API but is a fundamentally different system. Standard REST on this would melt the database. I'll cover that in the deep-dive."* Tee up the deep dive.

---

## 5. High Level Design (10–12 min)

### 5.1 Services (microservices, 1 line each)

**What to say:**
1. **API Gateway** — TLS termination, auth, rate limiting, routes to downstream services. Sits behind a global Load Balancer.
2. **Rider Service** — owns rider profile, ride request creation, rider-side ride lifecycle reads.
3. **Driver Service** — owns driver profile, vehicle, online/offline status, driver-side ride lifecycle actions (accept/decline/start/complete).
4. **Location Service** — ingests location updates, maintains the geospatial index of available drivers, answers "drivers near (lat, lng)" queries.
5. **Matching / Dispatch Service** — given a ride request, finds candidate drivers, ranks them, sends offers, handles accept/decline/timeout, and atomically commits the match.
6. **Trip Service** — owns the ride state machine; the single source of truth for "what state is this ride in." All state transitions go through here.
7. **Pricing / Surge Service** — computes fare estimates and current surge multipliers per H3 cell based on supply/demand.
8. **Notification Service** — push notifications + WebSocket fan-out to rider and driver apps for ride updates.

### 5.2 Databases

**What to say (for each: schema sketch + DB choice + why):**

| DB | Stores | Tech | Why |
|---|---|---|---|
| **User DB** | Users, Drivers, Vehicles | **PostgreSQL** | Strongly typed, low write volume (10s of QPS), needs joins for profile reads, ACID for signup. |
| **Ride DB** | Rides, Trip metadata | **PostgreSQL, sharded by `ride_id` (consistent hashing)** | Strong consistency required for state transitions; ~1K writes/sec is fine on a sharded cluster. Single-shard transactions for state changes. |
| **Location Store** | Driver current location, geospatial index | **Redis with H3 cell keys (in-memory)** | 2.5M writes/sec + sub-100ms range queries — only an in-memory store survives this. We don't need durability; if a node crashes, drivers re-emit within 4s. |
| **Trip History / Telemetry** | Full route polylines, completed trip records | **Cassandra** (or S3 + Parquet for analytics) | Append-only, write-heavy, time-series-shaped, no joins needed. Cassandra's wide-row model fits "all events for a ride." |
| **Pricing DB** | Surge multipliers per H3 cell, base fares | **Redis (hot cell → multiplier)** + **PostgreSQL** for base fare config | Surge changes every minute per cell — read-heavy, must be in-memory. |

**Why not other DBs (have these ready as one-liners):**
- *Why not MongoDB for rides?* — Ride state transitions need cross-row constraints (no double-dispatch). PSQL's transactional guarantees are easier to reason about.
- *Why not put locations in Cassandra?* — 2.5M writes/sec is feasible, but the **read** pattern is "give me all drivers in this 2km radius, sorted by distance" — that's a geospatial range query, not a key lookup. Redis with H3 indexing handles this; Cassandra would need a secondary index that wouldn't perform.
- *Why not DynamoDB?* — Could work for rides; Postgres just has richer relational primitives that I'll lean on for the dispatch transaction.

### 5.3 Cache Layer

**What to say:**
1. **Redis (geospatial index)** — sits in front of the Location Service. Drivers' current cells are stored as `H3_CELL_ID → SortedSet<driver_id, last_update_ts>`. The Matching Service queries this in O(log N) per cell to get candidates. **Cache-aside is the wrong frame here — Redis is the source of truth for driver location**, because we never persist every tick.
2. **Redis (driver session / online status)** — `driver_id → status (AVAILABLE / EN_ROUTE / IN_TRIP)` with a TTL of 10s, refreshed by heartbeats. Lets Matching skip a DB lookup on every dispatch.

### 5.4 Queue Layer

**What to say:**
1. **Kafka — `driver.location.updates` topic** — sits between the Location Ingestion Gateway and the Location Service. Drivers' apps push to the gateway, gateway publishes to Kafka (partitioned by `driver_id`), and the Location Service consumes and updates Redis. **Why Kafka?** Decouples ingestion spikes from the Redis write rate, gives us replay for debugging, and lets analytics consume the same stream.
2. **Kafka — `ride.events` topic** — sits between the Trip Service and downstream consumers (Notification, Pricing analytics, Data warehouse). Trip Service publishes a domain event on every state transition; consumers fan out. Classic event-driven fan-out pattern.

### 5.5 Architecture Sketch (draw this)

```
                        ┌────────────────────────┐
   [Rider App]───┐      │  Global Load Balancer  │      ┌──── [Driver App]
   [Rider App]───┼─────▶│      (Anycast)         │◀─────┤───── [Driver App]
                 │      └────────────┬───────────┘      │
                 │                   │                  │
                 │            ┌──────▼──────┐           │
                 │            │ API Gateway │           │   (high-frequency
                 │            └──┬───────┬──┘           │    location path —
                 │               │       │              │    bypasses gateway)
                 ▼               ▼       ▼              ▼
         ┌──────────────┐ ┌────────────┐ ┌──────────────────────┐
         │ Rider Service│ │Trip Service│ │  Driver Service      │
         └──────┬───────┘ └─────┬──────┘ └──────────┬───────────┘
                │               │                   │
                │         ┌─────▼──────┐            │
                │         │  Ride DB   │◀───────────┘
                │         │ (PSQL,     │      ┌─────────────────┐
                │         │  sharded)  │      │ Location        │
                │         └─────┬──────┘      │ Ingestion       │◀──────[Driver
                │               │             │ Gateway         │       location
                │               │             └────────┬────────┘       UDP/gRPC]
                │               │                      │
                │               ▼                      ▼
                │       ┌───────────────┐      ┌────────────────┐
                │       │ Kafka         │      │ Kafka          │
                │       │ ride.events   │      │ driver.location│
                │       └───────┬───────┘      └────────┬───────┘
                │               │                       │
                ▼               ▼                       ▼
         ┌────────────┐ ┌────────────────┐    ┌────────────────────┐
         │Notification│ │ Matching /     │◀───│ Location Service   │
         │ Service    │ │ Dispatch Svc   │    │ + Redis (H3 index) │
         └────────────┘ └────────────────┘    └────────────────────┘
                                │
                                ▼
                         ┌──────────────┐
                         │ Pricing/Surge│
                         │ Service +    │
                         │ Redis        │
                         └──────────────┘
```

**How to say it:**
Build the diagram **incrementally** as you talk — don't pre-draw it. Start from the rider/driver clients on the outside, then LB → gateway → services → DBs → queue → consumers. Use **boxes for services, cylinders for DBs, parallelograms for queues, arrows for data flow with labels** ("publish", "consume", "read-through"). Most candidates fail by drawing a rats' nest; clarity here is half the score.

When you draw, **say the data flow narratively**: *"A rider opens the app → hits the LB → gateway → Rider Service creates a ride request → publishes to Kafka → Matching Service picks it up → queries Location Service for nearby drivers → offers the ride to the top candidate via Notification Service → driver accepts via Driver Service, which updates the Trip Service state machine → state-change event published, Notification fans out to rider."*

---

## 6. Deep Dives & Trade-offs (12–15 min)

> **Pick 4 components. Use the 5-point template every time:** (1) what + where, (2) interactions, (3) why critical, (4) alternatives + trade-offs, (5) recommendation.

### Deep Dive 1 — Geospatial Indexing with H3

1. **What + where:** **Uber's H3** is a hexagonal hierarchical geospatial index. We assign every driver to an H3 cell (resolution ~9, ~150m edge length). The index lives in Redis as `cell_id → SortedSet<driver_id>` and sits inside the Location Service.
2. **Interactions:** The Location Service updates the index on every location-update consume from Kafka (`SREM old_cell, SADD new_cell` if cell changed). The Matching Service queries it during dispatch via `kRing(cell, k=2)` to fetch all drivers within ~2 cells of the rider's pickup, then ranks by Haversine distance.
3. **Why critical:** "Find drivers within 2km of point P" with 10M drivers at 2.5M updates/sec is the hardest single query in the system. Without a spatial index, this is a full scan — completely infeasible. H3 turns it into an O(K) lookup where K is the number of cells in the radius (~7–19 cells for a 2km radius at res 9).
4. **Alternatives + trade-offs:**
   - **Quadtree:** rectangular cells, variable cell size — historically used by Yelp, Airbnb. Trade-off: rectangles distort badly near poles and at boundaries; neighbor queries need tree walks. H3 hexagons have **uniform neighbor distance** (every neighbor is the same distance away) — much better for "expand the search radius" logic.
   - **Geohash:** lexicographic prefix index, easy to implement on any KV store. Trade-off: rectangular cells have the same distortion problem, **and** two geographically close points can have very different prefixes (the "edge problem"), forcing you to query 9 cells defensively.
   - **PostGIS:** full SQL geospatial, very capable. Trade-off: not built for 2.5M writes/sec; would need heavy sharding and you lose the simplicity.
5. **Recommendation:** **H3 in Redis.** Hex uniformity, O(1) cell math, and Redis sustains the write/read rates. If we ever need historical geospatial queries (analytics), we replicate the same data into PostGIS asynchronously.

**How to say it:** Mention "this is what Uber actually published — H3 is their open-source library" — shows you've read the engineering blog. Draw a hex grid on the board and shade the kRing(2).

---

### Deep Dive 2 — Dispatch & Double-Dispatch Prevention

1. **What + where:** The Matching/Dispatch Service offers a ride to one driver at a time. The hard correctness invariant: **a driver can be offered to at most one ride simultaneously, and a ride is matched to at most one driver.** Sits between the Trip Service (consumes new ride requests) and the Driver Service (sends offers via Notification Service).
2. **Interactions:** On a new ride request, Dispatch (a) fetches top-K candidates from Location Service, (b) **atomically reserves** the top candidate by transitioning their status from `AVAILABLE` to `OFFER_PENDING` via a conditional update in Postgres or `SETNX driver:{id}:offer ride_id NX EX 10` in Redis, (c) sends the offer with a 10–15s TTL, (d) on accept → transitions ride state via Trip Service; on decline/timeout → releases the reservation and tries the next candidate.
3. **Why critical:** Without this, a popular driver in a busy zone could be simultaneously offered to 5 ride requests. If two riders' apps both showed "matched" → angry users, support tickets, and a bad press cycle. The correctness here is the entire product's reputation.
4. **Alternatives + trade-offs:**
   - **Optimistic — broadcast offer to N drivers, first-accept wins:** simpler dispatch, faster match. Trade-off: N-1 drivers waste time accepting only to be told "too late," driver experience tanks, and the race condition surface for "two accepts arrive within ms" is real.
   - **Distributed lock via Zookeeper/etcd:** correctness is rock-solid. Trade-off: extra component, extra latency (~20–50ms per dispatch) — at 1K dispatches/sec this becomes a hot path.
   - **DB-row-level lock with `SELECT FOR UPDATE`:** simple and correct. Trade-off: contention on hot drivers in busy zones; but actually fine since there are few rides per driver per minute.
   - **Redis `SETNX` lock:** fast, simple, the lock TTL handles driver-app crashes naturally.
5. **Recommendation:** **Redis `SETNX` with a 15s TTL** for the offer reservation, backed by a Postgres durable record of the offer + ride state. Redis gives us the speed and atomicity; Postgres gives us audit + recovery. Avoid the broadcast pattern — driver experience matters more than a 100ms faster match.

**How to say it:** This is where you earn the senior bar. Walk through a **specific race condition** ("imagine driver D is the closest match for both ride R1 and R2 arriving 50ms apart — without atomic reservation, both Matching workers read `AVAILABLE`, both send offers, D accepts the first one, the second one is now in a corrupt state…"), then show how the lock prevents it. Concrete examples > abstract claims.

---

### Deep Dive 3 — Location-Update Ingestion Pipeline (2.5M QPS)

1. **What + where:** A dedicated **Location Ingestion Gateway** that bypasses the main API Gateway, accepts location updates over a persistent connection (gRPC streaming or WebSocket, with UDP for areas of poor connectivity), and publishes to a Kafka topic partitioned by `driver_id`. The Location Service consumes and updates Redis.
2. **Interactions:** Driver app → Ingestion Gateway (persistent connection, no per-message TLS handshake) → Kafka (`driver.location.updates`, 1000+ partitions for parallelism) → Location Service consumers → Redis (H3 index update). Downstream analytics consumers tap the same Kafka topic.
3. **Why critical:** 2.5M writes/sec at 100 bytes each = 250 MB/sec. If we tried to put this through normal REST + DB, we'd need ~1,000 web servers and the DB would be the bottleneck. The architecture must absorb spikes (Friday 6pm), survive a Redis failure (Kafka buffers), and not lose the location signal during a brief network blip.
4. **Alternatives + trade-offs:**
   - **REST POST per update:** simplest. Trade-off: TLS handshake + HTTP overhead per request → 5–10x more CPU per update. Doesn't scale.
   - **Direct write to Redis from gateway (no Kafka):** lowest latency. Trade-off: no buffering, no replay, no easy fan-out to analytics. A Redis hiccup would either drop updates or backpressure into the driver apps.
   - **MQTT broker:** purpose-built for IoT-style telemetry. Trade-off: another piece of infra; Kafka can do this and we already need Kafka for ride events.
5. **Recommendation:** **Persistent gRPC stream → Kafka → Redis.** Kafka is the shock absorber and the fan-out hub; Redis is the read store; the persistent connection avoids per-message overhead. Sample updates at the **driver app** when stationary (don't emit if `delta < 5m AND speed ≈ 0`) — easy 30–50% reduction in QPS for free.

**How to say it:** Make the *bypass-the-gateway* point explicitly: *"REST + standard gateway is fine for ride requests at 1K QPS — it's the wrong tool for 2.5M QPS of telemetry. We treat the location path as a separate subsystem with its own ingress."* That mental separation is what an L5/SDE2 should articulate.

---

### Deep Dive 4 — Sharding the Ride DB with Consistent Hashing

1. **What + where:** The Ride DB (PostgreSQL) is sharded across N nodes. Shard key is `ride_id` (UUID), routed via consistent hashing. The shard router lives in the Trip Service / Rider Service / Driver Service data-access layer.
2. **Interactions:** Any service that reads/writes a ride does `shard = hash_ring.lookup(ride_id)` → routes the query. Cross-shard queries (analytics, "all rides for rider X") are handled via a denormalized read replica indexed by `rider_id` and `driver_id`, fed asynchronously from Kafka (`ride.events`).
3. **Why critical:** At 1K writes/sec sustained and 10x peaks, a single Postgres instance survives but has **zero headroom** for growth or for a multi-region active-active setup. Sharding gives us horizontal scale + a natural blast radius (one shard down = 1/N rides affected, not 100%). Consistent hashing specifically is critical because adding a shard moves only `1/N` of keys, not all of them — re-sharding without consistent hashing means hours of downtime.
4. **Alternatives + trade-offs:**
   - **Geographic sharding (by city/region):** intuitive, locality-friendly. Trade-off: hot shards on big cities (NYC, SF) and cold shards on small ones — load imbalance. Also makes cross-region rides (airport pickups, etc.) painful.
   - **Range sharding by `ride_id`:** simple. Trade-off: sequential IDs create a hot shard on the latest range — every new ride goes to the same node.
   - **Hash sharding without consistent hashing:** even distribution. Trade-off: adding a node remaps almost every key — operational nightmare during scale-up.
   - **Cockroach / Spanner / Vitess:** managed distributed SQL, hides sharding. Trade-off: cost, vendor lock-in, sometimes worse latency on simple OLTP.
5. **Recommendation:** **Consistent hashing on `ride_id`** with virtual nodes (~256 vnodes/physical node) for smooth load distribution. Keep a CDC pipeline (Debezium → Kafka → ElasticSearch / read replica) for cross-shard query patterns. Geographic sharding is tempting but the load imbalance makes it worse than hash sharding in practice.

**How to say it:** When you say "consistent hashing," **draw the ring** on the board and show the vnode trick. Most candidates name-drop consistent hashing without showing they actually understand why it beats modulo hashing — drawing it cements that you do.

---

## 7. Wrap-Up (2–3 min)

### Summary of the design

**What to say:**
1. *"The design satisfies the FRs end-to-end: a rider hits the API Gateway, the Trip Service creates a ride and publishes it to Kafka, the Matching Service uses H3-indexed Redis to find candidates and Redis-backed locks to prevent double-dispatch, the driver gets an offer via WebSocket, accepts, and the state machine drives the ride to completion — with location updates flowing on a dedicated high-QPS path that bypasses the main gateway."*
2. *"The trade-offs we made were guided by the 2,500x asymmetry between location writes and ride writes — that single observation drove the dual-path architecture, the choice of Redis as the location source of truth, and Kafka as the shock absorber."*

### Trade-offs

1. **Strong consistency for ride state, eventual for location.** We accept ~1–2s staleness on driver positions in exchange for being able to actually serve the write rate. We never compromise on the no-double-dispatch invariant.
2. **Maintainability:** the price is operational complexity — Kafka, Redis, sharded Postgres, and H3 are 4 things to run well. We mitigate with strong observability (Kafka lag dashboards, Redis hit-rate, shard balance metrics) and a clean service boundary so each team owns one thing.

### Performance optimizations

1. **Bypass the main gateway for location ingestion**, persistent gRPC connections, and client-side sampling (don't emit when stationary) cut the location QPS by an order of magnitude before it ever hits the server.
2. **Redis for the geospatial hot path + Postgres for durable ride state** is a deliberate split — each component does what it's best at, and we don't pay for properties we don't need (e.g., we don't pay for Postgres durability on every location tick).

### Extensibility

1. **Pool / shared rides** plug into the same Matching Service with a different ranking algorithm (group multiple riders to one driver) — no architectural change needed, just a new matcher strategy.
2. **Future improvements:** ML-based ETA prediction (replace Haversine + average-speed with a model trained on historical traffic), multi-region active-active for the ride DB (today the design assumes regional active-active is bolt-on later), and a real-time supply/demand prediction service to pre-position drivers before surges happen.

### Follow-up questions to ask the interviewer

1. *"How would you want to handle the **driver-app-offline** edge case — say a driver loses connectivity mid-trip? We'd buffer location updates locally and replay on reconnect, and the rider's app shows the last known position with a 'reconnecting' banner; the trip state machine doesn't change."*
2. *"Where would you push back on the choice to use Redis as the source of truth for driver location? My answer: durability via Kafka replay — if Redis goes, we can rebuild the index from the last 30s of Kafka in <60s, and during that window driver apps re-emit anyway."*
3. *"How would you scale this to handle Pool rides and dynamic re-routing? My answer: Matching becomes a multi-rider optimization problem (clustering nearby requests), and the Trip Service state machine grows a new state for 'multi-rider in progress.' The data plane is unchanged."*
4. *"How would surge pricing avoid feedback loops — i.e., surge attracts drivers, which lowers surge, which makes them leave, which raises surge again? My answer: Pricing Service uses dampened EWMA over multi-minute windows, with hysteresis so the multiplier doesn't oscillate."*

**How to say it:**
Keep the wrap-up tight — interviewers are usually time-pressured by now. Don't repeat what you've already said; **summarize the design's *shape***, not the design. End on the follow-up questions — interviewers love a candidate who's still thinking about the next problem. The follow-ups also let you signal awareness of things you didn't have time to deep-dive (multi-region, ML ETAs, surge dynamics).

---

## Cheat Sheet — The Two Sentences That Earn the Senior Bar

> **Sentence 1 (during estimation):** *"The headline number is 2.5M writes/sec on locations vs. 1K rides/sec — that's a 2,500x asymmetry, and it shapes most of the architectural choices coming up."*
>
> **Sentence 2 (during deep-dive 2):** *"The single hardest correctness property here is no-double-dispatch — one driver, one offer at a time. Everything else is performance; this is correctness, and I want to spend a few minutes on exactly how we guarantee it."*

If you remember nothing else, anchor the conversation on those two sentences. Everything else is filler around the senior-level insight they signal.
