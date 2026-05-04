# Design Uber Eats — Nearby Restaurant Search (Geospatial)

> **Interview context:** Uber SDE2 — HLD / Bar Raiser (60 min)
> **Question:** Design the nearby-restaurant-search feature of Uber Eats.
> **Bar-raiser focus:** Efficient storage/retrieval of restaurant locations using a quadtree (or geohash). Rebuilding on updates. Bootstrapping new nodes. Serialization/deserialization.

This document is structured as an **end-to-end interview script** following the 7-phase AlgoMaster framework. Each phase has:
- **What I'd say** (verbatim talking points)
- **What I'd draw** (ASCII whiteboard)
- **Why** (signal-sending reasoning)

---

## Time budget (60-min interview)

| Phase | Duration | Cumulative |
|---|---|---|
| 1. Requirements | 5–6 min | 6 min |
| 2. Estimation | 3–4 min | 10 min |
| 3. API design | 3–4 min | 14 min |
| 4. High-level design | 8–10 min | 24 min |
| 5. Database / data model | 5–6 min | 30 min |
| 6. Deep dives (geo index) | 22–25 min | 55 min |
| 7. Wrap-up | 4–5 min | 60 min |

The bar-raiser already signaled the deep-dive will be about geo storage. So I deliberately compress phases 1–5 (still touching every box for completeness) and **pre-load** the geo index discussion so phase 6 can really shine.

---

## Phase 1 — Requirements clarification (5–6 min)

### What I'd say

> "Before I jump in, I want to scope this. Uber Eats is huge — restaurant discovery, menu browsing, cart, checkout, real-time delivery tracking, ratings, promotions. I'm going to assume the core thing you want me to design is the **discovery experience: a user opens the app, we show the restaurants near them that can deliver in a reasonable time, they can filter and sort, and they can tap one to see the menu**. Ordering, payments, and courier dispatch are out of scope unless you'd like me to fold any in."
>
> *(pause for confirmation)*
>
> "Given that, let me confirm functional and non-functional requirements."

### Functional requirements (in scope)

1. **Nearby search** — given user lat/lng (and optional radius), return open restaurants ranked by distance / ETA / popularity.
2. **Filters** — cuisine, price tier, min rating, dietary tags, "delivers in X min".
3. **Restaurant detail + menu** — tap-through view.
4. **Restaurant lifecycle** — onboarding, going live, going offline (closed for the night, permanently shut, paused due to volume).

### Out of scope (explicitly stated)

Cart, checkout, payments, courier matching, ETA computation (other than display), reviews/ratings ingestion, promotions, search by free text ("biryani near me" — that's a separate text-search service).

### Non-functional requirements

| Attribute | Target | Rationale |
|---|---|---|
| Search p99 latency | **< 200 ms** | App home-screen load — anything slower feels broken |
| Availability | **99.99%** | Tier-1 user-facing surface; fall back to last-known list rather than 5xx |
| Consistency | **Eventual (~seconds)** | A restaurant going offline or onboarding does not need to be globally visible in <1s. A user-perceived stale window of 30s–1min is fine |
| Scale | **~5M restaurants globally, 100M DAU, multi-region** | Order of magnitude of real Uber Eats |
| Durability | **Strong for restaurant CRUD** | If we lose a restaurant record, ops gets paged |

### What I'd write on the board

```
SCOPE
  IN:  nearby search, filters, restaurant detail/menu, lifecycle
  OUT: cart, payments, courier, free-text search, ratings ingest

NFR
  p99 search < 200ms | 99.99% avail | eventual consistency OK
  5M restaurants | 100M DAU | global / multi-region
```

### Why I structure it this way

- Stating what's **out of scope** is as valuable as what's in. It tells the interviewer "I see the iceberg, I'm choosing the tip on purpose."
- I anchor consistency early so when I later say "we'll propagate updates via Kafka with a few seconds of lag" they don't pushback.

---

## Phase 2 — Back-of-envelope estimation (3–4 min)

### What I'd say

> "Let me get to numbers — they'll tell me whether I need a single Postgres or 100 sharded geo indexes."

### Calculations

**Search QPS**
```
DAU                = 100M
Searches / user / day ≈ 5      (open app, scroll, refine filter, switch tab → multiple geo queries)
Total searches/day  = 500M
Avg QPS             = 500M / 86,400 ≈ 5,800   → call it 6K
Peak QPS            = 6K × 3 ≈ 18K            (lunch + dinner spikes; weekend even higher)
Per region (top-5)  = ~3K–5K peak QPS
```

**Write QPS (restaurant updates)**
```
5M restaurants × ~5 state changes/day (open/close/menu edit) = 25M / day ≈ 290 QPS avg, ~1K peak
```

So we're **read-heavy by ~20×**. This is the single most important fact for the design.

**Storage (restaurant records)**
```
Per restaurant: ~5 KB  (name, address, lat/lng, hours, cuisines, rating, photo URL, hash of menu)
Total:          5M × 5 KB = 25 GB
```

**That fits in RAM on a single beefy node (256 GB).** This unlocks an in-memory geo index — no disk seeks on the hot path.

Menu data is bigger (~50 KB × 5M ≈ 250 GB) but it's only fetched on tap-through, so SSD-backed and CDN-cacheable.

**Bandwidth**
```
Search response: ~20 restaurants × ~1 KB summary = 20 KB
At 18K peak QPS: 18K × 20 KB = 360 MB/s ≈ 3 Gbps egress. CDN handles tiles/images separately.
```

### What I'd write on the board

```
READ:  ~6K avg, ~18K peak QPS  (search)
WRITE: ~300 avg, ~1K peak QPS   (restaurant updates)
R:W ≈ 20:1   →  read-optimized; pre-computed geo index; aggressive caching

DATA:  25 GB restaurant core   →  fits in memory!
       250 GB menus            →  SSD + CDN
```

### Why these numbers matter for the design

| Number | Design implication |
|---|---|
| 18K peak search QPS | Need horizontal scale on search service; in-memory index per node |
| R:W 20:1 | Async update path; eventual consistency on the geo index is fine |
| 25 GB restaurant core fits in RAM | **In-memory quadtree per node is feasible** — this is the linchpin |
| 1K peak write QPS | Writes can go through Kafka → fanout to indexes; no need for distributed write path |

I explicitly say: *"Because the entire restaurant set fits in 25 GB, every search node can carry a full in-memory geo index. That's the architectural choice that makes <200ms p99 achievable."*

---

## Phase 3 — API design (3–4 min)

### What I'd say

> "I'll define just the contracts on the search path and lifecycle. Rest is mechanical."

### Public APIs (client-facing)

```http
GET /v1/restaurants/nearby
  ?lat=37.7749
  &lng=-122.4194
  &radius_m=3000           # optional, default = "auto" (system picks)
  &cuisine=italian,thai    # optional, repeated
  &min_rating=4.0          # optional
  &open_now=true
  &cursor=<opaque>         # cursor-based pagination
  &limit=20

→ 200 OK
{
  "results": [
    { "id": "r_123", "name": "Tony's", "lat":..., "lng":...,
      "distance_m": 412, "eta_min": 22, "rating": 4.6,
      "cuisines": ["italian","pizza"], "thumb": "cdn://..." },
    ...
  ],
  "next_cursor": "..."
}
```

```http
GET /v1/restaurants/{id}        # detail + menu metadata
GET /v1/restaurants/{id}/menu   # full menu (separately cacheable)
```

### Internal / admin APIs

```http
POST   /v1/admin/restaurants            # onboard
PATCH  /v1/admin/restaurants/{id}       # edit (status, hours, location)
DELETE /v1/admin/restaurants/{id}       # remove (soft delete)
```

### Decisions I call out (each one is a "senior signal")

- **Cursor pagination, not offset.** Restaurant ranking shifts as ETAs change; offset would let users see duplicates / gaps.
- **Idempotency keys** on admin POST/PATCH so retries during onboarding don't double-create.
- **Rate limiting** at the gateway — abusive clients scraping the geo index would be a real problem.
- **Auth** — public endpoints behind app-attestation tokens, admin behind mTLS + RBAC.
- **Compact response.** I deliberately return ~1KB summaries, not full restaurant objects. Detail is a separate fetch, cached.

---

## Phase 4 — High-level design (8–10 min)

### What I'd say

> "I'm going to start with the simplest thing that could work and add components only when the numbers force me to."

### Step 1 — Naive

```
[ Client ] → [ API server ] → [ Postgres with PostGIS ]
```

> "PostGIS GIST index on the lat/lng column — works fine up to maybe a few hundred QPS. We have 18K peak. Each query is `ST_DWithin(...)` which is ~5–20ms on a hot index. At 18K QPS that's saturating connection pools and we have no headroom. Also a single Postgres is a SPOF."

### Step 2 — Add load balancing + read replicas

```
[ Client ] → [ LB ] → [ API ×N ] → [ PG primary ]
                                  → [ PG replicas ×K ]   (reads)
```

> "Better, but Postgres replicas still cap us around a few thousand QPS each, and we're paying disk-page costs per query. Given the data fits in 25 GB of RAM, we should be doing this in-memory."

### Step 3 — Introduce an in-memory **Geo Index Service**

This is the architecture I'd commit to. I draw it big and explain each box.

```
                         ┌─────────────┐
                         │   Mobile /  │
                         │   Web app   │
                         └──────┬──────┘
                                │ HTTPS
                                ▼
                       ┌─────────────────┐
                       │   CDN (Akamai)  │  ← static, menu thumbnails
                       └────────┬────────┘
                                ▼
                       ┌─────────────────┐
                       │  Edge LB / GW   │  ← TLS, auth, rate-limit
                       └────────┬────────┘
                                ▼
        ┌───────────────────────┴───────────────────────┐
        ▼                                               ▼
┌───────────────┐                              ┌────────────────┐
│ Search Service│                              │ Restaurant Svc │
│   (stateless) │                              │  (CRUD, admin) │
└───────┬───────┘                              └────────┬───────┘
        │ gRPC                                          │
        ▼                                               ▼
┌─────────────────────────┐                    ┌────────────────┐
│  Geo Index Service      │◀───────reads───────│   Postgres     │
│  (in-memory quadtree)   │                    │  (source of    │
│   sharded + replicated  │                    │   truth, sharded│
└──────┬──────────────────┘                    │   by region)   │
       │ details by ID                         └───────┬────────┘
       ▼                                               │
┌──────────────┐                                       │
│ Redis cache  │◀──────────────────────────────────────┘
│ (restaurant  │   (write-through / TTL refresh)
│   details)   │
└──────────────┘
       ▲
       │
       │  ┌───────────────┐    update events      ┌────────────────┐
       └──│ Kafka topic:  │◀─────────────────────│ Restaurant Svc │
          │ restaurant.   │                       │   on every     │
          │ updates       │                       │   write        │
          └───────┬───────┘                       └────────────────┘
                  │
                  ▼
        ┌───────────────────┐
        │ Geo Index workers │  ← apply incremental updates
        │  (one per shard)  │     to in-memory quadtree
        └───────────────────┘
```

### Walkthrough of the two main flows

**Read flow — "show me restaurants near me"**

1. App sends `GET /nearby?lat,lng,…` to Edge LB.
2. LB → stateless **Search Service**.
3. Search Service routes to the **Geo Index shard** that owns the user's region (consistent-hash on coarse cell ID).
4. Geo Index does a **quadtree range query** → returns ~50–100 candidate restaurant IDs (over-fetch to allow filtering + ranking).
5. Search Service hydrates each ID from **Redis** (LRU-cached restaurant summaries). Misses fall through to Postgres.
6. Apply filters (cuisine, rating, open-now) → rank by distance/ETA → trim to 20 → return.

**Write flow — "Tony's just closed for the night"**

1. Restaurant ops calls `PATCH /admin/restaurants/r_123 {status: closed}`.
2. **Restaurant Service** writes Postgres (source of truth) in a transaction.
3. Same transaction (transactional outbox) emits to **Kafka** topic `restaurant.updates`.
4. Each **Geo Index worker** consumes the partition for its shard and applies the mutation to the in-memory quadtree (mark closed, or remove if shut down permanently).
5. Redis cache is invalidated for that restaurant ID.
6. End-to-end staleness: typically <2s, bounded by Kafka lag.

### Why each component exists (I say each one out loud)

| Component | Why |
|---|---|
| CDN | Thumbnails / static menu images dominate bytes; offload from origin |
| Edge LB / Gateway | TLS termination, app-attestation, rate-limit, geo-routing to nearest region |
| Search Service (stateless) | Scale horizontally; handles ranking + filtering; isolated from index lifecycle |
| Geo Index Service | The crown jewel — in-memory quadtree, p99 <10ms range queries |
| Restaurant Service | CRUD, owns Postgres, only writer to truth |
| Postgres (sharded) | Source of truth, ACID for onboarding, recoverable |
| Redis | Restaurant summary cache (the rows fetched after geo lookup) |
| Kafka | Decouples writes from index fanout; lets us replay to bootstrap nodes |

### Acknowledging hand-waving (senior signal)

> "Two things I'm waving my hands at right now and want to come back to: (1) **how the quadtree handles sharding when New York is 30× denser than Wyoming**, and (2) **how a brand new geo-index node bootstraps its tree from cold**. Those belong in the deep dive."

---

## Phase 5 — Database / data-model design (5–6 min)

### Source-of-truth schema (Postgres, sharded by `region_id`)

```sql
-- restaurants (one row per location)
CREATE TABLE restaurants (
  id              UUID PRIMARY KEY,
  region_id       SMALLINT NOT NULL,        -- shard key (e.g. metro)
  name            TEXT NOT NULL,
  lat             DOUBLE PRECISION NOT NULL,
  lng             DOUBLE PRECISION NOT NULL,
  geohash         CHAR(12) NOT NULL,        -- precomputed, indexed
  cuisines        TEXT[] NOT NULL,
  price_tier      SMALLINT,                 -- 1..4
  rating          NUMERIC(2,1),
  status          SMALLINT NOT NULL,        -- 0=live 1=closed_now 2=paused 3=offboarded
  opens_at        TIME,
  closes_at       TIME,
  updated_at      TIMESTAMPTZ NOT NULL,
  version         BIGINT NOT NULL           -- for optimistic concurrency + Kafka ordering
);
CREATE INDEX ix_geohash ON restaurants USING BTREE (geohash);
CREATE INDEX ix_region   ON restaurants (region_id, status);

-- transactional outbox for Kafka
CREATE TABLE restaurant_outbox (
  id BIGSERIAL PRIMARY KEY,
  restaurant_id UUID,
  payload JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  delivered_at TIMESTAMPTZ
);
```

### Why these choices

- **Geohash column on disk.** Even though the hot path uses the in-memory quadtree, Postgres is what we rebuild from. Geohash + btree gives prefix-range scans that map naturally to "rebuild a tile of the quadtree".
- **`version` column.** Updates are out-of-order through Kafka. The geo index must reject stale events: `if event.version <= current.version: drop`.
- **Transactional outbox.** Guarantees Postgres write ⇒ Kafka emit. Without it we get the dual-write problem and silent index drift.
- **`region_id` as shard key.** A user only ever queries one region; cross-region traffic is rare.

### What I'd draw

```
restaurants                       restaurant_outbox
┌──────────────────────┐          ┌──────────────────┐
│ id (UUID, PK)        │          │ id (BIGSERIAL)   │
│ region_id (shard)    │          │ restaurant_id    │
│ lat, lng             │          │ payload (JSONB)  │
│ geohash (BTREE)      │ ────────▶│ created_at       │  → CDC poller →  Kafka
│ status, version      │          │ delivered_at     │
│ cuisines[], rating   │          └──────────────────┘
│ ...                  │
└──────────────────────┘
```

### Geo-index data model (in-memory)

This is **derived state**, not source of truth. Per shard:

```
QuadtreeNode:
  bbox: { lat_min, lat_max, lng_min, lng_max }
  is_leaf: bool
  if leaf:
    restaurants: [ {id, lat, lng, status, …minimal fields…} ]    // capacity ≤ N (e.g. 100)
  else:
    children: [NW, NE, SW, SE]
```

I keep **only the fields needed for filtering + ranking** in the index (id, lat/lng, status, cuisines bitset, price tier, rating). The full restaurant blob lives in Redis/Postgres. This keeps each shard's working set small and cache-friendly.

---

## Phase 6 — Deep dive: the geospatial index (22–25 min)

This is what the bar-raiser actually wants. I'd spend the most time here and treat it as four sub-deep-dives.

### 6.1 — Choice of index: quadtree vs geohash vs PostGIS R-tree vs Uber's H3

I open with the comparison because the interviewer prompt mentioned both quadtree and geohash.

| Index | Idea | Pros | Cons | When |
|---|---|---|---|---|
| **PostGIS / R-tree** | On-disk balanced spatial tree in DB | Zero ops; ACID; range + kNN built-in | DB-bound; tens of ms per query; hard to scale to 18K QPS | Small/mid scale |
| **Geohash (string-prefix index)** | Encode lat/lng as base-32 string; same prefix ⇒ nearby | Trivial to shard, store in any KV (Redis, DynamoDB); no rebalancing | Boundary effects (two adjacent points can have totally different prefixes); fixed grid → can't adapt to density | Simple workloads, write-heavy |
| **Quadtree** | Recursive 4-way space partition; leaves split when dense | **Adapts to density** (Manhattan splits deeper than Wyoming); fast in-memory; natural range query | Stateful; rebuild/rebalance is non-trivial; serialization is custom | Read-heavy, density-skewed (this!) |
| **H3 / S2** | Hierarchical hex/cube grid | Uniform cell shape; well-tooled at Uber; great for sharding | Fixed grid (like geohash) — doesn't auto-split | **Sharding key** |

**My pick — and what I'd say:**

> "I'd use a **two-layer scheme**: H3 cells at a coarse resolution (say level 5, ~250 km²) as the **sharding key**, and inside each shard an in-memory **quadtree** for the actual range query. H3 gives me clean shards and uniform routing; the quadtree gives me density-adaptive depth so a query in Manhattan and a query in Wyoming both touch ~the same number of nodes. Geohash would also work for sharding, but I'd still want the quadtree under it. Since the prompt mentioned Uber already uses quadtrees, and I know they actually use H3 at the routing layer, this matches their stack."

This sentence does a lot of work — it shows I know the landscape, picked deliberately, and didn't just memorize one answer.

### 6.2 — Quadtree mechanics

#### Structure & invariants

- Each **leaf** holds up to `CAPACITY` restaurants (I use **100** — enough that we're not making the tree pathologically deep, small enough that scanning a leaf is microseconds).
- Each **internal node** has exactly 4 children: NW, NE, SW, SE (split on midpoint of bbox).
- **Max depth** capped (e.g., 20) to bound worst case in pathologically dense spots like Times Square.

#### Insert

```
insert(node, r):
  if node.is_leaf:
    node.restaurants.append(r)
    if len(node.restaurants) > CAPACITY and node.depth < MAX_DEPTH:
      split(node)
  else:
    child = node.child_containing(r.lat, r.lng)
    insert(child, r)

split(node):
  create 4 children with quartered bboxes
  for r in node.restaurants:
    insert into appropriate child
  node.is_leaf = False
  node.restaurants = None
```

#### Range query (the hot path)

```
query(node, center, radius):
  if not node.bbox.intersects_circle(center, radius):
    return []
  if node.is_leaf:
    return [r for r in node.restaurants if dist(r, center) <= radius]
  results = []
  for child in node.children:
    results += query(child, center, radius)
  return results
```

#### Drawing the tree

```
                  ┌───────── world bbox ─────────┐
                  │                              │
                  │             ROOT             │
                  │           (internal)         │
                  └──────┬──────┬──────┬──────┬──┘
                         │      │      │      │
                   ┌─────▼┐  ┌──▼──┐ ┌─▼──┐ ┌─▼──┐
                   │ NW   │  │ NE  │ │ SW │ │ SE │
                   │leaf  │  │intnl│ │leaf│ │leaf│
                   │ 47   │  │     │ │ 12 │ │ 3  │
                   │restos│  └──┬──┘ └────┘ └────┘
                   └──────┘     │
                       ┌────────┼────────┬────────┐
                     ┌─▼─┐   ┌─▼─┐    ┌─▼─┐    ┌─▼─┐
                     │NW │   │NE │    │SW │    │SE │
                     │95 │   │88 │    │101│    │76 │  ← 101 over capacity → next split
                     └───┘   └───┘    └───┘    └───┘
```

I'd say: *"Manhattan ends up at depth 12–14; rural Nevada at depth 3–4. That's exactly the property we want — query work is bounded by leaf size, not by absolute density."*

#### Why leaf capacity 100 and not 1?

> "Two reasons. First, fewer pointers — better cache locality, fewer allocations. Second, less rebalancing churn on inserts/deletes. The trade-off is that a leaf scan is now O(100) instead of O(1), but at ~50ns per restaurant comparison, that's 5µs — negligible vs the tree descent itself."

### 6.3 — Updates: closing, opening, moving restaurants

This is where the interviewer dug in. I structure it carefully.

#### Three classes of update

| Update | Frequency | Action |
|---|---|---|
| Status flip (open ⇄ closed_now) | Hourly per restaurant — **dominant** | Mutate restaurant record in leaf; **no structural change** |
| Onboard / offboard | ~10K/day | Insert / remove; **may trigger split or merge** |
| Move location | Rare | Remove + insert |

The first class is by far the most common and is **structurally trivial** — flip a bit. I make sure to call this out, because it's a common interview trap to assume every update rebuilds something.

#### Soft delete vs hard delete on shutdown

I'd recommend:

- On `status = closed_now`: **mutate in place**. Don't remove from tree. Search filters it out via `open_now=true`. Avoids churn (the restaurant will reopen in 8 hours).
- On `status = offboarded` (permanent): **remove**, but **lazily**. Tombstone the entry, batch real removals every N minutes.

#### Splits & merges with hysteresis

Naive merge — "if leaf count drops below CAPACITY/2, merge with siblings" — causes oscillation when a restaurant flips between two leaves repeatedly. So:

- **Split threshold:** > 100
- **Merge threshold:** < 25 (across all 4 siblings combined)
- Hysteresis gap prevents thrash.
- Merges run as a **background sweep**, not on the write path.

#### Concurrency model

This is something a bar-raiser will probe. The geo index is in-memory and read-heavy:

> "I'd use a **single-writer, many-reader** model per shard. All Kafka updates for a shard land on one consumer thread. Readers use `RCU`-style snapshots: the writer publishes a new immutable subtree, readers atomically swap a pointer. No locks on the read path. P99 stays clean."

Alternative I'd mention: per-leaf RW locks. Simpler, but locking overhead at 18K QPS adds up. RCU wins for our R:W ratio.

#### What I'd draw

```
WRITE PATH (per shard, single writer)
  Kafka partition ──▶ apply(event) ──▶ mutate leaf ──▶
                                          │
                                          ├─ if leaf count > 100  ─▶ split (build new subtree)
                                          ├─ if subtree count < 25 ─▶ enqueue for bg merge
                                          └─ atomic publish (pointer swap)

READ PATH (lock-free)
  request ──▶ read root pointer ──▶ traverse ──▶ result
```

### 6.4 — Bootstrapping a new node + serialization (THE deep dive the interviewer cares about)

This is the part you flagged. I'd treat it as a formal design problem.

#### The problem

A new geo-index node spins up (autoscale, replacement after crash, deploy). It needs the full quadtree for its shard (~5M / N_shards restaurants). Three constraints:

1. Be queryable **fast** (target: under 60s from pod start).
2. Don't hammer Postgres on every cold start.
3. Don't miss updates that arrived during bootstrap.

#### Three approaches I present

**Approach A — Rebuild from Postgres**

```
boot:
  rows = SELECT * FROM restaurants WHERE region_id IN shard_regions  -- streamed
  for r in rows: insert(tree, r)
  start_consuming(kafka, from_offset=now())
```

Pros: simple, always fresh.
Cons: 5M rows / N shards is still 100K–500K rows; Postgres I/O at every node start; ~2–5 min cold start. Painful on autoscale events.

**Approach B — Snapshot from object store**

Periodically (every 15 min), the **leader** of each shard serializes its tree to S3. New nodes:

```
boot:
  snap, snap_offset = S3.get_latest(shard_id)
  tree = deserialize(snap)
  start_consuming(kafka, from_offset=snap_offset)   // catch up the gap
```

Pros: 10–20s cold start; doesn't touch Postgres.
Cons: snapshot can drift; need to track Kafka offset alongside snapshot.

**Approach C — Peer state transfer ("borrow the tree from a neighbor")**

```
boot:
  peer = service_discovery.healthy_replica(shard_id)
  stream tree from peer over gRPC
  start consuming kafka from peer.last_offset
```

Pros: always fresh; no S3 dep.
Cons: load on peers; harder when zero healthy peers exist (cold-start of an entire shard).

**My recommendation:**

> "Use **B as primary, C as fallback, A as disaster recovery.** Snapshot to S3 every 15 min. New nodes hydrate from snapshot in ~15s, then consume Kafka from the snapshot's recorded offset to catch up. If the snapshot is missing or corrupt, fall back to peer transfer. If neither works (cold-start an entire region), rebuild from Postgres."

#### Serialization format

This is a sub-deep-dive that the interviewer specifically asked about. I'd whiteboard it.

**Wire format — DFS pre-order, length-prefixed, Protobuf-encoded**

```
Header:
  magic        : u32   ("QTRE")
  version      : u16
  shard_id     : u32
  kafka_offset : u64    ← critical: where Kafka was when snapshot was taken
  bbox         : {lat_min, lat_max, lng_min, lng_max}
  count        : u32   (total restaurants — for sanity check)

Body (DFS pre-order):
  for each node:
    tag : u8           # 0 = internal, 1 = leaf
    if internal:
      bbox (4 doubles)
      [4 children written next, in NW, NE, SW, SE order]
    if leaf:
      bbox (4 doubles)
      n   : u16
      for each restaurant:
        id     : 16 bytes (UUID)
        lat    : f64
        lng    : f64
        status : u8
        # everything else (name, menu) is fetched from Redis on read
```

I draw this:

```
  [HEADER]
   ├─ magic "QTRE"
   ├─ version
   ├─ shard_id
   ├─ kafka_offset  ◀── must catch up from here on boot
   └─ bbox

  [BODY: DFS pre-order]
   ROOT(internal,bbox)
     NW(leaf, bbox, [r1,r2,...])
     NE(internal,bbox)
        NW(leaf,...)  NE(leaf,...)  SW(leaf,...)  SE(leaf,...)
     SW(leaf,...)
     SE(leaf,...)
```

#### Why DFS pre-order and not BFS?

> "Pre-order means I can deserialize with a single recursive descent and a single forward read of the byte stream — no random access, no offsets to fix up. It also streams: I can start building the tree before the whole file lands, useful at 100s of MB."

#### Why Protobuf and not JSON?

- 5–10× smaller on the wire; matters when this snapshot is 100s of MB and pulled by every booting node.
- Strict schema → can evolve safely (`version` field).

#### Compression & storage

- gzip the snapshot before upload (typically 3–5× on this kind of data).
- Snapshot per shard, named `snapshots/<shard_id>/<timestamp>.qtre.gz`.
- Retain last 3 snapshots for rollback.

#### The crux: catching up updates during bootstrap

This is the subtle correctness problem.

```
T0  : leader serializes tree, records kafka_offset = 1,000,000
T0+5: snapshot uploaded to S3
T1  : new node downloads snapshot, deserializes
T1+ : new node subscribes to Kafka starting at offset 1,000,000

→ Any updates between offset 1,000,000 and now get replayed.
→ Reaches "live" when consumer lag → 0.
→ DO NOT serve search traffic until lag < threshold (e.g. 1s).
```

I'd add a **readiness gate** that only flips the pod to "ready" once Kafka lag is under threshold. This is the operational detail that separates a working design from a robust one.

#### What I'd draw for the bootstrap flow

```
   ┌──────────┐                          ┌────────┐
   │  Leader  │ every 15 min ──serialize▶│   S3   │
   └──────────┘                          └────┬───┘
                                              │
   New node boots                             │
       │                                      │
       │  1. fetch latest snapshot ───────────┘
       │     → got tree + kafka_offset = K
       │
       │  2. subscribe to Kafka from offset K
       │     → catch up to head
       │
       │  3. wait until consumer_lag < 1s
       │
       └─▶ flip readiness probe to OK
                │
                ▼
         join LB pool, serve traffic
```

### 6.5 — A few extras I'd offer if there's time (or as the interviewer probes)

- **Hot region**: Manhattan/Mumbai have orders of magnitude more restaurants. Solution: the H3 sharding layer can split a hot cell into multiple shards. The geo index inside is unchanged.
- **kNN vs range**: I described range query (`within radius`). For k-nearest, do an iterative widening: start at a small radius, expand until ≥k results. Or maintain a min-heap during traversal.
- **Filters at index time vs after**: I push high-selectivity filters (e.g., cuisine) into the leaf scan as a bitset check; low-selectivity (rating) is post-filter.
- **Ranking**: distance is one signal; ETA, popularity, sponsored placement layer on top. Ranking is a separate service, downstream of geo.
- **Failure modes**: if geo index is unreachable, fall back to PostGIS query — slow but correct. Better than 500.

---

## Phase 7 — Wrap-up (4–5 min)

### What I'd say

> "Let me recap, call out the bottlenecks I see, and what I'd build next."

### Recap (30 seconds)

> "We have an in-memory quadtree per shard, sharded by H3 cells, fed by Kafka updates from a Postgres source-of-truth via a transactional outbox. Search Service hits the geo index for IDs and hydrates from Redis. New nodes bootstrap from S3 snapshots that pin a Kafka offset, then catch up before going live. P99 search well under 200ms; eventual consistency window <2s; survives a region failure via per-region replicas."

### Bottlenecks & risks I'm aware of

| Risk | Mitigation |
|---|---|
| **Hot shard during dinner rush in NYC** | Pre-split hot H3 cells; over-provision geo index replicas in those shards |
| **Kafka lag spike → stale index** | Monitor consumer lag; alarm at >5s; auto-scale workers |
| **Snapshot corruption** | Checksums; keep last 3; fall back to peer transfer / Postgres rebuild |
| **Restaurant write storm (e.g., bulk hours update)** | Rate-limit at admin API; batch in outbox poller |
| **Cross-shard queries near boundaries** | Router fans out to neighbor shards if query radius crosses cell boundary; merge results |
| **Cold start of an entire region** | Multi-region snapshots in S3 with cross-region replication |

### What I'd build next

1. **Personalized ranking** — currently we sort by distance/ETA. Plug a model service that scores `(user, restaurant)` pairs and resorts the top-100.
2. **Free-text search** — add an Elasticsearch index for `"biryani near me"`. The geo filter pushes down as a `geo_distance` query.
3. **Real-time ETA service** — currently we use static drive-time estimates; integrate live courier supply.
4. **Multi-tenant isolation** — separate geo index for groceries / pharmacy, same machinery.

### Curveball Q's I'd be ready for

- *"What if your in-memory tree is too big?"* → Drop fields per restaurant in the index; or split shards finer.
- *"What if a restaurant moves a few meters and pings 1000 updates/sec?"* → Debounce in Restaurant Service; coalesce updates in outbox.
- *"How do you A/B test a new ranking?"* → Ranking is downstream of geo; route a fraction of traffic to ranker_v2 in Search Service.
- *"Walk me through deploying a quadtree code change."* → Blue/green at the shard level. Drain reads, swap binary, hydrate, re-enable. Index is rebuildable so we can canary aggressively.

---

## Cheat-sheet: what to write on the board, in order

```
1. SCOPE / NFR box                            (top-left,  stays visible)
2. Numbers box (QPS, R:W, GB)                 (under scope)
3. API list                                   (right side, stays visible)
4. Architecture diagram                       (center, biggest)
5. Schema box                                 (bottom-left)
6. Quadtree drawing + leaf/split visual       (center, replace HLD when entering deep dive)
7. Bootstrap flow diagram + serialization     (right)
8. Bottlenecks list                           (final 3 min, bottom-right)
```

## Common mistakes I will NOT make

- Jumping into quadtree before scoping. (Bar-raisers eat candidates who skip Phase 1.)
- Pretending the design has no trade-offs. Every choice gets a "we lose X to gain Y."
- Going silent while drawing. I narrate every box.
- Over-engineering. No service mesh, no event sourcing, no CQRS unless asked. Simple wins.
- Forgetting the **`version` field / Kafka offset / readiness gate** triad. These are the unsexy correctness pieces that separate junior from senior.
- Conflating geohash and quadtree as either-or. They compose: H3/geohash for sharding, quadtree inside.

## One-line summary of the whole design

> *Sharded in-memory quadtree fed by Kafka from a Postgres source of truth via a transactional outbox, bootstrapped from versioned S3 snapshots that pin a Kafka offset, fronted by a stateless search service that hydrates from Redis.*

If you can say that sentence and then justify every clause, you're SDE2-ready.
