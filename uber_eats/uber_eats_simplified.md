# Uber Eats / Nearby Restaurant Search — HLD Interview Script

> **How to use this doc:** Each section has **"What to say"** (the actual lines you deliver) and **"How to say it"** (delivery cues, what to draw, what to emphasize). Stick to the time-boxes from the framework so you don't blow the 50-min window.

---

## 1. Requirements Clarification (3–5 min)

### How to say it
Open by anchoring the problem. Don't jump to design. Say: *"Before I design, let me lock down the scope so we're solving the same problem."* This buys you thinking time and signals seniority.

### Functional Requirements — what to say

> "I'll focus on the core flow. A user opens the app and we show nearby restaurants based on their location. They can search and filter, view a restaurant's menu, and place an order. Restaurant owners can onboard, update menus, and toggle availability."

List these 5–6 points on the board:
1. **Discover** — User sees a ranked list of restaurants near their current location (default radius ~5 km).
2. **Search & Filter** — User searches by cuisine, restaurant name, or dish; filters by rating, delivery time, price.
3. **View Menu** — User opens a restaurant and sees its menu items with prices and availability.
4. **Place Order** — User adds items to cart and checks out (we'll keep payment/delivery tracking out of scope).
5. **Restaurant Onboarding** — Restaurants can register, update their location, menus, and open/close status in real time.
6. **Real-time Availability** — When a restaurant goes offline (closed, overloaded), it disappears from search within seconds.

### 2–3 Clarifying Questions — what to say

> "A few things I want to confirm before estimating scale:"
1. *"Are we designing for global scale across cities, or a single region first? I'll assume global with regional sharding."*
2. *"For 'nearby' — is the radius user-configurable or fixed? I'll assume a fixed default (5 km) with a max of 20 km."*
3. *"Do we need to rank by ETA, rating, sponsored/ads — or is geo-distance enough for v1? I'll start with geo + rating, leave ranking pluggable."*

**How to say it:** Pause after each question. Don't answer them yourself unless the interviewer waves you on. If they say "your call," state your assumption clearly and move on.

### Non-Functional Requirements — what to say

> "Now the system qualities."

- **Scale:** ~100M DAU, ~10M restaurants globally, peak QPS ~500K for nearby-search reads.
- **Latency:** p99 < 200ms for nearby-search; menu fetch < 300ms; order placement < 500ms.
- **Availability:** 99.99% — read path must stay up even if write path degrades.
- **Consistency:** Eventual consistency is fine for restaurant location/menu updates (a 5–10s lag is acceptable). **Strong consistency** required for orders and inventory.
- **Domain-specific:**
  - **Geospatial correctness:** No restaurant >5 km away should appear in the "nearby" list — false positives hurt UX more than missed entries.
  - **Hot regions:** Manhattan/Bangalore have 1000× the restaurant density of suburban areas — design must not collapse on density skew.

**How to say it:** Write these as bullets, not sentences. Interviewers scan the board.

---

## 2. Estimation (2–3 min)

### How to say it
Don't drown in math. Round aggressively. *"Order-of-magnitude is what matters here."*

### Traffic (QPS) — what to say
- 100M DAU × ~10 search requests/day = **1B requests/day ≈ 12K QPS average**.
- Peak (lunch + dinner): ~5× average = **~60K QPS sustained**, **~500K QPS spike** during peak meal hours in dense regions.
- Read:Write ratio ≈ **100:1** (most traffic is "nearby search," writes are restaurant updates).

### Storage — what to say
- **Restaurants:** 10M × ~2 KB metadata = **~20 GB**.
- **Menu items:** 10M restaurants × ~50 items × ~1 KB = **~500 GB**.
- **Geospatial index (quadtree):** 10M points, each node ~100 bytes, ~2× overhead for tree structure = **~2 GB** — fits in memory per region.
- **Orders:** 10M orders/day × 1 KB × 365 days × 3 yrs retention = **~10 TB**.

### Domain-specific
- **Bandwidth:** Each search response carries ~50 restaurants × 500 bytes = ~25 KB. At 60K QPS = **~1.5 GB/sec egress** — CDN/edge-cache the result page.

**How to say it:** Just write the final numbers. Skip the multiplication on the board.

---

## 3. Core Entities (3–5 min) — Bottom-Up

### How to say it
> "Let me list the entities I'll use as the vocabulary for the rest of the design."

1. **User** — `userId, name, location (lat/lng), addresses, paymentMethods`
2. **Restaurant** — `restaurantId, name, location (lat/lng), cuisine, rating, isOpen, geohash`
3. **MenuItem** — `itemId, restaurantId, name, price, isAvailable, tags`
4. **Order** — `orderId, userId, restaurantId, items[], status, totalAmount, createdAt`
5. **Cart** — `cartId, userId, items[], restaurantId` (transient)
6. **Review** — `reviewId, userId, restaurantId, rating, comment`
7. **GeoCell** (quadtree node) — `cellId, bounds, restaurantIds[], childCells[]`

**How to say it:** Note the `geohash` on Restaurant and the explicit `GeoCell` entity — this signals you're already thinking about the geospatial deep dive.

---

## 4. API Design (3–5 min)

### How to say it
> "I'll list the public APIs. Internal service-to-service calls I'll cover in the architecture."

1. **Search Nearby Restaurants**
   - Returns restaurants near a given lat/lng with optional filters; cursor-paginated for infinite scroll.
   - `GET /v1/restaurants/nearby?lat=..&lng=..&radius=5km&cuisine=pizza&cursor=..` → `{ restaurants: [...], nextCursor }`

2. **Search by Keyword**
   - Full-text search across restaurant names + dish names, scoped to a geo radius.
   - `GET /v1/search?q=pizza&lat=..&lng=..` → `{ restaurants: [...], dishes: [...] }`

3. **Get Restaurant Details**
   - Returns full restaurant info + menu in one call (used on restaurant page open).
   - `GET /v1/restaurants/{restaurantId}` → `{ restaurant, menu: [...] }`

4. **Create Order**
   - Idempotent order placement; client passes an idempotency key to avoid double-charging.
   - `POST /v1/orders` body `{ restaurantId, items, addressId, paymentMethodId, idempotencyKey }` → `{ orderId, status }`

5. **Update Restaurant Availability**
   - Restaurant-side toggle for open/close; propagates to search index within seconds.
   - `PATCH /v1/restaurants/{id}/availability` body `{ isOpen: false }` → `{ ok: true }`

6. **Update Menu Item**
   - Adds/edits/removes a dish; triggers search index update.
   - `PUT /v1/restaurants/{id}/menu/{itemId}` body `{ name, price, isAvailable }`

7. **Get Order Status**
   - Polling or websocket for live status (we'll keep it simple: polling).
   - `GET /v1/orders/{orderId}` → `{ status, eta }`

**How to say it:** For each API, deliver the two lines: *(1) what it does, (2) request/response shape*. Don't pad. Interviewers love crisp APIs.

---

## 5. High-Level Design (10–12 min)

### How to say it
Draw left-to-right: **Client → API Gateway → Services → Data Stores**. Talk while drawing — don't go silent.

### Services (5–8)

> "I'll split this into focused services. Each owns one concern."

1. **API Gateway** — auth, rate-limit, request routing.
2. **Search Service** — handles nearby-search and keyword search; queries the geo index.
3. **Restaurant Service** — CRUD on restaurants, menus, availability toggling.
4. **Geo Indexing Service** — owns the quadtree; rebuilds it on restaurant changes, serves geo lookups to Search Service.
5. **Order Service** — order placement, idempotency, state machine.
6. **User Service** — profile, addresses, payment methods.
7. **Notification Service** — order status pushes to user.
8. **Ranking Service** — re-ranks search candidates by ETA, rating, sponsored — pluggable.

### Database Layer

> "Different data shapes need different stores. Polyglot persistence."

| DB | Stores | Why this DB |
|---|---|---|
| **PostgreSQL (Restaurant DB)** | Restaurants, MenuItems | Relational; menus are normalized; we need joins, transactions on menu edits. Sharded by `restaurantId`. |
| **PostgreSQL (Order DB)** | Orders | Strong consistency + ACID for payments. Sharded by `userId`. |
| **Cassandra (User-history / Reviews)** | Past orders, reviews | Write-heavy, append-only, time-series-ish. Wide-column fits. Scales horizontally. |
| **Elasticsearch (Search Index)** | Restaurant names, menu item names, tags | Inverted index for keyword search + geo-distance filter. Built for this. |
| **In-memory Quadtree (Geo Indexing Service)** | All restaurant `(lat,lng) → restaurantId` | Sub-millisecond geo lookups. Built from Restaurant DB; persisted snapshots to S3 for fast cold-start. |
| **Redis** | Hot restaurant pages, search results, session | Sub-ms reads, TTL-friendly. |

> "I considered MongoDB for restaurants — geo support is built-in via 2dsphere indexes — but at our scale a custom in-memory quadtree gives 10× lower p99, and Postgres for the source of truth gives me transactional menu edits. So I split: **Postgres = source of truth, in-memory quadtree = serving layer.**"

### Schemas (just the key ones — write on board)

```
Restaurant (Postgres)
  restaurantId PK, name, lat, lng, geohash (indexed), cuisine, rating, isOpen, updatedAt

MenuItem (Postgres)
  itemId PK, restaurantId FK (indexed), name, price, isAvailable, tags

Order (Postgres)
  orderId PK, userId, restaurantId, items JSONB, status, total, createdAt
  unique(idempotencyKey) -- for safe retries

GeoCell (in-memory, also serialized to S3)
  bounds: {minLat, maxLat, minLng, maxLng}
  restaurants: [restaurantId, lat, lng]  -- only at leaf
  children: [NW, NE, SW, SE] | null
```

### Cache Layer

1. **Redis (search results cache)** — sits between Search Service and the geo index / Postgres. Cache-aside: Search Service checks Redis first, on miss queries the index, writes back with 30s TTL. Key: `nearby:{geohash6}:{cuisine}:{filters}`.
2. **Redis (restaurant detail cache)** — sits in front of Restaurant Service. TTL 5 min; invalidated on menu edit via pub/sub from Restaurant Service.
3. **CDN (static menu images)** — restaurant photos and dish images served from CloudFront/Cloudflare.

### Queue Layer

1. **Kafka — `restaurant-updates` topic** — sits between Restaurant Service and downstream consumers (Geo Indexing Service, Elasticsearch indexer, cache invalidator). Fan-out pattern: any availability/menu change publishes once, all indexes catch up asynchronously. **This is what makes "restaurant goes offline" propagate in <5s.**
2. **Kafka — `order-events` topic** — sits between Order Service and Notification, Restaurant, Analytics services. Decouples order placement from downstream effects.

### Architecture Diagram — what to draw

Draw, in order:
```
[Client]
   ↓
[API Gateway] ─────────────────────┐
   ↓                               │
[Search Svc] ──→ [Redis] ──→ [Geo Indexing Svc (quadtree, in-mem)]
   ↓                                ↑
[Elasticsearch]                     │
                                    │  (consumes)
[Restaurant Svc] ──→ [Postgres] ───→[Kafka: restaurant-updates]──→[ES indexer]
                                                                 ──→[Cache invalidator]
[Order Svc] ──→ [Postgres] ──→ [Kafka: order-events] ──→ [Notification Svc]
```

**Use:** boxes for services, cylinders for DBs, parallelograms for queues, arrows for data flow. Label each arrow with sync/async.

---

## 6. Deep Dives & Trade-offs (12–15 min) — **THE QUADTREE SECTION**

### How to say it
This is where the interview is won or lost. The interviewer already steered you here. Lead with: *"The geospatial index is the heart of the read path. Let me go deep on three things: (1) the quadtree itself, (2) how we handle updates and rebuilds, (3) serialization and node bootstrapping."*

---

### Deep Dive 1 — Quadtree as the Geo Index

**(1) What it is and where it sits**

> "The quadtree lives in-memory inside the **Geo Indexing Service**. Each node represents a rectangular geographic region (bounds: `[minLat, maxLat, minLng, maxLng]`). A node holds restaurants directly until it exceeds a capacity threshold — say **100 restaurants per leaf**. When it overflows, it splits into 4 children (NW, NE, SW, SE) and redistributes."

**(2) Interactions**

> "Search Service receives a `nearby(lat, lng, radius)` request. It calls the Geo Indexing Service, which traverses the tree top-down: at each node it checks if the query circle intersects the node's bounds; if yes, recurse into matching children; at leaves, collect restaurants and filter by exact distance. Result is a candidate list — Ranking Service then re-orders by ETA + rating."

**(3) Why it's critical**

> "At 10M restaurants, a linear scan or even a Postgres `ST_DWithin` query can't hit our 200ms p99. Quadtree gives us **O(log N + K)** where K is restaurants in the radius — typically <100. In Manhattan, density skew means a uniform geohash grid has hot cells with thousands of restaurants; the quadtree adapts because dense regions split deeper, sparse regions stay shallow. **This adaptive partitioning is exactly why Uber-style systems prefer quadtrees over fixed-grid geohashing.**"

**(4) Other approaches & trade-offs**

> "I considered three alternatives:"

| Approach | Pros | Cons |
|---|---|---|
| **Geohash + Redis GEO** | Trivial to implement, Redis-native. | Fixed grid — bad for density skew; boundary queries hit 9 cells (the query cell + 8 neighbors). |
| **R-tree** | Handles polygons, not just points. Better for delivery zones. | More complex updates (splits/merges), worse cache locality than quadtree for points. |
| **Postgres PostGIS** | One DB, no extra service. | p99 ~50–100ms even with `GIST` index — fine for medium scale, breaks at 500K QPS. |
| **S2 (Google's library)** | Production-grade, hierarchical, used by Uber for H3 work. | Steeper learning curve, harder to reason about on a whiteboard. |

**(5) Recommendation**

> "I'd go with **quadtree** for the in-memory serving layer because (a) point data — we don't need polygon support, (b) skew-adaptive, (c) simple enough to whiteboard and own operationally. **Geohash stays as a secondary key in Postgres** for sharding and for Elasticsearch's geo filter — so we get both. In production I'd actually use **S2 cells**, which generalizes the quadtree idea to a sphere, but quadtree is the right pedagogical answer."

---

### Deep Dive 2 — Updates: Rebuild vs. Incremental

This is the part the interviewer pushed on. Be precise.

**(1) The problem**

> "Restaurants change state constantly: new restaurants onboard, existing ones shut down or go offline temporarily, locations get corrected. Naive question: do we rebuild the tree on every change? No — that's O(N) per update on a 10M-node tree. Unacceptable."

**(2) The strategy — three update paths**

> "I split updates by frequency and impact:"

- **Toggle `isOpen` (high-frequency, ~10K/min):** Don't touch the tree. Mark the restaurant as soft-deleted via a **bitmap / Redis set of `closedRestaurantIds`**. Search Service filters them out post-traversal. Cost: O(1) write, tiny memory.

- **New restaurant or location change (medium-frequency, ~100/min):** **Incremental insert**. Walk the tree to the leaf cell containing the new point, insert. If leaf overflows past capacity, split into 4 children. O(log N).

- **Restaurant permanently shut down (low-frequency, ~10/min):** **Incremental delete**. Find the leaf, remove the entry. If the leaf and its 3 siblings together hold fewer than capacity, **merge them back into the parent**. O(log N).

**(3) Why a periodic rebuild is still needed**

> "Incremental ops drift the tree from optimal balance — repeated inserts in one area without merges leave under-utilized cells. So I'd run a **background rebuild every 6–12 hours per region** on a separate node, then atomically swap the in-memory tree pointer (read-copy-update). During rebuild, reads continue on the old tree; new writes are journaled to a Kafka topic and replayed onto the new tree before swap. Zero downtime, no read latency hit."

**(4) Trade-offs**

> "Alternatives I considered:"
- **Full rebuild on every Nth update** — simple but causes latency spikes.
- **Lock-free concurrent quadtree (e.g., RCU-style)** — best perf, but very tricky to get right; I'd avoid in v1.
- **Sharded trees per region (one tree per city)** — much smaller trees, faster rebuilds, no cross-city queries needed because users don't search across cities. **I'd actually do this from day one.**

**(5) Recommendation**

> "Sharded **per-city quadtrees**, in-memory, with incremental updates + 6-hour background rebuild + atomic swap. `isOpen` toggles bypass the tree entirely via a Redis filter set. This gives us seconds-level freshness on closures and minimizes rebuild cost."

---

### Deep Dive 3 — Serialization, Bootstrap, and Borrowing the Tree

This was the part you spent time on the whiteboard for. Land this cleanly.

**(1) Why serialization matters**

> "The tree lives in memory. If a Geo Indexing Service node restarts, rebuilding from Postgres (10M rows) takes minutes — that's a cold-start hole in availability. Serialization solves this."

**(2) The format**

> "I'd serialize the tree as a **pre-order traversal flat buffer**:"

```
For each node:
  [bounds(4 floats=32B), isLeaf(1B), count(2B)]
  if leaf: [restaurantId(8B), lat(4B), lng(4B)] × count
  else:    recurse into 4 children in fixed order (NW, NE, SW, SE)
```

> "Pre-order means I can deserialize in one pass, allocating children as I go. Fixed child order means I don't need pointers in the file — position implies parent."

**(3) Storage & cadence**

> "Every 30 minutes, the active Geo Indexing Service node serializes its tree to **S3** at `s3://geo-snapshots/{region}/{timestamp}.qt`. Format is **Protobuf with snappy compression** — 10M restaurants ≈ ~200 MB compressed."

**(4) Bootstrap — how a new node joins**

> "When a new Geo Indexing Service node spins up, here's the sequence I'd whiteboard:"

```
1. New node starts, registers with service registry.
2. Pulls latest snapshot from S3 → deserializes (~5s for 200MB).
3. Subscribes to Kafka `restaurant-updates` topic FROM the snapshot's offset.
4. Replays missed events (e.g., last 30 min of updates) to catch up.
5. Once caught up, registers as "ready" in the load balancer.
6. Starts serving traffic.
```

> "Total cold-start: ~10–15 seconds vs. minutes for a Postgres rebuild. **The Kafka offset stored alongside the snapshot is the key trick** — without it, we'd have a gap between snapshot time and ready time."

**(5) Why "borrowing" from a peer is a viable alternative**

> "Instead of S3, the new node could **stream the tree directly from a healthy peer over gRPC**. Pros: even fresher than S3 snapshot. Cons: peer takes a CPU hit during serialization, and we now have a peer-dependency for bootstrap. **I'd use S3 as primary, peer-streaming as a fallback** if S3 is unreachable — defense in depth."

**Trade-offs summary:**
| Approach | Cold-start | Complexity | Failure mode |
|---|---|---|---|
| Rebuild from Postgres | minutes | low | safe but slow |
| S3 snapshot + Kafka replay | ~15s | medium | snapshot must be recent |
| Peer-stream over gRPC | ~10s | high | peer load spike |

**Recommendation:** S3 snapshot + Kafka catch-up. Peer-stream as fallback.

---

### Deep Dive 4 (if time permits) — Hot-Region Sharding & Consistency

> "One more thing — Manhattan has 1000× the density of a suburb. A single per-city quadtree for NYC could hit memory limits. I'd shard **within** dense cities by sub-region (e.g., split NYC into 5 boroughs, each its own tree). The Search Service uses a **geohash prefix → shard map** to route queries. Cross-shard queries (when the radius spans a shard boundary) scatter-gather across at most 4 shards — bounded fan-out."

> "On consistency: the quadtree is eventually consistent with Postgres. Worst case, a closed restaurant shows up for ~5 seconds before the Kafka event propagates. We mitigate by also checking the Redis `closedRestaurantIds` set at request time — that gets updated synchronously when the restaurant toggles."

---

## 7. Wrap-Up (2–3 min)

### How to say it
Don't trail off. Wrap with confidence: *"Let me summarize."*

### Summary — Design (2 points)
1. **Functional fit:** The design supports nearby search, keyword search, menu browse, ordering, and real-time availability via a polyglot stack — Postgres for source-of-truth, in-memory per-city quadtrees for sub-200ms geo queries, Elasticsearch for keyword search, and Kafka for fan-out propagation.
2. **Performance:** p99 < 200ms on nearby-search achieved through (a) in-memory quadtree, (b) Redis search-result cache, (c) per-city sharding to keep trees small.

### Summary — Trade-offs (2 points)
1. **Eventual consistency on availability** — accepted up to ~5s delay; mitigated with a synchronous `closedRestaurantIds` Redis set as a safety net.
2. **Maintainability:** Per-city quadtree shards mean more moving parts than one global tree, but each shard is independently rebuildable and the blast radius of a bad rebuild is one city, not the planet.

### Summary — Performance & Scalability (2 points)
1. **Cache-aside Redis** absorbs hot keys; CDN handles static images; both reduce origin QPS by an estimated 80%.
2. **Horizontal scaling**: Geo Indexing Service is stateless-on-disk (snapshots in S3) and stateful-in-memory — we can add nodes per city by streaming the snapshot. Postgres is sharded by `restaurantId` and `userId` so writes scale linearly.

### Extension (2 points)
1. **ETA prediction & live ranking:** Plug a ML-driven Ranking Service on top of the candidate list — courier supply, traffic, restaurant prep time.
2. **Polygon delivery zones:** Migrate from quadtree (point-based) to S2 / R-tree to support arbitrary delivery polygons per restaurant.

### Follow-up Questions to the Interviewer (3–4)

> "A few questions I'd ask in a real design review:"

1. **"How do we handle a restaurant that operates a ghost kitchen — multiple brands at one address? Do we model that as one geo point with N restaurant entities, or N points?"**
   *(Answer: one point, N entities sharing it — saves tree memory and avoids ranking dedup issues.)*

2. **"Should search results respect a minimum prep+delivery time SLA — i.e., hide restaurants that can't deliver in <45 min even if they're geographically close?"**
   *(Answer: yes, ETA filter at Ranking Service post candidate-fetch — keeps the geo layer pure.)*

3. **"For tree rebuilds, do we want a global coordinator or per-region autonomy?"**
   *(Answer: per-region autonomy — coordinator is a SPOF. Each region's lead replica triggers its own rebuild.)*

4. **"Are we planning to expose the geo index as a platform service for other Uber products (rides, freight)?"**
   *(Answer: if yes, the API contract should be product-agnostic — `nearby(lat, lng, radius, entityType)` — and we'd unify on S2 cells across products.)*

---

## Delivery Cheatsheet — read this 5 minutes before the interview

1. **Talk while you draw.** Silent whiteboarding looks like you're stuck.
2. **Time-box yourself.** Glance at the clock at the 15-min and 30-min marks. Cut deep dives if running over.
3. **Lead with the trade-off, then the choice.** *"There are three options: A, B, C. I'd pick B because…"* Always.
4. **When the interviewer pushes (like they did on quadtree updates), say: "Good question — let me think for 10 seconds."** Then think. Don't fill silence with filler.
5. **Numbers > adjectives.** Say "p99 200ms," not "fast." Say "10M restaurants," not "lots."
6. **Own the tree.** You said you'd assume Uber uses quadtree — commit to that confidently. *"I'm choosing quadtree because…"* not *"I think maybe quadtree…"*
7. **End strong.** The wrap-up is when the interviewer writes their hire/no-hire note. Don't mumble it.
