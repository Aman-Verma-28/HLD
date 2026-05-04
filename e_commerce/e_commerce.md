# HLD: Order Processing / E-Commerce System with Top-K Popular Items
**Uber SDE-2 Bar Raiser — End-to-End Interview Walkthrough**

> Source prompt: *"Design a backend for an e-commerce site that displays a list of merchandise. Show top items based on popularity (purchases × ratings). Build an offline batch pipeline to compute popularity. Scale: 50M items across 10K categories, p99 RPS of 10K. Focus on requirements gathering, high-level solution, API design, and DB choices."*

This document is written as the **transcript I'd want to deliver in a 60-minute Uber HLD bar-raiser**. It follows the AlgoMaster 7-phase framework. Each section starts with a one-line "what to actually say first" cue so you can rehearse it.

---

## Phase 0 — Reframe the problem (30 sec)

> *"Before I dive in, let me make sure I'm hearing the right problem. We're building the catalog + order side of an e-commerce backend — not the storefront UI, not search, not payments. The headline product feature is showing 'top-K popular items' (globally and per category), where popularity is computed offline from purchase counts and ratings. We also need item detail pages and the order placement path so popularity has something to feed off. Sound right?"*

This 30-second reframe **does the interviewer's job for them** — it tells them you understood the prompt and pre-empts ambiguity. Always do this first.

---

## Phase 1 — Requirements (5–7 min)

### Functional — propose, don't interrogate

> *"Let me propose a scope and you can push back if I'm off."*

**In scope:**
1. **Browse top-K popular items** — globally and filtered by category. Cursor-paginated.
2. **Item detail page** — name, price, description, images, average rating, review count, in-stock flag.
3. **Submit a rating** (1–5) on a purchased item. Drives popularity.
4. **Place an order** — checkout for one or more items. Decrement inventory atomically.
5. **Order lookup** — for the user's order history.
6. **Offline popularity pipeline** — recompute popularity scores from order + rating events on a schedule.

**Explicitly out of scope (call these out — saves 10 min later):**
- Free-text search / recommendations / personalized "For You" feed.
- Auth, user profile, payment processing (assume an upstream gateway).
- Cart UX, shipping, fulfillment, returns.
- Image upload / CMS for sellers.
- Analytics dashboards.

### Non-functional — anchor every NFR to a number

| NFR | Target | Why it matters |
|---|---|---|
| **Catalog read RPS** | 10K p99 (given) | Drives caching + read replicas |
| **Order write RPS** | ~200 peak (assume ~2% of browse converts) | Drives sharding for orders DB |
| **Top-K read latency** | p99 < 150 ms | Top-K must be served from cache, not computed online |
| **Item detail latency** | p99 < 200 ms | Cache-aside on Redis |
| **Order placement latency** | p99 < 500 ms | Synchronous inventory check is acceptable here |
| **Availability** | 99.99 % catalog reads, 99.95 % order writes | Reads must survive partial outages; orders can degrade to retry |
| **Consistency** | Eventual for popularity (minutes-stale OK), strong for inventory + orders | "No overselling" is a hard contract |
| **Popularity freshness** | 1-hour batch + near-real-time speed layer | Trending items shouldn't lag a full day |
| **Scale** | 50M items, 10K categories (~5K items/cat avg, but heavily skewed) | Skew is the design challenge |

### Pin down the skew — most candidates miss this

> *"One thing I want to confirm: with 10K categories, traffic is going to follow a power law. Maybe 100 categories drive 80% of reads — Electronics, Apparel, Home. Should I design for that hot-tail explicitly? **Yes? Good — that'll change my caching and replication strategy in the deep dive.**"*

Write the requirements somewhere visible on the whiteboard. Refer back to them when justifying every component.

---

## Phase 2 — Back-of-Envelope Estimation (3–5 min)

> *"Let me anchor a few numbers. I'll round aggressively — order of magnitude is what matters."*

### Reads (the dominant workload)

```
Average read RPS                     = 10,000 (given as p99, treat as peak)
Daily catalog reads                  = 10K × 86,400 ≈ 0.86 B/day
Top-K reads as % of catalog reads    = ~30% (homepage + category landing)
                                     ≈ 3,000 RPS for top-K alone
Item detail reads                    = ~70%
                                     ≈ 7,000 RPS
```

**Implication:** top-K reads are heavy AND hot-keyed (everyone hits the same K items per category). Pure DB serving is dead-on-arrival — must be a Redis sorted set + edge cache.

### Writes

```
Orders/sec (assume 2% conversion of browses)  ≈ 200 peak
Items per order (avg)                         ≈ 2
Order-line writes                             ≈ 400/sec
Rating submissions                            ≈ 50/sec (much rarer than orders)
Inventory decrements                          ≈ 400/sec
```

**Implication:** order writes are *small*. We don't need exotic write-scaling. We DO need ACID for inventory + order to avoid overselling.

### Storage

```
Items metadata                       50M × 5 KB                   = 250 GB
Item images                          50M × 5 images × 200 KB      = 50 TB → S3 + CDN
Orders (3-year retention)            200 × 86400 × 365 × 3 × 1KB  ≈ 19 TB
Ratings                              ~5% of orders submit a rating
                                     ≈ 1B rows × 200 B            = 200 GB
Top-K cache                          10K cats × top-1000 × 16 B   = 160 MB (trivial)
```

**Implication:** items metadata fits on a small Cassandra ring (or even a sharded Postgres). Images go to S3 + CDN — never on the hot path. Orders need partitioning for 3-year retention.

### Bandwidth

```
Catalog read response               ~10 KB (item summaries)
Bandwidth                           10K × 10 KB                  = 100 MB/s = 800 Mbps
                                                                 → CDN handles this
```

### The 4 numbers I'll use to justify decisions

1. **10K read RPS, hot-keyed** → mandatory Redis layer + CDN for top-K endpoints.
2. **200 order RPS, ACID** → single-region Postgres works, sharded by `user_id`.
3. **50M items × 5KB = 250 GB** → fits in Cassandra cluster or sharded Postgres easily.
4. **10K categories, power-law skew** → top 100 categories need replication / request collapsing.

---

## Phase 3 — API Design (3–5 min)

> *"I'll define five endpoints. These map 1:1 to the functional requirements."*

### 1. Get Top-K Popular Items

```http
GET /v1/items/top?category_id={cid}&k=20&cursor={opaque}
Authorization: Bearer <jwt>

200 OK
{
  "items": [
    { "item_id": "i_abc",
      "name": "Sony WH-1000XM5",
      "price_cents": 39999,
      "currency": "USD",
      "thumbnail_url": "https://cdn.../thumb.jpg",
      "avg_rating": 4.6,
      "rating_count": 12483,
      "popularity_score": 0.94,    // optional, for debugging
      "in_stock": true
    },
    ...
  ],
  "next_cursor": "eyJvIjoyMH0="
}
```

- `category_id` optional — omitting returns global top-K.
- **Cursor-based pagination, not offset.** Top-K data shifts under us; cursors stay stable.
- `k` capped server-side at 100.

### 2. Get Item Details

```http
GET /v1/items/{item_id}
200 OK
{
  "item_id": "i_abc",
  "name": "...",
  "description": "...",
  "price_cents": 39999,
  "images": ["https://cdn.../1.jpg", "https://cdn.../2.jpg"],
  "category_id": "c_audio",
  "avg_rating": 4.6,
  "rating_count": 12483,
  "stock_count": 142,        // approximate; not authoritative
  "attributes": { "brand": "Sony", "color": "Black", ... }
}
```

### 3. Place an Order

```http
POST /v1/orders
Idempotency-Key: 7f3a-...      ← critical, see deep dive
{
  "items": [
    { "item_id": "i_abc", "quantity": 1 },
    { "item_id": "i_xyz", "quantity": 2 }
  ],
  "shipping_address_id": "...",
  "payment_token": "tok_..."
}

201 Created
{ "order_id": "o_456", "status": "CONFIRMED", "total_cents": 79998 }

409 Conflict { "code": "OUT_OF_STOCK", "item_id": "i_abc" }
```

### 4. Submit a Rating

```http
POST /v1/items/{item_id}/ratings
{ "order_id": "o_456", "stars": 5, "review": "..." }
```
- `order_id` enforced — only verified buyers can rate. Anti-gaming.

### 5. Get User's Orders

```http
GET /v1/users/me/orders?cursor={opaque}&limit=20
```

### Cross-cutting API decisions to mention

- **Cursor pagination everywhere** that returns lists.
- **Idempotency keys** on `POST /orders` (24h TTL) — clients retry on timeout.
- **Rate limiting at the gateway** — per-user (catalog) and per-IP (order). Not designed here, just acknowledged.
- **gRPC for service-to-service**, REST/JSON at the edge.

---

## Phase 4 — High-Level Design (8–10 min)

> *"I'll build this in three steps. Each step solves a problem the previous step couldn't."*

### Step 1 — Naive: just make it work

```
[Client] → [API server] → [Postgres (items, orders, inventory, ratings)]
```

**Problem:** 10K read RPS would hammer Postgres on hot top-K queries (`ORDER BY popularity_score LIMIT 20` on a 50M-row table). Also no top-K compute path.

### Step 2 — Split read path, add a cache, separate the popularity compute

```
[Client] → [CDN] → [LB] → [Catalog Service] → [Redis cache] → [Items DB]
                       └→ [Order Service]   → [Orders DB + Inventory DB]
                       └→ [Rating Service]  → [Ratings DB]

Order events ──▶ [Kafka: orders, ratings] ──▶ [Popularity Pipeline] ──▶ [Top-K store: Redis ZSET]
```

**Problem:** popularity pipeline is a black box. And no resilience to hot categories yet.

### Step 3 — Final architecture (what I'd draw on the whiteboard)

```
                                 ┌─────────────────────────────────────────┐
                                 │              CLIENTS                    │
                                 │  Mobile · Web · Partner APIs            │
                                 └───────────────────┬─────────────────────┘
                                                     │
                                ┌────────────────────▼────────────────────┐
                                │  CDN  (item images, top-K JSON 30s TTL) │
                                └────────────────────┬────────────────────┘
                                                     │
                                ┌────────────────────▼────────────────────┐
                                │   API Gateway (auth · rate-limit · TLS) │
                                └─┬──────────────────┬──────────────┬─────┘
                                  │                  │              │
                       ┌──────────▼─────┐  ┌─────────▼────────┐  ┌──▼────────────┐
                       │ Catalog Svc    │  │ Order Svc        │  │ Rating Svc    │
                       │ (top-K + item) │  │ (place + lookup) │  │ (submit)      │
                       └──┬─────────┬───┘  └────┬─────────────┘  └─────┬─────────┘
                          │         │            │                      │
                  ┌───────▼───┐ ┌───▼─────────┐  │                      │
                  │ Redis     │ │ Items DB    │  │                      │
                  │ ZSET top-K│ │ (Cassandra) │  │                      │
                  │ HASH item │ └─────────────┘  │                      │
                  └───────────┘                  │                      │
                                                 │                      │
                                  ┌──────────────▼────┐                 │
                                  │  Inventory Svc    │                 │
                                  │  (Postgres CAS)   │                 │
                                  └──────────────┬────┘                 │
                                                 │                      │
                                  ┌──────────────▼────┐                 │
                                  │  Orders DB        │                 │
                                  │  (Postgres,       │                 │
                                  │   sharded by user)│                 │
                                  └──────────────┬────┘                 │
                                                 │                      │
                                                 ▼                      ▼
                              ┌──────────────────────────────────────────────┐
                              │       Kafka (order_events, rating_events)    │
                              └─────────────┬────────────────────┬───────────┘
                                            │                    │
                                ┌───────────▼─────────┐ ┌────────▼──────────┐
                                │  Flink (speed layer)│ │  Spark batch      │
                                │  near-RT count-min  │ │  hourly recompute │
                                │  sketch per cat     │ │  full popularity  │
                                └───────────┬─────────┘ └────────┬──────────┘
                                            │                    │
                                            └─────────┬──────────┘
                                                      ▼
                                         ┌──────────────────────────┐
                                         │  Top-K Builder           │
                                         │  merges batch + speed    │
                                         │  writes Redis ZSETs      │
                                         └──────────────────────────┘
```

### Walk through the data flows

**Flow A — Browse top-K of category "Audio":**
1. Client → CDN (cache hit on hot categories, ~30s TTL → already done in many cases).
2. Miss → API Gateway → Catalog Service.
3. Catalog Service: `ZREVRANGE topk:cat:c_audio 0 19 WITHSCORES` → 20 item IDs.
4. Pipelined `MGET item:i_abc item:i_def ...` against Redis HASH → item summaries.
5. Cache miss on any item → fall back to Cassandra `SELECT * FROM items WHERE item_id IN (...)`, write back to Redis (TTL 10 min).
6. Return JSON. **End-to-end p99: ~80 ms.**

**Flow B — Place an order for 2 items:**
1. Client → Gateway (with `Idempotency-Key`).
2. Order Service checks idempotency cache → if dup, return prior result.
3. Order Service calls Inventory Service: `decrement(item_id, qty)` per line. CAS in Postgres: `UPDATE inventory SET available = available - $qty WHERE item_id = $id AND available >= $qty`.
4. If any line fails → roll back successful decrements (Saga compensating action) → return 409.
5. All succeed → Order Service writes the order row to sharded Postgres (`shard = hash(user_id)`).
6. Order Service publishes `order_placed` to Kafka.
7. Returns 201 to client. **End-to-end p99: ~300 ms.**

**Flow C — Popularity refresh (the headline ask):**
1. `order_placed` and `rating_submitted` events stream into Kafka.
2. **Speed layer (Flink):** maintains a per-category Count-Min sketch over the last 1 hour, flushes deltas to Redis every 60s as `topk:cat:{cid}:speed`.
3. **Batch layer (Spark, hourly):** scans last 7 days of orders + ratings from S3 (Kafka → S3 via Kafka Connect), computes the full popularity score per item per category, writes to a `popularity_scores` table.
4. **Top-K Builder job (hourly):** reads the batch table + speed deltas, merges, picks top-1000 per category, atomically swaps into `topk:cat:{cid}` Redis ZSET.
5. Catalog Service reads from these ZSETs — **never recomputes online**.

**Flow D — Submit a rating:**
1. Client → Rating Service. Rating Service verifies `order_id` belongs to user and item.
2. Insert into ratings table.
3. Update materialized `(avg_rating, rating_count)` on the item (write-through to Redis HASH).
4. Publish `rating_submitted` to Kafka → feeds the same popularity pipeline.

### Why each component exists (this is what scores points)

| Component | Justified by |
|---|---|
| CDN | Top-K JSON is highly cacheable; shaves origin RPS by 60–80% |
| Redis ZSET | O(log N) top-K reads, no DB scan |
| Cassandra (items) | 50M rows, write-once-read-many, item-id keyed access pattern |
| Postgres (orders/inventory) | ACID for "no overselling" — non-negotiable |
| Kafka | Decouples write path from popularity compute; replayable |
| Flink (speed) | Trending items shouldn't wait for the next batch run |
| Spark (batch) | Authoritative reprocessing; fixes drift in the speed layer |
| Saga / compensating actions | No 2-phase commit across Inventory + Orders |

---

## Phase 5 — Database Design (5–7 min)

> *"Different stores have different needs. Let me pick per data type, then justify each."*

### Per-store choice table

| Data | Store | Why |
|---|---|---|
| Items metadata | **Cassandra** | 50M items, primary access by `item_id`, write-once-read-many, easy to scale reads |
| Items by category | **Cassandra (denormalized table)** | Avoid global secondary index; partition by `category_id` |
| Orders | **Postgres, sharded by user_id** | ACID, complex queries (user history), 200 RPS easy for sharded Postgres |
| Inventory | **Postgres** (separate cluster) | Need conditional updates (CAS); contention requires row-level locks |
| Ratings | **Cassandra** | High-cardinality, append-only, partitioned by `item_id` |
| Item denormalized aggregates (avg_rating, rating_count) | **Postgres** items table OR Redis HASH | Updated by stream processor; needs strong-ish consistency for display |
| Top-K serving | **Redis ZSET per category** | O(log N) range; sub-ms |
| Item summary cache | **Redis HASH** | Pipelined MGET for batch-fetch |
| Popularity scores (offline) | **HDFS / S3 (Parquet)** for raw + **Postgres** for the materialized snapshot | Spark output land on S3; small enough snapshot for Postgres |
| Idempotency keys | **Redis** with 24h TTL | Cheap dedup |
| Order events / rating events | **Kafka** | Replayable log; not a long-term store |

### Schemas

#### `items` (Cassandra)

```cql
CREATE TABLE items (
  item_id          TEXT PRIMARY KEY,
  name             TEXT,
  description      TEXT,
  price_cents      INT,
  category_id      TEXT,
  attributes       MAP<TEXT, TEXT>,
  image_urls       LIST<TEXT>,
  avg_rating       FLOAT,
  rating_count     BIGINT,
  created_at       TIMESTAMP
);
```

#### `items_by_category` (Cassandra — denormalized for category browse)

```cql
CREATE TABLE items_by_category (
  category_id      TEXT,
  popularity_score DOUBLE,
  item_id          TEXT,
  PRIMARY KEY ((category_id), popularity_score, item_id)
) WITH CLUSTERING ORDER BY (popularity_score DESC, item_id ASC);
```
- Partition key `category_id` keeps a category together.
- Clustering by `popularity_score DESC` lets us slice top-K with a single read.
- **But** — popularity changes hourly; we'd need to delete + reinsert on every batch run. **That's why I put Redis ZSET in front of this — Cassandra is the cold-store fallback.**

#### `orders` (Postgres, sharded by `user_id`)

```sql
CREATE TABLE orders (
  order_id        UUID PRIMARY KEY,
  user_id         BIGINT NOT NULL,
  status          order_status NOT NULL,    -- enum: PENDING, CONFIRMED, SHIPPED, ...
  total_cents     INT NOT NULL,
  idempotency_key TEXT UNIQUE,              -- prevents dup orders from retries
  created_at      TIMESTAMP DEFAULT now()
);
CREATE INDEX ON orders (user_id, created_at DESC);

CREATE TABLE order_lines (
  order_id        UUID REFERENCES orders,
  item_id         TEXT NOT NULL,
  quantity        INT NOT NULL,
  price_cents     INT NOT NULL,             -- snapshot at order time
  PRIMARY KEY (order_id, item_id)
);
```

**Sharding:** `shard = hash(user_id) % N`. Co-locates a user's orders, keeps "my orders" page on one shard.

#### `inventory` (Postgres)

```sql
CREATE TABLE inventory (
  item_id    TEXT PRIMARY KEY,
  available  INT NOT NULL CHECK (available >= 0),
  version    BIGINT NOT NULL DEFAULT 0
);

-- Atomic decrement (the critical query):
UPDATE inventory
   SET available = available - :qty,
       version   = version + 1
 WHERE item_id  = :id
   AND available >= :qty;
-- Returns 0 rows = out of stock → caller raises 409.
```

The `CHECK (available >= 0)` is belt-and-suspenders. **No race possible** because `WHERE available >= :qty` is part of the conditional update.

#### `ratings` (Cassandra)

```cql
CREATE TABLE ratings (
  item_id     TEXT,
  rating_id   TIMEUUID,
  user_id     BIGINT,
  order_id    UUID,
  stars       TINYINT,
  review      TEXT,
  PRIMARY KEY ((item_id), rating_id)
) WITH CLUSTERING ORDER BY (rating_id DESC);
```
- Partition by `item_id` — get all reviews for an item efficiently.
- TIMEUUID gives us recency ordering for free.

#### Top-K serving (Redis)

```
ZSET   topk:cat:{category_id}              member=item_id, score=popularity
ZSET   topk:global                         member=item_id, score=popularity
HASH   item:{item_id}                      summary fields for MGET
HASH   item_aggs:{item_id}                 avg_rating, rating_count (write-through from rating service)
SET    idem:{key}                          idempotency keys, TTL 24h
```

### Sharding strategy summary

| Store | Shard key | Reason |
|---|---|---|
| Cassandra `items` | `item_id` | Default; uniform |
| Cassandra `items_by_category` | `category_id` | Locality; **needs hot-cat mitigation** (deep dive) |
| Postgres `orders` | `user_id` | "My orders" co-located |
| Postgres `inventory` | `item_id` (or single cluster — 50M rows is fine) | Item-level CAS |
| Cassandra `ratings` | `item_id` | "All reviews for X" co-located |

### Why I'm NOT using a single Postgres for everything

- 50M items × 5 KB with 10K read RPS would need a beefy read-replica fleet.
- Cassandra gives us linear read scaling for free at item-keyed access.
- I'm using Postgres precisely where its strengths matter: **transactional inventory + orders**.

This split — Cassandra for the catalog, Postgres for the transactional path — is exactly what Amazon and eBay do in practice. Worth saying out loud.

---

## Phase 6 — Deep Dives (12–15 min)

> *"Pick whichever you want me to go deep on, or I have four candidates: the offline popularity pipeline, the popularity score formula, hot-category cache strategy, and inventory consistency. I think the pipeline is the headline since you mentioned offline batch processing."*

### Deep Dive 1 — The Offline Popularity Pipeline (the headline ask)

#### The problem

We can't compute "top-K per category" online — that's `ORDER BY popularity_score LIMIT K` over 50M rows for every read at 10K RPS. We need to **precompute**, refresh **frequently enough** to feel real-time, and serve from a sub-ms store.

#### Three approaches

**A. Pure batch (Spark hourly)**
- Spark job reads last N days of `order_events` + `rating_events` from S3.
- Computes popularity per `(item, category)`.
- Writes top-1000 per category to Redis ZSETs via atomic swap.
- **Pros:** simple, deterministic, easy to backfill.
- **Cons:** up to 1-hour stale → trending item posted at 12:01 doesn't appear until 1:00.

**B. Pure streaming (Flink real-time)**
- Flink consumes Kafka, maintains per-category sliding window of (item → score).
- Pushes to Redis as scores change.
- **Pros:** seconds-fresh.
- **Cons:** no easy reprocessing, drift over time, harder to tune/debug, expensive at 50M items.

**C. Hybrid (Lambda) — what I'd choose**

```
                 Kafka  ──▶  Flink (speed layer)         ──▶  Redis: topk_speed:cat:{c}
                                 (last 1h, deltas)               (overlay)

                 Kafka  ──▶  S3  ──▶  Spark (batch)      ──▶  popularity_scores (Postgres)
                                       hourly, 7-day             (authoritative)

                                                              Top-K Builder (hourly)
                                                                  merges both
                                                                       │
                                                                       ▼
                                                           Redis: topk:cat:{c}  (served)
```

- Batch layer is **authoritative** — corrects drift; reprocessable from Kafka/S3.
- Speed layer **is overlaid** — accounts for the last hour of activity not yet in batch.
- Catalog service reads `topk:cat:{c}` (final merged) — never the raw layers.
- "**Lambda architecture**" is the named term; say it explicitly.

#### Why count-min sketch in the speed layer?

50M items, but the speed layer only cares about the top contenders per category. A Count-Min Sketch:
- bounded memory (~MBs per category instead of GBs),
- O(1) increment per event,
- approximate top-K is fine here — we're feeding into a merge with the authoritative batch anyway.

#### The atomic swap trick

```
# Build new ZSET in a shadow key, then swap
redis> DEL topk:cat:c_audio:new
redis> ZADD topk:cat:c_audio:new <score> <item_id>  ... (1000 entries)
redis> RENAME topk:cat:c_audio:new topk:cat:c_audio
```
RENAME is atomic. Readers see the old set, then the new one, never a half-built one.

---

### Deep Dive 2 — The Popularity Score Formula

> *"Naive popularity = purchase count is wrong in three ways. Let me walk through what to actually compute."*

**Naive:**
```
score = num_purchases_30d
```

**Problems:**
1. **Rating ignored.** A 1-star item with 1000 buys shouldn't outrank a 4.9-star item with 800.
2. **No time decay.** A holiday-spike item from 29 days ago shouldn't equal trending today.
3. **Gameable.** Sellers buy their own product or run rating bot farms.

**Better formula:**

```
score = w1 * normalized_purchases_decayed
      + w2 * wilson_lower_bound(positive_ratings, total_ratings)
      + w3 * conversion_rate
      - w4 * fraud_signal
```

Where:
- `normalized_purchases_decayed = Σ exp(-λ * age_days) for each purchase`, `λ ≈ 0.05` → half-life ~14 days.
- `wilson_lower_bound` instead of raw average — accounts for sample size. A 5.0 rating with 1 review shouldn't beat a 4.7 rating with 10K reviews.
- `conversion_rate = purchases / item_views` — popularity that's NOT just "we showed it to everyone".
- `fraud_signal` = downweight from anomaly detection (rating velocity spikes, single-IP rating clusters).

Weights `w1..w4` are tuned via offline A/B (CTR + GMV uplift on top-K position).

**Anti-gaming hardening:**
- Only count purchases from accounts ≥ 30 days old.
- Only count ratings from `verified_purchase = TRUE` (we enforce this at the API layer).
- Cap any single user's contribution per item.
- Quarantine items with rating-velocity > 5σ for manual review.

> *"In a real Uber-style env this would be its own ranking team; for the interview, I'd say 'we'll iterate on this with offline A/B and CUPED variance reduction.'"*

---

### Deep Dive 3 — Hot Category Cache Strategy

#### The problem

10 categories drive 80% of reads. `topk:cat:c_electronics` becomes a hot key — single Redis node CPU-bound.

#### Mitigations (defense in depth)

| Layer | Technique |
|---|---|
| **Edge** | CDN cache the JSON response with `Cache-Control: public, max-age=30, stale-while-revalidate=60`. 30s of staleness on top-K is invisible to users; 60–80% of reads never hit our origin. |
| **Service-level** | In-process LRU cache in Catalog Service, 5s TTL. Even within a single Catalog instance, we collapse 1000s of identical reads into 1 Redis call. |
| **Cache layer** | **Hot-key replication** — store hot keys with random suffix (`topk:cat:c_electronics:{0..9}`), client picks one. 10x throughput. Or use Redis cluster's read replica routing. |
| **Request collapsing** | If a Redis miss happens during a batch swap, only 1 request rebuilds; others wait on a shared future. |

I'd say:
> *"Edge cache is the biggest lever — 30 seconds of staleness on top-K is invisible, and CDN absorbs the bulk of the QPS. The other tiers are belt-and-suspenders for the cold-cache window after a deploy."*

---

### Deep Dive 4 — Inventory Consistency / No Overselling

#### The problem

Two users buy the last unit at the same time. Both succeed = oversold. Both fail = lost sale. We need exactly-one-wins.

#### Why not 2PC across Order + Inventory?

2PC is brittle, slow, and doesn't compose with Cassandra/Kafka. Skip.

#### What I'd do — Saga + conditional update

**Place order = a Saga of conditional decrements:**

```
1. For each line item:
       UPDATE inventory SET available = available - qty
        WHERE item_id = $id AND available >= qty;
   If 0 rows updated → record failure, break.

2. If any failed → compensate: re-increment all previously decremented.
                   Return 409 OUT_OF_STOCK to client.

3. All succeeded → INSERT order + order_lines in single Postgres transaction.
                   Publish order_placed to Kafka.
                   Return 201.
```

Conditional update + Postgres `CHECK (available >= 0)` makes overselling **impossible**. Two concurrent transactions: one succeeds, the other gets 0 rows updated → returns 409. Linearizable per item.

#### Idempotency on retries

Client times out, retries. We must not double-charge / double-decrement.

```
Order Service receives request with Idempotency-Key K.
  if Redis SET NX idem:K → process normally, store result.
  else → look up prior result, return it.
```

The Postgres `idempotency_key UNIQUE` constraint is the second line of defense (Redis evicts; DB doesn't).

---

### Deep Dive 5 — Cold Start for New Items

A new item has zero purchases → zero popularity → never surfaces → never gets purchases. Death spiral.

**Mitigations:**
- **Exploration slot:** reserve top-K positions K-3 through K (e.g. positions 18–20 of top-20) for items with low confidence intervals. Bandit-style ε-greedy.
- **Category baseline boost:** new items get a synthetic score = 50th percentile of the category for first 7 days.
- **Manual editorial seeding** for big launches — separate `editorial_pin:cat:{c}` ZSET that overlays at position 1.

---

### Deep Dive 6 — Failure Modes (be ready for these)

| Failure | Behavior |
|---|---|
| Redis ZSET cluster down | Catalog Service falls back to Cassandra `items_by_category` query (slower, ~500 ms, but correct). Circuit breaker. |
| Spark batch job fails | Top-K Builder skips merge → keeps last-good ZSETs. Alerts page on-call. |
| Flink speed layer down | Top-K still served, just batch-fresh (1h stale). Acceptable degradation. |
| Postgres orders primary down | Failover to replica. New orders rejected during failover (~30s) — surface friendly retry message. |
| Kafka partition stuck | Order Service writes to local disk WAL → flushes on recovery. Don't fail the order over async pipeline issues. |

---

## Phase 7 — Wrap-Up (2–3 min)

> *"Quick recap, then bottlenecks and what I'd do next."*

### Summary

A read-optimized catalog (Cassandra + Redis ZSET) sits in front of a transactional order/inventory path (Postgres with CAS + Saga). Order and rating events stream through Kafka into a **Lambda pipeline** — Spark for the authoritative hourly recompute, Flink for sub-minute trending — that materializes top-K Redis ZSETs per category. Catalog reads serve from those ZSETs; inventory is updated atomically with conditional updates so overselling is structurally impossible.

### Bottlenecks I'd watch

1. **Hot category Redis keys** — addressed via CDN + LRU + key replication, but I'd dashboard `topk:cat:*` per-key QPS.
2. **Spark batch SLA** — if the hourly run slips past an hour, top-K freshness degrades. Alert on job runtime > 50 min.
3. **Inventory contention on viral items** — a single SKU with 100 RPS of purchases will queue on the row lock. Mitigation: pre-shard inventory into N counters (`available_0..available_N-1`), aggregate.
4. **Kafka backpressure on rating fraud floods** — bots can flood `rating_submitted`. Rate-limit at API + drop in pipeline.

### Future improvements (one-line each)

- Personalization layer: per-user re-ranking on top of category top-K (collaborative filtering).
- Search service (Elasticsearch) — explicitly out-of-scope today.
- Multi-region active-active for catalog reads (orders stay regional).
- A/B testing harness for popularity score weights.
- ML-based fraud scoring on rating events.
- Tiered popularity windows: `topk:cat:{c}:1d`, `:7d`, `:30d` so UI can offer "Trending Now" vs "Bestsellers".

### Curveball table — be ready for these

| Question | Answer in 30 seconds |
|---|---|
| *"Handle 10× traffic — 100K read RPS"* | CDN absorbs most of it. Scale Redis cluster horizontally. Catalog Service is stateless — autoscale. The bottleneck would shift to image bandwidth → CDN edge tier. |
| *"What if the popularity batch job is 6 hours late?"* | Top-K Builder keeps last-good ZSETs; speed layer continues to overlay; UI shows slightly stale top-K. Page on-call after 90 min. |
| *"What if a region goes down?"* | Catalog reads route to another region via DNS failover (data is replicated async). Order writes pause for that region's users (~30s) until shard primary fails over. |
| *"Migrate from a monolith to this"* | Strangler-fig. Stand up Catalog Service first, dual-write items to Cassandra for 2 weeks, shadow-read to verify, cut over. Order/inventory last because they're hardest. |
| *"How do you re-rank in real-time per user without breaking the cache?"* | Top-K Redis ZSET gives a top-200 candidate set; per-user re-rank happens in the Catalog Service in <10 ms on those 200 items only. Personalization without breaking cacheability. |

---

## Cheat Sheet (memorize before walking in)

### Numbers to anchor

- **10K read RPS** — given, the design pivot.
- **200 order RPS** — derived from 2% conversion; small.
- **50M items × 5 KB = 250 GB** metadata.
- **10K categories, power-law skewed** — top 100 drive 80%.
- **50 TB images** → S3 + CDN, never on hot path.
- **Hourly batch + sub-minute speed layer** — popularity freshness target.

### Killer phrases (drop these)

- *"Top-K is read 30% of the time and hot-keyed — Redis ZSET with CDN in front, never recomputed online."*
- *"Lambda architecture: Spark is authoritative, Flink fixes freshness."*
- *"Inventory uses conditional update + Saga, not 2PC. Overselling is structurally impossible because `WHERE available >= qty` is part of the atomic decrement."*
- *"Wilson lower bound for ratings, time-decayed purchase counts, plus a fraud downweight — the formula's an offline-tuned linear combo."*
- *"Cassandra for the catalog, Postgres for the transactional path. Right tool, right job."*
- *"Hot category mitigations are layered — CDN, in-process LRU, key replication, request collapsing."*

### Common mistakes to avoid

1. ❌ Trying to compute top-K online (`ORDER BY popularity LIMIT K`).
2. ❌ Using a single Postgres for items, orders, inventory, and ratings.
3. ❌ Forgetting idempotency on `POST /orders`.
4. ❌ Saying "use 2PC" for inventory/order coordination.
5. ❌ Leaving the popularity formula at "purchase count" — interviewer is testing whether you've thought about gaming and decay.
6. ❌ Ignoring the hot-category problem (10K cats sound uniform; they aren't).
7. ❌ Designing the offline pipeline as pure batch and accepting 1-hour staleness without acknowledging a speed layer.
8. ❌ Hand-waving DB choices instead of justifying per-store.

### Time budget (60-min)

| Phase | End by minute |
|---|---|
| Reframe + Requirements | 7 |
| Estimation | 12 |
| API Design | 17 |
| High-Level Design | 27 |
| Database Design | 34 |
| Deep Dives | 54 |
| Wrap-Up | 60 |

---

## Review

This document covers the full 7-phase HLD answer for an Uber SDE-2 bar raiser on the order-processing + top-K popular items prompt. Emphasis matches the interviewer's stated focus areas:

- **Requirements gathering** — proposal-driven scoping, functional/non-functional tables, explicit out-of-scope list, skew callout.
- **High-level solution** — built incrementally in 3 steps so the interviewer can follow each component's justification.
- **API design** — five endpoints with idempotency + cursor pagination + rate limiting noted.
- **DB choices evaluated** — explicit per-store choice table with rationale (Cassandra for catalog, Postgres for transactional, Redis for serving, Kafka for events, S3 for raw, Spark/Flink for compute), schemas with shard keys.
- **Offline batch pipeline** — full Lambda architecture deep dive with the atomic-swap trick and Count-Min Sketch detail.
- **Five deep dives** picked for this specific question: pipeline, score formula, hot-category cache, inventory consistency, cold start, plus failure-mode table.
- **Curveball table + cheat sheet** so this is rehearsable, not just readable.
