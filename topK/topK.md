# Live Leaderboard — Uber SDE2 HLD (Bar Raiser)

> Question recap (3 progressive asks):
> 1. Design a simple leaderboard.
> 2. Add caching.
> 3. Make it thread-safe under high concurrency.
>
> This document is structured as a 45–60 min interview answer using the
> 7-phase framework from `HLD/answering_framework.txt`. Each phase has:
> - **What to say** (the verbal script).
> - **What to draw** (the whiteboard artifact).
> - **Why it matters** (the senior-engineer reasoning the bar raiser is grading).

---

## 0. Opening (≈ 30 seconds)

> **Say this verbatim before anything else:**
>
> "Before I draw anything, I want to spend a few minutes nailing down what
> *kind* of leaderboard we're building, the scale, and the consistency
> bar — because those three things will completely change the architecture.
> I'll then estimate, sketch APIs, walk a high-level design, talk data,
> and finish with deep dives on caching and concurrency, since those tend
> to be the interesting failure modes for this problem."

That one sentence sets the contract for the whole interview. The bar
raiser now knows you have a plan and won't dive into Redis on slide one.

---

## Phase 1 — Requirements Clarification (5 min)

### 1.1 Functional requirements (propose, then refine)

> "I'll propose a scope and you tell me where to push or pull. I'm
> thinking the core is:
>
> 1. **Submit score** — a user submits a score (or score delta) for a game / contest.
> 2. **Get top-K** — anyone can fetch the top K players for a leaderboard.
> 3. **Get my rank** — a user can fetch their own rank and surrounding neighbors (e.g. ±5).
> 4. **Time windows** — daily, weekly, all-time. Most leaderboards in the wild are time-bucketed.
>
> Out of scope unless you push back: anti-cheat / fraud, social graph,
> achievements/badges, payouts, multi-tenant SaaS leaderboards."

**Why this framing wins for Uber:** Uber runs leaderboards in several
real contexts — driver incentive programs, Uber Eats restaurant
rankings, gamified rider promos. All of them share the same shape:
high-write-volume score updates, low-latency rank reads, time-windowed.
Mentioning that you've thought about *which* leaderboard signals
product-thinking.

### 1.2 Non-functional requirements

| NFR              | Target                                  | Why it matters                               |
| ---------------- | --------------------------------------- | -------------------------------------------- |
| Scale            | 100M DAU, 10M concurrent during events  | Forces sharding + horizontal scale           |
| Write QPS        | ~300K avg, 1M peak                      | Single Redis node ≈ 100K ops/s, so we shard  |
| Read QPS         | ~1.5M peak (5:1 read:write)             | CDN/edge cache + Redis replicas              |
| Read latency p99 | < 100 ms                                | Pre-computed top-K, no DB on read path       |
| Write latency p99| < 200 ms                                | Async fan-out is OK; ack on Redis update     |
| Consistency      | Eventual (≤ 2s lag) for top-K           | Allows async pipeline                        |
|                  | Read-your-writes for *my rank*          | Users notice if their own score doesn't move |
| Availability     | 99.99%                                  | Redis Cluster + multi-AZ + Kafka replay      |
| Durability       | No score loss                           | Kafka WAL + periodic Redis snapshots         |

> **Say this:** "I'll call out the one consistency nuance up front —
> *globally* eventual consistency is fine, but a user has to see *their
> own* score update immediately on the next read. That's read-your-writes
> consistency for the submitter, eventual for everyone else. That single
> distinction drives a lot of the design."

That sentence alone is a senior-level signal. Most candidates conflate
the two.

### 1.3 Write the requirements box on the board (keep visible)

```
┌─ REQUIREMENTS ─────────────────────────────────────────┐
│ Functional:                                            │
│   - submitScore(user, game, delta)                     │
│   - getTopK(game, window, K)                           │
│   - getRank(user, game, window)  + neighbors           │
│   - windows: daily / weekly / all-time                 │
│                                                        │
│ Non-functional:                                        │
│   - 100M DAU, 1M write QPS peak, 1.5M read QPS peak    │
│   - Read p99 < 100ms, Write p99 < 200ms                │
│   - Eventual globally; read-your-writes for self       │
│   - 99.99% avail, no score loss                        │
└────────────────────────────────────────────────────────┘
```

---

## Phase 2 — Back-of-Envelope Estimation (3 min)

> "Let me convert those requirements into numbers, because the numbers
> will dictate every component I draw."

### 2.1 Traffic

```
DAU                       = 100M
Submissions per user/day  = 10        (conservative; gaming is bursty)
Daily writes              = 10^9
Avg write QPS             = 10^9 / 10^5 sec ≈ 10K/s   (using 100K sec/day)
Peak write QPS (×10 burst)= ~100K/s baseline,
                            spikes to 1M/s during global events
Read:Write                = 5:1   →   peak read QPS ≈ 5M/s on hot windows
```

### 2.2 Storage

```
Active leaderboards       = 10K (events × windows)
Players per board (p99)   = 10M
Bytes per entry           = ~32B (user_id 8B + score 8B + ZSET overhead 16B)
Hot data in Redis         = 10K × 10M × 32B ≈ 3.2 TB
                          → shard across ~30 Redis nodes (128GB each)

Durable history (Cassandra/Dynamo):
  10^9 events/day × 100B × 90d retention ≈ 9 TB
```

### 2.3 Bandwidth

```
Read response (top 100)   ≈ 100 × 64B ≈ 6.4 KB
Peak read bandwidth       ≈ 5M QPS × 6.4KB ≈ 32 GB/s
                          → hard requirement for CDN edge caching
                            of the top-K page (≤ 1s TTL)
```

> **Say this:** "These three numbers — 1M write QPS, 32 GB/s read
> bandwidth, 3 TB hot data — tell me three things: I cannot put the
> read path on the database, I cannot serve top-K from origin, and I
> cannot keep the leaderboard on a single Redis node. So we'll need
> CDN, sharded Redis, and an async write pipeline. Let me draw that."

That's the explicit estimation→architecture handoff the framework
calls out as a senior signal.

---

## Phase 3 — API Design (3 min)

> "Three core endpoints, plus one optional WebSocket for live UI."

### 3.1 Submit score

```http
POST /v1/leaderboards/{game_id}/scores
Authorization: Bearer <jwt>
Idempotency-Key: <uuid>           ← for safe client retries

{
  "user_id":     "u_8821",
  "delta":       50,              ← INCREMENT semantics, not SET
  "occurred_at": "2026-05-02T12:00:01Z",
  "window":      ["daily","weekly","all_time"]
}

→ 202 Accepted
{ "submission_id": "...", "current_score": 1250, "current_rank": 184 }
```

**Why `delta` and not `score`:** atomic increments are commutative — two
concurrent +50s always end at +100. Concurrent SETs race and one wins.
This single API choice eliminates an entire class of concurrency bugs.

**Why `Idempotency-Key`:** clients retry. Without it, a flaky network
double-counts the score. This is bar-raiser table stakes.

**Why 202 (Accepted) not 200:** the durable write is async via Kafka;
we ack as soon as the event is in Kafka, not after Redis is updated.
Keeps p99 tight. We optimistically return the predicted rank from the
local Redis update (read-your-writes).

### 3.2 Get top-K

```http
GET /v1/leaderboards/{game_id}/top?window=daily&limit=100&cursor=<opaque>
→ 200 OK
{
  "entries":   [{ "rank": 1, "user_id": "u_42", "score": 9821 }, ...],
  "next":      "<opaque-cursor>",
  "as_of":     "2026-05-02T12:00:00Z",   ← so client knows freshness
  "version":   "v_18229"                 ← for client-side cache validation
}
```

Cursor-based, not offset-based — offset breaks when ranks shift between
pages (which they do constantly here).

### 3.3 Get my rank (+ neighbors)

```http
GET /v1/leaderboards/{game_id}/rank?user_id=u_8821&window=daily&neighbors=5
→ 200 OK
{
  "rank":      184,
  "score":     1250,
  "percentile": 99.81,
  "neighbors": [{"rank":179,...}, ..., {"rank":189,...}]
}
```

### 3.4 Live updates (optional)

```
WebSocket /v1/leaderboards/{game_id}/stream?window=daily
  ← server pushes top-K diffs every 1–2s
```

Mention it, don't design it now — promise to revisit in deep dive if asked.

---

## Phase 4 — High-Level Design (8 min)

> "I'll start with the dumbest possible design and evolve it. That way
> every component on the board has an explicit reason to exist."

### Step 1 — The naive design

```
[Client] → [API] → [Postgres: scores table]
                      ↓
                 SELECT user_id, SUM(delta)
                 FROM scores WHERE game_id=?
                 GROUP BY user_id ORDER BY SUM DESC LIMIT 100
```

> "This works for 1K users. At 1M write QPS this query is a full table
> scan on every read — dead on arrival. Three problems: (a) reads scan
> writes, (b) writes contend on the same hot rows, (c) no pre-computed
> ordering."

### Step 2 — Pre-compute the ranking with Redis Sorted Sets

```
[Client] → [API] → [Redis ZSET]   ZINCRBY leaderboard:game42:daily 50 u_8821
                      ↓ (async)
                  [Postgres]      durable history
```

> "Redis sorted sets are *the* primitive for this problem. ZINCRBY is
> O(log N) and atomic. ZREVRANGE 0 99 is O(log N + K). We get top-K,
> rank-of-user, and atomic increment in one data structure. That alone
> kills the read-side problem."

### Step 3 — Decouple writes with Kafka

```
[Client] → [API] → [Kafka: scores topic]
                        ↓
                  [Score Workers] ──► [Redis ZSET]    (hot leaderboard)
                        └─────────► [Cassandra]      (durable history)
```

> "Why Kafka here: (1) a 1M-QPS spike doesn't crush Redis or the DB —
> Kafka absorbs and we drain at our pace. (2) Kafka is the durable WAL,
> so if Redis dies we replay. (3) We get a free audit log for fraud /
> anti-cheat later. The API just appends to Kafka and returns 202."

### Step 4 — Shard Redis (this is the concurrency story preview)

```
                                       ┌─► Redis shard 0  (hash(user)%N == 0)
[Score Workers] ── partition by user ──┼─► Redis shard 1
                                       ├─► ...
                                       └─► Redis shard N-1
```

> "Single Redis ≈ 100K ops/s. We need 1M, so ≥ 16 shards with headroom.
> I'll shard by `user_id` because increments target a single user's
> score — that pins all writes for one user to one shard, which gives
> us atomicity for free without distributed locks. We'll talk about
> what this *costs* (cross-shard top-K) in the deep dive."

### Step 5 — Read path: edge cache + replicas

```
[Client] → [CDN edge]  (1s TTL on top-K page; absorbs 90% of read QPS)
              ↓ miss
           [Read API] → [Redis replicas]    (top-K + my-rank)
                            ↓ replica lag
                         [Redis primaries]  (writes only)
```

> "The top-K leaderboard is the same response for every viewer for
> 1–2 seconds. CDN edge cache turns 5M read QPS into ~50K origin QPS.
> *My rank* can't be edge-cached — it's per-user — so it goes to a
> Redis replica directly."

### Step 6 — Final architecture (draw this clean)

```
         ┌──────────────────────────────────────────────────────────────┐
         │                          CLIENTS                             │
         │                  (mobile / web / driver app)                 │
         └─────────────┬──────────────────────────────────┬─────────────┘
                       │ writes                           │ reads
                       ▼                                  ▼
                ┌────────────┐                   ┌──────────────┐
                │   API GW   │                   │     CDN      │   ← top-K page
                │  +rate lim │                   │  (1s TTL)    │      cached
                └─────┬──────┘                   └──────┬───────┘
                      │                                 │ miss
                      ▼                                 ▼
              ┌───────────────┐                ┌────────────────┐
              │ Score Service │                │  Read Service  │
              │  (stateless)  │                │  (stateless)   │
              └──────┬────────┘                └────────┬───────┘
                     │ produce                          │ ZREVRANGE
                     ▼                                  │ ZREVRANK
              ┌───────────────┐                         │
              │   KAFKA       │                         │
              │ scores topic  │                         │
              │ (partition by │                         │
              │   user_id)    │                         │
              └───────┬───────┘                         │
                      │ consume                         │
                      ▼                                 │
              ┌───────────────┐                         │
              │ Score Workers │                         │
              │ (idempotent)  │                         │
              └───┬───────┬───┘                         │
                  │       │                             │
        ZINCRBY   │       │  INSERT                     │
                  ▼       ▼                             ▼
        ┌──────────────────┐  ┌────────────┐  ┌────────────────┐
        │  REDIS CLUSTER   │  │ CASSANDRA  │  │ REDIS REPLICAS │
        │  (sharded ZSETs) │  │  (events,  │  │ (read-only)    │
        │  primaries       │──▶ history,   │  │                │◀┐
        │                  │  │  audit)    │  │                │ │
        └────────┬─────────┘  └────────────┘  └────────────────┘ │
                 │ async replication ──────────────────────────► │
                 │                                                │
                 ▼                                                │
        ┌──────────────────┐                                      │
        │ Snapshot to S3   │  every 5 min  (cold-start recovery)  │
        └──────────────────┘                                      │
                                                                   │
  ┌───────────────────────────────────────────────────────────────┘
  │ live updates
  ▼
┌──────────────────┐    pub/sub     ┌──────────────────┐
│ WebSocket Layer  │  ◀────────────│ Score Workers    │
│ (fan-out top-K   │                │  publish diffs   │
│  diffs to UIs)   │                └──────────────────┘
└──────────────────┘
```

### Walk the data flow out loud (do this!)

**Write path (submit score):**
1. Client `POST /scores` with idempotency key.
2. API authenticates, rate-limits, produces to Kafka partition `hash(user_id) % P`. Returns 202.
3. Score worker for that partition consumes, dedupes by idempotency key (Redis SET NX with 24h TTL).
4. Worker `ZINCRBY leaderboard:{game}:{window} delta user_id` on the user's Redis shard.
5. Worker writes the raw event to Cassandra (`scores_by_user`, `scores_by_game_time`).
6. Worker publishes a `score_changed` event to a Redis pub/sub channel; WebSocket layer fans out diffs.

**Read path (top-K):**
1. Client `GET /top?window=daily&limit=100`.
2. CDN serves cached page if fresh (≤ 1s old). 90% of traffic stops here.
3. On miss, Read Service does scatter-gather: `ZREVRANGE 0 99 WITHSCORES` on each Redis shard, merges with a min-heap of size 100, returns.

**Read path (my rank):**
1. Client `GET /rank?user_id=...`.
2. Read Service routes to the Redis shard that owns this `user_id`.
3. `ZREVRANK key user_id` → returns rank within shard.
4. To get *global* rank we need the shard's local rank + count of users in higher-scoring shards above this user's score → `ZCOUNT key (score +inf` on all other shards. Mention: "this is one of the trade-offs of sharding by user; I'll cover it in the deep dive."

---

## Phase 5 — Database Design (5 min)

We have **three** stores, each picked for a specific access pattern.

### 5.1 Redis Sorted Sets — the hot leaderboard

```
KEY: leaderboard:{game_id}:{window}:{bucket}   (e.g. leaderboard:g42:daily:2026-05-02)
TYPE: ZSET
MEMBER: user_id
SCORE: cumulative score in that window

Ops we use:
  ZINCRBY  key delta user_id            ─ atomic increment
  ZREVRANGE key 0 K-1 WITHSCORES        ─ top K
  ZREVRANK key user_id                  ─ user rank (0-indexed)
  ZSCORE   key user_id                  ─ user score
  ZCOUNT   key (score +inf              ─ # of users above a score (for global rank)
  EXPIREAT key <end-of-window>          ─ auto-purge daily/weekly
```

**Why ZSET wins:** every operation we need is built-in, atomic, and
O(log N). Anything we built on top of a generic KV store would be
strictly worse.

### 5.2 Cassandra — durable event history

Two tables (denormalized, designed around queries):

```
TABLE scores_by_user (
  user_id    text,
  game_id    text,
  ts         timeuuid,
  delta      int,
  idem_key   text,
  PRIMARY KEY ((user_id, game_id), ts)
) WITH CLUSTERING ORDER BY (ts DESC);
-- query: "show user X's score history in game Y"

TABLE scores_by_game_time (
  game_id    text,
  bucket     text,        -- e.g. '2026-05-02-12' (hour bucket)
  ts         timeuuid,
  user_id    text,
  delta      int,
  PRIMARY KEY ((game_id, bucket), ts)
);
-- query: "replay all events in game Y between T1 and T2"
-- bucketing prevents unbounded partitions
```

**Why Cassandra (not Postgres):** writes are 1M QPS, append-only,
key-on-user — Cassandra's hash partitioning + LSM tree is the textbook
fit. Postgres would need aggressive partitioning + manual sharding to
match it.

### 5.3 Postgres — small relational reference data

```
users(user_id PK, name, region, created_at)
games(game_id PK, name, starts_at, ends_at, kind)
windows_config(game_id, window, ttl_seconds)
```

Tiny, transactional, rarely written. ACID matters here (account creation,
game lifecycle). Strong fit for Postgres.

### 5.4 Idempotency store

```
KEY: idem:{game_id}:{idempotency_key}   (Redis)
VALUE: <submission_id>
TTL: 24h
SET NX  ─ atomic "first writer wins"
```

### 5.5 Sharding strategy summary

| Store      | Shard key             | Why                                     |
| ---------- | --------------------- | --------------------------------------- |
| Redis ZSET | `user_id` mod N       | Pins all writes for a user to one shard → atomic ZINCRBY without distributed locks |
| Kafka      | `user_id` hash        | Same partition for a user → ordered, single-consumer, idempotent |
| Cassandra  | `(user_id, game_id)`  | Even distribution; user history queries land on one node |

> "Notice all three stores shard on `user_id`. That's not a
> coincidence — it's how we get end-to-end ordering and atomicity for
> a single user's score without ever taking a lock."

That sentence wins the round.

---

## Phase 6 — Deep Dives (15 min — **this is the bar raiser**)

The interviewer's three asks map directly to deep dives 1 and 2 below.
Lead with caching, then go deep on concurrency, then briefly touch
hot-key handling because they will probably ask.

---

### Deep Dive 1 — Caching strategy

**The challenge:** 5M read QPS. Origin can't serve that. Naive caching
goes stale instantly because the leaderboard moves every millisecond.

**Layered cache topology:**

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Client cache │→ │  CDN edge    │→ │ App-tier LRU │→ │ Redis (auth.)│
│ (5s, ETag)   │  │  (1–2s TTL)  │  │ (200ms TTL)  │  │ source-of-   │
│              │  │  ~95% hit    │  │ ~70% hit     │  │ truth        │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
```

| Layer        | What's cached       | TTL     | Invalidation        |
| ------------ | ------------------- | ------- | ------------------- |
| Client       | Top-K JSON          | 5s      | ETag / If-None-Match |
| CDN edge     | Top-K JSON per game | 1–2s    | TTL only (cheap, fine) |
| App-tier LRU | Top-K + neighbors   | 200ms   | TTL + version bump  |
| Redis (auth) | The leaderboard     | n/a     | Source of truth     |

**Three caching decisions that show seniority:**

**(a) Cache the *response*, not the query.** Top-K is identical for every
viewer. Cache the full serialized JSON page at the edge with a short
TTL — 90% of read traffic dies at the CDN. This is by far the highest-
leverage decision.

**(b) Use staleness, not invalidation.** Active invalidation across a
CDN at 1M+ req/s is a nightmare. A 1–2s TTL with eventual consistency
is operationally simpler and matches the product (users don't notice a
1s stale leaderboard). **Tell the interviewer:** "I deliberately chose
TTL over invalidation because the freshness requirement (≤2s) makes
TTL cheap and correct. Invalidation would buy us nothing here and
introduces a thundering-herd risk."

**(c) "My rank" is not edge-cacheable** — it's per-user. So it skips
the CDN entirely and goes straight to a Redis replica. Replica reads
are ~1ms; that's fine for p99 < 100ms.

**Cache stampede protection.** When the CDN entry expires, 1000s of
edge nodes hit origin simultaneously for the same key. Two defenses:
1. **Request coalescing** at the read service (single-flight per key).
2. **Probabilistic early refresh** — refresh the cache 100ms before
   TTL with probability proportional to remaining TTL. Smooths the
   thundering herd without ever serving stale > TTL.

**Cold-start recovery.** Redis dies → in-memory ZSETs gone. Recovery:
1. Reload latest snapshot from S3 (5 min stale).
2. Replay Kafka from snapshot offset forward (catch up the last 5 min).
3. Workers are idempotent (idempotency key store), so replay is safe.

> "So caching here isn't 'put Redis in front of Postgres' — Redis *is*
> the source of truth for the live leaderboard. The actual cache layer
> is the CDN, and the design choice that matters is using short TTLs
> instead of invalidation. That's the bit I'd defend."

---

### Deep Dive 2 — Concurrency & thread-safety (the bar-raiser meat)

Walk through the full ladder of approaches. The grading criterion isn't
"pick the right one" — it's "show you understand the trade-off space."

#### Approach A — Naive: row-level lock in Postgres

```
BEGIN;
  SELECT score FROM scores WHERE user_id=? AND game_id=? FOR UPDATE;
  UPDATE scores SET score = score + 50 WHERE ...;
COMMIT;
```

- ✅ Correct.
- ❌ At 1M QPS this lock-thrashes a single row. p99 → seconds.
- ❌ Top user becomes a global hot row.
- **Verdict:** correct, doesn't scale. Walking away.

#### Approach B — Optimistic concurrency control (CAS w/ version)

```
expected_version = read(user.version)
new_score = old_score + delta
UPDATE scores
   SET score=new_score, version=version+1
 WHERE user_id=? AND version=expected_version
-- retry on conflict
```

- ✅ No locks held across network.
- ❌ Under contention, retry storms — p99 still terrible for hot users.
- ❌ Wastes CPU on retried work.
- **Verdict:** better than A, still wrong shape for 1M QPS.

#### Approach C — Atomic increments in Redis (ZINCRBY)

```
ZINCRBY leaderboard:g42:daily 50 u_8821
```

- Redis is **single-threaded per shard**. Every command is atomic by
  construction. No locks, no CAS, no retries.
- Two concurrent `ZINCRBY +50` from different threads on different API
  servers are serialized inside Redis and both apply correctly.
- Throughput per shard: ~100K ops/s. We shard, we hit 1M.

> **Say this:** "The single-threaded event loop in Redis is doing the
> serialization for us. We're outsourcing the lock to Redis. That's why
> ZINCRBY is the right primitive — concurrency safety is a free
> by-product of the data store, not a thing we build on top."

- ✅ Atomic, no app-side coordination.
- ✅ Sharding by `user_id` keeps the same user's writes on one shard,
     which preserves per-user ordering.
- **Verdict:** **This is the answer.** A and B are useful only as
  context to justify why C is right.

#### Approach D — Kafka partition ordering as the global lock

```
producer.send(topic="scores", key=user_id, value=event)
```

- Kafka guarantees ordering within a partition. Partition by `user_id`
  → all of one user's events land on one partition, consumed by one
  worker thread, in order.
- Even before Redis, the ordering is *already* guaranteed at the queue.
- Workers are single-consumer per partition → no concurrent writes for
  the same user at the worker layer either.

> "So we actually have **two** layers of serialization: Kafka partition
> ordering at ingestion, and Redis single-threadedness at the data
> store. Either alone is sufficient; together they're belt-and-braces
> and they cost us nothing extra because we wanted both for other
> reasons (durability, throughput)."

This is the moment that earns the SDE2 → bar-raiser pass. You're not
reaching for `synchronized` blocks or distributed locks; you're showing
that the *architecture* makes concurrency safe.

#### Approach E — Distributed locks (Redlock) — when?

> "Honest answer: not for this problem. Distributed locks are for
> mutual exclusion across operations that span multiple resources,
> like 'transfer money from account A to B atomically across shards.'
> Our problem is single-key increment, which Redis already serializes.
> Redlock here would add latency, failure modes, and split-brain risk
> for zero correctness gain. I'd reject it."

The bar raiser is *trying* to bait you into Redlock. Decline cleanly.

#### Approach F — Hot-key / celebrity problem

What if one user (or one bot) generates 200K writes/sec? That blows
past a single Redis shard's 100K limit.

**Counter sharding (the real fix):**

```
For hot user `u_42`:
  shard the counter into M sub-keys
  ZINCRBY leaderboard:g42:daily:shard0 +50 u_42   (write goes to a random sub-shard)
  ZINCRBY leaderboard:g42:daily:shard1 +50 u_42
  ...
At read time:
  total = ZSCORE shard0 + ZSCORE shard1 + ... + ZSCORE shardM
```

- Detect hot keys via metrics (writes/sec per user > threshold).
- Promote them dynamically to sharded counters.
- Trade-off: read for `getRank` becomes O(M) instead of O(1). M=16
  is fine.

This pattern is identical to Twitter's celebrity fan-out; calling that
parallel out loud is good.

#### Approach G — Read-your-writes for the submitter

When a user submits a score, the API ack should reflect their new
rank — not the rank from 2 seconds ago. Implementation:

1. API server calls `ZINCRBY` synchronously on the user's Redis shard
   (this is the only sync Redis call in the write path).
2. Returns predicted rank in the 202 response.
3. The Kafka pipeline still runs async for durability + downstream fan-out.

> "Doing both — sync Redis update *and* async Kafka — sounds like
> double work, but it's actually clean: Redis gives us read-your-writes
> for the submitter, Kafka gives us durability and replayability.
> They're answering different questions."

#### Concurrency summary table (draw this!)

| Approach                  | Correct | 1M QPS? | Hot key safe? | Where it lives    |
| ------------------------- | :-----: | :-----: | :-----------: | ----------------- |
| Postgres row lock         | ✅      | ❌      | ❌            | DB                |
| Optimistic CAS            | ✅      | ❌      | ❌            | DB / app          |
| Redis ZINCRBY             | ✅      | ✅      | ❌            | Redis (per shard) |
| Kafka partition ordering  | ✅      | ✅      | ❌            | Ingestion         |
| **Redis + Kafka (chosen)**| ✅      | ✅      | ⚠️ + counter sharding | End-to-end |
| Redlock                   | ✅      | ❌      | n/a           | Cross-shard ops   |

---

### Deep Dive 3 — Cross-shard top-K (if asked)

We sharded by `user_id`, so no single Redis node has the global
ranking. How do we compute global top 100?

**Scatter-gather merge:**

```
parallel for each of N shards:
  local_top = ZREVRANGE shard_key 0 99 WITHSCORES   ─ each shard's top 100
merge:
  min-heap of size 100 over (local_top_0 ∪ ... ∪ local_top_{N-1})
return heap as global top 100
```

- N=16 shards, each returns 100 entries → 1600 entries → heap merge
  is microseconds.
- Latency = max(per-shard latency) ≈ 2ms. Cached at the CDN for 1s,
  so origin is hit ≤ once per game per second per region.

**Why it's safe:** the global #1 must be the local #1 of *some* shard.
Same for top-K: top K globally ⊆ union of top K of each shard.

**Global rank of a user:** harder. Compute via:
```
global_rank(user) = local_rank(shard(user))
                  + Σ (count of users in other shards with score > my_score)
                  = ZREVRANK on home shard
                  + Σ ZCOUNT(score, +inf) on all other shards
```
Cost: N parallel ZCOUNTs ≈ 2ms. Acceptable.

**Alternative:** maintain a separate "global top-1000" ZSET fed by
periodic merge — gives O(1) global rank for top users. Mention as a
follow-up optimization, don't implement.

---

## Phase 7 — Wrap-Up (3 min)

### Summary (30 sec)

> "To recap: we have a stateless API tier producing to Kafka,
> partitioned by user. Workers consume and do two things — atomically
> increment a sharded Redis ZSET for the live leaderboard, and append
> to Cassandra for durable history. Reads hit a CDN edge cache for
> top-K (1s TTL absorbing 95% of traffic) and Redis replicas for per-
> user rank. Redis's single-threaded event loop combined with Kafka
> partition ordering gives us thread-safety end-to-end without any
> application-level locking."

### Bottlenecks I'd watch (30 sec)

1. **Hot keys** — celebrity / bot users. Mitigation: counter sharding.
2. **CDN cache stampede** at TTL boundary — request coalescing + probabilistic early refresh.
3. **Cross-shard global rank** — fanout cost grows with shard count;
   bounded by maintaining a separate global top-1000 ZSET.
4. **Kafka rebalance pauses** — partition ownership changes stall a
   user's writes for seconds. Mitigation: cooperative rebalancing,
   stable assignor.
5. **Redis primary failover** — write outage during the failover
   window. Mitigation: Redis Sentinel / Cluster, multi-AZ replicas,
   Kafka backlog absorbs the gap.

### Future improvements (30 sec)

- **Anti-cheat** — score-velocity anomaly detection on the Kafka stream (Flink).
- **Global / regional leaderboards** — replicate Redis cross-region with eventual consistency.
- **Friends-only leaderboards** — secondary index `user → friend_ids`, intersect with the global ZSET; or per-user materialized ZSET if friend graph is small.
- **Time-decay scoring** — score(t) = Σ delta_i × e^{-λ(t - t_i)}; needs periodic recompute, can't use ZINCRBY.
- **Tie-breaking** — encode (score, -timestamp) into a single 64-bit ZSET score so earlier achievers win ties.

### Likely follow-up curveballs (be ready)

| Question                                  | One-line answer                                         |
| ----------------------------------------- | ------------------------------------------------------- |
| "10× the scale?"                          | More Redis shards (linear), more Kafka partitions, multi-region read replicas. |
| "Region goes down?"                       | Active-active across two regions; Kafka MirrorMaker; Redis CRDT or eventual reconciliation. |
| "How do you handle a user disputing their rank?" | Cassandra is the audit log; replay events for user X to verify. |
| "How would you migrate from Postgres?"    | Dual-write to Postgres + Kafka, backfill historical events into Kafka, cut reads over once Redis is warm. |
| "Why not use ElasticSearch?"              | ZSET ops are O(log N) and atomic; ES doesn't give us atomic increment. ES would be a downstream sink for analytics, not the live store. |

---

## Cheat sheet — what to draw, in order

```
1.  Requirements box       (Phase 1)
2.  Numbers (3 lines)      (Phase 2:  1M QPS / 32GB/s / 3TB)
3.  3 API signatures       (Phase 3)
4.  Architecture diagram   (Phase 4 — build it in 6 steps live)
5.  3 storage boxes        (Phase 5:  Redis ZSET / Cassandra / Postgres)
6.  Concurrency table      (Phase 6:  6 approaches, ✅/❌)
7.  Bottlenecks list       (Phase 7)
```

## Time budget (45-min interview)

| Phase                    | Time   | Cumulative |
| ------------------------ | -----: | ---------: |
| 1. Requirements          | 5 min  |  5 min     |
| 2. Estimation            | 3 min  |  8 min     |
| 3. API design            | 3 min  | 11 min     |
| 4. High-level design     | 8 min  | 19 min     |
| 5. Database design       | 5 min  | 24 min     |
| 6. Deep dives (cache+conc.) | 15 min | 39 min  |
| 7. Wrap-up               | 3 min  | 42 min     |
| Buffer / Q&A             | 3 min  | 45 min     |

## Ten lines that buy you the offer

Drop these verbatim — they're the lines a bar raiser writes down:

1. *"Read-your-writes for the submitter, eventual for everyone else — that distinction drives the whole design."*
2. *"`delta` not `score` — atomic increments are commutative; SETs race."*
3. *"Idempotency key on writes; clients retry."*
4. *"Redis's single-threaded event loop *is* our concurrency primitive — we outsource the lock to the data store."*
5. *"Kafka partition ordering by user_id gives us serialization at ingestion; Redis gives it at storage. Belt and braces."*
6. *"I'd reject Redlock here — single-key increment doesn't need cross-shard mutual exclusion."*
7. *"Cache the response, not the query — top-K is identical for every viewer for ~1s."*
8. *"TTL over invalidation — the freshness budget makes TTL correct and cheap."*
9. *"Hot keys via counter sharding, same pattern as celebrity fan-out."*
10. *"Cassandra is the audit log; we can always reconstruct Redis from Kafka + snapshot."*
