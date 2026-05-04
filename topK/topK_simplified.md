# HLD — Live Leaderboard with Caching + Concurrency (Uber SDE-2 Interview Script)

> **Prompt:** Design a Live Leaderboard.
> **Interviewer asks:** (1) Design a simple use case. (2) Implement caching. (3) Make it thread-safe.

> **Framework used:** [answering_simplified.txt](Local/HLD/answering_simplified.txt)
> **Total time budget:** ~40 min

---

## Opening (15 seconds — say this verbatim)

> "Cool — Live Leaderboard. Before I dive in, let me restate so we're aligned: a system where users submit scores against one or more leaderboards, and at any moment any client can fetch the top-K, a specific user's rank, or the slice of users around a given user. It's read-heavy on top-K, write-heavy during contests, and the interesting bit is that score updates are concurrent and have to be lossless. I'll spend ~5 min on requirements + estimation, ~10 min on entities + APIs + HLD, ~15 min on deep dives — with extra time on caching and concurrency since you flagged those — and ~3 min wrapping up. Push back if you want me to focus elsewhere."

**How to say it:** Calm and structured. The phrase *"score updates are concurrent and have to be lossless"* signals up front that you understand where the hard part is — bar-raisers love when the candidate names the difficulty before being asked.

---

## Phase 1 — Requirements Clarification (3-5 min)

### Functional Requirements (5-6 points)

> "Let me list what I think is in scope, push back if I'm missing anything:"

1. **Submit / update a score** for a `(user, leaderboard)` pair — either set-absolute (`score = 1500`) or increment (`score += 10`). Idempotency on the request side.
2. **Get top-K** of a leaderboard — `K ∈ [10, 100, 1000]`. Cursor-paginated for larger K.
3. **Get a user's rank** in a leaderboard — "you're #4,217 globally".
4. **Get the slice around a user** — "ranks 4,212 to 4,222" — needed for the social/competitive UI ("you're 5 spots from #4,210").
5. **Multiple concurrent leaderboards** — global, weekly, daily, by region/category. A single score event may update many leaderboards (fan-out).
6. **Near-real-time freshness** — top-K should reflect a score change within ~1s; rank can lag a bit more.

### Questions to ask the interviewer (2-3)

> "Three clarifying questions before I size this:"

1. **"Are scores monotonic (only go up) or can they decrease?"** — affects whether we can use `ZADD GT` (Redis-native conditional update). I'll **assume monotonic for the headline use case** (game scores, distance covered, points earned) and call out the non-monotonic case in concurrency.
2. **"How many leaderboards is one score event fanned out to — at most?"** — affects fan-out write amplification. I'll **assume 5-10** (global + region + weekly + daily + category).
3. **"Tie-breaking rule?"** — equal scores → break by `earliestTimestamp` ascending (whoever got there first wins). Encoded into the score itself so Redis sorts it for us.

### Non-Functional Requirements

| NFR | Target | Why |
|---|---|---|
| **Scale** | 10M registered users, 100K concurrent during a contest, **100K writes/sec peak**, **1M reads/sec peak** (top-K is hammered) | Reads >> writes, top-K is a hot key |
| **Latency** | Top-K p99 **< 100ms**, rank/around-me p99 **< 200ms**, write ack **< 50ms** | Live leaderboard means users refresh — slow reads kill engagement |
| **Availability** | **99.9%** for reads, **99.99%** for writes (lost score = lost trust) | Asymmetric — drop-a-read is recoverable, drop-a-write isn't |
| **Consistency** | **Eventual on top-K** (1-2s lag OK), **strong-ish on per-user rank** (read-your-write within the same user) | Top-K can be slightly stale; my own rank seeing my own update is non-negotiable UX |
| **Domain-specific #1** | **No lost updates under concurrency** — two parallel `+10` increments must end at `+20`, never `+10` | This is the headline of the concurrency deep dive |
| **Domain-specific #2** | **Hot-leaderboard tolerance** — one global leaderboard takes most of the load, must not bottleneck | Drives the sharded-leaderboard deep dive |

**Killer phrase to drop:** *"The interesting NFR here isn't latency — it's that score updates are concurrent and lossless. Two `+10` increments racing on the same user has to end at `+20`. That single requirement drives every interesting decision downstream."*

---

## Phase 2 — Estimation (2-3 min)

> "Let me size this so the design choices anchor to numbers, not vibes."

### Traffic (QPS)

- **Writes:** 10M users × 1 score event/min during contest = **~167K writes/sec peak**. Round to **100K writes/sec sustained**, **300K writes/sec burst** for the final minute of a contest.
- **Read amplification:** every active user polls top-K every 5s + checks own rank every 10s. 100K concurrent → **~30K top-K + 10K rank reads/sec** at the application layer. With CDN cache, origin sees 100x less.
- **Fan-out:** 1 score event → 5-10 leaderboard writes (global + region + weekly + daily + category). So **100K logical writes/sec → 500K-1M leaderboard updates/sec** to Redis.

### Storage

- **Hot ZSET:** 10M users × ~100 B per entry (userId + score + tie-breaker) ≈ **1 GB per leaderboard**. With 50 active leaderboards, **~50 GB hot Redis** — fits comfortably in a 12-node cluster.
- **Score history (for audit + recompute):** 100K events/sec × 86,400s × 200 B = **~1.7 TB/day**. Goes to Cassandra or S3-backed Iceberg.
- **Snapshots:** every 5 min, persist the leaderboard state to Postgres for cold-start recovery — 50 leaderboards × 1 GB × 12/hr × 24 = ~14 TB/day if naively, so we keep last 24h hot + sparse hourly snapshots after.

### Bandwidth

- Writes: 100K × 200 B = ~20 MB/s ingress — trivial.
- Reads (top-K of 100): 100 entries × 100 B × 30K reads/sec = ~300 MB/s — significant; **CDN/edge cache absorbs 99% of this**.

### Domain-specific number — read/write asymmetry

> "The number that matters most: read-to-write ratio is roughly **10:1 globally, but 1000:1 on the top-K of the most popular leaderboard**. So the design has to optimize hot-key reads ruthlessly — that's what drives the multi-tier cache and the sharded leaderboard later."

**How to say it:** Slow down. Write the 100K writes / 1M reads / 1M leaderboard updates/sec on the whiteboard. These are the numbers you'll point back to in every later trade-off.

---

## Phase 3 — Core Entities (3-5 min, bottom-up)

> "Let me list the entities — these will become tables, ZSETs, and topics later."

| # | Entity | Key fields | Notes |
|---|---|---|---|
| 1 | **User** | `id`, `displayName`, `region`, `avatarUrl` | Source of truth for display data; not on the hot path |
| 2 | **Leaderboard** | `id`, `name`, `scope` (global/region/category), `period` (all-time/weekly/daily), `resetAt`, `tieBreaker` | Defines a single ranking |
| 3 | **Score** | `userId`, `leaderboardId`, `score`, `version`, `lastUpdatedAt` | Current authoritative value; `version` is the CAS counter |
| 4 | **ScoreEvent** | `eventId`, `userId`, `delta` or `absolute`, `source`, `timestamp` | Append-only log; the input to the system |
| 5 | **LeaderboardEntry** | `userId`, `score`, `tieBreakerTs` | Materialized in Redis ZSET; `member = userId`, `score = encoded(score, tieBreakerTs)` |
| 6 | **RankSnapshot** | `leaderboardId`, `takenAt`, `entries[]` | Periodic dump for recovery + cold storage |

**How to say it:** Read them off in 60 seconds. Highlight `version` on the Score entity — that's foreshadowing the concurrency deep dive.

---

## Phase 4 — API Design (3-5 min)

> "I'll define 6 APIs. All authenticated via service-to-service JWT for ingest, user-token for read."

### 1. **Submit Score API** — the hot write path

- Handles a score submission from a game/ride/order completion. Idempotency-Key makes retries safe.
- `POST /v1/leaderboards/{leaderboardId}/scores`
  Request: `{ userId, delta: 10 }` *or* `{ userId, absolute: 1500 }`
  Headers: `Idempotency-Key: <uuid>`
  Response: `202 Accepted { eventId, queuedAt }`

### 2. **Get Top-K API** — the hot read path

- Returns the top K of a leaderboard. Heavily cached (CDN + Redis + in-process).
- `GET /v1/leaderboards/{leaderboardId}/top?k=100`
  Response: `{ entries: [{ userId, displayName, score, rank }, ...], generatedAt }`
  Cache-Control: `public, max-age=2, stale-while-revalidate=10`

### 3. **Get User Rank API**

- Returns a single user's rank + score. Hot for "my profile" views.
- `GET /v1/leaderboards/{leaderboardId}/rank/{userId}`
  Response: `{ userId, rank, score, totalParticipants }`

### 4. **Get Around-Me API**

- Returns N entries above and N below a given user — used for the "you and your neighbors" UI.
- `GET /v1/leaderboards/{leaderboardId}/around/{userId}?range=5`
  Response: `{ entries: [...11 entries centered on user] }`

### 5. **Create Leaderboard API** (admin)

- Defines a new leaderboard with scope, period, reset rules.
- `POST /v1/leaderboards`
  Request: `{ name, scope, period, resetAt, tieBreaker }`
  Response: `201 { leaderboardId }`

### 6. **Get User's Leaderboard History**

- For a user's profile — "all leaderboards I'm on, my best ranks". Lower priority, served from Postgres.
- `GET /v1/users/{userId}/leaderboards?cursor=<opaque>`
  Response: `{ entries: [{ leaderboardId, currentRank, bestRank }, ...], nextCursor }`

**Killer detail to drop:** *"Notice the submit API is `202 Accepted` with an eventId — not `200 OK` with the new rank. We acknowledge the write, persist it durably, then process asynchronously. Synchronous rank-back would tightly couple the API to Redis availability and add ~50ms p99 we don't need. The client can poll `/rank/{userId}` if it wants the new value — and it usually doesn't."*

---

## Phase 5 — High-Level Design (10-12 min)

> "I'll build this in 3 layers — ingest, ranking, serving — then draw the full picture."

### Services (5-8)

| Service | Responsibility |
|---|---|
| **Leaderboard API Gateway** | Auth, rate limit, request validation, idempotency check, publishes writes to Kafka, routes reads to cache → Redis → DB |
| **Score Ingest Service** | Consumes `score.events`, applies dedup by `eventId`, writes ScoreEvent to Cassandra (audit log), publishes to per-leaderboard fan-out topic |
| **Ranking Service** | Consumes per-leaderboard topic, applies score to Redis ZSET (atomic), updates Postgres `Score` row with CAS, emits `score.updated` event |
| **Read Service** | Serves top-K, rank, around-me — reads from in-process LRU → Redis ZSET → Postgres fallback |
| **Snapshot Service** | Every 5 min, snapshots each Redis ZSET to Postgres for durability + cold-start recovery |
| **Reset Scheduler** | Cron-driven; at period boundaries (midnight UTC for daily, Sunday for weekly), creates a new ZSET and atomically swaps the active key |

### Database Layer

| Store | Tech | Why this DB (and why not others) |
|---|---|---|
| **Hot leaderboards (rank/top-K)** | **Redis ZSET** (Sorted Set) | O(log N) `ZADD` and `ZINCRBY`, O(log N + K) `ZREVRANGE` for top-K, O(log N) `ZRANK` — exactly the operations a leaderboard needs. Single-threaded model gives atomicity for free. **Not Postgres** — `ORDER BY score LIMIT 100` over 10M rows is ~50ms even with indexes; `ZREVRANGE` is sub-ms. **Not DynamoDB** — no native sorted-set; would need GSI gymnastics. |
| **Authoritative Score state** | **PostgreSQL** (sharded by `userId`) | Source of truth for score + version (CAS counter). Need transactions and conditional updates. **Not Cassandra** — LWT (lightweight transactions) work but are expensive; Postgres CAS via `UPDATE … WHERE version = ?` is cheaper and the volume fits. |
| **ScoreEvent audit log** | **Cassandra** (partition: `userId`, cluster: `eventTs DESC`) | Write-heavy append-only at 100K/sec, time-series access ("show me all score events for user X"). Perfect Cassandra shape. |
| **Leaderboard config** | **PostgreSQL** | Tiny volume, structured, occasionally read on cache miss. |
| **User profile cache** | **Redis** (cache-aside) | Top-K returns userId + display fields; we hydrate from Redis user-profile cache to avoid Postgres round-trip per entry. |
| **Snapshots / cold storage** | **S3 (Parquet)** for >24h, **Postgres** for hot snapshots | Cheap, queryable for offline analytics. |

### Cache Layer

- **CDN / edge cache (Fastly/Cloudflare)** — sits in front of `/top?k=100` for public leaderboards. `Cache-Control: max-age=2, stale-while-revalidate=10`. Absorbs ~99% of read traffic on the most popular leaderboard. **This is the single biggest performance lever.**
- **In-process LRU (Caffeine, Java) on Read Service** — sits between request handler and Redis. Caches the top-100 of each leaderboard for 1 second. At 1M reads/sec on a hot leaderboard, this collapses ~1M Redis ops/sec to ~1 op/sec/instance.
- **Redis ZSET itself is the cache** for ranking — the authoritative state lives in Postgres but we'd never serve top-K from Postgres. Cache-aside semantics: on Redis cold start (eviction or restart), the Snapshot Service rebuilds from the latest Postgres snapshot + replays Cassandra events newer than the snapshot.

### Queue Layer

- **Kafka — `score.events` topic** — single ingest topic, **partitioned by `userId`**. Sits between API Gateway and Score Ingest Service. **Why partition by `userId`?** Guarantees all updates for a single user land on the same partition → consumed by a single thread → serialized per user. **This is the foundation of the concurrency story** (Deep Dive 3). 60-day retention for replay.
- **Kafka — `leaderboard.{leaderboardId}` topics** — one per leaderboard, partitioned by `userId`. Sits between Score Ingest and Ranking Service. **Why per-leaderboard?** Fan-out happens once at ingest, then each leaderboard scales independently. The hot global leaderboard has 32 partitions; obscure category ones have 1.
- **Kafka — `score.updated` topic** — emitted after Redis update; consumed by analytics, push notifications, webhooks. Off the hot path.

### Architecture Diagram

```
                ┌──────────────────┐
                │  Clients         │
                │  (mobile / web)  │
                └────────┬─────────┘
                         │ reads          writes
                ┌────────┴────────┬───────────────┐
                ▼                 │               ▼
          ┌──────────┐            │      ┌───────────────┐
          │   CDN    │            │      │ Leaderboard   │
          │  /top    │            │      │ API Gateway   │
          └────┬─────┘            │      │ (auth, ratel, │
               │ miss             │      │  idempotency) │
               ▼                  │      └───────┬───────┘
       ┌───────────────┐          │              │
       │  Read Service │          │              ▼
       │  ┌──────────┐ │          │   ╔═══════════════════════╗
       │  │ Caffeine │ │          │   ║ Kafka: score.events    ║
       │  │  LRU     │ │          │   ║ (partition by userId)  ║
       │  └────┬─────┘ │          │   ╚══════════╤════════════╝
       └───────┼───────┘          │              │
               │ miss             │              ▼
               ▼                  │     ┌──────────────────┐
       ┌────────────────────┐     │     │  Score Ingest    │
       │  Redis Cluster     │◄────┴─────┤  Service         │
       │  ┌──────────────┐  │           │  (dedup by       │
       │  │ ZSET per     │  │           │   eventId, fanout│
       │  │ leaderboard  │  │           │   per board)     │
       │  └──────────────┘  │           └────────┬─────────┘
       │  ┌──────────────┐  │                    │
       │  │ User profile │  │                    ▼
       │  │ cache        │  │     ╔═══════════════════════════╗
       │  └──────────────┘  │     ║ Kafka: leaderboard.{id}   ║
       └─────────┬──────────┘     ║ (partition by userId)     ║
                 │ miss / rebuild ╚═════════╤═════════════════╝
                 ▼                          │
       ┌─────────────────┐                  ▼
       │  Snapshot Svc   │       ┌──────────────────────────┐
       │  (every 5 min)  │       │  Ranking Service          │
       └────────┬────────┘       │  ┌──────────────────────┐ │
                │                │  │ Lua: ZADD GT +       │ │
                ▼                │  │ Postgres CAS         │ │
       ┌─────────────────┐       │  └──────────┬───────────┘ │
       │  PostgreSQL     │ ◄─────┤             │             │
       │  (Score, config,│       └─────────────┼─────────────┘
       │   snapshots)    │                     │
       │  sharded/userId │                     ▼
       └─────────────────┘            ┌──────────────────┐
                                      │  Cassandra       │
                                      │  (ScoreEvent     │
                                      │   audit log)     │
                                      └──────────────────┘
```

**Data flow walkthrough — narrate this verbally:**

> "Client calls `POST /scores` with `{userId: 42, delta: +10}` and an Idempotency-Key. Gateway authenticates, checks the idempotency key in Redis (`SET NX EX 600`), and publishes to `score.events` partitioned by userId. Score Ingest pulls it, dedupes again on eventId (defense in depth), writes to Cassandra audit log, and republishes to `leaderboard.global`, `leaderboard.weekly-1234`, `leaderboard.daily-2026-05-02`. Ranking Service consumers — one per leaderboard, partitioned by userId so user 42's updates always land on the same thread — pull from each topic. Each runs a Lua script that does `ZADD GT` on the ZSET *and* the Postgres CAS update in a single round-trip. Done. On the read side: `GET /top?k=100` hits CDN, miss falls to Read Service, Caffeine LRU has it, return in 2ms. Cold path: Caffeine miss → Redis ZREVRANGE → 5ms. Stone-cold path (Redis evicted): Snapshot Service replays Postgres snapshot + Cassandra delta → repopulate Redis."

---

## Phase 6 — Deep Dives & Trade-offs (12-15 min)

> "Let me pick four deep dives. Two are the asks you flagged — caching and concurrency — and two support them: the Redis ZSET primitive itself, and sharding for hot leaderboards. I'll spend the most time on concurrency since that's the one you said I should expand on."

### Deep Dive 1 — Redis ZSET as the Leaderboard Primitive (Ask 1: simple use case)

1. **Component & where it sits.** Redis Cluster, used as the *active state* of every leaderboard. One ZSET per leaderboard, key = `lb:{leaderboardId}`, member = `userId`, score = the encoded score value. The Ranking Service writes; the Read Service reads.
2. **Interaction.** Three primary ops:
   - `ZADD lb:global GT <score> <userId>` — set if greater (monotonic case).
   - `ZINCRBY lb:global <delta> <userId>` — atomic increment.
   - `ZREVRANGE lb:global 0 99 WITHSCORES` — top-100 in ~O(log N + K).
   - `ZREVRANK lb:global <userId>` — user's rank in O(log N).
   - `ZRANGE lb:global (rank-5) (rank+5)` — around-me in O(log N + range).
3. **Why critical.** Building this on Postgres means `SELECT ... ORDER BY score DESC LIMIT 100` over 10M rows on every read — even with a B-tree index, that's 30-50ms and locks at write time. ZSET gives sub-ms reads, atomic single-key writes, and the operations map 1:1 to the API.
4. **Alternatives.**
   - **Postgres with materialized views** — refresh lag (1-5s), serialization on refresh, doesn't scale to 1M reads/sec.
   - **Elasticsearch** — supports sorted aggregations, but write throughput is 10-100× slower than Redis and durability semantics are weaker for this access pattern.
   - **Custom skip-list service** — academically pure (this is what Redis itself uses internally), but writing your own distributed skip list at SDE-2 level is a red flag, not a green one. Use the boring building block.
   - **Cassandra clustering keys** — you can model `(leaderboardId, score DESC, userId)` and do range scans, but `ZINCRBY`-equivalent atomic increment requires LWT and is much slower than Redis.
5. **Recommendation.** **Redis ZSET, one per leaderboard, with `ZADD GT` for monotonic and `ZINCRBY` for additive scores.** The encoded score trick is the SDE-2 detail: encode tie-breaking timestamp into the low bits of the score so ties resolve naturally — `score = (gameScore << 32) | (~earliestTs & 0xFFFFFFFF)`. Killer phrase: *"Redis ZSETs are a skip list under the hood — that's exactly the data structure a leaderboard needs, no need to invent one."*

### Deep Dive 2 — Multi-Tier Caching (Ask 2: caching)

1. **Component & where it sits.** Three concentric tiers — CDN edge → Caffeine in-process → Redis ZSET. Each absorbs a different traffic profile.
2. **Interaction.**
   - **Tier 1 — CDN / edge.** Public top-K endpoints (`/v1/leaderboards/global/top?k=100`) get `Cache-Control: public, max-age=2, stale-while-revalidate=10`. With 1M reads/sec on the global top-100, even at 95% hit rate the CDN absorbs 950K reads/sec, origin sees 50K. The 2-second TTL is the key trade-off — leaderboard *feels* live, but the origin gets a 50× protection factor.
   - **Tier 2 — Caffeine in-process LRU on Read Service.** Caches `(leaderboardId, K)` → entries for 1s with refresh-ahead. Each Read Service instance independently caches; even on CDN miss, 99% of those land on Caffeine. Sized to ~10K entries (100 hot leaderboards × 100 K-values).
   - **Tier 3 — Redis ZSET.** This is *both* the cache and the active state — there's no separate "warm cache" layer because Redis IS the warm tier. Postgres is the source of truth for durability, never directly served.
3. **Why critical.** At 1M reads/sec on a hot leaderboard, no single Redis cluster node can serve that — even at 100K ops/sec/node, you'd need 10+ replicas, and replica lag eats into freshness. Multi-tier collapses traffic to manageable levels at each layer. The CDN tier alone is the difference between "100 origin servers" and "10 origin servers".
4. **Alternatives.**
   - **Cache-aside in front of Postgres** (no Redis as primary). Means top-K *miss* hits Postgres `ORDER BY score LIMIT 100` — ~50ms even on a warm DB. Awful tail latency.
   - **No CDN, just Redis.** Forces Redis cluster to serve 1M reads/sec — doable but expensive (~$$$ replicas) and read-after-write becomes complicated across replicas.
   - **No in-process cache.** Every request hits Redis over the network — adds 1-2ms RTT and saturates Redis network bandwidth before CPU.
   - **Pre-rendered HTML / JSON in S3 + CDN.** Used by some leaderboard products (e.g. Steam) — works for *very* slow updates, breaks down at our 1s freshness budget.
5. **Recommendation.** **CDN (2s TTL, SWR 10s) → Caffeine (1s TTL, refresh-ahead) → Redis ZSET (authoritative active state) → Postgres + Cassandra (durability).** Critical detail to mention: **cache invalidation is *time-based*, not event-based**. Pushing a "leaderboard changed" event to invalidate CDN/Caffeine is theoretically more current but practically a stampede — every score event would invalidate the hottest cache key on Earth, defeating the cache. Time-based eviction with short TTL is the right answer for a leaderboard. *"Eventual freshness within 1-2 seconds, with predictable load on the origin"* — write that down on the whiteboard.

### Deep Dive 3 — Concurrency & Thread-Safety (Ask 3: the headline)

> "This is the deep dive I want to spend the most time on. The naive design loses updates under concurrency. I'll walk through where the races are, then layer in fixes from cheapest to strongest, and recommend the combination."

1. **Component & where the races live.** Three layered races, from outermost in:
   - **(R1) Two API replicas receive `+10` for the same user simultaneously.** Both read score=100 from cache, both compute 110, both write — final value 110, not 120. Lost update.
   - **(R2) Two Ranking Service threads consume the same user's events from the same partition out of order** (after a rebalance + retry). Same lost-update problem inside the consumer.
   - **(R3) Redis update succeeded but Postgres CAS failed**, or vice versa — the two stores diverge.

2. **Interaction & layered defenses.** I'll address each with a different mechanism — defense in depth:
   - **Defense for R1 — Kafka partition-by-userId.** API Gateway never updates Redis directly; it publishes to Kafka with `partitionKey = userId`. Kafka guarantees per-partition ordering, and there's exactly one consumer thread per partition in the Ranking Service. So all updates for a single user are *serialized into a single thread* without any explicit locking. **This is the single most important design decision in the whole concurrency story** — it eliminates the distributed lock problem by routing.
   - **Defense for R2 (within consumer) — atomic Redis ops.** The consumer doesn't read-then-write. It calls `ZINCRBY` (additive case) or `ZADD GT` (monotonic absolute case), both of which are atomic single-command operations on Redis's single-threaded event loop. No read-modify-write window.
   - **Defense for R3 (cross-store consistency) — Lua script + Postgres CAS.**
     - The Ranking Service runs a Lua script in Redis: read current ZSET score, compute new, write back. Lua executes atomically inside Redis. Returns `(oldScore, newScore, version)`.
     - Then, using that version, run `UPDATE Score SET score=?, version=version+1 WHERE userId=? AND leaderboardId=? AND version=?`. If 0 rows — someone won a race; re-read and retry. This is **optimistic concurrency control with a version counter**.
     - If Postgres update fails after Redis succeeded → enqueue compensation event to revert Redis. In practice this almost never fires because the per-userId partition serialization (R1 defense) eliminates the producer-side race.

3. **Why critical.** Lost updates in a leaderboard mean a user sees their `+10` "disappear" — that's the kind of bug that gets screenshotted on Twitter and lives forever. Worse, it's silent — no error, just wrong numbers. Every defense layer has to assume the layer below could fail. At 100K writes/sec, even a 0.001% race rate is 1 lost update/sec — completely unacceptable.

4. **Alternatives considered (this is the meat — show you know all of them).**

   | Approach | How it works | Why I didn't pick it as the primary |
   |---|---|---|
   | **Pessimistic distributed lock (Redlock)** | Acquire a lock on `userId` before update, release after | High latency (3-5ms per lock), failure modes are nasty (lock leaks on crash → use TTL, but TTL too short = lost work, too long = blocks recovery). Redlock has known correctness debates (Kleppmann vs antirez). Reserved for **rare cross-shard reset operations**, not the hot path. |
   | **Single global mutex / DB row lock** | `SELECT … FOR UPDATE` on Postgres before write | Serializes everyone through Postgres, cap ~5K writes/sec — kills the 100K target. |
   | **CRDTs (G-Counter)** | Conflict-free replicated counter, each replica has own slot, sum on read | Beautiful for additive scores, but breaks `ZADD GT` (monotonic absolute) and breaks rank queries (you can't sort a CRDT counter cheaply). Niche. |
   | **Event sourcing — score derived purely from event log** | Compute score by replaying ScoreEvents | We already do this for audit, but serving rank from a replay is too slow for reads. The ZSET is a materialized view *over* the event log — that's actually exactly what we have. |
   | **Single-writer per leaderboard via leader election (ZooKeeper/etcd)** | One node owns each leaderboard, all writes funnel through it | Simple, but the leader is a SPOF and a bottleneck. The Kafka partition approach is better — it's effectively per-userId leader election done implicitly by the partition assignment. |
   | **Pure CAS retry loop on Postgres alone (no Kafka)** | Read version, compute, conditional update, retry on conflict | Works, but at 100K writes/sec on a hot user (e.g. live-streamer in a contest) the retry rate explodes — could see 5-10 retries per write. Kafka serialization sidesteps the contention. |
   | **Lua script in Redis only (no Postgres CAS)** | Skip Postgres on the hot path | We lose the durable source of truth — Redis is in-memory and any flush before persistence drops events. Postgres CAS is the durability anchor. |

5. **Recommendation.** **Layered defense:**
   - **Layer 1 (routing):** Kafka partition-by-userId → single-threaded consumer per user → eliminates 99% of contention without locks.
   - **Layer 2 (atomicity):** Lua script in Redis for compound read-modify-write → atomic inside Redis.
   - **Layer 3 (durability + cross-store consistency):** Postgres `version`-based CAS → single source of truth, detects any drift.
   - **Layer 4 (escape hatch):** Redlock *only* for cross-leaderboard ops like period-reset (creating a new ZSET, swapping the active key) — used rarely, not on the hot path.

   **Killer phrase to drop:** *"The hot path doesn't have a distributed lock. We don't lock — we route. Kafka partition-by-userId means every write for a given user is serialized through one thread, and that single fact does more for correctness than any lock could. Locks become the *exception* — only for cross-shard operations like period reset."*

   **Real-world parallel:** *"This is exactly how Kafka-based stream processors solve concurrency — partition by the consistency boundary. Flink, Samza, Kafka Streams all do this. Locking is the SQL-era answer; partitioning is the streaming-era answer."*

### Deep Dive 4 — Hot-Leaderboard Sharding (when one ZSET isn't enough)

1. **Component & where it sits.** When the global leaderboard's write rate (or memory footprint) exceeds a single Redis node, we shard the *single leaderboard* across multiple sub-ZSETs and merge on read.
2. **Interaction.**
   - Hash-shard users into N sub-leaderboards: `lb:global:0`, `lb:global:1`, …, `lb:global:N-1`. Each user lives in exactly one shard, by `hash(userId) % N`.
   - Writes go to the shard owning that user — no cross-shard work.
   - Top-K read: query each shard for its local top-K (`ZREVRANGE 0 K-1`), merge in the Read Service via a min-heap of size K. With N=16 shards and K=100, that's 16 small queries done in parallel + a tiny in-memory merge. Total latency ~5ms.
   - Rank read for a single user: read their shard's `ZSCORE` to get their score, then in parallel query each *other* shard for "how many entries have score > mine" via `ZCOUNT`. Sum + 1 = global rank. ~10ms.
3. **Why critical.** A single hot ZSET on Redis caps at ~100K writes/sec/node and ~10 GB practical memory. The global all-time leaderboard during a viral contest can blow past both. Without sharding, you're forced to scale vertically and eventually hit a wall.
4. **Alternatives.**
   - **Replicate the ZSET across multiple Redis primaries** — doesn't help writes (still one primary), only reads.
   - **Use a managed sorted-key store (DynamoDB sorted GSI, etc.)** — works but loses Redis's atomicity story (Lua scripts, etc.) we built the concurrency design on.
   - **Probabilistic top-K (Count-Min Sketch + Heavy Hitters)** — useful for *approximate* top-K at extreme scale (Twitter-trending kind of problem), but loses exact rank. Wrong fit for a contest leaderboard where rank is the product.
5. **Recommendation.** **Don't shard prematurely** — start with 1 ZSET per leaderboard. Add shard-on-write when one of: (a) sustained writes >50K/sec/leaderboard, (b) ZSET >5GB, or (c) p99 latency on `ZREVRANGE` > 5ms. When you do shard, **keep the per-userId Kafka partition mapping aligned with the Redis shard mapping** — same hash function — so the consumer thread that serializes a user's updates also writes to that user's specific shard, preserving the no-lock invariant from Deep Dive 3.

**How to say this section:** Speak slowly on Deep Dive 3. Draw the Kafka-partition-to-consumer-thread arrow on the whiteboard. The interviewer asked specifically about thread-safety — this is where you're being graded. Don't rush.

---

## Phase 7 — Wrap-Up (2-3 min)

### Summary of the design (2 points)

1. **Functionally:** A 3-stage pipeline — *ingest* (API + Kafka partition-by-userId for serialization), *rank* (Lua-atomic Redis update + Postgres CAS for durability), *serve* (multi-tier cache CDN → Caffeine → Redis ZSET, never hits Postgres on the hot read path). Score-update + top-K + rank + around-me all map naturally to Redis ZSET ops.

2. **Architecturally:** Read-heavy on cache (1M reads/sec absorbed at the CDN), write-heavy on Kafka (100K events/sec serialized per-user), with Postgres as the boring-but-durable anchor. Three different storage profiles cleanly separated, and the headline trick — **partition by the consistency boundary instead of locking** — is what makes 100K writes/sec lossless without distributed locks.

### Trade-offs (2 points)

1. **Eventual consistency on top-K (1-2s lag).** Trade for cache-driven scale. The product can absolutely live with this; nobody refreshes a leaderboard 5 times a second expecting strict serializability.
2. **Layered defense vs. simpler single-mechanism design.** Kafka partition + Lua + Postgres CAS is more moving parts than "use Redlock for everything", but each layer addresses a specific failure mode and the hot path stays lock-free. The maintainability cost is offset by the operational simplicity of *not* dealing with lock leaks at 3am.

### Performance optimizations (2 points)

1. **Encoded score with tie-breaker timestamp** — `score = (gameScore << 32) | (~ts & 0xFFFFFFFF)` lets Redis sort ties for free with no extra round-trip, no application-side merge.
2. **Refresh-ahead on Caffeine** — when an entry's TTL is 80% elapsed, async-refresh from Redis instead of evicting and forcing the next request to wait. Eliminates the "1 in N requests sees a 5ms cache miss" tail.

### Extensibility (2 points)

1. **Personalized leaderboards** ("friends only", "people in my city") — same engine, just a new leaderboard scope. The fan-out at ingest already supports arbitrary per-event leaderboard sets; extending is purely a config change in the leaderboard registry.
2. **Time-windowed leaderboards** ("last 1 hour") — current design handles fixed periods (daily/weekly via Reset Scheduler). For a sliding 1-hour window we'd need a stream-processing layer (Flink) maintaining a windowed ZSET, but the API surface and downstream consumers stay identical.

### Follow-up questions to ask the interviewer (3-4)

1. **"How do you want me to handle the period-reset moment — say weekly leaderboards rolling over at midnight Sunday?"**
   *Answer if pushed:* Pre-create the new ZSET with the next period's key 5 minutes before reset, atomic config swap of the "active leaderboard pointer" at the boundary, snapshot the old one to Postgres, and TTL the old ZSET 7 days later for late queries. Redlock used here — this is the rare cross-shard op where locking is justified.

2. **"What if the same user submits the same score twice with the same Idempotency-Key from a flaky network?"**
   *Answer:* API Gateway short-circuits on the cached idempotency response — never reaches Kafka. Even if it did, ScoreEvent dedup by `eventId` in the Score Ingest consumer catches it. Defense in depth.

3. **"What's your strategy if Redis loses the entire ZSET?"**
   *Answer:* Snapshot Service has the latest 5-min snapshot in Postgres + the Cassandra event log for delta. Recovery is: load snapshot into a new ZSET, replay Cassandra events newer than `snapshot.takenAt`, then atomic-swap the active key. RTO ~2 minutes for a 1GB leaderboard.

4. **"Where does this architecture fail at 10× scale?"**
   *Answer:* The single ZSET per hot leaderboard becomes the wall — addressed by the sharded leaderboard design from Deep Dive 4. Beyond that, the Kafka topic per leaderboard with userId partitioning hits ceilings around ~1M writes/sec on a single topic; we'd need a hierarchy (regional ingest → global aggregator) similar to how Cloudflare or Twitter aggregate region-local hot keys.

---

## Cheat Sheet (memorize this the night before)

### Numbers
- 10M users, 100K concurrent, **100K writes/sec** peak, **1M reads/sec** peak (top-K hot key)
- 1 GB per leaderboard ZSET, ~50 GB total hot Redis
- p99 budgets: top-K **<100ms**, rank **<200ms**, write ack **<50ms**
- CDN absorbs ~99% of read traffic; Caffeine collapses remaining ~99%

### Killer phrases
1. *"Redis ZSETs are a skip list under the hood — that's exactly the data structure a leaderboard needs."*
2. *"The hot path doesn't have a distributed lock. We don't lock — we route. Kafka partition-by-userId serializes all updates for one user into one thread."*
3. *"Cache invalidation is time-based, not event-based — invalidating on every score event would stampede the hottest key on Earth."*
4. *"Defense in depth: routing eliminates 99% of races, Lua eliminates the rest within Redis, Postgres CAS catches any cross-store drift."*
5. *"Locks are the SQL-era answer; partitioning is the streaming-era answer."*

### Common mistakes to avoid
- **Don't propose a distributed lock as the primary concurrency mechanism.** It signals you don't know the partition-by-key trick. Mention Redlock only as the *exception* for cross-shard period reset.
- **Don't serve top-K from Postgres** — `ORDER BY score LIMIT 100` over millions of rows on every read is a ~50ms operation. ZSET is sub-ms.
- **Don't event-invalidate caches.** Every score update would invalidate the hottest cache key on Earth — stampede. Time-based TTL is right.
- **Don't forget tie-breaking** — encode timestamp into the score so Redis sorts it natively.
- **Don't skip the snapshot/recovery story** — interviewer will ask "what if Redis dies?" — have the Postgres snapshot + Cassandra replay answer ready.
- **Don't claim "exactly-once" anywhere.** At-least-once + idempotent consumer + dedup-by-eventId is the real answer.

### Order of presentation
1. Restate problem + propose scope (15s)
2. FRs (90s) → questions (60s) → NFRs with table (90s)
3. Estimation with numbers on whiteboard (2m)
4. Entities table (90s)
5. APIs with one-line + req/resp each (3m)
6. Services table → DB-per-store table → cache → queue → diagram (10m)
7. **4 deep dives** — Redis ZSET, multi-tier cache, **concurrency (longest)**, hot-leaderboard sharding (12-15m)
8. Wrap-up — summary + trade-offs + extensibility + 3-4 follow-up questions you ask back (3m)

### What to say at each phase (verbal cues)

| Phase | Opening sentence to use |
|---|---|
| Requirements | *"Let me restate the problem and propose scope so we're aligned…"* |
| Estimation | *"Let me size this so the design choices anchor to numbers, not vibes…"* |
| Entities | *"Quickly, the nouns — these become tables and ZSETs later…"* |
| APIs | *"Six APIs. The interesting one is the submit endpoint — note it returns 202, not 200, and here's why…"* |
| HLD | *"I'll build this in three layers — ingest, rank, serve — then draw the full picture…"* |
| Deep Dive Caching | *"The single biggest performance lever is the CDN. Let me walk through the three tiers…"* |
| Deep Dive Concurrency | *"This is the most interesting part. There are three races, and I'll layer fixes cheapest-to-strongest…"* |
| Wrap-Up | *"To close: the design is a 3-stage pipeline — the headline trick is partition-by-userId instead of distributed locks…"* |

---

## Review

Wrote `Local/HLD/topK/topK_simplified.md` as a rehearsal-ready, end-to-end interview script for the Uber SDE-2 Live Leaderboard HLD prompt, following the simplified 7-phase framework in `answering_simplified.txt` and matching the voice/structure of the existing `notification_simplified.md`.

**Key choices:**

- **Opening calls out the difficulty up front** — *"score updates are concurrent and have to be lossless"* — signals to the bar-raiser the candidate sees the hard part before being asked. Mirrors the `propose-and-confirm` opening pattern from the prior HLD scripts.
- **Phase 1 NFR table foregrounds "no lost updates" as a domain-specific NFR** — this primes the concurrency deep dive and tells the interviewer this candidate isn't going to hand-wave thread-safety.
- **Phase 2 explicitly calls out the read/write asymmetry (10:1 globally, 1000:1 on hot top-K)** — this is the number that justifies multi-tier caching as the right answer.
- **Phase 5 DB-per-store table** with explicit "why this / why not the alternatives" — Redis ZSET (skip list under the hood), Postgres for CAS source-of-truth, Cassandra for audit log.
- **Architecture diagram** is bottom-up with a verbal walkthrough script (`"Client calls POST /scores…"`) so the candidate can narrate the golden path.
- **Four deep dives chosen for senior signal**, with weights matching the interviewer's explicit asks:
  1. **Redis ZSET primitive** — answers Ask 1 (simple use case), with the encoded-score-tie-breaker SDE-2 detail.
  2. **Multi-tier caching** — answers Ask 2, with the *time-based-not-event-based* invalidation insight (most candidates miss this).
  3. **Concurrency / thread-safety** — the headline. Includes a 7-row alternatives table comparing Redlock, DB row lock, CRDTs, event sourcing, leader election, pure CAS, and Lua-only. Ends with the layered-defense recommendation (Kafka partition → Lua → Postgres CAS → Redlock-only-for-period-reset).
  4. **Hot-leaderboard sharding** — what to do when one ZSET isn't enough; aligns the shard hash with the Kafka partition hash to preserve the lock-free invariant.
- **Killer phrases section** with 5 memorizable sound bites — the *"we don't lock — we route"* and *"locks are SQL-era; partitioning is streaming-era"* phrases are designed to stick.
- **Verbal-cues table** at the end gives the candidate the literal opening sentence for each phase — designed for the pre-interview night-before review.

**Deliberately scoped out:** ML for fraud/cheat detection on score events, anti-griefing rate limits, cross-region active-active leaderboard consistency, GDPR right-to-be-forgotten — flagged in the wrap-up as future improvements rather than day-one architecture, mirroring the framework's guidance to keep deep dives tight.
