# Uber SDE2 — HLD Bar Raiser: Design an In-Memory / Distributed Cache

> **Round 4: High-Level Design (In-Memory Cache).**
> **Topics:** Key-value storage, Eviction (LRU, TTL), Concurrency, Read/write optimization, Write Ahead Log.
> **Functional Requirements:**
> 1. `PUT(key, value)` — add a key-value pair
> 2. `DELETE(key)` — remove a key-value pair
> 3. `GET(key)` — fetch value by key
> 4. `GET_RANDOM()` — return a random (key, value) pair
> 5. Optimized **time and space complexity** for each operation.

This document follows the [AlgoMaster Answering Framework](https://algomaster.io/learn/system-design-interviews/answering-framework) — 7 phases, ~45 minutes total. It is written as I would speak it in the interview, with the diagrams I would draw on the whiteboard at each step.

---

## How I Will Open the Interview (the first 30 seconds)

> "Cool problem. Before I jump into design, let me clarify scope and scale, because an in-memory cache for a single Redis-like node looks very different from a distributed cache that powers Uber's read path. I'll spend ~5 minutes on requirements, ~3 on estimation, ~5 on APIs and data structures, then build up the architecture iteratively. I'll keep `getRandom` in mind throughout because it has a non-obvious data-structure cost."

This sets the contract with the interviewer up front and earns time for the framework.

---

# Phase 1 — Requirements Clarification (5 min)

## 1.1 Functional Requirements (what I'd propose, then confirm)

| # | Operation | Notes |
|---|-----------|-------|
| 1 | `PUT(k, v, ttl?)` | Insert / overwrite. Optional TTL. |
| 2 | `DELETE(k)` | Idempotent — deleting a missing key is a no-op. |
| 3 | `GET(k)` | Returns value or "miss". |
| 4 | `GET_RANDOM()` | Returns a uniformly random (k, v) currently in the cache. |
| 5 | Eviction | LRU when memory pressure hits; TTL for time-based expiry. |

**Out of scope** (I'd ask, then park): pub/sub, range queries, secondary indexes, transactions, server-side scripting (Lua).

## 1.2 Non-Functional Requirements (questions I would ask)

> "What's the deployment shape — a library/sidecar, a single shared cluster, or a multi-tenant service?"
> "What's the read/write ratio? Cache layers are usually 10:1 to 100:1 read-heavy."
> "What latency do callers expect? P99 1 ms for a colocated cache, ~5 ms across AZ?"
> "Is the cache the source of truth, or is there a backing store? That changes durability requirements."
> "Is `getRandom` for sampling/eviction internals, or is it a real client-facing API?" *(big architectural impact)*

**Assumptions I'd write on the board if the interviewer is vague:**

- Multi-tenant distributed cache; backing store exists (cache is **not** source of truth).
- Read:Write ≈ **10:1**.
- Latency: P99 < **5 ms** end-to-end, < **1 ms** in-process per node.
- Availability: **99.99%** (cache miss is acceptable; data corruption is not).
- Consistency: **eventual** is fine; reads can be slightly stale.
- WAL is desired so a node restart doesn't dump warm state and stampede the DB.
- `getRandom` is a real API — used for sampling-based eviction, A/B picking, and chaos testing of fleet data (e.g., random driver pings).

## 1.3 Why Uber cares about this

| Use case | Cached object | Why a cache |
|---|---|---|
| Driver location lookups | `driver_id → (lat, lng, h3_cell)` | 100K+ writes/sec, sub-ms reads |
| Surge pricing | `h3_cell → multiplier` | Hot read path, infrequent writes |
| Rider session | `session_id → user_ctx` | Avoid auth DB on every request |
| Feature flags | `flag → variant` | Read on every request, must be fast |
| Geo dispatch sampling | `region → random_driver` | **`getRandom` use case** — pick a candidate driver from a hot pool |

The `getRandom` requirement is not academic — it maps to "give me any driver in this region right now."

---

# Phase 2 — Back-of-Envelope Estimation (3 min)

I'll keep the math coarse — order-of-magnitude is what matters.

## 2.1 Traffic
```
Daily cache ops:        ~50 B  ops/day  (Uber-scale, all services combined)
Avg QPS:                50e9 / 86_400  ≈ 580K QPS
Peak QPS  (3x):         ~1.7M QPS
Read QPS  (10:1):       ~1.5M reads/s
Write QPS:              ~150K writes/s
```

## 2.2 Storage / Memory
```
Hot working set:        ~500 M entries
Avg entry size:         ~1 KB  (key + value + metadata + pointers)
Raw memory:             ~500 GB
Replication factor:     3
Total RAM in cluster:   ~1.5 TB

Per node:               64 GB usable RAM   →  ~24 nodes
With headroom (50%):    ~36–40 nodes
```

## 2.3 Bandwidth
```
Per request:            ~1 KB in + 1 KB out
Peak in/out:            1.7M * 1 KB ≈ 1.7 GB/s   →  ~14 Gbps
Per node:               14 Gbps / 36 nodes ≈ 0.4 Gbps   (well within 10/25 Gbps NIC)
```

## 2.4 What these numbers mean for design

| Number | Implication |
|---|---|
| 1.7M peak QPS | Single node can't take it → **shard via consistent hashing** |
| 10:1 read ratio | Replication helps reads → **leader + N followers per shard** |
| 500 GB hot set | Doesn't fit one box → distributed, but one shard does fit one box |
| 1 ms P99 | No disk on hot path → in-memory; WAL must be **async / batched fsync** |

> "Given those numbers I'd target ~40 cache nodes with 64 GB each, sharded by consistent hashing with replication factor 3."

---

# Phase 3 — API Design (4 min)

I'll define a thin RPC. HTTP/JSON for clarity; in production I'd push for gRPC or a Redis-style binary protocol.

```
PUT    /v1/cache/{key}            body={ value, ttl_ms? }     → 200 / 409
GET    /v1/cache/{key}                                         → 200 {value} / 404
DELETE /v1/cache/{key}                                         → 204
GET    /v1/cache/random                                        → 200 {key, value} / 404 (empty)
```

**Design decisions I would call out:**

- **Idempotency:** `DELETE` is naturally idempotent. `PUT` with the same `(k, v)` is idempotent; we'll use a monotonic version per key for last-writer-wins.
- **Batching:** Add `MGET` / `MPUT` later — saves round-trips at scale.
- **Backpressure:** Server returns `429` with retry-after when a shard is hot.
- **Auth & rate limiting:** at the proxy/gateway layer, not in the cache hot path.
- **Random consistency:** `GET /random` is a **uniform sample of currently-resident keys on the chosen shard**, not a global uniform sample. We will discuss the distinction in deep dives.

---

# Phase 4 — High-Level Design (8 min)

I'll start from one box and add components only when the numbers force me to.

## Step 1 — Single-node cache (the building block)

```
   ┌────────┐    PUT/GET/DEL/RANDOM    ┌──────────────────┐
   │ Client │ ───────────────────────► │  Cache Node      │
   └────────┘                           │  (in-memory KV)  │
                                        └──────────────────┘
```

This works for a small service. But: 1.7M QPS, 500 GB → won't fit. And one box dying loses the whole cache.

## Step 2 — Shard with consistent hashing

```
   ┌────────┐
   │ Client │ ──► smart-client picks shard via consistent-hash(key)
   └────────┘
        │
        ├────► Shard 0  (Node A)
        ├────► Shard 1  (Node B)
        ├────► Shard 2  (Node C)
        ├────► ...
        └────► Shard N  (Node Z)
```

Why **consistent hashing** and not modulo: adding/removing a node only remaps `1/N` of keys, not all of them. Critical when we autoscale or lose a node.

## Step 3 — Replicate each shard (availability + read scale)

```
                            Shard 0
                       ┌─────────────────┐
                       │  Leader (A1)    │  ← writes
                       └────────┬────────┘
                                │ async repl
                       ┌────────▼────────┐
                       │  Follower (A2)  │  ← reads (optional)
                       └─────────────────┘
                       ┌─────────────────┐
                       │  Follower (A3)  │  ← reads (optional)
                       └─────────────────┘
```

- Writes go to the leader; followers replicate asynchronously (eventual consistency, fine for a cache).
- Reads can be served from any replica (read-after-write goes to leader if needed).

## Step 4 — Add the supporting services

```
 ┌────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐
 │ Client │─►│   Cache Proxy   │─►│  Consistent-Hash Ring           │
 │  SDK   │  │  (LB + routing) │  │  (Shard 0 .. N, each replicated)│
 └────────┘  └─────────────────┘  └────────────┬────────────────────┘
                                                │
                                  ┌─────────────▼─────────────┐
                                  │   Cluster Manager         │
                                  │   (membership, gossip)    │
                                  └───────────────────────────┘
                                                │
                                  ┌─────────────▼─────────────┐
                                  │   WAL on local SSD        │
                                  │   (one log per node)      │
                                  └───────────────────────────┘
```

**Components (and why each exists):**

| Component | Purpose | Pulled from which estimation number |
|---|---|---|
| Smart client SDK / proxy | Routes `key → shard` via consistent hashing; client-side load balancing | 1.7M QPS — can't go through a single LB |
| Cache nodes | In-memory KV store with LRU + TTL + WAL | 500 GB hot set / 64 GB per box → ~40 |
| Cluster manager (gossip / etcd / ZK) | Membership, leader election per shard, ring updates | Need to detect/recover from node failure |
| WAL on local SSD | Append-only log for restart-warmup | Avoid thundering-herd to backing DB on restart |
| Backing store (caller's DB) | Source of truth on cache miss | Cache is not durable storage |

## Step 5 — Data flow walkthrough

**`PUT(k, v)`**
1. Client SDK hashes `k` → picks shard's leader.
2. Leader takes per-stripe write lock.
3. Append `PUT k v ts` to WAL (async fsync, batched).
4. Update in-memory structures (hash map + LRU list + random-array; see Phase 5).
5. Send replication record to followers (fire-and-forget, with seq number).
6. Ack client.

**`GET(k)`**
1. Hash `k` → pick a replica (leader or follower based on policy).
2. Lookup in hash map.
3. If TTL expired → lazy delete, return miss.
4. Move node to head of LRU list (under stripe lock).
5. Return value.

**`GET_RANDOM()`**
1. Pick a random shard (weighted by shard size for true uniformity).
2. On that shard: pick a random index in the keys-array → return the entry at that slot.
3. O(1) — see Phase 5 for why this works.

**`DELETE(k)`**
1. Hash `k` → leader.
2. WAL append `DEL k ts`.
3. Remove from hash map, LRU list, and the random-array (swap-with-last trick).
4. Replicate, ack client.

---

# Phase 5 — Data Structure / "Database" Design (6 min)

This is the **most important phase for this question.** The interviewer specifically asked about complexity per op, and `getRandom` forces a non-obvious combination of structures.

## 5.1 The core insight

| Structure | Gives me |
|---|---|
| `HashMap<Key, Entry*>` | O(1) lookup |
| Doubly linked list | O(1) LRU promote / evict tail |
| Dynamic array `keys[]` + `index_of[k]` | O(1) random pick (random index) and O(1) delete (swap-with-last) |

**Combine all three. Each `Entry` knows its own list-node and array-index** so all three structures can be updated in O(1) without searches.

## 5.2 The Entry struct (single-node, in-process)

```text
Entry {
    key:        K
    value:      V
    expires_at: u64           // for TTL
    list_node:  *DLLNode       // pointer into the LRU list
    arr_index:  u32            // index into keys[] for random pick
    version:    u64            // for replication ordering
}

CacheShard {
    map:       HashMap<K, Entry*>      // O(1) get/put/delete
    lru_head:  *DLLNode                 // most recently used
    lru_tail:  *DLLNode                 // eviction candidate
    keys:      Vec<K>                   // dense array, no holes
    capacity:  usize
    locks:     [Mutex; 32]              // striped, hash(k) % 32
}
```

## 5.3 Visual layout

```
HashMap                LRU (doubly linked list)        keys[] (random pool)
─────────              ─────────────────────────       ──────────────────
"a" ─► E_a ─┐          head ► E_b ⇄ E_c ⇄ E_a ◄ tail   [0]"a"  [1]"b"  [2]"c"
"b" ─► E_b ─┤                  ▲    ▲    ▲              ▲       ▲       ▲
"c" ─► E_c ─┘                  └────┴────┘              │       │       │
                               (each Entry has a        each Entry stores
                               list_node pointer)       its own arr_index
```

## 5.4 Complexity — what I'd write on the board

| Operation | Time | Space (extra) | How |
|---|------|---------------|-----|
| `PUT(k, v)`     | **O(1)** | O(1) per entry | hashmap insert + LRU push-front + keys.append + index update |
| `GET(k)`        | **O(1)** | — | hashmap lookup + LRU move-to-front |
| `DELETE(k)`     | **O(1)** | — | hashmap erase + LRU unlink + **keys[i] ← keys.last(); keys.pop(); index_of[swapped] = i** |
| `GET_RANDOM()`  | **O(1)** | — | `keys[rand() % keys.len()]` |
| LRU evict (under pressure) | **O(1)** amortized | — | take `lru_tail`, then run DELETE on that key |
| TTL expiry (lazy) | **O(1)** per access | — | check on GET, evict in place |
| TTL expiry (active) | **O(N) sweep** but amortized O(1) per key | — | background thread, sample 20 random keys (Redis-style) |

**Total space:** `O(N)` — one hashmap entry, one DLL node, one slot in `keys[]` per item. ~1.5–2x the raw key+value bytes due to pointers and metadata.

## 5.5 The swap-with-last delete trick (this is the part I would draw)

```
Before DELETE("b") :   keys = [ "a" , "b" , "c" , "d" ]
                                 0     1     2     3
                                       ▲
                                 index_of["b"] = 1

Step 1: copy last  →   keys = [ "a" , "d" , "c" , "d" ]   (overwrite slot 1)
Step 2: pop last   →   keys = [ "a" , "d" , "c" ]
Step 3: fix index  →   index_of["d"] = 1
Step 4: erase "b"  →   index_of.erase("b")
```

All four steps are O(1). This is *the* idiom that makes `getRandom` + `delete` both O(1).

## 5.6 SQL vs NoSQL? It's neither — it's in-memory

For this question I would explicitly call this out: "We're not picking PostgreSQL or Cassandra — the cache *is* the data store. The persistence layer is a **WAL on local SSD**, not a database. The backing store of truth lives in whatever service called us."

If asked about the WAL format:

```
[seq=12345] [op=PUT] [key="ride:42"] [val=...] [ts=...] [crc32]
[seq=12346] [op=DEL] [key="user:7"]              [ts=...] [crc32]
[seq=12347] [op=PUT] [key="surge:h3-89"] [val=...]
```

- Append-only, segmented (rotate every 256 MB), compacted in background.
- `fsync` policies: per-write (safest, slow), per-N-ms (default, ~1ms group commit), never (fastest, ok if cache isn't truth).
- **Replay on restart** to warm hot keys before opening for traffic.

---

# Phase 6 — Deep Dives (12–15 min)

The interviewer will pick 2–3 of these. I'd lead with the ones the prompt called out (concurrency, eviction, WAL) and have the others ready.

## 6.1 Concurrency — striped locks, not one global lock

**Problem:** A single `Mutex` around the shard at 50K QPS/node will serialize everyone. We can't afford that.

**Approach 1 — Single global lock.**
- Pros: simple, correct.
- Cons: throughput cliff under contention.

**Approach 2 — Striped locks (recommended).**
- 32 or 64 stripes; `stripe = hash(key) % 32`.
- Each PUT/GET/DELETE only locks its stripe.
- Random pick takes a *short* read lock on the keys-array.
- Pros: ~32x parallelism on uncorrelated keys, simple to reason about.
- Cons: hot key still serializes within its stripe.

**Approach 3 — Lock-free / sharded per-thread.**
- `crossbeam`-style lock-free hash map; per-CPU shards (Facebook's `mcrouter`, `memcached` slabs).
- Pros: max throughput.
- Cons: hard to get right, and the LRU list across threads is the real bottleneck (the DLL is inherently shared mutable state).

**My recommendation:** Striped locks for the map and DLL, **plus** a CLOCK approximation of LRU instead of a strict DLL — CLOCK is friendlier to concurrency because it doesn't require reordering on every read. Used by Memcached and Postgres' buffer cache.

```
 CLOCK eviction (approximate LRU)
 ┌──────────────────────────────────┐
 │ ring of entries with a "ref" bit │
 │  on GET: set ref=1 (no DLL move) │
 │  on evict: hand sweeps; if ref=1 │
 │     clear it; if ref=0 evict     │
 └──────────────────────────────────┘
```

## 6.2 Eviction — LRU + TTL working together

**Two pressures, two mechanisms:**

| Pressure | Mechanism | Cost |
|---|---|---|
| Memory full | LRU (CLOCK approximation) | O(1) per evict |
| Time-based expiry | TTL — lazy + active sampling | Amortized O(1) |

**TTL active expiry, Redis-style:**
- Background thread, every 100 ms.
- Sample 20 random keys with TTL.
- Delete the expired ones.
- If ≥ 25% were expired, repeat immediately.
- Bound CPU at e.g. 25% of one core.

This avoids the naïve "min-heap of expirations" approach, which is O(log N) per put and creates memory churn. Random sampling gives us probabilistic O(1) amortized.

## 6.3 `getRandom` in a *distributed* setting (this is the curveball)

In one node, `getRandom` is trivially O(1) via the keys-array. Across the cluster:

**Naïve:** pick a random shard, then random key. **Bug:** non-uniform if shards have different sizes.

**Correct: weighted pick.**
1. Each shard reports its `size` to the proxy via gossip (eventually consistent, fine).
2. Proxy picks shard `i` with probability `size_i / sum(sizes)`.
3. Forwards `GET /random` to that shard.

```
Shards:   S0 (1M)   S1 (3M)   S2 (2M)   S3 (4M)
Weights:  10%       30%       20%       40%
Pick a uniform u in [0,1) → cumulative-sum lookup → shard
```

If perfect uniformity is not required (e.g., dispatch sampling), uniform-shard pick is fine and simpler.

## 6.4 Write-Ahead Log — what we use it for

**Why WAL in a cache?** Cache restarts are common (deploys, OOM, autoscaling). Without warmup, every cold node sends ~1 ms of cache misses to the DB at 50K QPS = the DB falls over. The WAL lets a node restart with 90%+ of its hot set already loaded.

**Design:**
- Append-only segments on local NVMe (~500 MB/s sequential, plenty).
- Group commit: batch writes into 1 ms windows, single fsync per window.
- Segment rotation at 256 MB; old segments compacted (fold PUT+DELETE for the same key).
- On startup: replay newest segment first (most recent state wins via `seq`).

**Trade-off discussion to volunteer:**

| Mode | Durability | Write latency | When to use |
|---|---|---|---|
| `fsync` per write | Strongest | ~1–5 ms | Cache is source of truth (we said it isn't) |
| Group commit (1 ms) | Lose ≤1 ms on crash | ~1 ms p99 | **Default — best balance** |
| Async-only / no fsync | Lose seconds | sub-ms | Pure ephemeral cache |

## 6.5 Hot key problem (the celebrity tweet of caches)

When one key (say, surge multiplier in SF on NYE) gets 200K QPS, the single shard owning it melts.

**Mitigations, layered:**
1. **Client-side cache** with 100 ms TTL — collapses 200K req/s into a few req/s per client.
2. **Request coalescing** at the proxy — in-flight `GET` for the same key serves all waiters from one upstream call.
3. **Hot-key replication** — proxy detects (via top-K sketch like Count-Min) and clones the key onto N replicas; reads round-robin.
4. **Write absorption** — if the hot key is also write-hot, batch writes in the proxy and flush every ~10 ms.

## 6.6 Consistent hashing — why a virtual-node ring

```
 hash space (0 ─► 2^32) arranged as a ring
 ●  ●     ●        ●  ●           ●     ●     ●
 │  │     │        │  │           │     │     │
 NodeA NodeB    NodeA NodeC      NodeB NodeA NodeC
 (each physical node owns 100–200 virtual tokens)

 key "ride:42" hashes to position p → walk clockwise → nearest token wins
```

- 100–200 vnodes per physical node smooths load.
- Adding `Node D`: only the keys between D's tokens and the next token move.
- Removing a node: its keys move to the next clockwise owner.

## 6.7 Replication & failure handling

- **Per-shard:** 1 leader + 2 followers.
- **Async replication:** leader streams WAL records to followers with seq numbers.
- **Failover:** cluster manager (Raft-based, e.g., etcd) detects leader loss via heartbeat → promotes the follower with the highest `seq`.
- **Bounded staleness:** followers usually < 5 ms behind; if they fall further, take them out of read rotation.

Trade-off: a cache that loses ~5 ms of writes on failover is fine — the DB is the source of truth. If we needed strong consistency we'd use Raft inside the shard, but that's overkill here.

## 6.8 Comparison table I'd draw if asked "what about Memcached vs Redis"

| Feature | Memcached | Redis | Our design |
|---|---|---|---|
| Data structures | KV only | KV + sets/lists/hashes/streams | KV + random-pool |
| Eviction | LRU/CLOCK | LRU/LFU | CLOCK + TTL sample |
| Persistence | None | RDB + AOF | WAL (AOF-style) |
| Replication | None (memcached-router does it) | Async leader-follower | Async leader-follower |
| `getRandom` | No | `RANDOMKEY` (O(1) via slot scan) | **O(1) via keys-array** (faster) |
| Threading | Multithreaded | Single-threaded per shard | Multithreaded + striped locks |

---

# Phase 7 — Wrap-Up (3 min)

## 7.1 Recap (30 seconds, said out loud)

> "We have a sharded, replicated in-memory KV cache. Each node uses a hash map for O(1) lookup, a doubly linked list / CLOCK for LRU eviction, and a dense keys-array with index map to make `getRandom` and `delete` both O(1). Striped locks give us per-CPU concurrency. A WAL on local SSD lets a node restart with a warm cache. The cluster shards via consistent hashing and replicates each shard 3-ways with async replication. `getRandom` across the cluster does a size-weighted shard pick to stay uniform."

## 7.2 Bottlenecks I would acknowledge

| Bottleneck | Mitigation |
|---|---|
| Hot keys | Client cache + coalescing + hot-key replication |
| GC pauses on a 64 GB heap | Off-heap allocation (slab) or write in Rust/C++ |
| WAL fsync latency | Group commit, NVMe |
| Network (1.7 GB/s) | gRPC + protobuf, batching, compression |
| Cluster-wide `getRandom` skew | Weighted shard pick using gossiped sizes |
| Cold start (cache stampede) | WAL replay before opening port |

## 7.3 Future improvements

- **Tiered storage:** memory → local NVMe → S3, transparently.
- **Cross-region replication:** active-active with CRDT-style conflict resolution.
- **Adaptive eviction:** TinyLFU instead of LRU (better hit rate).
- **Auto-shard splitting** when a shard exceeds ~75% memory.
- **Pluggable consistency:** opt-in linearizable reads via Raft for users who need it.
- **Encryption at rest** for the WAL (PII like driver locations).

## 7.4 Curveballs I'd be ready for

| Question | One-line answer |
|---|---|
| "How do you handle 10x traffic?" | Add shards (consistent hashing minimizes remap), then add followers for read scale. |
| "What if a region dies?" | Active-active across regions with last-writer-wins; cache is recoverable from DB anyway. |
| "Migrate from Redis to this?" | Dual-write phase, then dual-read with shadow comparison, then cut over per-tenant. |
| "Make `getRandom` globally uniform with no gossip." | Reservoir sampling across shards via scatter-gather — slower but exact. |
| "Why not just use Redis Cluster?" | Honest answer — for most teams you should. We're designing this because the prompt asked us to, and Uber has scale + hot-key patterns where a custom cache (à la `cherami`/`ringpop`) earns its keep. |

---

# Time budget I would actually try to hit

| Phase | Target | Why |
|---|---|---|
| Requirements | 5 min | Pin down scope and `getRandom` semantics early |
| Estimation | 3 min | Justifies sharding + replication |
| API | 4 min | Short — RPC is straightforward here |
| HLD | 8 min | Build up incrementally; show sharding logic |
| Data structures | 6 min | **The differentiator for this problem** |
| Deep dives | 14 min | Concurrency + eviction + WAL + hot-keys |
| Wrap-up | 3 min | Recap + bottlenecks + future work |
| **Total** | **43 min** | Leaves slack for the interviewer's questions |

---

# Cheat-sheet I would whiteboard at the end

```
              ┌─────────────────────────────────────────────┐
              │                  CACHE NODE                  │
              │                                              │
              │  HashMap<K, Entry*>   ──── O(1) get/put/del  │
              │  DLL  (head ⇄ ⇄ tail) ──── O(1) LRU          │
              │  keys[] + index_of    ──── O(1) random+del   │
              │  WAL (append-only)    ──── crash recovery    │
              │  Striped locks (×32)  ──── concurrency       │
              │                                              │
              └────────────────────┬────────────────────────┘
                                   │
                ┌──────────────────┼──────────────────┐
                │                  │                  │
            Shard 0            Shard 1   ...      Shard N
         leader+2 followers   leader+2 followers
                ▲                  ▲                  ▲
                └──────────────────┼──────────────────┘
                                   │
                            Consistent-Hash Ring
                                   │
                            Smart Client / Proxy
                                   │
                                Clients
```

---

# Operation complexity — the table I'd leave on the board

| Op | Time (avg) | Time (worst) | Space | Notes |
|---|---|---|---|---|
| `PUT(k, v)` | O(1) | O(1) amortized* | O(1) extra | *worst case = hashmap rehash |
| `GET(k)` | O(1) | O(1) | — | LRU promote piggybacks on the read |
| `DELETE(k)` | O(1) | O(1) | — | Swap-with-last in `keys[]` |
| `GET_RANDOM()` | O(1) | O(1) | — | `keys[rand() % size]` |
| LRU eviction | O(1) | O(1) | — | CLOCK or DLL tail |
| TTL expiry | O(1) amortized | O(N) sweep | — | Lazy + sampled active expiry |

This is the answer to "Optimized TC and SC expected for each operation." All four required ops are **O(1) time and O(N) total space**, with the keys-array trick being the key insight that makes `getRandom` and `delete` both O(1) simultaneously.
