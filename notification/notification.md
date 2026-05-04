# Notification System at Scale — Uber SDE2 (Bar Raiser) HLD

> Question: Design a system that can deliver **100M+ notifications/day** across push (APNs/FCM), SMS (Twilio), email (SES), and in-app channels, with **retries, deduplication, and per-user preferences**.

This document follows the 7-phase framework from [HLD/answering_framework.txt](../answering_framework.txt). Each section shows **what to say**, **what to draw**, and **why**, the way you would actually deliver it in a 60-minute Uber bar-raiser.

---

## Interview Posture (read this first)

For an Uber SDE2 bar-raiser, the bar is not "did you draw the right boxes?" It is:

1. **Did you anchor the design in the business?** Notifications at Uber are not generic — *"Your driver is arriving"* is on the critical path of a ride. Promotions are not. Treat them differently.
2. **Did your numbers drive the design?** 100M/day sounds large but average QPS is small. The interesting number is **burst** (surge alerts, promo blasts) and **fan-out** (one event → millions of recipients).
3. **Did you handle reliability honestly?** Retries, idempotency, DLQ, provider failure — these are the deep dives. Hand-waving here fails the round.
4. **Did you communicate trade-offs?** Every choice (Kafka vs SQS, push vs in-app, sync vs async preference resolution) has a cost. Name it.

**Time budget (60 min):** Requirements 6 / Estimation 4 / API 5 / HLD 9 / DB 6 / Deep dives 25 / Wrap 5.

---

## Phase 1 — Requirements Clarification (~5 min)

> "Before I draw anything, let me lock down scope and SLOs. Notification systems at Uber's scale split into very different products depending on what we prioritize."

### Functional requirements (propose, then negotiate)

| In scope | Out of scope (call out, ask) |
|---|---|
| Send notifications via **push (APNs/FCM), SMS (Twilio), Email (SES), In-app** | Rich analytics dashboard, A/B testing framework |
| **Per-user preferences** (channel opt-outs, quiet hours, categories) | ML send-time optimization |
| **Retries** on transient failure, **DLQ** on permanent failure | Marketing campaign builder UI |
| **Deduplication** by client-supplied idempotency key | Two-way SMS / inbound email |
| **Templating** with variable substitution + localization | In-app inbox UI (we expose API, app team builds UI) |
| **Scheduled** sends + immediate sends | Voice calls / WhatsApp (extensible later) |
| **Priority classes**: transactional vs promotional | |

> "I want to confirm — are we owning the *delivery platform* (a service other Uber teams call), or also the *campaign tool* marketers use? I'll assume the platform; the campaign tool is a client of us."

### Non-functional requirements

Ask, don't assume. Typical answers for an Uber-scale problem:

| Attribute | Target | Why it matters |
|---|---|---|
| Throughput | 100M/day sustained, **bursts to 50–100K QPS** during surge events / promo blasts | Drives queueing + worker fleet sizing |
| Latency | **Transactional p99 < 2s** end-to-end (event → provider accepted); promotional: minutes OK | Splits into hot/cold paths |
| Availability | **99.95%** for transactional; 99.9% for promotional | Can degrade promo without paging |
| Durability | **Zero loss for transactional** notifications | At-least-once + persistent queue |
| Consistency | Eventual is fine for preferences (with read-your-writes for the updating user) | Lets us cache aggressively |
| Dedup window | **24h** by default, configurable per template | Long enough for retries + replays |

### Document it on the board

```
SCOPE
- Channels: push, SMS, email, in-app
- Features: prefs, retries, dedup, templates, scheduled, priority
- Out: analytics UI, A/B, voice
SLO
- 100M/day, peak ~100K QPS (surge)
- Transactional p99 < 2s, 99.95%
- Zero loss transactional, dedup 24h
```

Leave this on the board the whole interview. You will point at it when justifying choices.

---

## Phase 2 — Back-of-Envelope Estimation (~3 min)

> "Let me convert these into numbers that drive the architecture."

**Average QPS**
```
100M / 86,400s ≈ 1,160 QPS average
Peak (3×) ≈ 3,500 QPS sustained
Burst (surge / promo blast) ≈ 50,000–100,000 QPS for short windows
```

**Channel mix (state the assumption)**
```
Push     60%  →  60M/day
Email    25%  →  25M/day
SMS      10%  →  10M/day  (expensive — provider $$)
In-app    5%  →   5M/day
```

**Storage** (notification records, 30-day retention)
```
record ≈ 1 KB (id, user_id, channel, status, attempts, payload ref, ts)
100M × 1KB × 30 days = 3 TB
With 3× replication + indexes ≈ 10 TB → must shard
```

**Dedup keys**
```
100M keys/day × 100B × 1 day TTL ≈ 10 GB → fits a small Redis cluster
```

**Provider budget reality check (Uber-specific)**
```
SMS at $0.005 each × 10M/day = $50K/day = $18M/year
→ design must enforce per-user/per-template SMS caps
```

**What these numbers tell us**
- Average QPS is small; **burst** is what we design for → buffered queueing + horizontal channel workers.
- 10 TB data → **shard by user_id** in a wide-column store.
- SMS cost → **preference + dedup + rate-limit at the API**, not at the worker.

---

## Phase 3 — API Design (~4 min)

> "I'll define a small public API and one internal contract. Producers (Rides, Eats, Payments) call us; we hide channel complexity from them."

### 3.1 Send a notification (the main one)

```
POST /v1/notifications
Idempotency-Key: ride_accepted:trip_98f3a1     ← also used for dedup

{
  "user_id":      "u_8123",
  "template_id":  "ride.driver_arriving",
  "category":     "transactional",             ← drives priority + bypasses promo opt-out
  "channels":     "auto",                      ← or ["push","sms"]
  "data":         { "driver": "John", "eta_min": 2, "plate": "7XYZ123" },
  "send_at":      null,                        ← ISO ts for scheduled
  "ttl_seconds":  300                          ← drop if not delivered in 5 min
}
→ 202 Accepted
  { "notification_id": "n_01HX...", "status": "accepted" }
```

**Talking points to mention without implementing:**
- `Idempotency-Key` doubles as the dedup key (24h window).
- `ttl_seconds` matters for transactional: a 30-min-late "Driver arriving" is worse than no message.
- `channels: "auto"` lets the platform pick based on user preferences and the template's allowed channels.

### 3.2 Bulk send (promotional)

```
POST /v1/notifications:bulk
{
  "template_id": "promo.weekend_eats_15off",
  "category":    "promotional",
  "audience":    { "segment_id": "seg_us_active_eaters_2026Q2" },
  "send_at":     "2026-05-04T17:00:00Z"
}
→ 202 { "campaign_id": "c_...", "estimated_recipients": 8400000 }
```

Why a separate endpoint: the synchronous `POST /v1/notifications` is per-recipient and on the hot path. Bulk is a fan-out job — different rate limits, different SLOs, different audit trail.

### 3.3 Preferences

```
GET /v1/users/{user_id}/preferences
PUT /v1/users/{user_id}/preferences
{
  "channels":     { "push": true, "sms": false, "email": true },
  "categories":   { "promotional": false, "transactional": true, "security": true },
  "quiet_hours":  { "start": "22:00", "end": "08:00", "tz": "America/Los_Angeles" }
}
```

### 3.4 Status (for producers / support tools)

```
GET /v1/notifications/{id}
→ { id, status: "delivered" | "failed" | "suppressed_by_pref" | "pending",
    attempts, last_error, channel_results: [...] }
```

**Mention but don't dwell on:**
- Cursor pagination on history endpoints.
- mTLS + signed JWTs between Uber services.
- The idempotency key is the contract — producers must generate stable keys (e.g., `event_type:event_id`).

---

## Phase 4 — High-Level Design (~8 min)

> "I'll start with the simplest thing that could possibly work, then evolve it as my numbers force changes."

### Iteration 1 — naïve

```
[Producer service] → [Notification API] → [DB] → [Provider SDK] → APNs/FCM/Twilio/SES
```

Problem: synchronous provider calls. Twilio hiccup → producer timeout → ride flow blocks. Unacceptable.

### Iteration 2 — make it async

```
[Producer] → [Notification API] → [Kafka] → [Channel Worker] → [Provider]
                                              ↓
                                         [Status DB]
```

Better. API returns 202 immediately. But:
- No dedup.
- No preferences.
- One topic for all channels = one slow channel blocks others.

### Iteration 3 — full architecture

```
                        ┌────────────────────────────────────────┐
                        │              CLIENTS / PRODUCERS       │
                        │  Rides svc │ Eats svc │ Payments │ ... │
                        └─────────────────┬──────────────────────┘
                                          │ HTTPS (mTLS)
                                          ▼
                            ┌─────────────────────────┐
                            │   API Gateway / LB      │
                            └────────────┬────────────┘
                                         ▼
                        ┌────────────────────────────────────┐
                        │   Notification API (stateless)     │
                        │  • auth                            │
                        │  • dedup check  ──► [Redis dedup]  │
                        │  • pref lookup  ──► [Pref cache]   │
                        │  • template id validate            │
                        │  • emit ingest event               │
                        └────────────┬───────────────────────┘
                                     │
                                     ▼
                          ┌─────────────────────┐
                          │   Kafka: ingest     │  partitioned by user_id
                          │  topics: hot / bulk │  (ordering per user)
                          └──────────┬──────────┘
                                     ▼
                          ┌─────────────────────┐
                          │  Router / Planner   │   one-of:
                          │  • resolve channels │   - splits 1 logical
                          │  • render template  │     notification into N
                          │  • apply quiet hrs  │     channel jobs
                          │  • rate-limit user  │
                          └──────────┬──────────┘
                                     ▼
              ┌──────────────┬───────┴───────┬──────────────┐
              ▼              ▼               ▼              ▼
       Kafka push.q   Kafka sms.q     Kafka email.q   Kafka inapp.q
              │              │               │              │
              ▼              ▼               ▼              ▼
         Push Workers   SMS Workers    Email Workers   In-app Workers
              │              │               │              │
        APNs/FCM         Twilio            SES         WebSocket / store
              │              │               │              │
              └──────────────┴───────┬───────┴──────────────┘
                                     ▼
                          ┌─────────────────────┐
                          │  Status / Audit DB  │  (Cassandra)
                          │  Retry scheduler    │  (delayed queue)
                          │  DLQ topic          │
                          └─────────────────────┘

Side services:
  • Preference service  (Postgres + Redis cache)
  • Template service    (Postgres + S3 for bodies, Redis cache)
  • Device registry     (DynamoDB: user_id → tokens)
  • Audience service    (for bulk: segment_id → user_id stream)
```

### Walk the interviewer through one transactional event

> *"Driver accepted ride trip_98f3a1." Let me trace it.*

1. Rides service POSTs `/v1/notifications` with `Idempotency-Key: ride_accepted:trip_98f3a1`.
2. Notification API authenticates, then `SET key NX EX 86400` against Redis dedup. Hit → return existing id, 202. Miss → continue.
3. API writes a row to `notifications` (status=`accepted`) and produces to `ingest.hot` partitioned by `user_id`. Returns 202 (~10ms).
4. Router consumes, looks up preferences (cache → DB), expands `channels:auto` → `[push, sms]` for transactional, renders template (`"John is 2 min away"`), and emits one job per channel to `push.q` and `sms.q`.
5. Push worker fetches device tokens, calls APNs/FCM, updates status row to `delivered` or `failed`.
6. SMS worker calls Twilio. Twilio 5xx → push to retry topic with backoff metadata.

### Walk the interviewer through one bulk campaign

> *"Send weekend Eats promo to 8M US active eaters at 5pm Saturday."*

1. Bulk API stores the campaign, schedules a job at `send_at`.
2. At fire time, **Audience service** streams user_ids in shards of, say, 10K, into `ingest.bulk`.
3. From there it flows through the same Router/Workers — but `ingest.bulk` has fewer partitions / lower-priority worker pools so it cannot starve the hot path.
4. Promo opt-out filtering happens in Router; quiet hours defer to next allowed window or drop (template-controlled).

### Components to call out (and why)

| Component | Why it earns its place |
|---|---|
| **Kafka** (not SQS) | Per-user ordering via partition key; replay for incidents; high burst absorption |
| **Two ingest topics** (`hot`, `bulk`) | Promo blast cannot block "driver arriving" |
| **Per-channel topics** | Twilio outage doesn't back up push |
| **Router/Planner** as a stage | Keeps API thin (low p99) and isolates rendering CPU |
| **Redis dedup** | Sub-ms `SET NX EX` is the cheapest correct dedup |
| **Cassandra for status** | Write-heavy time-series, partition by user_id |
| **Retry scheduler / delayed queue** | Cleaner than sleeping in workers |
| **Device registry separate** | Token rotation + invalid-token feedback loop |

---

## Phase 5 — Database Design (~5 min)

> "Three core stores, each picked for its access pattern."

### 5.1 `notifications` — Cassandra

Why: write-dominated, time-series-by-user, must scale horizontally.

```
TABLE notifications_by_user (
  user_id         text,         -- partition key
  created_at      timeuuid,     -- clustering, DESC
  notification_id text,
  template_id     text,
  category        text,         -- transactional | promotional | security
  channel         text,         -- push | sms | email | inapp
  status          text,         -- accepted|queued|sent|delivered|failed|suppressed
  attempts        int,
  last_error      text,
  payload_ref     text,         -- pointer to S3 if large
  PRIMARY KEY ((user_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

**Access patterns this serves:**
- "Show user's notification history" → single partition read, naturally sorted.
- "What's the status of notification X?" → secondary lookup table by id.

```
TABLE notifications_by_id (
  notification_id text PRIMARY KEY,
  user_id         text,
  created_at      timeuuid
) -- pointer table, then read main
```

**Hot user problem** (a driver in a busy city gets 1000s of events/day): partition stays a few MB — fine. We're not storing trip details here.

### 5.2 `user_preferences` — Postgres (primary) + Redis (cache)

Why Postgres: small (~hundreds of MB total), needs read-your-writes, schema is stable, and the team operating it already runs Postgres.

```
TABLE user_preferences (
  user_id        bigint PRIMARY KEY,
  prefs          jsonb NOT NULL,     -- channels, categories, quiet_hours, locale
  version        int NOT NULL,        -- bumped on update
  updated_at     timestamptz
);
```

Read path: **Redis cache, TTL 5 min, key `pref:{user_id}`, value includes `version`.**
Write path: write Postgres → publish `pref.changed` event → cache invalidation worker `DEL`s the key. The 5-min TTL is the safety net.

### 5.3 `device_tokens` — DynamoDB

Why: massive, simple key-value, hot writes from app installs.

```
PK: user_id        SK: device_id
attrs: platform (ios|android|web), token, app_version, locale, last_seen, valid (bool)
```

Push worker queries `(user_id)` partition, filters `valid = true`. APNs/FCM "unregistered" feedback flips `valid = false` (and records timestamp; we hard-delete after 30 days).

### 5.4 `templates` — Postgres + S3

Postgres holds metadata (id, allowed_channels, allowed_categories, locale_set). S3 holds rendered body templates (Mustache/Handlebars). Both are aggressively cached in the Router (templates rarely change; on change, a Kafka invalidation event clears caches).

### 5.5 Dedup — Redis

```
SET dedup:{idempotency_key} {notification_id} NX EX 86400
```

Atomic. Single-shot. Cluster-mode Redis with hash-tag-free keys spreads load.

### Sharding/scale notes (mention, don't draw)

- Cassandra: shard by `user_id` is built-in; rebalance via vnodes.
- Kafka: 64+ partitions per topic, scaled to consumer count.
- Postgres prefs: one writer is enough at this scale; read replicas for the cache-fill path.
- DynamoDB: on-demand, sharded by `user_id` automatically.

---

## Phase 6 — Deep Dives (~15–20 min)

> "Pick 3-4. Tell the interviewer up front you have several you can go into, and let them steer."

The five most likely deep dives for this prompt at Uber:

1. **Reliable delivery: at-least-once, retries, DLQ, exactly-once illusion**
2. **Deduplication & idempotency under retries and replays**
3. **Per-user preferences, quiet hours, and category overrides**
4. **Bulk fan-out + provider rate limits + per-user caps**
5. **Provider failure handling & multi-region**

---

### Deep Dive 1 — Reliable Delivery

**Problem.** A "Driver arriving" notification cannot be lost. But every layer (Kafka producer, worker, provider, network) can fail.

**Approach.**

1. **Persist before ack.** API writes a row in Cassandra (`status=accepted`) *and* produces to Kafka *before* returning 202. If Kafka publish fails after Cassandra write, a sweeper picks up `accepted` rows older than N seconds and re-emits.
2. **At-least-once Kafka consumption.** Workers commit offsets *after* the provider confirms. Crash → re-deliver → dedup catches the duplicate.
3. **Exponential backoff retries.** 5 attempts: 1s, 5s, 30s, 5m, 30m. Implemented via a **delayed retry topic** (`retry.t60s`, `retry.t300s`, …) — a scheduler service moves matured messages back into the channel queue. This avoids workers sleeping and holding partitions.
4. **TTL discipline.** Each notification carries `expires_at`. Workers check before sending; expired → status `expired`, never retried.
5. **DLQ** after max attempts. DLQ is consumed by a triage service that bumps an alert if rate exceeds 0.1%.
6. **Provider-specific errors are not all retryable.**

| Error class | Action |
|---|---|
| 429 / 503 / network | Retry with backoff |
| APNs `BadDeviceToken` | Mark token invalid in registry; do not retry |
| Twilio "blacklisted recipient" | Suppress, log, do not retry |
| SES bounce | Update bounce table, suppress future to that address |

**Trade-off.** True exactly-once is impossible across an external provider. We get **effectively-once** = (at-least-once delivery) + (idempotent provider where possible, dedup where not). Most providers accept idempotency keys (Twilio, SES) — we pass our `notification_id` so a retried HTTP call to Twilio doesn't send a second SMS.

---

### Deep Dive 2 — Deduplication

**Problem.** Two sources of duplicates:
- **Producer retries** (Rides service times out, retries with same key).
- **Internal retries** (consumer crashes between provider call and offset commit).

**Solution.**

```
On ingest:
  ok = SET dedup:{idempotency_key} {notification_id} NX EX 86400
  if !ok:
      existing_id = GET dedup:{idempotency_key}
      return 202 with existing_id, status="duplicate"

On provider call:
  use {notification_id} as the provider-side idempotency key
  → Twilio / SES return same SID for retried call, no second send
```

**Edge cases to volunteer:**
- *"What if Redis is down?"* Fail-open for transactional (better to send a duplicate than miss a critical message), fail-closed for promotional. Make this configurable per category.
- *"What if the same key arrives in two regions simultaneously?"* Cross-region active-active is rare for the same user; we accept the tiny risk and pin a user to a home region via consistent hashing on `user_id`. Cross-region dedup would need a global store (DynamoDB Global Tables, etc.) and isn't worth the latency cost here.
- *"What if the producer didn't send a key?"* API generates one as `hash(user_id, template_id, body, minute_bucket)` so it's still safe within a 1-minute window. Document that producers should send their own.

---

### Deep Dive 3 — Preferences, Quiet Hours, Categories

**Problem.** A user opts out of "marketing." Two seconds later we send a promo. That's a regulatory issue (CAN-SPAM, GDPR, TCPA) — not just bad UX.

**Resolution flow** (in the Router stage):

```
1. Load preferences (Redis cache, fallback Postgres)
2. category == "transactional" or "security"?
     → bypass promo opt-out; still respect channel disable
3. category == "promotional"?
     → if categories.promotional == false  → status=suppressed_by_pref, stop
4. For each requested channel:
     → if channels[ch] == false             → drop that channel
5. Quiet hours apply (template-controlled flag respect_quiet_hours)?
     → if now in quiet window in user's tz:
         transactional → send anyway
         promotional   → reschedule to quiet_end (push to delayed queue)
6. Per-user rate limits (token bucket in Redis: 10 promo/day)
     → if exceeded, drop with status=rate_limited
```

**Read-your-writes for the user updating their own prefs.** The cache invalidation event is async, but the user's own client should see their change immediately. Solution: when the user updates prefs, the API writes Postgres + writes a short-TTL "fresh" key in Redis, then publishes the invalidation. Reads check fresh-key first.

**Localization piggybacks on prefs.** `prefs.locale` chooses template variant.

---

### Deep Dive 4 — Bulk Fan-out + Provider Rate Limits

**Problem.** Marketing schedules a promo to 8M users at 5pm. Naïve fan-out → 8M messages hit `ingest` in seconds → workers saturate Twilio's account-level rate limit → transactional SMS get queued behind promo.

**Solution layers.**

1. **Audience streaming, not materialization.** Audience service streams user_ids out of a segment store (built on a periodic Spark job materializing segments to S3 or a columnar store). The streamer paces itself to a target ingest rate.
2. **Hot vs bulk lanes are physically separate.** `ingest.hot` and `ingest.bulk` have separate worker pools and provider client pools. Hot pool is sized for transactional SLO; bulk pool gets the leftover provider quota.
3. **Provider-side rate limiting with a token-bucket per provider account, in Redis.** Channel workers `INCR` a window counter before each call; if over, sleep / push to retry. This protects us from Twilio's hard limit (e.g., 100 SMS/sec/account, varies by region).
4. **Per-user caps** in Redis (`cap:{user_id}:promo:day` → max 3) to prevent spamming a single user across multiple campaigns.
5. **Time-spread for promotional.** Optional: spread the 8M sends over 30 minutes by adding random `send_at` jitter. Costs nothing, dramatically smooths load.

**What you'd say out loud:** *"The answer to bulk is not 'bigger workers' — it's 'don't let bulk pretend it's transactional.' Different topics, different worker pools, different rate-limit budgets."*

---

### Deep Dive 5 — Provider Failures & Multi-Region

**Problem.** Twilio's us-east endpoint goes down at 8am Monday in NYC.

**Approach.**

1. **Circuit breakers per provider.** After 50% errors over 30s, open the breaker for that provider. Channel workers route to the **secondary provider** (e.g., MessageBird as a Twilio backup) configured at the template/channel level.
2. **Per-region active-active.** Two Uber regions (us-east, us-west) each run a full stack. Users are pinned by hash to a home region but every region can serve any user with degraded latency. Kafka is per-region; cross-region replication only for **device tokens** and **preferences** (low-write, must-be-global).
3. **Cassandra multi-DC replication** on the status table with `LOCAL_QUORUM` writes — no cross-region latency on the hot path.
4. **Failover drill: a region falls over.** Health checks at the API gateway shift traffic; in-flight Kafka messages in the dead region wait for the region to recover (we accept temporary delay for promo, route transactional via the secondary provider in the surviving region using the global token registry).
5. **Graceful channel degradation.** Template can declare `fallback_chain: [push, sms]`. If push fails permanently for a transactional alert ("Trip canceled"), SMS sends. Cost: occasional SMS we didn't budget for, but correctness > cost on transactional.

---

## Phase 7 — Wrap-Up (~3 min)

### Summary in 30 seconds

> "We have a stateless **Notification API** doing auth + dedup + ingest, fronting **Kafka** with separate hot/bulk lanes. A **Router** stage resolves preferences and templates, splits per channel, and emits to per-channel queues. **Channel workers** call APNs/FCM/Twilio/SES with per-provider rate limits and circuit breakers. **Cassandra** is the system of record for notification status; **Redis** handles dedup and preference caching; **Postgres** owns preferences and templates; **DynamoDB** owns device tokens. Retries flow through delayed topics; permanent failures land in DLQs with alerting."

### Bottlenecks I'd watch in production

| Bottleneck | Signal | Mitigation |
|---|---|---|
| Twilio account QPS | Rising 429 rate | Multiple subaccounts; secondary provider |
| Kafka consumer lag in `ingest.bulk` | Lag dashboard | Auto-scale bulk worker pool; throttle audience streamer |
| Redis dedup hot keys | Single-key CPU on Redis cluster | Already random keys → unlikely; if templates collide, hash-tag rebalance |
| Pref cache stampede on mass invalidation | Postgres CPU spike | Probabilistic early refresh; lock-and-fill |
| Cassandra wide-row on celebrity-driver | Partition size | Compaction tuning; cap per-user write rate |

### What I'd build next (with more time)

- **Send-time optimization** (ML on per-user open-rate by hour).
- **Smart channel selection** (try push; if no engagement in 60s for transactional, auto-fall-back to SMS).
- **Real-time campaign analytics** (delivered / opened / clicked) via a Flink job on the status stream.
- **In-app inbox service** with proper sync semantics for offline mobile clients.

### Common follow-ups to be ready for

| Question | One-line answer |
|---|---|
| "10× the traffic — what breaks first?" | Twilio account quota; we'd need provider sharding by recipient region. |
| "How do you migrate Rides off the old notif system?" | Dual-write with a comparison job; flip read traffic per template after parity check. |
| "Why Kafka, not SQS?" | Per-user ordering, replay, and burst absorption. SQS gets throughput but loses ordering guarantees we need for status transitions. |
| "How do you know notifications were delivered?" | Provider receipt → status DB → opt-in delivery webhook. "Delivered" ≠ "opened"; we track both. |
| "What about GDPR / right to erasure?" | Tombstone in `user_preferences` (`deleted=true`); nightly job purges history older than legal retention. |

---

## Common Mistakes to Actively Avoid in This Prompt

1. **Treating all notifications the same.** Promo and "driver arriving" cannot share fate. If you draw a single queue, you'll be challenged.
2. **Hand-waving dedup as "use Redis".** Walk through `SET NX EX`, the failure modes, and the producer contract.
3. **Forgetting cost.** SMS at scale is millions of dollars. Per-user caps and preferences are cost controls, not just UX.
4. **Saying "exactly once".** Don't. Say "at-least-once with idempotency keys at every external boundary."
5. **No invalid-token feedback loop.** APNs/FCM will tell you a device is dead. Your design must consume that and stop sending.
6. **Skipping quiet hours / regulatory.** TCPA/GDPR fines are real; an interviewer at Uber will probe this.
7. **Drawing the architecture before doing estimation.** The numbers justify the queues and the lanes. Without them, the design looks over-engineered.

---

## Uber-Specific Color (sprinkle these in)

- **Trip-state notifications are on the user's critical path.** "Driver is here" arriving 30s late looks like a bug to the rider — design for p99 < 2s, not average.
- **Geographic spread.** Riders in India, drivers in Brazil — SMS provider mix varies. Channel workers should be region-local with a provider-routing table keyed by recipient country.
- **Surge pricing notifications** are a classic burst: city goes into surge → tens of thousands of nearby users notified within seconds. This is *the* test case for the bulk lane.
- **Driver vs rider notifications** have different SLOs and templates but share infra. Mention multi-tenancy of templates by `producer_id`.
- **Two-sided marketplace.** Failure of notifications hurts both sides — driver doesn't know a trip was assigned, rider doesn't know driver is en route. Cost of failure > cost of duplicate.

---

## One-Slide Cheat Sheet (memorize this shape)

```
   API → Redis(dedup) → Kafka(hot/bulk) → Router(prefs+template)
       → Kafka(push|sms|email|inapp) → Workers → Providers
       → Status(Cassandra) + Retry(delayed topic) + DLQ
   Side: Pref(PG+Redis) | Templates(PG+S3) | Devices(Dynamo)
   Cross-cutting: idempotency-key everywhere, circuit breakers per
   provider, per-user + per-provider rate limits, TTL on every msg.
```

If you can draw this shape, label every arrow with what flows on it, and explain *why* each box exists with reference to your estimation numbers — you've cleared the bar raiser.
