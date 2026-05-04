# HLD — Notification System at 100M+/day (Uber SDE-2 Interview Script)

> **Prompt:** Design a system that can deliver 100M+ notifications/day across push (APNs/FCM), SMS (Twilio), email (SES), and in-app channels, with retries, deduplication, and per-user preferences.

> **Framework used:** `Local/HLD/answering_simplified.txt`
> **Total time budget:** ~40 min

---

## Opening (15 seconds — say this verbatim)

> "Cool — Notification System. Before I dive in, let me restate the problem so we're aligned: a write-heavy, fan-out-by-channel system that takes a single 'send notification' request and reliably delivers it across one or more of push / SMS / email / in-app, respecting user preferences, with deduplication so users aren't spammed, and with retries so transient failures don't drop messages. I'll spend ~5 min on requirements + estimation, ~10 min on entities + APIs + HLD, ~15 min on deep dives, and ~3 min wrapping up. Push back if you want me to focus elsewhere."

**How to say it:** Calm, structured, propose-and-confirm. Bar-raisers love candidates who scope the problem before solving it.

---

## Phase 1 — Requirements Clarification (3-5 min)

### Functional Requirements (5-6 points)

> "Let me list what I think is in scope, you tell me if I'm missing anything:"

1. **Send a notification** to a single user across one or more channels: push (APNs for iOS, FCM for Android), SMS (via Twilio), email (via SES), in-app feed.
2. **Per-user preferences** — users can opt out of channels, configure quiet hours, mute categories (promo, transactional, security).
3. **Deduplication** — if the same logical event fires twice within a window, the user gets one notification, not two.
4. **Retries with backoff** — transient provider failures (Twilio timeout, FCM 500) retry automatically; permanent failures (invalid token, unsubscribed) don't.
5. **Templating + personalization** — clients send a template ID + variables, the system renders the final body server-side. (Localized per user.)
6. **Priority lanes** — transactional (OTP, ride-confirmed) bypass marketing traffic; marketing has its own slow lane.

### Questions to ask the interviewer (2-3 questions)

> "A few clarifying questions before I size this:"

1. **"Bulk campaign sends in scope?"** — i.e., 'send to 10M users in this segment'. This changes the design significantly (need a fan-out planner). I'll **assume yes, but as a separate ingress** — campaign service expands to per-user notifications and feeds the same pipeline.
2. **"Tracking opens / clicks?"** — analytics adds a webhook ingestion path. **Assume yes**, but I'll keep it as a separate async stream so it doesn't impact the hot send path.
3. **"Multi-region or single-region?"** — affects DB topology and provider latency. **Assume multi-region active-active**, since Uber is global and APNs/FCM/SES have regional endpoints.

### Non-Functional Requirements

| NFR | Target | Why |
|---|---|---|
| **Scale** | 100M/day → ~1.2K QPS avg, **~12K QPS peak** (10× burst on incidents/promos), **~50K QPS** during a campaign send | Fan-out is the dominant workload |
| **Latency** | Transactional p99 **<1s** end-to-end (API → device); Marketing eventual (minutes OK) | OTPs are useless if late |
| **Availability** | **99.99%** for the ingest API; channel workers can be eventually-consistent | A dropped OTP = locked-out user |
| **Consistency** | **At-least-once delivery + idempotent dedup** at the consumer side | "Exactly-once" is a lie at scale; idempotency is the real answer |
| **Domain-specific #1** | **Provider failure isolation** — Twilio outage must not block FCM | Per-channel circuit breakers + DLQs |
| **Domain-specific #2** | **Backpressure** — a misbehaving client (or campaign) can't drown out OTPs | Priority queues + per-tenant rate limits |

**Killer phrase to drop:** *"Exactly-once delivery is a myth at scale — I'll design for at-least-once with idempotent consumers and dedup keys, which is what every real system actually does."*

---

## Phase 2 — Estimation (2-3 min)

> "Let me size this so the architecture choices are anchored to numbers, not vibes."

### Traffic (QPS)

- **100M / day** = 100M / 86,400s ≈ **1,157 QPS average**
- **Peak factor 10×** (incident notifications, evening commute, Black Friday) → **~12K QPS peak**
- **Campaign burst:** 10M-user campaign delivered over 5 min = **~33K QPS for 5 min**, so design for **~50K QPS sustained capacity**
- **Fan-out:** each logical notification touches ~1.5 channels on average (some users get push + email) → **~75K channel-deliveries/sec at peak**

### Storage

- **Notification record** ≈ 1 KB (id, userId, channel, template, vars, status, timestamps)
- **Daily volume:** 100M × 1 KB = **100 GB/day**
- **Retention:** 90 days hot (audit, retry-window) → **~9 TB hot storage**
- **Delivery attempts:** ~1.2 attempts avg × 100M = 120M/day × 200 B = **24 GB/day** in attempts table
- **Dedup keys** (Redis, TTL 24h): 100M keys × 100 B ≈ **10 GB in Redis**, fits in a small cluster

### Bandwidth

- Egress to providers ≈ 100M × 1 KB = **100 GB/day** outbound to APNs/FCM/Twilio/SES
- Provider response/webhook callbacks: ~50 GB/day

### Domain-specific number — fan-out skew

> "One thing worth calling out: not every user is equal. A 'ride started' notification fans to 1 user (the rider), but a 'price drop' campaign fans to 10M. This is a **3-orders-of-magnitude variance** that drives the dual-ingress design — I'll have a campaign service that pre-expands fan-out before the message hits the main pipeline, so the core is always **per-user-already**."

**How to say it:** Slow down here. These numbers are the foundation of every design choice you make later. Write them on the whiteboard if there's one.

---

## Phase 3 — Core Entities (3-5 min, bottom-up)

> "Let me lay out the core entities — these will become tables and queues later."

| # | Entity | Key fields | Notes |
|---|---|---|---|
| 1 | **Notification** | `id (uuid)`, `userId`, `channels[]`, `templateId`, `vars{}`, `priority`, `dedupKey`, `status`, `createdAt`, `scheduledFor` | The logical message |
| 2 | **DeliveryAttempt** | `notificationId`, `channel`, `attempt#`, `providerResponseCode`, `latencyMs`, `status`, `attemptedAt` | One row per send try; retries append |
| 3 | **User** | `id`, `email`, `phone`, `locale`, `timezone` | Source of truth for contact info |
| 4 | **UserPreferences** | `userId`, `channelOptIns{}`, `quietHours`, `categoryMutes[]`, `lastUpdated` | Read on every send — must be cached |
| 5 | **DeviceToken** | `userId`, `token`, `platform (iOS/Android)`, `appVersion`, `lastSeenAt` | Multiple per user; expires |
| 6 | **Template** | `id`, `name`, `channel`, `localeBody{}`, `version`, `category` | Versioned, locale-keyed |
| 7 | **DedupKey** | `key`, `notificationId`, `expiresAt` | Redis, TTL'd |
| 8 | **Campaign** | `id`, `segmentId`, `templateId`, `scheduledAt`, `status` | Bulk send job; expands to N notifications |

**How to say it:** Read them off quickly — don't dwell. The interviewer wants to see that you're thinking in nouns before verbs.

---

## Phase 4 — API Design (3-5 min)

> "I'll define 6 external + 1 internal API. All external are authenticated; senders use service-to-service JWTs."

### 1. **Send Notification API** — the hot path
- Handles a single notification request from any internal service (Rides, Payments, Eats, etc.). Idempotency-Key header makes retries safe.
- `POST /v1/notifications`
  Request: `{ userId, channels: ["push","email"], templateId, vars: {...}, priority: "transactional", dedupKey: "ride-123-started" }`
  Headers: `Idempotency-Key: <uuid>`
  Response: `202 Accepted { notificationId, status: "queued" }`

### 2. **Bulk / Campaign Send API**
- Kicks off a campaign — segment expansion happens async, returns a job handle. Doesn't block on fan-out.
- `POST /v1/campaigns`
  Request: `{ segmentId, templateId, scheduledAt, channels }`
  Response: `202 { campaignId, estimatedRecipients }`

### 3. **Get Notification Status**
- For internal services to check delivery status (debugging, audit).
- `GET /v1/notifications/{id}`
  Response: `{ id, status, attempts: [...], deliveredAt }`

### 4. **In-App Feed API** (read path)
- The mobile app pulls in-app notifications on launch. Cursor-paginated.
- `GET /v1/users/{userId}/inbox?cursor=<opaque>&limit=20`
  Response: `{ notifications: [...], nextCursor }`

### 5. **Update Preferences API**
- User-facing — "mute promo emails", "set quiet hours 10pm-7am". Writes are write-through to cache.
- `PUT /v1/users/{userId}/preferences`
  Request: `{ channelOptIns: {email: true, sms: false}, quietHours: {start: "22:00", end: "07:00", tz: "America/Los_Angeles"} }`
  Response: `200 OK`

### 6. **Register Device API** (push tokens)
- Mobile clients register/refresh APNs/FCM tokens on app start.
- `POST /v1/devices`
  Request: `{ userId, token, platform: "iOS", appVersion }`
  Response: `200 { deviceId }`

### 7. **(Internal) Provider Webhook Receiver**
- Receives delivery confirmations & bounces from Twilio/SES. Updates DeliveryAttempt, feeds analytics.
- `POST /internal/webhooks/{provider}` — auth via shared secret + signature verification.

**Killer detail to drop:** *"Notice the send API takes a `dedupKey` separate from the `Idempotency-Key`. The Idempotency-Key handles network retries from the calling service. The `dedupKey` handles logical deduplication — two different services both deciding to send 'ride started'. They solve different problems."*

---

## Phase 5 — High-Level Design (10-12 min)

> "Let me build this in 3 layers — ingest, processing, delivery — then I'll draw the full picture."

### Services (5-8)

| Service | Responsibility |
|---|---|
| **Notification API Gateway** | Auth, rate limit, request validation, idempotency check, publishes to ingest topic |
| **Campaign Service** | Expands segment → per-user notifications, scheduled via cron, feeds same ingest pipeline |
| **Notification Service (Orchestrator)** | Consumes ingest topic, hydrates (preferences + device tokens + template), applies dedup, fans out to channel topics |
| **Channel Workers** (Push / SMS / Email / In-App) — **separate service per channel** | Pulls from channel-specific topic, calls provider (APNs/FCM/Twilio/SES), writes attempt result, retries via DLQ |
| **Preference Service** | CRUD on user preferences, write-through to Redis |
| **Device Registry Service** | Manages device tokens, prunes stale tokens based on APNs/FCM feedback |
| **Template Service** | Stores + versions templates, renders on demand (or pre-renders to cache) |
| **Webhook Ingestor** | Receives provider callbacks, updates delivery state, feeds analytics |
| **Analytics Service** (below the line) | Consumes events, populates dashboards, drives ML for send-time optimization |

### Database Layer

| Store | Tech | Why this DB (and why not others) |
|---|---|---|
| **Notifications + Attempts** | **Cassandra** (partition key: `userId`, clustering: `createdAt DESC`) | Write-heavy (12K-50K writes/sec), append-only, time-series access pattern, need horizontal scale. **Not Postgres** — would need heavy sharding and rotation. **Not DynamoDB** — fine, but Cassandra gives more knobs for retention TTL on column families. |
| **User Preferences** | **PostgreSQL** | Low volume (~10s of writes/sec), structured, transactional (multi-field updates atomic). **Not Cassandra** — overkill, and we want secondary indexes. |
| **Device Tokens** | **PostgreSQL** with `(userId, platform)` index | Same reasoning as prefs — low write rate, frequent reads, joins make audit easy. |
| **Templates** | **PostgreSQL + S3 for large bodies** | Versioned, infrequently changed, structured. |
| **Dedup Keys** | **Redis** with 24h TTL | Need O(1) `SET NX` semantics with TTL for atomic dedup. **Not Postgres** — TTL semantics are clunky and write rate is too high. |
| **Pref/Token Cache** | **Redis** (cache-aside, write-through on update) | Hot read on every notification — must be sub-ms. |
| **In-App Inbox** | **Cassandra** (partition: `userId`, cluster: `createdAt DESC`, TTL 90d) | Same shape as notifications log; lets us query "last 50 for user" cheaply. |
| **Analytics events** | **Kafka → ClickHouse** | OLAP scans, not OLTP; 100M+ events/day. |

### Cache Layer

- **Redis cluster (preferences + device tokens)** — sits between Notification Service and Postgres. **Cache-aside pattern**: orchestrator reads from Redis on send; on miss, reads Postgres and populates. **Write-through on preference update** so a user who just disabled SMS never gets one in the next millisecond. TTL 1h as a safety net for cache poisoning.
- **Redis (template cache)** — hot templates pre-rendered with locale variants. Sits between Channel Workers and Template Service.
- **Redis (dedup)** — sits in front of Notification Service. `SET NX EX 86400` on `dedupKey`. If returns `false` (key exists), drop the notification, log a `deduped` event.

### Queue Layer

- **Kafka — `notifications.ingest` topic** — main entry queue. Partition by `userId` (so same user's notifications hit the same orchestrator instance, helps with rate-limiting per user). Sits between API Gateway and Notification Service. **30-day retention** so we can replay during incidents.
- **Kafka — `channel.push`, `channel.sms`, `channel.email`, `channel.inapp`** — per-channel topics. Partitioning by `userId`. Sits between Notification Service and Channel Workers. **Why per-channel?** A Twilio outage backs up only `channel.sms`, doesn't block FCM.
- **Priority lanes:** within each channel, **two topics: `channel.X.transactional` and `channel.X.marketing`**. Workers consume transactional with higher concurrency. Marketing throttled.
- **DLQ — `channel.X.dlq`** — after N retries, message lands here. Separate consumer with longer backoff + alert.

### Architecture Diagram

```
                                ┌─────────────────────┐
  Internal services             │  Campaign Service    │
  (Rides, Eats, Pay…)           │  (segment → fan-out) │
        │                       └──────────┬───────────┘
        ▼                                  │
  ┌─────────────────────┐                  │
  │  Notification API   │ ◄────────────────┘
  │  Gateway            │
  │  (auth, ratelimit,  │
  │   idempotency)      │
  └──────────┬──────────┘
             │
             ▼
   ╔═══════════════════════════════╗
   ║  Kafka: notifications.ingest  ║   (partitioned by userId, 30d retention)
   ╚═══════════════╤═══════════════╝
                   │
                   ▼
        ┌──────────────────────┐    ┌─────────────────┐
        │ Notification         │◄──►│  Redis: prefs,  │ ◄── Preference
        │ Service              │    │  tokens, dedup  │     Service
        │ (hydrate + dedup +   │    └─────────────────┘    (Postgres)
        │  fan-out per channel)│            ▲
        └──────────┬───────────┘            │
                   │                        │
                   ▼                        │
   ╔═══════════════════════════════╗       │
   ║  Kafka: channel.{push|sms|    ║       │
   ║  email|inapp}.{txn|mkt}       ║       │
   ╚═══╤══════╤══════╤══════╤══════╝       │
       │      │      │      │              │
       ▼      ▼      ▼      ▼              │
   ┌─────┐┌────┐┌──────┐┌──────┐           │
   │Push ││SMS ││Email ││InApp │           │
   │Wkr  ││Wkr ││Wkr   ││Wkr   │           │
   └──┬──┘└──┬─┘└───┬──┘└───┬──┘           │
      │     │      │       │               │
      ▼     ▼      ▼       ▼               │
   APNs/  Twilio  SES    Cassandra         │
   FCM                  (in-app inbox)     │
      │     │      │                       │
      └──┬──┴──────┘                       │
         ▼                                 │
   ┌──────────────┐                        │
   │ Webhook      │ ──► DeliveryAttempt    │
   │ Ingestor     │     (Cassandra)        │
   └──────┬───────┘                        │
          ▼                                │
   ┌──────────────┐                        │
   │ Analytics →  │                        │
   │ ClickHouse   │                        │
   └──────────────┘                        │
                                           │
   Notifications log (Cassandra) ──────────┘
```

**Data flow walkthrough — narrate this verbally:**

> "Ride service publishes 'ride started' to `POST /v1/notifications` with userId, dedupKey 'ride-{rideId}-started', priority transactional. Gateway authenticates, checks idempotency, writes to `notifications.ingest`. Orchestrator pulls it, does a `Redis SET NX` on the dedup key — if it loses the race, drops it as duplicate. Otherwise hydrates: pulls preferences from Redis (cache-miss falls through to Postgres), checks if push is opt-in and quiet hours don't apply, then publishes to `channel.push.transactional`. Push worker pulls, renders template, calls FCM, writes DeliveryAttempt to Cassandra. FCM webhook later confirms delivery — Webhook Ingestor updates the attempt status. Total p99 budget: 800ms — Kafka adds ~50ms, Redis ~5ms, FCM ~300ms, the rest is buffer."

---

## Phase 6 — Deep Dives & Trade-offs (12-15 min)

> "Let me pick four areas where senior judgment matters most: the Kafka partitioning + priority strategy, the dedup design, the retry + circuit breaker logic, and the preference-cache hot-key problem."

### Deep Dive 1 — Kafka Partitioning + Priority Lanes (the throughput backbone)

1. **Component & where it sits.** Kafka clusters between API Gateway → Notification Service, and between Notification Service → Channel Workers. Two layers: `notifications.ingest` (single topic, ~64 partitions) and `channel.{push|sms|email|inapp}.{txn|mkt}` (8 topics, ~32 partitions each).
2. **Interaction.** API Gateway is the producer to ingest; Notification Service is consumer-of-ingest and producer-to-channel; Channel Workers are consumers-of-channel. **Partition key = `userId`** everywhere — guarantees ordering per user (so a 'cancelled' never overtakes a 'started' for the same ride) and enables per-user rate limiting at the consumer.
3. **Why critical.** Without per-channel separation, a Twilio outage backs up the whole pipeline and OTPs queue behind marketing emails. Without priority lanes, a 10M-user campaign delays every transactional notification for 5 minutes. At 100M/day with bursts to 50K QPS, the queue *is* the system.
4. **Alternatives considered.**
   - **RabbitMQ** — simpler, supports priority queues natively, but tops out around 50K msg/sec per node and lacks Kafka's replayability. Trade-off: lower ops complexity, but I'd hit ceilings during campaign bursts.
   - **AWS SQS** — managed, but no ordered partitions; FIFO queues cap at 3K msg/sec/group which is insufficient.
   - **Single mega-topic with priority field** — simpler routing but a slow SMS provider would block fast push delivery (head-of-line blocking).
5. **Recommendation.** **Kafka with per-channel + per-priority topics.** The 8-topic explosion is worth it: each provider's failure is isolated, each priority gets its own consumer-group concurrency. Partition by `userId` for ordering. Retain ingest for 30 days so we can replay during a postmortem. Killer phrase: *"head-of-line blocking is the enemy at fan-out — separate topics per failure domain."*

### Deep Dive 2 — Deduplication (idempotent at the consumer)

1. **Component & where it sits.** Redis cluster sits between Notification Service consumer logic and the channel-fan-out step. Single-purpose: dedup key store with TTL.
2. **Interaction.** When the orchestrator pulls a message from `notifications.ingest`, it computes a dedup key — preferring the caller-provided `dedupKey`, falling back to `hash(userId + templateId + variables)`. It runs `SET dedupKey notificationId NX EX 86400`. If `OK`, proceeds. If `nil` (key existed), drops the message, increments a `deduped` metric.
3. **Why critical.** With at-least-once delivery (Kafka redelivery + service retries + caller retries), the *same* logical notification can hit the orchestrator 3-4 times. Without dedup, a user gets 4 push notifications for the same ride. Worse, with `dedupKey` from callers, two services can both decide to send 'ride started' (Rides + Trip Receipt) — same logical event, different code paths — and we collapse them to one.
4. **Alternatives.**
   - **DB-based dedup** (Postgres `INSERT … ON CONFLICT`). Works, but at 50K QPS peak this hammers Postgres — Redis `SET NX` is 100× faster.
   - **Bloom filter** for dedup. Memory-efficient but probabilistic; a false positive *drops* a legit notification (worst-case: missed OTP). Unacceptable.
   - **Exactly-once Kafka transactions.** Doesn't help — only covers Kafka→Kafka, not the provider call. The dedup problem is broader.
   - **Leave it to the caller.** Pushes burden onto every caller team; inevitable that some service forgets and spams users.
5. **Recommendation.** **Redis `SET NX EX` keyed on caller-provided `dedupKey` (or hash fallback), 24h TTL.** Centralizes the contract, protects against caller bugs, fast enough for hot path. Caveat noted out loud: *"if Redis is down we degrade to no-dedup rather than blocking — duplicate notifications are worse than lost ones, but only marginally."* This is the kind of trade-off bar-raisers want to hear you reason about.

### Deep Dive 3 — Retries, DLQ, and Provider Circuit Breakers

1. **Component & where it sits.** Lives inside each Channel Worker. Three concentric layers: in-process retry → DLQ topic with delayed redelivery → circuit breaker per provider.
2. **Interaction.** Worker calls provider (e.g., Twilio). On **transient failure** (HTTP 5xx, timeout, rate-limit 429), increments retry counter, republishes to `channel.sms.dlq` with a `retry-after` header. A **DLQ consumer** sleeps until `retry-after` then moves it back to the main topic. Cap at 5 retries with exponential backoff (5s → 25s → 2m → 10m → 1h). On **permanent failure** (invalid number, unsubscribed, 400), no retry — log and emit a `permanent_failure` event so Device Registry can prune the token. **Circuit breaker** wraps every provider call: if error rate >50% over the last 30s, the breaker opens, all calls instantly fail-fast and route to DLQ for 60s, then half-open to test recovery.
3. **Why critical.** Twilio has ~99.95% availability — that's ~4 hours/year of outage. Without circuit breakers, during a Twilio outage every SMS worker thread blocks on a 30s timeout, exhausting connection pools, and the SMS lane chokes. Without DLQ delay semantics, retries hammer the provider and slow recovery. At 100M/day, even a 0.1% transient failure rate is 100K messages — must be reliable.
4. **Alternatives.**
   - **Retry inline in the worker with `Thread.sleep`.** Blocks the worker, drops throughput, no observability. Hard no.
   - **Retry by re-delivering to the same Kafka topic.** Floods the main topic with stale retries, breaks ordering. Bad.
   - **AWS Step Functions for retry orchestration.** Works at low scale, too expensive at 100M/day.
   - **No circuit breaker, just timeouts.** During an outage, you eventually drain the queue, but workers spend 90% of CPU on timeouts. Wastes resources, prolongs recovery.
5. **Recommendation.** **Per-channel DLQ topics with delayed redelivery + Hystrix-style circuit breaker per provider.** Five-attempt cap with exponential backoff to prevent infinite loops. After cap, message goes to a *dead-letter audit topic* for human review. Critical detail: **circuit breakers are per (channel, provider, region)** — if APNs-East is degraded, APNs-West still works. Real-world parallel: this is exactly how Netflix's notification platform (and Uber's actual stack) handles SES + Twilio.

### Deep Dive 4 — Preference Cache (the hot-read bottleneck)

1. **Component & where it sits.** Redis cluster, accessed by every Notification Service instance on every send. Cache-aside reads, write-through on preference updates.
2. **Interaction.** On send, orchestrator does `GET prefs:{userId}`. On miss, falls through to Postgres, populates Redis with 1h TTL. On preference update via API, Postgres write commits, *then* Redis write happens — **write-through**, not write-back, so a user who just disabled SMS never gets one. We pin the user's preferences on the same Redis shard as their device tokens to colocate hot reads.
3. **Why critical.** At 50K QPS peak, every send is at minimum 2 reads (prefs + tokens). That's 100K reads/sec. If we hit Postgres directly, we'd need 50+ replicas; with Redis we run 12 nodes total. Cache hit rate matters: at 99% hit rate, Postgres sees 1K QPS — fine. At 90%, Postgres sees 10K QPS — borderline. So the cache *is* the system's load-bearing wall.
4. **Alternatives.**
   - **No cache, scale Postgres.** Possible with read replicas, but $$$ and the read-after-write problem (replica lag means a just-disabled SMS can still fire) is real.
   - **Local in-process cache** (Caffeine) per orchestrator instance. Ultra-fast but stale on preference updates — if a user disables SMS, we'd send for up to TTL minutes. Unacceptable for a privacy-sensitive setting.
   - **Cache-aside without write-through.** Simpler but allows the just-described staleness. We invalidate on write — works, but two ops, one of which can fail.
   - **Push-based invalidation via Kafka.** When pref changes, publish to `prefs.changed`, all orchestrator instances invalidate. Useful at extreme scale but adds a moving part.
5. **Recommendation.** **Redis cluster, cache-aside on read + write-through on update, 1h TTL as backstop.** The write-through is non-negotiable for compliance — a user opting out must be honored *immediately*, not eventually. Bonus: **negative cache** for users who don't exist (bot traffic, bad UUIDs) — `GET prefs:bogus-uuid` shouldn't slam Postgres. Cache misses for nonexistent keys for 60s with a sentinel value.

**How to say this section:** Speak slowly, draw small sub-diagrams for each. The goal is to demonstrate that you've thought about *what breaks* and *how to fix it*, not that you can name 12 technologies.

---

## Phase 7 — Wrap-Up (2-3 min)

### Summary of the design (2 points)
1. **Functionally:** A 3-stage pipeline — *ingest* (API + campaign expansion → Kafka), *orchestrate* (dedup + preference + template hydration → per-channel topics), *deliver* (channel workers → providers + DLQ + webhooks). Each stage is independently scalable; failure domains are isolated per channel.
2. **Architecturally:** Read-heavy on the cache (preferences/tokens), write-heavy on the log (Cassandra notifications + attempts), and asynchronous on the slow providers — three different storage profiles cleanly separated.

### Trade-offs (2 points)
1. **At-least-once + dedup, not exactly-once.** Simpler, faster, and matches real-world systems. Cost: Redis dedup is a single-point-of-degradation; we accept rare duplicates over rare drops.
2. **Per-channel topic explosion (8 topics).** More ops surface, but failure isolation is worth it; a Twilio outage doesn't poison FCM. Maintainability cost mitigated by topic auto-provisioning + standardized worker code.

### Performance optimizations (2 points)
1. **Pre-rendered templates in Redis** for the hottest 1% of templates (cuts render time from ~5ms to <1ms; with 50K QPS that's significant CPU).
2. **Connection pooling + HTTP/2** to providers — keeps APNs/FCM connections warm; saves ~50ms per send vs. cold TLS handshakes.

### How the system extends (2 points)
1. **ML-driven send-time optimization.** The analytics stream feeds a model that predicts user open-rate by time-of-day; the orchestrator can defer marketing notifications to each user's optimal window. Drops directly into the `scheduledFor` field — no architectural change.
2. **Smart channel selection.** Today the caller specifies channels. Future: the system picks based on user engagement history (push first, fall back to SMS if not opened in 5 min). Adds a `ChannelSelector` step in the orchestrator, fully backward-compatible.

### Follow-up questions to ask the interviewer (3-4)

1. **"How would you handle compliance — GDPR right-to-be-forgotten — given we have 90 days of notification logs?"**
   *Answer if pushed:* TTL-based deletion in Cassandra, hard-delete in Postgres on user delete event, scrub Redis caches, plus a tombstone in dedup keys so we don't re-create.

2. **"What's your strategy if APNs goes down for an hour?"**
   *Answer:* Circuit breaker opens within 30s, all push messages flow to DLQ with a 1h delay; transactional pushes get fallback-routed to SMS via a `fallbackChannel` rule in the template; we drain the DLQ when APNs recovers.

3. **"How would you stop a misbehaving service from spamming users?"**
   *Answer:* Per-tenant token bucket rate limit at the API Gateway (500/sec/service default), plus per-user rate limit at the orchestrator (max 10 notifications/hour/user for non-transactional). Circuit-breaker on caller if abuse pattern detected.

4. **"Where do you see this architecture failing first as we 10× scale?"**
   *Answer:* The Notification Service orchestrator fan-out — it's the only stateful step that does multiple Redis hops + a fan-out write. Mitigation: shard orchestrator by `userId`, keep that user's prefs/tokens in process via consistent-hash routing, drop Redis from the hot path. Beyond that, Cassandra hot partitions on celebrity users (campaigns to influencers) — addressed via partition-key salting.

---

## Cheat Sheet (memorize this the night before)

### Numbers
- 100M/day = 1.2K avg / 12K peak / 50K campaign-burst QPS
- 9 TB hot storage (90 days)
- 75K channel-deliveries/sec at peak (1.5× fan-out)
- p99 budget: 800ms transactional end-to-end

### Killer phrases
1. *"Exactly-once is a myth at scale — at-least-once + idempotent consumers + dedup keys is what real systems do."*
2. *"Head-of-line blocking is the enemy at fan-out — separate topics per failure domain."*
3. *"Circuit breakers are per-(channel, provider, region) — APNs-East down doesn't take APNs-West down."*
4. *"Write-through cache on preferences is non-negotiable for compliance — a user opting out is honored immediately, not eventually."*

### Common mistakes to avoid
- Don't propose exactly-once delivery — show you know it's a myth.
- Don't put preferences in the same DB as notifications — different access patterns.
- Don't forget per-channel DLQ — Twilio outage will reveal it on day 1.
- Don't skip the campaign service — bulk sends will saturate the ingest API otherwise.
- Don't forget device-token pruning — APNs/FCM rejection feedback must update Device Registry, otherwise dead tokens retry forever.
- Don't over-index on "what database" — interviewers care more about *why this DB for this access pattern*.

### Order of presentation
1. Restate problem + propose scope (15s)
2. FRs (90s) → questions (60s) → NFRs with table (90s)
3. Estimation with numbers on whiteboard (2m)
4. Entities table (90s)
5. APIs with one-line + req/resp each (3m)
6. Services table → DB-per-store table → cache → queue → diagram (10m)
7. **4 deep dives, 5 points each** (12m) — this is where the interview is won or lost
8. Wrap-up — summary + trade-offs + extensibility + 3-4 follow-up questions you ask back (3m)

---

## Review

Wrote `Local/HLD/notification/notification_simplified.md` as a rehearsal-ready, end-to-end interview script for the Uber SDE-2 notification HLD prompt, following the simplified 7-phase framework in `answering_simplified.txt`.

**Key choices:**

- **Opens with a 15-second restatement + scope proposal**, matching the framework's `propose-and-confirm` opening guidance and the pattern from the existing Uber/E-commerce/Surge HLD scripts in the repo.
- **NFR table foregrounds 99.99% availability + p99 < 1s for transactional**, since OTP/security notifications are the highest-stakes use case and should drive the priority-lane design.
- **Estimation surfaces the campaign burst (~50K QPS for 5 min)** explicitly because it's the dominant capacity-planning number and a common candidate miss — most people only size for the daily average.
- **Phase 5 splits the DB-per-store choice into a table** with explicit "why this / why not the alternatives" — interviewers grade this section heavily.
- **Architecture diagram built bottom-up** with a verbal walkthrough script ("Ride service publishes…") so the candidate can narrate the golden path.
- **Four deep dives chosen for senior signal**: (1) Kafka partitioning + per-channel + per-priority topic explosion as failure isolation, (2) Redis `SET NX EX` dedup with the trade-off "duplicates are worse than drops, but only marginally" called out explicitly, (3) DLQ + circuit-breaker-per-(channel, provider, region) — the kind of operational detail that distinguishes SDE-2 from SDE-1, (4) Cache-aside + write-through on preferences with the compliance angle (opt-outs honored immediately).
- **Killer phrases section** at the end so the candidate has 4 memorizable sound bites.
- **Follow-up Q&A includes "where does this fail at 10×"** — pre-empts the most common bar-raiser curveball.

**Deliberately scoped out:** ML send-time optimization, smart channel fallback, compliance GDPR deep dive — flagged in the wrap-up as future improvements rather than day-one architecture, mirroring the framework's guidance to keep deep dives tight.
