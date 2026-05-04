# Uber SDE2 HLD — Gap Analysis for Stock Price Alert System

> Companion to `stock_trade_price_alert_simplified.md`. This document captures the gaps an Uber interviewer is likely to probe and what to add to flip the signal from "strong SDE2 pass" to "stretch-hire / L5 lean."

---

## Overall Verdict

The base script is **strong** — likely L5 (Senior) signal if delivered well, comfortably above the SDE2/L4 bar.

**Senior moves already present:**
- Opener flags the two hard problems (skew + exactly-once) before diving in
- Hot-key sub-key splitting in Deep Dive 1
- Outbox + co-partitioned in-memory cache in Deep Dive 4
- Honest failure-mode walkthrough in Deep Dive 3 (Redis failover duplicate window)
- Numbers tied to design decisions (the 5×10¹⁰ evaluations/sec → aggregate-path shift)

But Uber's bar has specific flavors that the current script under-indexes on. The gaps below are ranked by how likely an Uber interviewer probes each one.

---

## Gaps Ranked by Probe Likelihood

### Gap 1 — Multi-Region Story is Absent (HIGHEST PRIORITY)

**Why Uber cares:** Uber lives and dies by geo-distributed systems. Riders, drivers, dispatch, payments — all inherently regional. An interviewer will ask this within the first deep dive.

**What's missing in the script:**
- Where do exchanges, services, and users live geographically?
- Active-active or active-passive across regions?
- Cross-region Kafka replication (MirrorMaker2 / Cluster Linking)?
- Region-local Redis dedup state — what happens when a user's notification could be dispatched from either region?

**What to add — the answer:**
- **Ingestion is region-pinned to the exchange.** NYSE/NASDAQ feeds terminate in us-east; you don't ingest the same feed from two regions because exchange contracts forbid it and you'd double-count.
- **Aggregation runs in the ingestion region** and replicates finalized aggregates (not raw ticks) to other regions via Kafka MirrorMaker2 on the `aggregates.updated` topic. Raw ticks stay region-local; only the compressed aggregate stream crosses regions.
- **Alert evaluation runs in every region**, each consuming the replicated `aggregates.updated`. Alerts are sharded by `user_id`, not ticker — so a given user's alerts evaluate in their home region.
- **Notification dedup must be globally consistent.** Region-local Redis SETNX is not enough — a Kafka rebalance during a region failover can re-deliver an event in a different region. Solutions: (a) globally-replicated Redis (e.g., AWS ElastiCache Global Datastore, async, accept tiny window) or (b) DynamoDB Global Tables with conditional-put for dedup on the cross-region path. Trade latency for correctness on this specific hop.
- **Failover plan:** If us-east ingestion goes down, secondary feed in us-west takes over from a backup exchange agreement; aggregates resume from the new region. RTO ~30s, RPO = whatever's in flight in Kafka (zero loss, possible replay).

**One-liner to drop in the interview:** *"Tick ingestion is region-pinned by exchange contract, so I replicate the much smaller aggregate stream cross-region instead of raw ticks — that's the only thing my multi-region alert eval needs to be correct."*

---

### Gap 2 — Backpressure & Load Shedding

**Why Uber cares:** Uber asks "what happens when X is slow" relentlessly. At 1M ticks/sec, 30 seconds of consumer lag means GBs of buffered data and a notification arriving after the price has reversed.

**What's missing in the script:**
- No discussion of behavior when Notification Service backs up
- No discussion of Twilio/FCM provider degradation
- No SLO on consumer lag

**What to add — the answer:**
- **Consumer lag SLO:** `aggregates.updated` consumer lag p99 < 500ms during market hours. Alert on > 2s sustained for 60s.
- **Tiered degradation on Notification Service:**
  1. Normal: dispatch to all subscribed channels (push + email + SMS).
  2. Degraded (lag > 5s): drop email/SMS, keep push only — push is the channel users care about most for stock alerts.
  3. Critical (lag > 30s): drop static-threshold alerts that are already stale (price has moved past the threshold by more than the threshold itself), keep percent-move alerts which are still actionable.
- **Provider circuit breakers:** Twilio at >5% error rate over 30s → trip circuit, route SMS to a buffer topic for later replay, do not block other channels. Same pattern for FCM/SES, independent breakers per provider.
- **Ingestion never sheds.** The ingestion path is the source of truth — it must accept every tick or the data is lost forever. Backpressure here means scaling Kafka brokers, not dropping. Everything downstream is allowed to shed under load.

**One-liner:** *"Ingestion can't shed — the tick is gone if I drop it. Everything downstream has a tiered degradation contract. Push notification is the floor; everything else is best-effort under load."*

---

### Gap 3 — State Recovery on Service Restart

**Why Uber cares:** Production systems restart. Every deploy, every crash, every scale-out event. Senior engineers design for restart from day one.

**What's missing in the script:**
- Aggregation Service holds in-flight 1m/5m/1h/1d/1w buckets in memory
- No discussion of what happens on deploy or crash

**What to add — the answer:**
- **Two-tier state model:** in-memory hot state (current in-flight buckets) + durable checkpoint (Redis snapshot every 1s).
- **On restart, Aggregation Service:**
  1. Reads the last Redis checkpoint to recover in-flight bucket state.
  2. Replays Kafka `ticks.raw` from the offset stored in the checkpoint.
  3. Resumes producing `aggregates.updated` once caught up.
- **Idempotency on aggregate writes:** finalized bars are keyed by `(ticker, window_size, bucket_start)` — re-writing the same bar on replay is a no-op, not a duplicate.
- **Alternative: RocksDB-backed local state** (Kafka Streams pattern). More resilient (survives Redis loss), more operational complexity. Trade-off worth naming explicitly.
- **For the in-memory alert cache (Alert Evaluation Service):** rehydrate from PostgreSQL on startup, then catch up on the `alerts.changed` outbox topic from the last committed offset. Cold start ~30s for a pod owning ~50 hot tickers.

**One-liner:** *"In-flight aggregate state is in-memory for speed, checkpointed to Redis every second, and reconstructible from Kafka replay if both fail. Aggregate writes are idempotent on `(ticker, window, bucket_start)`, so replay is safe."*

---

### Gap 4 — Observability / SLOs Per Stage

**Why Uber cares:** Uber's M3/Jaeger stack is core to engineering culture. "How do you know it's working?" is a guaranteed question.

**What's missing in the script:**
- Latency *budgets* are listed (50/100/200/500ms) but no measurement story
- No tracing across the pipeline
- No meta-monitoring (is the alerting system itself working?)

**What to add — the answer:**
- **Trace propagation:** every tick gets a trace ID at the ingestion gateway, propagated via Kafka headers through aggregation → eval → notification. Sampling at 0.1% of ticks (still 1000/sec at peak — plenty of data) plus 100% of fired alerts (low volume, high value).
- **Per-stage SLIs:**
  - Ingestion: tick acceptance rate, ingestion-to-Kafka p99 latency.
  - Aggregation: `aggregates.updated` lag, bucket-finalization correctness (count of bars produced vs expected).
  - Alert eval: alerts evaluated/sec per pod, false-positive evaluation rate (evaluated but not fired).
  - Notification: dispatch p99 per channel, dedup-hit rate (sanity check — should be near zero in steady state, spikes mean retries).
- **End-to-end SLO:** p99 tick-to-notification < 1s, measured by injecting synthetic ticks every minute and timing the notification round-trip.
- **Meta-monitoring:** if zero alerts fire for 1 hour during market hours, page oncall — that's almost certainly a broken pipeline, not genuinely zero matches.
- **Dashboards:** one per service, one cross-cutting "alert pipeline health" dashboard with the end-to-end p99 as the headline number.

**One-liner:** *"The hardest bug in this system is silent failure — the pipeline runs but no alerts fire. Synthetic ticks every minute and a 'zero alerts in an hour' page are the cheapest way to catch that."*

---

### Gap 5 — Concrete Capacity Sizing

**Why Uber cares:** Senior signal is grounding throughput claims in actual cluster sizes. Hand-wavy "scale horizontally" is L4 ceiling; "10 brokers, 200 partitions, RF=3, here's why" is L5.

**What's missing in the script:**
- Reasoning is about throughput in the abstract
- Never grounds in box counts

**What to add — back-of-envelope cluster sizes:**
- **Kafka `ticks.raw`:** 1M ticks/sec × 100 bytes (Avro) = 100 MB/sec ingress. RF=3 → 300 MB/sec replication. ~12 brokers (i3.2xlarge equivalent), 256 partitions (oversharded for headroom + hot-key sub-splitting).
- **Cassandra:** 1M writes/sec, RF=3 → 3M writes/sec cluster-wide. ~30 nodes (i3.4xlarge) at 100K writes/sec/node sustained. Storage: 7 days × 1.2 TB/day raw + aggregates ≈ 10 TB working set, comfortable on 30 nodes with 1.5 TB local NVMe each.
- **Redis (cluster mode):** ~50 GB working set (current prices + in-flight aggregates + dedup keys with TTL). 6-shard cluster with 3 replicas each = 18 nodes (cache.r6g.xlarge).
- **PostgreSQL:** 50M alerts × 200 bytes ≈ 10 GB. Single primary + 2 replicas, no sharding needed. Read replicas for the eval-service hydration path.
- **Aggregation Service pods:** ~20 pods (one per Kafka partition group + headroom), each holding state for ~500 tickers.
- **Alert Evaluation Service pods:** ~50 pods, co-partitioned with aggregates topic.

**One-liner:** *"This isn't a 1000-node design — most of the throughput is on Kafka and Cassandra, which are linearly scalable, and the rest is small. ~100 boxes total, mostly Kafka and Cassandra."*

---

### Gap 6 — WebSocket Scaling Deep-Dive (or Explicit Deferral)

**Why Uber cares:** 10M DAU streaming live prices = millions of concurrent WS connections. Uber runs comparable connection counts in driver-app dispatch.

**What's missing in the script:**
- Listed as a service but never deep-dived
- No discussion of sticky routing, reconnection storms, payload throttling

**What to add — the answer (or deferral):**
- **Per-pod limit:** ~50K concurrent WS connections per pod (kernel/file-descriptor bound). 10M DAU at 10% concurrent = 1M connections → 20 pods, but oversized to 50 for failure tolerance.
- **Sticky routing:** clients hash to a pod by `user_id`; consistent-hash ring on the load balancer so that a pod loss only re-shuffles its own slice.
- **Reconnection storm on deploy:** rolling deploy with 5% pod drain per minute, plus client-side jittered reconnect (exponential backoff with jitter, max 30s).
- **Payload throttling:** push at most 10 updates/sec per ticker per client; coalesce intermediate ticks into the latest. Saves bandwidth and battery on mobile.
- **Subscription state:** stored in Redis keyed by connection ID, TTL = WS heartbeat + buffer. Allows reconnection with state recovery.

**Or, the explicit deferral:** *"WS fan-out is a generic pub/sub problem at this scale — sticky routing, jittered reconnect, payload coalescing, ~50K connections per pod. Happy to drill in if you want, otherwise I'd rather spend the time on the alert pipeline."* This is a senior move — knowing what to skip is signal.

---

### Gap 7 — Clock Skew & Event-Time vs Processing-Time

**Why Uber cares:** Real-time systems with sliding windows fail subtly when clocks drift. This is a classic L5 question on stream-processing designs.

**What's missing in the script:**
- Percent-move alerts depend on "price 1h ago"
- Two timestamps exist (`ts_exch`, `ts_ingest`) but the script never says which is authoritative

**What to add — the answer:**
- **Use `ts_exch` (exchange-stamped event time) as the bucket assignment key**, not `ts_ingest`. Exchange clocks are NTP-disciplined to microseconds; service clocks may drift by hundreds of ms.
- **Late-arriving ticks:** allow up to 5 seconds of lateness; ticks with `ts_exch` older than the current bucket boundary by > 5s go to a "late" topic for offline reconciliation, not into the live aggregate.
- **Watermarks (Flink-style):** advance the bucket-finalize watermark when min(ts_exch across active partitions) crosses the boundary. Prevents premature finalization when one partition is slow.
- **Aggregation pod clock drift:** doesn't matter for bucket assignment (event-time wins), but matters for the *finalize timer* — use a single ZK/etcd-backed coordinator for boundary-crossing decisions, or accept ±100ms slop.

**One-liner:** *"Event-time, not processing-time, drives bucket assignment. Watermarks finalize buckets. Late ticks within 5 seconds go in; older than that, they're a reconciliation problem, not a real-time problem."*

---

### Gap 8 — Schema Evolution

**Why Uber cares:** Uber runs Avro + a schema registry (Confluent). Multi-year pipelines with evolving schemas are the norm. One-line gap to close.

**What to add (one paragraph):**
Tick and aggregate schemas use **Avro with Confluent Schema Registry**. Forward + backward compatibility enforced at registry level — producers can't ship a breaking change without explicit override. Consumers reading old data get the old schema; new fields default to null. This buys multi-year operability without coordinated deploys.

---

### Gap 9 — "What Breaks at 10x Scale?"

**Why Uber cares:** This is a guaranteed Uber question. They want you to predict the next bottleneck.

**What to add — be ready to answer:**
At 10x (10M ticks/sec, 500M alerts), **the in-memory alert cache per Alert Evaluation Service pod is the first bottleneck.** Co-partitioning bounds it to one pod's tickers, but with 500M alerts and ~50 hot tickers per pod, hot-pod cache size grows to GBs.

**Mitigations:**
1. Tier the cache: hot alerts (recently evaluated, recently fired) in memory; cold alerts in Redis with 100ms lookup penalty.
2. Sub-shard within a pod: split a hot ticker's alerts across multiple eval threads with separate caches.
3. Push the heaviest filter (direction, threshold range) into a Bloom filter or sorted index, only deserialize the alert if the price could plausibly fire it.

**Second bottleneck at 10x:** Cassandra write fan-out for raw ticks. At 10M ticks/sec with RF=3, you're at 30M writes/sec, which is feasible but expensive. Move raw tick storage to a columnar format (Parquet on S3 with hourly batches) and keep Cassandra only for aggregates.

---

## Uber-Specific Reframing — Use Marketplace Analogies

Uber's interviewer is mentally mapping your design to Uber's known patterns. Make it explicit:
- *"Hot-key skew on AAPL is the same pattern as surge demand in SF on New Year's Eve — a few cells take 30% of the load."*
- *"Co-partitioned in-memory state in the alert evaluator is how dispatch services hold driver indices per city — the city is the partition key, the state is bounded, no cross-shard queries on the hot path."*
- *"Exactly-once notification is the same problem as exactly-once payment capture — Kafka transactional semantics get you between brokers, but the side-effect boundary (Twilio call, Stripe call) is where you need an idempotency key contract."*

One of these dropped naturally into a deep dive shifts the conversation from "candidate solved a stock problem" to "candidate thinks in our patterns."

---

## SDE2 vs L5 — Where the Bar Sits

| Dimension | SDE2 (L4) bar | L5 stretch | Current script |
|---|---|---|---|
| Requirements framing | Lists FRs/NFRs | Flags hard problems up front | ✅ L5 |
| Trade-off articulation | Names alternatives | Explains *why* the chosen one wins, names limits | ✅ L5 |
| Scaling reasoning | Identifies bottleneck | Predicts next bottleneck at 10x | ⚠️ Add Gap 9 |
| Failure modes | Mentions retries | Walks the failure-tree, names mitigation limits | ✅ L5 (Deep Dive 3) |
| Multi-region | Often skipped | Treated as a first-class concern | ❌ Gap 1 |
| Operational maturity | Mentions monitoring | Specifies SLIs, traces, meta-monitoring | ⚠️ Add Gap 4 |
| Capacity sizing | "Scale horizontally" | Concrete cluster sizes with reasoning | ⚠️ Add Gap 5 |
| Domain fit (Uber) | Generic answer | Marketplace-pattern analogies | ❌ Add reframing |

**Net:** Add Gaps 1, 4, 5, 9 + the marketplace reframing. The other gaps (2, 3, 6, 7, 8) are nice-to-haves to keep in the back pocket as deep-dive options.

---

## Recommended Plan of Attack

1. **Memorize the multi-region story (Gap 1).** This is the highest-probability probe and the biggest current weakness. Have a 60-second answer ready.
2. **Have the 10x answer (Gap 9) ready.** This is asked verbatim in most Uber HLD rounds.
3. **Add capacity sizing numbers (Gap 5) to the cheat sheet.** Concrete box counts during the HLD draw.
4. **Drop one marketplace analogy** in Deep Dive 1 or Deep Dive 4. Single sentence, high leverage.
5. **Keep Gaps 2, 3, 6, 7, 8 as a fifth deep-dive option** if the interviewer steers toward operations or stream-processing internals.

Doing all five flips the signal from "strong SDE2 pass" to "the panel is debating whether to push for senior."
