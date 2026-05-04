# Uber HLD — Gaps & Senior-Bar Analysis (SDE2)

> Companion to [uber_simplified.md](uber_simplified.md). The base script is ~80% of the way to a strong-hire signal. This doc lists what's missing, ordered by how likely an Uber interviewer is to probe it. Close these gaps and the signal moves from "competent SDE2" to "this person could lead a sub-team."

---

## Verdict at a glance

| Area | Current signal |
|---|---|
| Structure & delivery | Strong hire |
| Estimation & math (the 2,500x asymmetry) | Strong hire |
| Core HLD (services, DBs, queues) | Hire |
| Deep dives (H3, Dispatch, Ingestion, Sharding) | Strong hire |
| Correctness & idempotency | **Mixed — fix this** |
| Multi-region & failure handling | **Lean no-hire on Uber's bar — fix this** |
| Real-time fan-out / WebSockets at 11M connections | **Mixed — fix this** |
| Domain depth (matching ranking, ETA, surge) | Hire, not strong hire |

---

## 1. Critical gaps (almost certainly probed)

### 1.1 Idempotency — completely missing
The single biggest hole. Uber cares deeply because:
- Rider double-taps "Request" → must not create two rides.
- Driver "Accept" arrives twice (network retry) → must not double-commit the match.
- Payment capture retries → must not charge twice.

**What to add:**
- All mutating endpoints accept an `Idempotency-Key` header.
- Server stores `(key → response_hash, status_code)` in Redis with a 24h TTL.
- On replay: if key exists and request body matches, return the cached response; if body differs, return 409.
- Driver state transitions use CAS (`UPDATE rides SET state = 'MATCHED' WHERE ride_id = ? AND state = 'REQUESTED'`) — second arrival is a no-op.

One paragraph in API design + one mention in Deep Dive 2 closes this.

### 1.2 Matching ranking algorithm is too thin
The base doc says "rank by Haversine distance." That's leetcode-tier. Real Uber dispatch ranks on:
1. **ETA** (not raw distance — a driver 500m across a river is worse than 1km on the same road).
2. **Driver acceptance rate / cancellation history** — penalize drivers who routinely decline.
3. **Vehicle / ride-type compatibility** (UberX vs XL vs Black, capacity, accessibility).
4. **Driver destination preference** (heading-home filter — rare, but real).
5. **Pool/batching potential** for shared rides.
6. **Earnings fairness** — don't always pick the same top driver in a busy zone.

Even citing 3 of these in Deep Dive 2 elevates the signal. Frame it as `score = w1·(1/ETA) + w2·acceptance_rate + w3·compatibility - w4·recent_offer_count`.

### 1.3 WebSocket / real-time fan-out has no design
The base doc references WebSocket but never designs the connection layer. The math:
- 10M concurrent driver connections + 1M concurrent rider connections = **~11M persistent sockets**.
- A single node handles ~50K–100K connections → **~150–200 Push nodes**.

Probable probe topics:
- **Routing**: sticky LB by `user_id` hash (consistent hashing) so reconnects land on the same node.
- **Connection registry**: `user_id → push_node_id` in Redis; the Notification Service looks up which node owns the socket and forwards the message.
- **Backbone**: Redis pub/sub or Kafka between Notification Service and Push nodes (Uber's RAMEN system uses a similar pattern).
- **Failure**: on Push node death, clients reconnect (exponential backoff, jitter), missed events replayed from Kafka by `last_event_id`.
- **Backpressure**: drop low-priority events (location pings to passive observers) before high-priority ones (ride state changes).

Worth promoting to a 5th deep dive, or beefing up Notification Service in §5.1.

### 1.4 Multi-region failover is a footnote, not a design
The base doc *assumes* multi-region in NFRs but never designs it. Likely probe: *"A rider is mid-trip in SF and the SF region goes down. What happens?"*

**Position to take:**
- **Regional active-active**: each region owns rides where pickup falls in its geography. `ride_id` carries an embedded region prefix → routing layer dispatches to home region.
- **Postgres**: shards are primary in home region with **async cross-region replication** to a standby region. Documented RPO ~30s, RTO ~2min via promote-replica + DNS failover.
- **Redis (location)**: regional, not replicated. On region failure, drivers in that region re-emit within 4s into the takeover region.
- **Kafka**: MirrorMaker2 for cross-region for `ride.events` (analytics/audit), not for `driver.location.updates` (too high volume).
- **Trade-off explicitly stated**: we accept ~30s of in-flight ride writes lost on a region death — this is preferable to the latency cost of synchronous cross-region commits.

You don't need to fully solve this. You need a *coherent position* and a stated trade-off.

### 1.5 Ride state machine — declared but not designed
States are listed but transitions, invariants, and failure modes are missing.

**Add a transition table:**

| From | Action | To | Actor | Validation |
|---|---|---|---|---|
| (none) | `CREATE` | `REQUESTED` | Rider | rider has no active ride |
| `REQUESTED` | `MATCH` | `MATCHED` | Dispatch | driver atomically reserved |
| `REQUESTED` | `CANCEL` | `CANCELLED` | Rider | no fee |
| `MATCHED` | `START_PICKUP` | `EN_ROUTE_TO_PICKUP` | Driver | assigned driver only |
| `MATCHED` | `CANCEL` | `CANCELLED` | Either | rider fee if > 2min |
| `EN_ROUTE_TO_PICKUP` | `BEGIN_TRIP` | `IN_TRIP` | Driver | within 100m of pickup |
| `IN_TRIP` | `COMPLETE` | `COMPLETED` | Driver | within 100m of dropoff (or override) |

**Implementation invariants:**
- Every transition is a **CAS update**: `UPDATE rides SET state = ?, updated_at = now() WHERE ride_id = ? AND state = ?`. Second arrival is a no-op (idempotent).
- Invalid transitions return 409 Conflict with the current state.
- Cancellation race (rider cancels in the 50ms window between driver accept and Trip Service commit): rider's `CANCEL` arrives → CAS expects `REQUESTED` but finds `MATCHED` → returns 409 with current state → rider app shows "cancellation fee may apply" and reissues as a post-match cancellation.

---

## 2. Significant gaps (likely probed)

### 2.1 Payment integration is one bullet
"Thin payments integration" hand-waves real complexity. At minimum cover:
- **Pre-auth at ride request** for the fare-estimate high bound; **capture at trip completion** for the actual fare. Industry standard.
- **Failed pre-auth** → ride request rejected before matching starts ("payment method declined").
- **Failed capture at completion** → ride completes, debt added to user account, blocks next ride request until resolved.
- **Idempotent capture** keyed on `ride_id` so payment retries don't double-charge.
- **Refunds & adjustments** flow asynchronously through `ride.events` → Payment Service.

3–4 lines is enough.

### 2.2 ETA service is invisible
The base doc references `eta_to_pickup` and `eta_seconds` but never designs the ETA service. At minimum:
- `ETA = f(road_network, current_traffic, historical_patterns_by_time_of_day, weather)`.
- Map/routing layer: Mapbox / Google Maps SDK, or in-house OSRM-derived service.
- **Caching**: `(origin_h3, dest_h3, hour_of_week) → average_travel_time_seconds`, refreshed every ~5 min from observed trip data flowing through `ride.events`.
- ML upgrade path: Uber's deepETA replaces the heuristic with a learned residual on top of the routing engine's estimate.

### 2.3 Surge pricing is one row in a table
The base doc handles the *follow-up question* about feedback loops well but never designs the surge mechanism itself.
- **Streaming job** (Flink/Spark Streaming) consumes `ride.events` + `driver.online` events.
- Per-H3-cell window: `unmet_demand_count / available_supply_count` over rolling 60s.
- Threshold table maps ratio → multiplier (1.0x, 1.2x, 1.5x, 2.0x, 3.0x).
- Output: `surge_multiplier:{h3_cell}` in Redis with 60s TTL, read by Pricing Service on every fare estimate.
- **Damping**: EWMA over 3-min window + hysteresis (don't drop multiplier unless ratio falls below `threshold * 0.8`) to avoid oscillation.

### 2.4 Driver supply / "no driver found" path
What happens when matching exhausts candidates?
- Initial search: `kRing(pickup_cell, k=2)` ≈ 2km.
- On exhaustion: expand to `k=3`, then `k=4`. Hard cap at ~5km for UberX.
- Total dispatch budget: 30–60s before failing back to rider with "no drivers available — try again."
- Failed-match ride record is kept (state = `EXPIRED`) for analytics — supply gaps drive surge and driver-incentive pushes.

### 2.5 Authentication / authorization
Zero mention in the base doc.
- **Edge**: OAuth2 / JWT at API Gateway. Claims include `user_id`, `role` (rider/driver), `device_id`.
- **Service-to-service**: mTLS with SPIFFE-style identities, or signed internal JWTs.
- **Authorization check** in Trip Service: a driver can only `PATCH /rides/{ride_id}` where `ride.driver_id == jwt.user_id`.
- **Location ingest**: gateway validates the `driver_id` in the stream matches the JWT — prevents driver A spoofing driver B's position.

---

## 3. Polish items (smaller, but earn points)

- **PACELC framing**: explicitly state "ride state is CP, location is AP." Vocabulary precision = senior signal.
- **Capacity numbers**: ~16 Postgres shards (8 primary + 8 replicas), ~32 Redis nodes for the geo index, ~1,000 Kafka partitions on `driver.location.updates`. Be ready if asked.
- **Driver-app local buffering** on disconnect: belongs in Deep Dive 3, not just the wrap-up follow-up.
- **PII / data privacy**: location data is extremely sensitive. One line on encryption-at-rest, encryption-in-transit, audit logs, and a retention policy (e.g., raw location ticks deleted after 90 days; sampled trip polylines kept indefinitely with rider consent).
- **Cold start / disaster recovery**: Redis flush recovery = drivers re-emit within 4s + Kafka replay of last 30s. RTO < 60s. Worth stating.
- **Observability / SLOs**: name the primary SLO ("p99 match latency < 5s, error budget 0.01% = ~4.3min/month"), and 3–4 key dashboards (Kafka consumer lag, Redis hit rate, shard balance, dispatch success rate).
- **Rate limiting**: per-user (e.g., 10 ride requests/min/user), per-IP, and per-endpoint at gateway. Token bucket in Redis.

---

## 4. What Uber specifically looks for in an SDE2

Beyond raw technical depth, the signals Uber interviewers (especially L4/L5 panels) weight heavily:

1. **Domain reasoning over pattern-matching.** Derive "we need a separate location pipeline" from the *2,500x number* — not because you read a blog. ✅ Base doc does this well.
2. **Correctness instinct.** Idempotency, exactly-once dispatch, state-machine integrity. ⚠️ Base doc nails double-dispatch but misses idempotency.
3. **Operational maturity.** What breaks in prod, blast radius, observability, rollback, multi-region. ⚠️ Light in base doc.
4. **Product empathy.** Driver experience as a constraint (the dispatch deep dive does this — good), rider trust during edge cases.
5. **Honest trade-offs.** Don't claim everything is great. "We accept ~1–2s staleness" framing is exactly right. ✅
6. **Familiarity with Uber's actual stack** is a bonus, not required: H3 (already cited ✅), Schemaless, Ringpop, RAMEN, Cadence/Temporal, M3. Dropping one or two earns a small bump.

---

## 5. Priority order if you have limited prep time

If you have ~3 hours left:

1. **Hour 1** — Idempotency (§1.1) + State machine transition table (§1.5). Highest ROI.
2. **Hour 2** — Matching ranking algorithm (§1.2) + Multi-region failover position (§1.4).
3. **Hour 3** — WebSocket fan-out as a 5th deep dive (§1.3) + Surge mechanism (§2.3).

Skip §3 polish items unless §1 and §2 are tight.

---

## 6. The two anchor sentences (already in base doc — keep them)

> *"The headline number is 2.5M writes/sec on locations vs. 1K rides/sec — that's a 2,500x asymmetry, and it shapes most of the architectural choices coming up."*
>
> *"The single hardest correctness property here is no-double-dispatch — one driver, one offer at a time. Everything else is performance; this is correctness, and I want to spend a few minutes on exactly how we guarantee it."*

Add a third for idempotency:

> *"Every mutating action — request a ride, accept an offer, capture payment — is idempotent on a client-supplied key. Network retries are the common case at this scale, and the design has to assume duplicates rather than try to prevent them."*

That third sentence, dropped during API design, single-handedly closes the largest gap in the current doc.
