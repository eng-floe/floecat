# Stats Sync-First Resolution Policy

`StatsOrchestrator.resolve()` follows a sync-first policy: on a store miss it attempts a
bounded synchronous capture before falling back to an async reconcile job. This lets queries
receive fresh statistics within a latency budget rather than always degrading to no-stats-available
on a cold cache.

## Resolution flow

```
resolve(StatsCaptureRequest)
   │
   ├─ store read ──────────────────────────────────────── HIT  (stats returned, no capture)
   │
   └─ miss
       │
       ├─ executionMode == SYNC
       │  && latencyBudget present
       │  && floecat.stats.sync.enabled == true
       │      │
       │      ├─ enqueue CAPTURE_ONLY reconcile job
       │      │   poll every 100 ms until terminal or budget exceeded
       │      │      │
       │      │      ├─ job succeeded → re-read store
       │      │      │       ├─ record present ───────── CAPTURED (stats returned, no follow-up)
       │      │      │       └─ record absent ─────────  PARTIAL  (async follow-up enqueued)
       │      │      │
       │      │      ├─ budget exceeded ──────────────── TIMEOUT  (async follow-up enqueued)
       │      │      └─ job failed / no connector ────── FAILED   (async follow-up enqueued)
       │
       └─ otherwise (ASYNC mode / no budget / sync disabled)
               └────────────────────────────────────── SKIPPED  (async job enqueued)
```

## Outcome reference

| Outcome    | Stats present | Async follow-up | When                                              |
|------------|:-------------:|:---------------:|---------------------------------------------------|
| `HIT`      | yes           | no              | Record already in store                           |
| `CAPTURED` | yes           | no              | Sync capture succeeded; record now in store       |
| `PARTIAL`  | no            | yes             | Sync capture succeeded but record not yet visible |
| `TIMEOUT`  | no            | yes             | Sync capture exceeded latency budget              |
| `FAILED`   | no            | yes             | Sync capture error or no upstream connector       |
| `SKIPPED`  | no            | yes             | ASYNC mode, no budget, or sync disabled           |

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `floecat.stats.sync.enabled` | `true` | Master switch for sync-first resolution |
| `floecat.stats.sync.latency-budget` | `1s` | Per-request sync capture budget |
| `floecat.stats.sync.max-latency-budget` | `10s` | Hard ceiling applied to any request budget |

Environment variable overrides follow the pattern `FLOECAT_STATS_SYNC_ENABLED`,
`FLOECAT_STATS_SYNC_LATENCY_BUDGET`, `FLOECAT_STATS_SYNC_MAX_LATENCY_BUDGET`.

## Planner integration

`StatsProviderFactory` reads `floecat.stats.sync.enabled` and `floecat.stats.sync.latency-budget`
at startup and sets `executionMode(SYNC)` + `latencyBudget(...)` on every
`StatsCaptureRequest` it creates for query-time resolution. Set `enabled=false` to revert to
async-only for all queries without changing other behavior.

`PlannerStatsBundleService` maps `StatsResolutionResult` outcomes to planner quality:

- `HIT` / `CAPTURED` → `FOUND`
- `TIMEOUT` / `FAILED` / `SKIPPED` → `NOT_FOUND` (planner proceeds without stats)

## Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `floecat.service.stats.sync_outcomes.total` | counter | `result`, `component`, `operation` | Count per sync outcome |
| `floecat.service.stats.sync.latency` | timer (ms) | `result`, `component`, `operation` | End-to-end resolve latency |

`result` values match `StatsSyncOutcome` names: `HIT`, `CAPTURED`, `PARTIAL`, `TIMEOUT`,
`FAILED`, `SKIPPED`.

## Troubleshooting

**High TIMEOUT rate**

The most common cause is the capture budget being shorter than actual connector round-trip time.
Check `floecat.service.stats.sync.latency` (p95) against `floecat.stats.sync.latency-budget`.
Increase the budget (up to `max-latency-budget`) or set `floecat.stats.sync.enabled=false` to
stop blocking queries on sync attempts.

**FAILED outcomes with no connector**

Tables without an upstream connector cannot be captured synchronously. The orchestrator returns
`FAILED` immediately and enqueues an async follow-up. This is expected for system tables and
non-connected tables.

**Disabling sync globally**

Set `floecat.stats.sync.enabled=false` (or `FLOECAT_STATS_SYNC_ENABLED=false`). All queries
fall back to `SKIPPED` + async enqueue, which matches the pre-sync-policy behavior.

**Verifying counters**

Query `/q/metrics` after triggering a stats miss. Look for lines containing
`floecat_service_stats_sync_outcomes_total`.
