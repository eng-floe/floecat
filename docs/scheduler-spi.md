# Scheduler Policy SPI

This document explains how the stats scheduler works, how to tune it through
configuration, and how to write a custom scheduler profile as a CDI bean.

## What the scheduler does

Every async stats capture job — whether triggered by a user batch request, a
new snapshot, or a repair follow-up — passes through the scheduler before it
lands in the reconcile job queue. The scheduler decides:

1. **Priority class** — which of the four dispatch buckets the job belongs to
2. **Score** — tie-breaking order within a priority class (higher score dispatched first)
3. **Lane** — which fairness lane the job belongs to (WRR across lanes)
4. **Admission** — whether to admit, defer, or reject the job given current queue health

Synchronous P0 jobs bypass the scheduler entirely; they are always enqueued at
`P0_SYNC` directly by `StatsOrchestrator` and cannot be intercepted by a policy.

## Priority classes

| Class | Ordinal | Meaning |
|---|---|---|
| `P0_SYNC` | 0 | Query-time synchronous stats. Hardcoded in `StatsOrchestrator`. Policy never sees these. |
| `P1_FRESHNESS` | 1 | New-snapshot indexing. Must be dispatched before background work. Always admitted. |
| `P2_REPAIR` | 2 | Async follow-up after a partial or failed sync capture. |
| `P3_BACKGROUND` | 3 | Routine refresh and maintenance. May be deferred under queue pressure. |

## Health bands and admission

The store tracks a health band (`GREEN` → `YELLOW` → `ORANGE` → `RED`) based on
queue depths and P0 starvation. The band influences admission:

| Band | P0 | P1 | P2 | P3 |
|---|---|---|---|---|
| GREEN | ✅ | ✅ | ✅ | ✅ |
| YELLOW | ✅ | ✅ | ✅ | 50% admit |
| ORANGE | ✅ | ✅ | ✅ | Deferred |
| RED | ✅ | ✅ | Deferred | Deferred |

The `reconcile.health_band` metric (value = band ordinal) is the recommended
autoscaler trigger. At `ORANGE` or above, scale out executor capacity.

## The three policy interfaces

All three live in `core/stats/src/main/java/ai/floedb/floecat/stats/spi/scheduler/`.

### `SchedulerPriorityPolicy`

Called at enqueue time for every async job. Returns a `PriorityAssignment`
carrying the priority class, score, and lane key.

```java
public interface SchedulerPriorityPolicy {

    PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context);

    // Called for PLAN_SNAPSHOT and EXEC_FILE_GROUP reconcile jobs (not stats-orchestrator jobs).
    // Default implementation: P1_FRESHNESS when isNewSnapshot=true, P3_BACKGROUND otherwise.
    default PriorityAssignment assignForReconcileJob(
        ReconcileJobKind kind, String tableId, long snapshotId,
        boolean isNewSnapshot, SchedulerContext context) { ... }

    record PriorityAssignment(StatsPriorityClass priorityClass, long score, String laneKey) {}
}
```

**Rules:**
- Must never return `P0_SYNC`. The registry validates this at boot and throws if violated.
- Must not perform blocking I/O. The method is called on the enqueue hot path.

### `SchedulerAdmissionPolicy`

Called immediately after priority assignment. Returns `ADMIT`, `DEFER`, or
`REJECT`.

```java
public interface SchedulerAdmissionPolicy {

    AdmissionDecision decide(PriorityAssignment assignment, SchedulerHealthBand currentBand);

    enum AdmissionDecision { ADMIT, DEFER, REJECT }
}
```

**Rules:**
- Must return `ADMIT` for `P0_SYNC` and `P1_FRESHNESS` in every health band.
  The registry validates both at boot and throws if violated.
- `DEFER` causes the job to be enqueued with a delayed `nextAttemptAtMs`. The
  store adds its own band-based deferral on top; the effective delay is the max
  of the two.
- `REJECT` discards the job entirely and returns a degraded result to the caller.
  Never use `REJECT` for P0 or P1.
- Must not perform blocking I/O.

### `SchedulerPreemptionPolicy`

Selects a victim `EXEC_FILE_GROUP` job to cancel when a high-priority job
cannot be dispatched because all workers are busy.

```java
public interface SchedulerPreemptionPolicy {

    Optional<String> selectVictim(
        String incomingJobId,
        List<RunningJobInfo> candidates,
        SchedulerContext context);

    record RunningJobInfo(
        String jobId, StatsPriorityClass priorityClass,
        long startedAtMs, int completedFiles, int totalFiles) {}
}
```

**Rules:**
- Must never return a job with `priorityClass == P0_SYNC`. The registry
  validates this at boot.
- Return `Optional.empty()` if no suitable victim exists.
- Preemption is gated by `floecat.stats.scheduler.preemption.enabled` (default:
  `false`). This interface is defined for forward compatibility; the dispatch
  loop does not call it until the flag is set.

### `SchedulerContext`

Read-only view of scheduler state passed to all policy calls.

```java
public interface SchedulerContext {
    SchedulerHealthBand currentBand();
    Map<StatsPriorityClass, Long> queueDepthByClass();
    OptionalLong lastSuccessfulCaptureMs(String tableId);  // for age scoring
    CoverageLevel coverageLevel(String tableId, long snapshotId);  // NONE/PARTIAL/FULL
    OptionalLong snapshotDeltaRows(String tableId, long snapshotId);
    long recentPlannerRequestCount(String tableId);
    long recentColumnRequestCount(String tableId, String normalizedColumnSelector);
}
```

`lastSuccessfulCaptureMs`, `coverageLevel`, and `snapshotDeltaRows` are backed
by the in-process `SchedulerSignalIndex` populated by finalize/planner paths.
Custom profiles should still tolerate empty returns for cold-start or partial
signal-population windows.

---

## Tuning without writing code

For most deployments, tuning the default profile via configuration is sufficient.

```properties
# Active profile name. Changing this requires a restart.
floecat.stats.scheduler.profile=default

# Scoring factor weights (default profile only).
# Higher weight → that factor dominates within-class ordering.
floecat.stats.scheduler.scoring.weight.coverage=3
floecat.stats.scheduler.scoring.weight.delta=2
floecat.stats.scheduler.scoring.weight.age=1

# Age at which the age factor saturates at its maximum contribution (default: 24h).
floecat.stats.scheduler.scoring.max-age-ms=86400000

# Preemption (disabled by default — enable only after soak testing).
floecat.stats.scheduler.preemption.enabled=false
```

The default profile's admission logic:
- P0 and P1: always `ADMIT`
- P2: `DEFER` only in `RED`
- P3: `DEFER` in `ORANGE` and `RED`; `YELLOW` is handled probabilistically by
  the store's own deferral layer

---

## Writing a custom profile

A custom profile is a CDI bean that implements one or more of the three
interfaces and carries `@SchedulerProfile(name = "my-profile")`.

### Step 1 — Create the bean

```java
package com.example.floecat;

import ai.floedb.floecat.stats.spi.scheduler.SchedulerAdmissionPolicy;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerContext;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerPreemptionPolicy;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerPriorityPolicy;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerProfile;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
@SchedulerProfile(name = "latency-first")
public class LatencyFirstSchedulerProfile
    implements SchedulerPriorityPolicy, SchedulerAdmissionPolicy, SchedulerPreemptionPolicy {

    // --- SchedulerPriorityPolicy ---

    @Override
    public PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context) {
        // Always prefer freshness over background work.
        StatsPriorityClass cls = request.columnSelectors().isEmpty()
            ? StatsPriorityClass.P3_BACKGROUND
            : StatsPriorityClass.P1_FRESHNESS;
        String laneKey = request.tableId().getAccountId() + ":" + request.tableId().getId();
        return new PriorityAssignment(cls, 0L, laneKey);
    }

    // --- SchedulerAdmissionPolicy ---

    @Override
    public AdmissionDecision decide(PriorityAssignment assignment, SchedulerHealthBand band) {
        // P0 and P1 MUST always be ADMIT — the registry validates this at boot.
        if (assignment.priorityClass().order <= StatsPriorityClass.P1_FRESHNESS.order) {
            return AdmissionDecision.ADMIT;
        }
        // Defer all background work under any non-GREEN band.
        if (band != SchedulerHealthBand.GREEN && assignment.priorityClass() == StatsPriorityClass.P3_BACKGROUND) {
            return AdmissionDecision.DEFER;
        }
        return AdmissionDecision.ADMIT;
    }

    // --- SchedulerPreemptionPolicy ---

    @Override
    public Optional<String> selectVictim(
        String incomingJobId, List<RunningJobInfo> candidates, SchedulerContext context) {
        // Most recently started job has the least completed work — cheapest to redo.
        return candidates.stream()
            .filter(c -> c.priorityClass() != StatsPriorityClass.P0_SYNC)
            .max(java.util.Comparator.comparingLong(RunningJobInfo::startedAtMs))
            .map(RunningJobInfo::jobId);
    }
}
```

You do not need to implement all three interfaces. The registry falls back to
the default profile's implementation for any interface your bean does not cover.
If you only want to customize admission, implement only `SchedulerAdmissionPolicy`.

### Step 2 — Register your JAR

Put the bean in a JAR on the Quarkus classpath. Quarkus CDI will discover it
automatically via bean discovery. No `beans.xml` descriptor is needed for
`@ApplicationScoped` beans.

### Step 3 — Activate the profile

Set the config key to match the name in your `@SchedulerProfile` annotation:

```properties
floecat.stats.scheduler.profile=latency-first
```

A restart is required. On startup the registry will:
1. Find all beans annotated `@SchedulerProfile(name = "latency-first")`.
2. Wire the first matching bean for each interface.
3. Validate the three safety invariants. If any fails, startup aborts with a
   clear error message before the service accepts traffic.

### Step 4 — Write tests using `TestSchedulerPriorityPolicy`

`TestSchedulerPriorityPolicy` (in `core/stats`, package
`ai.floedb.floecat.stats.spi.testing`) provides a simple deterministic policy
for unit tests that does not require CDI:

```java
var policy = TestSchedulerPriorityPolicy.alwaysP1();
PriorityAssignment a = policy.assign(request, mockContext);
assertEquals(StatsPriorityClass.P1_FRESHNESS, a.priorityClass());
```

---

## Boot-time invariant validation

The registry validates these invariants before the application accepts traffic.
Violations throw `IllegalStateException` with a descriptive message.

| Invariant | What is checked |
|---|---|
| P0 always admitted | `admissionPolicy.decide(P0_SYNC_assignment, band)` returns `ADMIT` for all four bands |
| P1 always admitted | `admissionPolicy.decide(P1_FRESHNESS_assignment, band)` returns `ADMIT` for all four bands |
| No P0 preemption victim | `preemptionPolicy.selectVictim(...)` never returns a P0 job from a synthetic candidate list |
| No P0 assignment | `priorityPolicy.assign(...)` never returns `PriorityAssignment(P0_SYNC, ...)` |

---

## Metrics emitted by the scheduler

All metrics are defined in `ServiceMetrics.Reconcile`.

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `reconcile.queue.depth_by_class` | GAUGE | `priority_class` | Ready jobs per class |
| `reconcile.health_band` | GAUGE | — | Current band ordinal (0=GREEN … 3=RED). **Autoscaler trigger** |
| `reconcile.admission.deferred` | COUNTER | `priority_class`, `reason` | Jobs deferred by policy or band |
| `reconcile.admission.rejected` | COUNTER | `priority_class`, `reason` | Jobs rejected by policy |
| `reconcile.aging.promotions` | COUNTER | `from_class` | Jobs promoted to a higher class after waiting |
| `reconcile.lane.wait_ms` | GAUGE | `lane_key` | Wait time for the top-10 busiest lanes |
| `reconcile.scoring.score_dist` | SUMMARY | `priority_class` | Score distribution at enqueue |
| `reconcile.policy.profile` | GAUGE (info) | `profile_name` | Active profile name, value=1 |

---

## Module boundaries

| Layer | Module | What lives here |
|---|---|---|
| SPI contracts | `core/stats` | `SchedulerPriorityPolicy`, `SchedulerAdmissionPolicy`, `SchedulerPreemptionPolicy`, `SchedulerContext`, `@SchedulerProfile` |
| Test support | `core/stats` | `TestSchedulerPriorityPolicy` |
| Default implementation | `service` | `DefaultSchedulerProfile` |
| Registry and wiring | `service` | `SchedulerPolicyRegistry` |
| Store integration | `reconciler`, `service` | `SchedulerStoreHelpers`, `SchedulerBandState`, `AgingPromotionTracker` |

Custom profiles only need to depend on `core/stats`. They do not need to
reference anything from the `service` or `reconciler` modules.
