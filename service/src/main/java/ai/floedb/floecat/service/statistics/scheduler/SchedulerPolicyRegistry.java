/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Inject;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * CDI registry that resolves and validates the active scheduler policy profile at application
 * startup.
 *
 * <h2>Profile resolution</h2>
 *
 * <p>The active profile name is read from {@code floecat.stats.scheduler.profile} (default: {@code
 * "default"}). All CDI beans annotated with {@link SchedulerProfile} are iterated; the first bean
 * whose {@link SchedulerProfile#name()} matches the configured name is selected for each policy
 * interface. If no match is found, startup fails with an {@link IllegalStateException}.
 *
 * <h2>Boot-time invariant validation</h2>
 *
 * <p>Three safety invariants are validated at startup. Violation causes an {@link
 * IllegalStateException} before the application accepts traffic:
 *
 * <ol>
 *   <li>The active {@link SchedulerAdmissionPolicy} must return {@link
 *       SchedulerAdmissionPolicy.AdmissionDecision#ADMIT} for both P0_SYNC and P1_FRESHNESS
 *       assignments in every health band. P1 freshness jobs feed new-snapshot indexing latency and
 *       must never be dropped.
 *   <li>The active {@link SchedulerPreemptionPolicy} (if present) must never return a {@link
 *       StatsPriorityClass#P0_SYNC} job from a synthetic candidate list.
 *   <li>The active {@link SchedulerPriorityPolicy} must never return a {@link PriorityAssignment}
 *       with class {@link StatsPriorityClass#P0_SYNC} for an async request.
 * </ol>
 */
@ApplicationScoped
public class SchedulerPolicyRegistry {

  private static final Logger LOG = Logger.getLogger(SchedulerPolicyRegistry.class);

  private static final Tag COMPONENT = Tag.of(TagKey.COMPONENT, "service");
  private static final Tag OPERATION = Tag.of(TagKey.OPERATION, "scheduler");

  private final String profileName;
  private final Instance<SchedulerPriorityPolicy> priorityPolicies;
  private final Instance<SchedulerAdmissionPolicy> admissionPolicies;
  private final Instance<SchedulerPreemptionPolicy> preemptionPolicies;
  private final ReconcileJobStore jobs;
  private final Observability observability;

  private SchedulerPriorityPolicy activePriorityPolicy;
  private SchedulerAdmissionPolicy activeAdmissionPolicy;
  // null if no bean with the active profile implements SchedulerPreemptionPolicy
  private SchedulerPreemptionPolicy activePreemptionPolicy;

  private SchedulerContext context;

  // Held as a field so the gauge supplier is strongly reachable for the lifetime of this
  // ApplicationScoped bean. A local variable would be eligible for GC after init() returns,
  // which could silently drop the gauge in frameworks that hold supplier references weakly.
  private final AtomicLong profileGaugeValue = new AtomicLong(0L);

  @Inject
  SchedulerPolicyRegistry(
      @ConfigProperty(name = "floecat.stats.scheduler.profile", defaultValue = "default")
          String profileName,
      @Any Instance<SchedulerPriorityPolicy> priorityPolicies,
      @Any Instance<SchedulerAdmissionPolicy> admissionPolicies,
      @Any Instance<SchedulerPreemptionPolicy> preemptionPolicies,
      ReconcileJobStore jobs,
      Observability observability) {
    this.profileName = profileName;
    this.priorityPolicies = priorityPolicies;
    this.admissionPolicies = admissionPolicies;
    this.preemptionPolicies = preemptionPolicies;
    this.jobs = jobs;
    this.observability = observability;
  }

  @PostConstruct
  void init() {
    SchedulerProfileLiteral qualifier = new SchedulerProfileLiteral(profileName);

    Instance<SchedulerPriorityPolicy> selectedPriority = priorityPolicies.select(qualifier);
    if (selectedPriority.isUnsatisfied()) {
      throw new IllegalStateException(
          "No SchedulerPriorityPolicy found for scheduler profile '"
              + profileName
              + "'. Check that a bean annotated @SchedulerProfile(name=\""
              + profileName
              + "\") exists on the classpath.");
    }
    activePriorityPolicy = selectedPriority.get();

    Instance<SchedulerAdmissionPolicy> selectedAdmission = admissionPolicies.select(qualifier);
    if (selectedAdmission.isUnsatisfied()) {
      throw new IllegalStateException(
          "No SchedulerAdmissionPolicy found for scheduler profile '"
              + profileName
              + "'. Check that a bean annotated @SchedulerProfile(name=\""
              + profileName
              + "\") exists on the classpath.");
    }
    activeAdmissionPolicy = selectedAdmission.get();

    Instance<SchedulerPreemptionPolicy> selectedPreemption = preemptionPolicies.select(qualifier);
    activePreemptionPolicy = selectedPreemption.isUnsatisfied() ? null : selectedPreemption.get();

    context = new ReconcileJobStoreContext(jobs);

    validateInvariants();

    // Emit POLICY_PROFILE info gauge: value always 1, profile_name tag identifies the profile.
    // Dashboards use this to annotate other scheduler metrics with the active profile.
    profileGaugeValue.set(1L);
    observability.gauge(
        ServiceMetrics.Reconcile.POLICY_PROFILE,
        profileGaugeValue::get,
        "Active scheduler policy profile (value=1; profile_name tag identifies the profile)",
        COMPONENT,
        OPERATION,
        Tag.of("profile_name", profileName));

    LOG.infof(
        "Scheduler policy profile '%s' loaded: priority=%s, admission=%s, preemption=%s",
        profileName,
        activePriorityPolicy.getClass().getSimpleName(),
        activeAdmissionPolicy.getClass().getSimpleName(),
        activePreemptionPolicy == null
            ? "none"
            : activePreemptionPolicy.getClass().getSimpleName());
  }

  // ---------------------------------------------------------------------------
  // Public accessors
  // ---------------------------------------------------------------------------

  /** Returns the active priority assignment policy. */
  public SchedulerPriorityPolicy activePriorityPolicy() {
    return activePriorityPolicy;
  }

  /** Returns the active admission policy. */
  public SchedulerAdmissionPolicy activeAdmissionPolicy() {
    return activeAdmissionPolicy;
  }

  /**
   * Returns the active preemption policy, or {@code null} if no preemption policy is configured for
   * the current profile.
   *
   * <p><b>Phase 4 (not yet wired):</b> this accessor exists so the dispatch path can call it once
   * preemption is integrated into {@code leaseNext()}. Until then the returned policy is validated
   * at boot but is never invoked at runtime. The feature is gated by {@code
   * floecat.stats.scheduler.preemption.enabled} (default {@code false}).
   */
  public SchedulerPreemptionPolicy activePreemptionPolicy() {
    return activePreemptionPolicy;
  }

  /**
   * Returns the scheduler context backed by the live job store.
   *
   * <p>Queue band and depth are derived from a short-lived {@link ReconcileJobStore#queueStats()}
   * snapshot (TTL: {@code ReconcileJobStoreContext.SNAPSHOT_TTL_MS} ms) so that multiple reads
   * within a single policy invocation see consistent data. Coverage, delta, and last-capture values
   * return conservative defaults until those data sources are wired.
   */
  public SchedulerContext activeContext() {
    return context;
  }

  // ---------------------------------------------------------------------------
  // Boot-time invariant validation
  // ---------------------------------------------------------------------------

  private void validateInvariants() {
    validateAdmissionP0Invariant(activeAdmissionPolicy, context);
    validatePreemptionP0Invariant(activePreemptionPolicy, context);
    validatePriorityP0Invariant(activePriorityPolicy, context);
  }

  /**
   * Validates that {@code policy} always returns {@link
   * SchedulerAdmissionPolicy.AdmissionDecision#ADMIT} for both P0_SYNC and P1_FRESHNESS across all
   * health bands.
   *
   * <p>Package-private to allow direct testing without CDI.
   */
  static void validateAdmissionP0Invariant(SchedulerAdmissionPolicy policy, SchedulerContext ctx) {
    for (StatsPriorityClass guardedClass :
        new StatsPriorityClass[] {StatsPriorityClass.P0_SYNC, StatsPriorityClass.P1_FRESHNESS}) {
      var assignment = new SchedulerPriorityPolicy.PriorityAssignment(guardedClass, 0L, "");
      for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
        var decision = policy.decide(assignment, band);
        if (decision != SchedulerAdmissionPolicy.AdmissionDecision.ADMIT) {
          throw new IllegalStateException(
              "Scheduler invariant violated: admission policy '"
                  + policy.getClass().getName()
                  + "' returned "
                  + decision
                  + " for "
                  + guardedClass
                  + " in band "
                  + band
                  + ". "
                  + guardedClass
                  + " must always be ADMIT.");
        }
      }
    }
  }

  /**
   * Validates that {@code policy} never selects a P0_SYNC job as a preemption victim. Does nothing
   * when {@code policy} is null (no preemption configured for the profile).
   *
   * <p>Package-private to allow direct testing without CDI.
   */
  static void validatePreemptionP0Invariant(
      SchedulerPreemptionPolicy policy, SchedulerContext ctx) {
    if (policy == null) {
      return;
    }
    var p0Candidate =
        new SchedulerPreemptionPolicy.RunningJobInfo(
            "synthetic-p0-job", StatsPriorityClass.P0_SYNC, System.currentTimeMillis(), 0, 10);
    var victim = policy.selectVictim("incoming", List.of(p0Candidate), ctx);
    if (victim.isPresent()) {
      throw new IllegalStateException(
          "Scheduler invariant violated: preemption policy '"
              + policy.getClass().getName()
              + "' selected a P0_SYNC job as a preemption victim. P0 jobs must never be"
              + " preempted.");
    }
  }

  /**
   * Validates that {@code policy} never returns P0_SYNC from either {@link
   * SchedulerPriorityPolicy#assign} or {@link SchedulerPriorityPolicy#assignForReconcileJob}.
   *
   * <p>Package-private to allow direct testing without CDI.
   */
  static void validatePriorityP0Invariant(SchedulerPriorityPolicy policy, SchedulerContext ctx) {
    // Validate assign() — the stats-orchestrator enqueue path.
    StatsCaptureRequest syntheticRequest =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("synthetic").setId("synthetic-tbl").build(),
                0L,
                StatsTarget.getDefaultInstance())
            .build();
    var asyncAssignment = policy.assign(syntheticRequest, ctx);
    if (asyncAssignment.priorityClass() == StatsPriorityClass.P0_SYNC) {
      throw new IllegalStateException(
          "Scheduler invariant violated: priority policy '"
              + policy.getClass().getName()
              + "' returned P0_SYNC from assign() for an async stats request."
              + " P0_SYNC may only be assigned by the stats orchestrator.");
    }

    // Also validate assignForReconcileJob() — the reconciler-side enqueue path.
    for (ai.floedb.floecat.reconciler.jobs.ReconcileJobKind kind :
        ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.values()) {
      var assignment = policy.assignForReconcileJob(kind, "tbl-synthetic", 0L, false, ctx);
      if (assignment.priorityClass() == StatsPriorityClass.P0_SYNC) {
        throw new IllegalStateException(
            "Scheduler invariant violated: priority policy '"
                + policy.getClass().getName()
                + "' returned P0_SYNC for a reconcile job of kind "
                + kind
                + ". P0_SYNC may only be assigned by the stats orchestrator.");
      }
      var newSnapshotAssignment =
          policy.assignForReconcileJob(kind, "tbl-synthetic", 1L, true, ctx);
      if (newSnapshotAssignment.priorityClass() == StatsPriorityClass.P0_SYNC) {
        throw new IllegalStateException(
            "Scheduler invariant violated: priority policy '"
                + policy.getClass().getName()
                + "' returned P0_SYNC for a new-snapshot reconcile job of kind "
                + kind
                + ". P0_SYNC may only be assigned by the stats orchestrator.");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // SchedulerContext adapter over ReconcileJobStore
  // ---------------------------------------------------------------------------

  /**
   * Lightweight {@link SchedulerContext} backed by the live {@link ReconcileJobStore}.
   *
   * <p>{@link #currentBand()} and {@link #queueDepthByClass()} share a single {@link
   * ReconcileJobStore#queueStats()} snapshot cached for {@link #SNAPSHOT_TTL_MS} milliseconds.
   * Policy methods that call both in the same decision therefore see consistent band and depth
   * values. Coverage, delta, and last-capture data are not yet tracked at this layer and return
   * conservative defaults: coverage returns {@link CoverageLevel#NONE} and delta / last-capture
   * return empty. These will be wired with real data sources in a future update.
   */
  private static final class ReconcileJobStoreContext implements SchedulerContext {

    /** How long a QueueStats snapshot is reused before the next call refreshes it (ms). */
    private static final long SNAPSHOT_TTL_MS = 100L;

    private final ReconcileJobStore jobs;
    private volatile ReconcileJobStore.QueueStats cachedStats;
    private volatile long cachedAtMs;

    ReconcileJobStoreContext(ReconcileJobStore jobs) {
      this.jobs = jobs;
    }

    /**
     * Returns a recent {@link ReconcileJobStore.QueueStats} snapshot, refreshing at most once per
     * {@link #SNAPSHOT_TTL_MS}. The small data race on the two volatile fields is intentional: in
     * the worst case a caller sees a stale snapshot one extra cycle, which is harmless for
     * scheduling decisions.
     */
    private ReconcileJobStore.QueueStats snapshot() {
      long now = System.currentTimeMillis();
      ReconcileJobStore.QueueStats cached = this.cachedStats;
      if (cached != null && (now - this.cachedAtMs) < SNAPSHOT_TTL_MS) {
        return cached;
      }
      ReconcileJobStore.QueueStats fresh = jobs.queueStats();
      this.cachedStats = fresh;
      this.cachedAtMs = now;
      return fresh;
    }

    @Override
    public SchedulerHealthBand currentBand() {
      return snapshot().healthBand;
    }

    @Override
    public Map<StatsPriorityClass, Long> queueDepthByClass() {
      return snapshot().queuedByClass;
    }

    @Override
    public OptionalLong lastSuccessfulCaptureMs(String tableId) {
      return OptionalLong.empty();
    }

    @Override
    public CoverageLevel coverageLevel(String tableId, long snapshotId) {
      return CoverageLevel.NONE;
    }

    @Override
    public OptionalLong snapshotDeltaRows(String tableId, long snapshotId) {
      return OptionalLong.empty();
    }
  }

  // ---------------------------------------------------------------------------
  // CDI qualifier literal for profile-name selection
  // ---------------------------------------------------------------------------

  private static final class SchedulerProfileLiteral extends AnnotationLiteral<SchedulerProfile>
      implements SchedulerProfile {

    private final String name;

    SchedulerProfileLiteral(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
      return SchedulerProfile.class;
    }
  }
}
