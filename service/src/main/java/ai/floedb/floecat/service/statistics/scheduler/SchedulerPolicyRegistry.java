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

import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
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
 *       SchedulerAdmissionPolicy.AdmissionDecision#ADMIT} for a P0_SYNC assignment in every health
 *       band.
 *   <li>The active {@link SchedulerPreemptionPolicy} (if present) must never return a {@link
 *       StatsPriorityClass#P0_SYNC} job from a synthetic candidate list.
 *   <li>The active {@link SchedulerPriorityPolicy} must never return a {@link PriorityAssignment}
 *       with class {@link StatsPriorityClass#P0_SYNC} for an async request.
 * </ol>
 */
@ApplicationScoped
public class SchedulerPolicyRegistry {

  private static final Logger LOG = Logger.getLogger(SchedulerPolicyRegistry.class);

  private final String profileName;
  private final Instance<SchedulerPriorityPolicy> priorityPolicies;
  private final Instance<SchedulerAdmissionPolicy> admissionPolicies;
  private final Instance<SchedulerPreemptionPolicy> preemptionPolicies;
  private final ReconcileJobStore jobs;

  private SchedulerPriorityPolicy activePriorityPolicy;
  private SchedulerAdmissionPolicy activeAdmissionPolicy;
  // null if no bean with the active profile implements SchedulerPreemptionPolicy
  private SchedulerPreemptionPolicy activePreemptionPolicy;

  private SchedulerContext context;

  @Inject
  SchedulerPolicyRegistry(
      @ConfigProperty(name = "floecat.stats.scheduler.profile", defaultValue = "default")
          String profileName,
      @Any Instance<SchedulerPriorityPolicy> priorityPolicies,
      @Any Instance<SchedulerAdmissionPolicy> admissionPolicies,
      @Any Instance<SchedulerPreemptionPolicy> preemptionPolicies,
      ReconcileJobStore jobs) {
    this.profileName = profileName;
    this.priorityPolicies = priorityPolicies;
    this.admissionPolicies = admissionPolicies;
    this.preemptionPolicies = preemptionPolicies;
    this.jobs = jobs;
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
   */
  public SchedulerPreemptionPolicy activePreemptionPolicy() {
    return activePreemptionPolicy;
  }

  /**
   * Returns the scheduler context backed by the live job store.
   *
   * <p>The returned context reflects the state of the job store at the time each method is called.
   * It does not cache values across calls.
   */
  public SchedulerContext activeContext() {
    return context;
  }

  // ---------------------------------------------------------------------------
  // Boot-time invariant validation
  // ---------------------------------------------------------------------------

  private void validateInvariants() {
    validateP0AlwaysAdmit();
    validatePreemptionNeverSelectsP0();
    validatePriorityNeverAssignsP0();
  }

  private void validateP0AlwaysAdmit() {
    var p0Assignment =
        new SchedulerPriorityPolicy.PriorityAssignment(StatsPriorityClass.P0_SYNC, 0L, "");
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      var decision = activeAdmissionPolicy.decide(p0Assignment, band);
      if (decision != SchedulerAdmissionPolicy.AdmissionDecision.ADMIT) {
        throw new IllegalStateException(
            "Scheduler invariant violated: admission policy '"
                + activeAdmissionPolicy.getClass().getName()
                + "' returned "
                + decision
                + " for P0_SYNC in band "
                + band
                + ". P0_SYNC must always be ADMIT.");
      }
    }
  }

  private void validatePreemptionNeverSelectsP0() {
    if (activePreemptionPolicy == null) {
      return;
    }
    // Synthetic candidate list containing a P0 job to verify the policy filters it out.
    var p0Candidate =
        new SchedulerPreemptionPolicy.RunningJobInfo(
            "synthetic-p0-job", StatsPriorityClass.P0_SYNC, System.currentTimeMillis(), 0, 10);
    var victims = activePreemptionPolicy.selectVictim("incoming", List.of(p0Candidate), context);
    if (victims.isPresent()) {
      throw new IllegalStateException(
          "Scheduler invariant violated: preemption policy '"
              + activePreemptionPolicy.getClass().getName()
              + "' selected a P0_SYNC job as a preemption victim. P0 jobs must never be"
              + " preempted.");
    }
  }

  private void validatePriorityNeverAssignsP0() {
    // Create a minimal synthetic request. We cannot easily instantiate StatsCaptureRequest
    // without protobuf tableId, so we validate by checking the default-impl contract via
    // assignForReconcileJob(), which is fully exercisable with primitive args.
    for (ai.floedb.floecat.reconciler.jobs.ReconcileJobKind kind :
        ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.values()) {
      var assignment =
          activePriorityPolicy.assignForReconcileJob(kind, "tbl-synthetic", 0L, false, context);
      if (assignment.priorityClass() == StatsPriorityClass.P0_SYNC) {
        throw new IllegalStateException(
            "Scheduler invariant violated: priority policy '"
                + activePriorityPolicy.getClass().getName()
                + "' returned P0_SYNC for a reconcile job of kind "
                + kind
                + ". P0_SYNC may only be assigned by the stats orchestrator.");
      }
      var newSnapshotAssignment =
          activePriorityPolicy.assignForReconcileJob(kind, "tbl-synthetic", 1L, true, context);
      if (newSnapshotAssignment.priorityClass() == StatsPriorityClass.P0_SYNC) {
        throw new IllegalStateException(
            "Scheduler invariant violated: priority policy '"
                + activePriorityPolicy.getClass().getName()
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
   * <p>Queue depth and health band are derived from {@link ReconcileJobStore#queueStats()} on each
   * call. Coverage, delta, and last-capture data are not yet tracked at this layer and return
   * conservative defaults: coverage returns {@link CoverageLevel#NONE} and delta / last-capture
   * return empty. These will be wired with real data sources in a future update.
   */
  private static final class ReconcileJobStoreContext implements SchedulerContext {

    private final ReconcileJobStore jobs;

    ReconcileJobStoreContext(ReconcileJobStore jobs) {
      this.jobs = jobs;
    }

    @Override
    public SchedulerHealthBand currentBand() {
      return jobs.queueStats().healthBand;
    }

    @Override
    public Map<StatsPriorityClass, Long> queueDepthByClass() {
      return jobs.queueStats().queuedByClass;
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
