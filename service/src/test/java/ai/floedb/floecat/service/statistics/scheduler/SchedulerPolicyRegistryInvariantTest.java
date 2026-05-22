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

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerAdmissionPolicy.AdmissionDecision;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy.PriorityAssignment;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.telemetry.NoopObservability;
import java.util.EnumMap;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link SchedulerPolicyRegistry} boot-time invariant validation fires correctly for all
 * three guarded invariants.
 *
 * <p>These tests exercise the invariant checks in isolation by constructing the registry directly
 * with synthetic policy implementations — no CDI context needed.
 */
class SchedulerPolicyRegistryInvariantTest {

  // ---------------------------------------------------------------------------
  // Invariant 1: admission policy must ADMIT P0 in all bands
  // ---------------------------------------------------------------------------

  @Test
  void bootFailsWhenAdmissionPolicyDeniesP0InAnyBand() {
    // Policy that incorrectly defers P0.
    SchedulerAdmissionPolicy badAdmission =
        (assignment, band) -> {
          if (assignment.priorityClass() == StatsPriorityClass.P0_SYNC
              && band == SchedulerHealthBand.RED) {
            return AdmissionDecision.DEFER; // violates invariant
          }
          return AdmissionDecision.ADMIT;
        };
    assertThrows(
        IllegalStateException.class,
        () -> SchedulerPolicyRegistry.validateAdmissionP0Invariant(badAdmission, stubContext()),
        "Boot must fail when admission policy defers P0");
  }

  @Test
  void bootFailsWhenAdmissionPolicyDeniesP1InAnyBand() {
    // Policy that incorrectly defers P1 in RED.
    SchedulerAdmissionPolicy badAdmission =
        (assignment, band) -> {
          if (assignment.priorityClass() == StatsPriorityClass.P1_FRESHNESS
              && band == SchedulerHealthBand.RED) {
            return AdmissionDecision.DEFER; // violates invariant
          }
          return AdmissionDecision.ADMIT;
        };
    assertThrows(
        IllegalStateException.class,
        () -> SchedulerPolicyRegistry.validateAdmissionP0Invariant(badAdmission, stubContext()),
        "Boot must fail when admission policy defers P1");
  }

  @Test
  void bootPassesWhenAdmissionPolicyAlwaysAdmitsP0AndP1() {
    SchedulerAdmissionPolicy good = (assignment, band) -> AdmissionDecision.ADMIT;
    // Should not throw for either P0 or P1.
    SchedulerPolicyRegistry.validateAdmissionP0Invariant(good, stubContext());
  }

  // ---------------------------------------------------------------------------
  // Invariant 2: preemption policy must never select P0
  // ---------------------------------------------------------------------------

  @Test
  void bootFailsWhenPreemptionPolicySelectsP0() {
    // Policy that ignores the P0 filter and returns any victim.
    SchedulerPreemptionPolicy badPreemption =
        (incomingJobId, candidates, context) -> candidates.stream().findFirst().map(r -> r.jobId());
    assertThrows(
        IllegalStateException.class,
        () -> SchedulerPolicyRegistry.validatePreemptionP0Invariant(badPreemption, stubContext()),
        "Boot must fail when preemption policy selects P0 victim");
  }

  @Test
  void bootPassesWhenPreemptionPolicyFiltersOutP0() {
    SchedulerPreemptionPolicy good =
        (incomingJobId, candidates, context) ->
            candidates.stream()
                .filter(c -> c.priorityClass() != StatsPriorityClass.P0_SYNC)
                .findFirst()
                .map(r -> r.jobId());
    // Should not throw — all P0 candidates are filtered.
    SchedulerPolicyRegistry.validatePreemptionP0Invariant(good, stubContext());
  }

  // ---------------------------------------------------------------------------
  // Invariant 3: priority policy must never return P0 from assign() or assignForReconcileJob()
  // ---------------------------------------------------------------------------

  @Test
  void bootFailsWhenAssignReturnsP0ForAsyncRequest() {
    SchedulerPriorityPolicy badPolicy =
        new SchedulerPriorityPolicy() {
          @Override
          public PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context) {
            return new PriorityAssignment(StatsPriorityClass.P0_SYNC, 0L, ""); // violates invariant
          }
        };
    assertThrows(
        IllegalStateException.class,
        () -> SchedulerPolicyRegistry.validatePriorityP0Invariant(badPolicy, stubContext()),
        "Boot must fail when assign() returns P0_SYNC for async request");
  }

  @Test
  void bootFailsWhenAssignForReconcileJobReturnsP0() {
    SchedulerPriorityPolicy badPolicy =
        new SchedulerPriorityPolicy() {
          @Override
          public PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context) {
            return new PriorityAssignment(StatsPriorityClass.P3_BACKGROUND, 0L, "");
          }

          @Override
          public PriorityAssignment assignForReconcileJob(
              ReconcileJobKind kind,
              String tableId,
              long snapshotId,
              boolean isNewSnapshot,
              SchedulerContext context) {
            return new PriorityAssignment(StatsPriorityClass.P0_SYNC, 0L, ""); // violates
          }
        };
    assertThrows(
        IllegalStateException.class,
        () -> SchedulerPolicyRegistry.validatePriorityP0Invariant(badPolicy, stubContext()),
        "Boot must fail when assignForReconcileJob() returns P0_SYNC");
  }

  @Test
  void bootPassesForDefaultSchedulerProfile() {
    var profile =
        new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, new NoopObservability());
    var ctx = stubContext();
    // Should not throw — DefaultSchedulerProfile satisfies all three invariants.
    SchedulerPolicyRegistry.validateAdmissionP0Invariant(profile, ctx);
    SchedulerPolicyRegistry.validatePreemptionP0Invariant(profile, ctx);
    SchedulerPolicyRegistry.validatePriorityP0Invariant(profile, ctx);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static SchedulerContext stubContext() {
    return new SchedulerContext() {
      @Override
      public SchedulerHealthBand currentBand() {
        return SchedulerHealthBand.GREEN;
      }

      @Override
      public Map<StatsPriorityClass, Long> queueDepthByClass() {
        return new EnumMap<>(StatsPriorityClass.class);
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
    };
  }
}
