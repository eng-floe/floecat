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
package ai.floedb.floecat.reconciler.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ReconcileWorkerAffinityTest {

  @Test
  void serverAffinityOverridesCallerAttribute() {
    ReconcileExecutionPolicy callerPolicy =
        ReconcileExecutionPolicy.of(
            ReconcileExecutionClass.HEAVY,
            "remote",
            Map.of(ReconcileWorkerAffinity.ATTRIBUTE, "caller", "other", "value"));

    ReconcileExecutionPolicy policy =
        ReconcileWorkerAffinity.of(" ci-branch ").applyTo(callerPolicy);

    assertThat(policy.executionClass()).isEqualTo(ReconcileExecutionClass.HEAVY);
    assertThat(policy.lane()).isEqualTo("remote");
    assertThat(policy.attributes())
        .containsEntry(ReconcileWorkerAffinity.ATTRIBUTE, "ci-branch")
        .containsEntry("other", "value");
  }

  @Test
  void disabledServerAffinityRemovesCallerAttribute() {
    ReconcileExecutionPolicy callerPolicy =
        ReconcileExecutionPolicy.of(
            ReconcileExecutionClass.DEFAULT,
            "",
            Map.of(ReconcileWorkerAffinity.ATTRIBUTE, "caller", "other", "value"));

    ReconcileExecutionPolicy policy = ReconcileWorkerAffinity.of(" ").applyTo(callerPolicy);

    assertThat(policy.attributes())
        .doesNotContainKey(ReconcileWorkerAffinity.ATTRIBUTE)
        .containsEntry("other", "value");
  }

  @Test
  void readsCohortBackFromPolicy() {
    ReconcileExecutionPolicy policy =
        ReconcileWorkerAffinity.of("ci-branch").applyTo(ReconcileExecutionPolicy.defaults());

    assertThat(ReconcileWorkerAffinity.fromPolicy(policy))
        .isEqualTo(ReconcileWorkerAffinity.of("ci-branch"));
    assertThat(ReconcileWorkerAffinity.fromPolicy(ReconcileExecutionPolicy.defaults()))
        .isEqualTo(ReconcileWorkerAffinity.DISABLED);
  }

  @Test
  void usesReservedStorageSegmentForDisabledAffinity() {
    assertThat(ReconcileWorkerAffinity.DISABLED.storageKeySegment())
        .isEqualTo(ReconcileWorkerAffinity.DISABLED_STORAGE_KEY_SEGMENT);
    assertThat(ReconcileWorkerAffinity.of("ci-branch").storageKeySegment()).isEqualTo("ci-branch");
  }

  @Test
  void reservedStorageSegmentCannotBeConfiguredAsAffinity() {
    assertThatThrownBy(
            () -> ReconcileWorkerAffinity.of(ReconcileWorkerAffinity.DISABLED_STORAGE_KEY_SEGMENT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved");
  }

  @Test
  void indexFilterValuePartitionsSlicesPerCohort() {
    String legacy = ReconcileWorkerAffinity.DISABLED.indexFilterValue("PLAN_SNAPSHOT");
    String branch = ReconcileWorkerAffinity.of("ci-branch").indexFilterValue("PLAN_SNAPSHOT");
    String steady = ReconcileWorkerAffinity.of("steady-state").indexFilterValue("PLAN_SNAPSHOT");

    // A disabled cohort must keep the pre-affinity filter value so existing slices still resolve.
    assertThat(legacy).isEqualTo("PLAN_SNAPSHOT");
    assertThat(branch).isNotEqualTo(legacy).isNotEqualTo(steady).contains("PLAN_SNAPSHOT");
  }

  @Test
  void cohortSeparatorCannotBeForgedFromAnExecutionLane() {
    // Lanes are operator-supplied, so a lane must not be able to spoof another cohort's slice.
    String spoofed =
        ReconcileWorkerAffinity.DISABLED.indexFilterValue("ci-branch\u001fPLAN_SNAPSHOT");
    String genuine = ReconcileWorkerAffinity.of("ci-branch").indexFilterValue("PLAN_SNAPSHOT");

    assertThat(spoofed).isNotEqualTo(genuine);
  }
}
