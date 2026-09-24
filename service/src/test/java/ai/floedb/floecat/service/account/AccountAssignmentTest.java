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

package ai.floedb.floecat.service.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import org.junit.jupiter.api.Test;

class AccountAssignmentTest {
  private static final String ACCOUNT = "acct-a";

  @Test
  void servingModeAdmitsWorkUntilProcessDrainStarts() {
    AccountAssignment assignment =
        AccountAssignment.forTesting(AccountAssignment.PartitionHooks.NONE);

    try (var mutation = assignment.admitMutation(ACCOUNT);
        var resolution = assignment.admitResolution(ACCOUNT);
        var gc = assignment.tryAcquireGc(ACCOUNT).orElseThrow()) {
      assertThat(gc.valid()).isTrue();
      assertThat(assignment.status(ACCOUNT).activeMutations()).isEqualTo(1L);
      assertThat(assignment.status(ACCOUNT).activeResolutions()).isEqualTo(1L);
      assertThat(assignment.status(ACCOUNT).activeGc()).isEqualTo(1L);
    }

    assertThat(assignment.status(ACCOUNT).activeMutations()).isZero();
    assertThat(assignment.status(ACCOUNT).activeResolutions()).isZero();
    assertThat(assignment.status(ACCOUNT).activeGc()).isZero();
  }

  @Test
  void processDrainRejectsNewWorkAndRevokesGcPermits() {
    AccountAssignment assignment =
        AccountAssignment.forTesting(AccountAssignment.PartitionHooks.NONE);
    AccountScope.GcPermit gc = assignment.tryAcquireGc(ACCOUNT).orElseThrow();

    LifecycleControl.Status drained = assignment.beginProcessDrain();

    assertThat(drained.processDraining()).isTrue();
    assertThat(gc.valid()).isFalse();
    assertThat(assignment.tryAcquireGc(ACCOUNT)).isEmpty();
    assertThatThrownBy(() -> assignment.admitMutation(ACCOUNT))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> assignment.admitResolution(ACCOUNT))
        .isInstanceOf(LifecycleDrain.DrainingException.class);
  }

  @Test
  void servingModeAdmitsPointerWrites() {
    AccountAssignment assignment =
        AccountAssignment.forTesting(AccountAssignment.PartitionHooks.NONE);

    assertThat(assignment.tryAcquireGc(ACCOUNT)).isPresent();
    assertThat(assignment.acquire(ACCOUNT, PlanningPointerIndex.Ownership.Access.WRITE))
        .isPresent();
  }
}
