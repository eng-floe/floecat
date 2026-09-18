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

import ai.floedb.floecat.service.account.AssignmentControl.AssignmentPhase;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Whether a control plane has ever reached this process, which is what the warning turns on. */
class AssignmentSelfCheckTest {
  private static final String MEMBER = "floecat-0";
  private static final String INCARNATION = "floecat-0/test";
  private static final String ACCOUNT = "acct-a";

  @Test
  void managedWithNothingAppliedHasNoControlPlane() {
    assertThat(managed().everAssigned()).isFalse();
  }

  @Test
  void anAppliedAccountIsAControlPlane() {
    AccountAssignment assignment = managed();
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(ACCOUNT), List.of(), INCARNATION);

    assertThat(assignment.everAssigned()).isTrue();
  }

  /**
   * The set a control plane pushes may legitimately be empty -- a process cordoned for maintenance,
   * or emptied before it is retired. Owning nothing is not the same as nobody assigning, and a
   * warning here would fire against a control plane that is plainly talking.
   */
  @Test
  void anEmptySetIsStillAControlPlane() {
    AccountAssignment assignment = managed();
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(), List.of(), INCARNATION);

    assertThat(assignment.status().accounts()).isEmpty();
    assertThat(assignment.everAssigned()).isTrue();
  }

  /**
   * An account let go stays listed as {@code UNASSIGNED} until a later epoch clears the entry, so
   * what the process is holding says nothing about whether anyone is assigning -- in either
   * direction.
   */
  @Test
  void aReleasedAccountLingersAndChangesNothing() {
    AccountAssignment assignment = managed();
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(ACCOUNT), List.of(), INCARNATION);
    assignment.apply(2L, AssignmentPhase.SERVING, List.of(), List.of(), INCARNATION);

    assertThat(assignment.status().accounts()).isNotEmpty();
    assertThat(assignment.status().account(ACCOUNT).orElseThrow().mode())
        .isEqualTo(AssignmentControl.AccountMode.UNASSIGNED);
    assertThat(assignment.everAssigned()).isTrue();
  }

  /**
   * Recovery is the other way a process comes to serve accounts, and the branch that silences the
   * warning without a control plane having spoken to this process at all.
   */
  @Test
  void recoveringFromTheStoreIsAControlPlane() {
    InMemoryPointerStore store = new InMemoryPointerStore();
    AccountAssignment first =
        AccountAssignment.managedForTesting(MEMBER, INCARNATION, store, new TestObservability());
    first.apply(1L, AssignmentPhase.SERVING, List.of(ACCOUNT), List.of(), INCARNATION);

    AccountAssignment restarted =
        AccountAssignment.managedForTesting(MEMBER, MEMBER + "/2", store, new TestObservability());
    assertThat(restarted.everAssigned()).isFalse();

    restarted.recoverFromStore();

    assertThat(restarted.status().accounts()).isNotEmpty();
    assertThat(restarted.everAssigned()).isTrue();
  }

  private static AccountAssignment managed() {
    return AccountAssignment.managedForTesting(
        MEMBER, INCARNATION, new InMemoryPointerStore(), new TestObservability());
  }
}
