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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Process lifecycle status used by the local drain endpoint. */
public interface AssignmentControl {

  /** Whether this process is managed by an external runtime and exposes the drain endpoint. */
  boolean managed();

  /**
   * Reserved for deployments that replace the default lifecycle policy with their own in-process
   * account policy. The default Floecat runtime policy does not accept pushed account sets.
   */
  Status apply(
      long epoch,
      AssignmentPhase phase,
      Collection<String> accountIds,
      Collection<String> gcAllowedAccountIds,
      String targetIncarnation);

  /** What this process has admitted and what is still in flight on the way out. */
  Status status();

  enum AssignmentPhase {
    DRAINING,
    SERVING
  }

  enum AccountMode {
    UNASSIGNED,
    SERVING,
    DRAINING
  }

  record AccountStatus(
      String accountId,
      AccountMode mode,
      boolean gcAllowed,
      long activeResolutions,
      long activeMutations,
      long activeGc,
      String pointerIndexState) {
    public boolean drained() {
      return activeResolutions == 0L && activeMutations == 0L;
    }
  }

  record Status(
      String memberId,
      String incarnation,
      long epoch,
      AssignmentPhase phase,
      boolean recoveredFromStore,
      boolean processDraining,
      List<AccountStatus> accounts) {
    public boolean drained() {
      return accounts.stream().allMatch(AccountStatus::drained);
    }

    public long activeResolutions() {
      return accounts.stream().mapToLong(AccountStatus::activeResolutions).sum();
    }

    public long activeMutations() {
      return accounts.stream().mapToLong(AccountStatus::activeMutations).sum();
    }

    public long activeGc() {
      return accounts.stream().mapToLong(AccountStatus::activeGc).sum();
    }

    public Optional<AccountStatus> account(String accountId) {
      return accounts.stream().filter(status -> status.accountId().equals(accountId)).findFirst();
    }
  }
}
