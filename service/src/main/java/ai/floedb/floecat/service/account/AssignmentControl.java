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

/**
 * Where a control plane tells this process which accounts it serves.
 *
 * <p>Floecat decides nothing here; this is the whole surface a control plane needs. The {@code
 * AccountAssignmentControl} RPC reaches these methods from outside the process, and a deployment
 * deciding in process calls them directly -- either way without reaching for {@link
 * AccountAssignment}, whose remaining surface is Floecat's own business.
 */
public interface AssignmentControl {

  /**
   * Whether this process takes assignments at all. False in {@code standalone}, which serves every
   * account without being told, and in {@code none}. An implementation that pushes anyway is
   * refused.
   */
  boolean managed();

  /**
   * One complete assignment for one epoch. Never a delta: the accounts named are the whole set this
   * process serves, and an account left out is one being taken away.
   *
   * <p>Rejected when {@code targetIncarnation} is not this process, when {@code epoch} is below the
   * last one this process applied or recovered, or when it is already draining for shutdown. An
   * epoch equal to the last one <em>applied</em> is further rejected if it changes the account set
   * or moves {@code SERVING -> DRAINING} -- but a process that restarted and recovered its accounts
   * has an epoch it never applied, so neither of those two holds until something is applied.
   *
   * <p>The epoch is the only ordering Floecat has, so it must come from one sequencer: two deciders
   * with different views would both issue epochs that look valid here.
   *
   * <p>Moving an account between processes takes both phases of one epoch, and both carry each
   * process its <em>final</em> set. A process drains an account because the account is absent from
   * the set it was just given -- the phase does not do that, so a {@code DRAINING} push repeating a
   * process's current set drains nothing, and re-pushing the same epoch with a smaller set is
   * refused. What the phase does is hold the winner back: an account is only taken on a {@code
   * SERVING} push. So push the final sets as {@code DRAINING}, wait for {@link #status()} to report
   * the account no longer {@code DRAINING} on the process losing it, then push the same sets as
   * {@code SERVING}. A process that restarts mid-handover recovers what it last served, so it may
   * take the account back with no push at all; the store fence settles that.
   *
   * <p>Whatever moved should not be in {@code gcAllowedAccountIds} until something that can see
   * running queries says none remain for it. Query contexts and GC roots are local to the process
   * that made them, so a new owner cannot see the pins the old one held.
   */
  Status apply(
      long epoch,
      AssignmentPhase phase,
      Collection<String> accountIds,
      Collection<String> gcAllowedAccountIds,
      String targetIncarnation);

  /**
   * What this process is serving, and what is still in flight on the way out. The incarnation is
   * the token to send back as {@code targetIncarnation}; a pod name is not enough, since a
   * restarted process keeps the name and none of the state.
   */
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
