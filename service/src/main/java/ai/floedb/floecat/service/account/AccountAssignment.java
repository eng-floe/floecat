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

import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-local lifecycle gate for account work.
 *
 * <p>Standalone Floecat owns every account. In Floe-managed deployments, Core/runtime controls
 * routing and Kubernetes lifecycle; Floecat only needs a local drain switch so preStop can stop new
 * pins, RPCs and GC before the process exits. Other OSS users can replace this module at the {@link
 * AccountScope} or {@link PlanningPointerIndex.Ownership} interfaces if their deployment needs
 * leases, fences or a coordinator.
 */
@ApplicationScoped
public class AccountAssignment
    implements AccountScope, LifecycleDrain, PlanningPointerIndex.Ownership {

  private final String memberId;
  private final String incarnation;
  private final ConcurrentHashMap<String, AccountState> accounts = new ConcurrentHashMap<>();
  private final CompletableFuture<Void> drainedSignal = new CompletableFuture<>();
  private long activeRpcs;
  private long activeResolutions;
  private long activeMutations;
  private long activeGc;
  private boolean processDraining;

  @Inject
  public AccountAssignment() {
    this("floecat-" + UUID.randomUUID());
  }

  AccountAssignment(String memberId) {
    this.memberId =
        memberId == null || memberId.isBlank() ? "floecat-" + UUID.randomUUID() : memberId;
    this.incarnation = UUID.randomUUID().toString();
  }

  public static AccountAssignment forTesting() {
    return new AccountAssignment("test");
  }

  public static AccountAssignment forTesting(String memberId) {
    return new AccountAssignment(memberId);
  }

  public String memberId() {
    return memberId;
  }

  public String incarnation() {
    return incarnation;
  }

  /**
   * Pointer ownership is the single mutation seam. Process drain is enforced at RPC admission;
   * allowing an already-admitted RPC to take another pointer permit lets a multi-write operation
   * finish instead of failing halfway through with an abort.
   */
  @Override
  public Optional<PlanningPointerIndex.Ownership.Permit> acquire(String accountId, Access access) {
    if (accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    if (access == Access.READ) {
      return Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP);
    }
    synchronized (this) {
      AccountState state = state(accountId);
      state.activeMutations++;
      activeMutations++;
      return Optional.of(new CountedPermit(accountId, state, Activity.MUTATION));
    }
  }

  @Override
  public PlanningPointerIndex.Ownership.Permit admitResolution(String accountId) {
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("Account is not admitted: " + accountId);
    }
    synchronized (this) {
      if (processDraining) {
        throw new LifecycleDrain.DrainingException();
      }
      AccountState state = state(accountId);
      state.activeResolutions++;
      activeResolutions++;
      return new CountedPermit(accountId, state, Activity.RESOLUTION);
    }
  }

  @Override
  public Optional<GcPermit> tryAcquireGc(String accountId) {
    if (accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    synchronized (this) {
      if (processDraining) {
        return Optional.empty();
      }
      AccountState state = state(accountId);
      long generation = state.generation;
      state.activeGc++;
      activeGc++;
      return Optional.of(new RuntimeGcPermit(accountId, state, generation));
    }
  }

  @Override
  public LifecycleControl.Status status() {
    synchronized (this) {
      return statusLocked();
    }
  }

  @Override
  public CompletionStage<Void> drained() {
    return drainedSignal;
  }

  @Override
  public LifecycleDrain.Permit admitRpc() {
    synchronized (this) {
      if (processDraining) {
        throw new LifecycleDrain.DrainingException();
      }
      activeRpcs++;
    }
    return new LifecycleDrain.Permit() {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public void close() {
        if (!closed.compareAndSet(false, true)) {
          return;
        }
        synchronized (AccountAssignment.this) {
          activeRpcs--;
          completeWhenDrainedLocked();
        }
      }
    };
  }

  public AccountStatus status(String accountId) {
    synchronized (this) {
      AccountState state = accounts.get(accountId);
      if (state == null) {
        return new AccountStatus(
            accountId,
            processDraining ? AccountMode.DRAINING : AccountMode.SERVING,
            !processDraining,
            0L,
            0L,
            0L,
            "");
      }
      return status(accountId, state);
    }
  }

  /** Stops admitting RPCs, pin resolutions and GC. Existing mutations are allowed to finish. */
  @Override
  public LifecycleControl.Status beginProcessDrain() {
    synchronized (this) {
      if (!processDraining) {
        processDraining = true;
        accounts.values().forEach(state -> state.generation++);
        completeWhenDrainedLocked();
      }
      return statusLocked();
    }
  }

  private LifecycleControl.Status statusLocked() {
    List<AccountStatus> statuses = new ArrayList<>();
    for (var entry : accounts.entrySet()) {
      statuses.add(status(entry.getKey(), entry.getValue()));
    }
    statuses.sort(Comparator.comparing(AccountStatus::accountId));
    return new Status(memberId, incarnation, processDraining, List.copyOf(statuses), activeRpcs);
  }

  private AccountStatus status(String accountId, AccountState state) {
    return new AccountStatus(
        accountId,
        processDraining ? AccountMode.DRAINING : AccountMode.SERVING,
        !processDraining,
        state.activeResolutions,
        state.activeMutations,
        state.activeGc,
        "");
  }

  /** Must be called while holding this assignment's monitor. */
  private AccountState state(String accountId) {
    return accounts.computeIfAbsent(accountId, ignored -> new AccountState());
  }

  private void release(String accountId, AccountState state, Activity activity) {
    synchronized (this) {
      if (activity == Activity.RESOLUTION) {
        state.activeResolutions--;
        activeResolutions--;
      } else {
        state.activeMutations--;
        activeMutations--;
      }
      removeIfIdle(accountId, state);
      completeWhenDrainedLocked();
    }
  }

  private void releaseGc(String accountId, AccountState state) {
    synchronized (this) {
      state.activeGc--;
      activeGc--;
      removeIfIdle(accountId, state);
      completeWhenDrainedLocked();
    }
  }

  private void removeIfIdle(String accountId, AccountState state) {
    if (state.activeResolutions == 0L && state.activeMutations == 0L && state.activeGc == 0L) {
      accounts.remove(accountId, state);
    }
  }

  private void completeWhenDrainedLocked() {
    if (processDraining
        && activeRpcs == 0L
        && activeResolutions == 0L
        && activeMutations == 0L
        && activeGc == 0L) {
      drainedSignal.complete(null);
    }
  }

  private enum Activity {
    RESOLUTION,
    MUTATION
  }

  private static final class AccountState {
    private long generation;
    private long activeResolutions;
    private long activeMutations;
    private long activeGc;
  }

  private final class CountedPermit implements PlanningPointerIndex.Ownership.Permit {
    private final String accountId;
    private final AccountState state;
    private final Activity activity;
    private final AtomicBoolean closed = new AtomicBoolean();

    private CountedPermit(String accountId, AccountState state, Activity activity) {
      this.accountId = accountId;
      this.state = state;
      this.activity = activity;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        release(accountId, state, activity);
      }
    }
  }

  private final class RuntimeGcPermit implements GcPermit {
    private final String accountId;
    private final AccountState state;
    private final long generation;
    private final AtomicBoolean closed = new AtomicBoolean();

    private RuntimeGcPermit(String accountId, AccountState state, long generation) {
      this.accountId = accountId;
      this.state = state;
      this.generation = generation;
    }

    @Override
    public String accountId() {
      return accountId;
    }

    @Override
    public long generation() {
      return generation;
    }

    @Override
    public boolean valid() {
      if (closed.get()) {
        return false;
      }
      synchronized (AccountAssignment.this) {
        return state.generation == generation;
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        releaseGc(accountId, state);
      }
    }
  }
}
