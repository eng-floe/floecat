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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.config.Config;

/**
 * Process-local lifecycle gate for account work.
 *
 * <p>Standalone Floecat owns every account. In Floe-managed deployments, Core/runtime controls
 * routing and Kubernetes lifecycle; Floecat only needs a local drain switch so preStop can stop new
 * pins, mutations and GC before the process exits. Other OSS users can replace this module at the
 * {@link AccountScope} or {@link PlanningPointerIndex.Ownership} interfaces if their deployment
 * needs leases, fences or a coordinator.
 */
@ApplicationScoped
public class AccountAssignment
    implements AccountScope, AssignmentControl, PlanningPointerIndex.Ownership {

  public enum Mode {
    STANDALONE,
    MANAGED,
    NONE;

    static Mode parse(String configured) {
      String value = configured == null ? "standalone" : configured.trim().toLowerCase(Locale.ROOT);
      return switch (value) {
        case "", "standalone" -> STANDALONE;
        case "managed" -> MANAGED;
        case "none" -> NONE;
        default ->
            throw new IllegalArgumentException(
                "floecat.account-assignment.mode must be standalone, managed or none: "
                    + configured);
      };
    }
  }

  public static GcPermit unfencedGcPermit(String accountId) {
    return new RuntimeGcPermit(accountId, null, 0L);
  }

  /** Compatibility hook for cache tests and external policy adapters. */
  public interface PartitionHooks {
    PartitionHooks NONE =
        new PartitionHooks() {
          @Override
          public void ownershipGained(String accountId) {}

          @Override
          public void ownershipLost(String accountId) {}

          @Override
          public String partitionState(String accountId) {
            return "";
          }
        };

    void ownershipGained(String accountId);

    void ownershipLost(String accountId);

    String partitionState(String accountId);
  }

  private final Mode mode;
  private final String memberId;
  private final String incarnation;
  private final ConcurrentHashMap<String, AccountState> accounts = new ConcurrentHashMap<>();
  private volatile boolean processDraining;

  @Inject
  public AccountAssignment(Config config) {
    this(
        Mode.parse(
            config
                .getOptionalValue("floecat.account-assignment.mode", String.class)
                .orElse("standalone")),
        config
            .getOptionalValue("floecat.account-assignment.member-id", String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseGet(() -> "floecat-" + UUID.randomUUID()));
  }

  AccountAssignment(Mode mode, String memberId) {
    this.mode = Objects.requireNonNull(mode, "mode");
    this.memberId =
        memberId == null || memberId.isBlank() ? "floecat-" + UUID.randomUUID() : memberId;
    this.incarnation = UUID.randomUUID().toString();
  }

  public static AccountAssignment standaloneForTesting(PartitionHooks ignored) {
    return new AccountAssignment(Mode.STANDALONE, "standalone-test");
  }

  public static AccountAssignment standaloneForTesting(
      Object ignoredStore, Object ignoredObservability) {
    return new AccountAssignment(Mode.STANDALONE, "standalone-test");
  }

  public static AccountAssignment managedForTesting(PartitionHooks ignored) {
    return new AccountAssignment(Mode.MANAGED, "managed-test");
  }

  public static AccountAssignment managedForTesting(
      String memberId,
      String ignoredIncarnation,
      Object ignoredStore,
      Object ignoredObservability) {
    return new AccountAssignment(Mode.MANAGED, memberId);
  }

  public Mode mode() {
    return mode;
  }

  @Override
  public boolean managed() {
    return mode == Mode.MANAGED;
  }

  public String memberId() {
    return memberId;
  }

  public String incarnation() {
    return incarnation;
  }

  @Override
  public Optional<PlanningPointerIndex.Ownership.Permit> acquire(String accountId, Access access) {
    if (mode == Mode.STANDALONE || PlanningPointerIndex.isAccountDirectoryPartition(accountId)) {
      return Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP);
    }
    if (mode == Mode.NONE || processDraining || accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    if (access == Access.READ) {
      return Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP);
    }
    AccountState state = state(accountId);
    synchronized (state) {
      if (processDraining) {
        return Optional.empty();
      }
      state.activeMutations++;
    }
    return Optional.of(new CountedPermit(state, Activity.MUTATION)::close);
  }

  @Override
  public Permit admitMutation(String accountId) {
    return acquire(accountId, Access.WRITE)
        .orElseThrow(() -> new PlanningPointerIndex.Ownership.NotOwnedException(accountId));
  }

  @Override
  public Permit admitResolution(String accountId) {
    if (mode == Mode.STANDALONE) {
      return PlanningPointerIndex.Ownership.Permit.NOOP;
    }
    if (mode == Mode.NONE || processDraining || accountId == null || accountId.isBlank()) {
      throw new PlanningPointerIndex.Ownership.NotOwnedException(accountId);
    }
    AccountState state = state(accountId);
    synchronized (state) {
      if (processDraining) {
        throw new PlanningPointerIndex.Ownership.NotOwnedException(accountId);
      }
      state.activeResolutions++;
    }
    return new CountedPermit(state, Activity.RESOLUTION);
  }

  @Override
  public Optional<GcPermit> tryAcquireGc(String accountId) {
    if (mode == Mode.STANDALONE) {
      return Optional.of(new RuntimeGcPermit(accountId, null, 0L));
    }
    if (mode == Mode.NONE || processDraining || accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    AccountState state = state(accountId);
    long generation;
    synchronized (state) {
      if (processDraining) {
        return Optional.empty();
      }
      generation = state.generation;
      state.activeGc++;
    }
    return Optional.of(new RuntimeGcPermit(accountId, state, generation));
  }

  @Override
  public Status apply(
      long epoch,
      AssignmentPhase phase,
      Collection<String> accountIds,
      Collection<String> gcAllowedAccountIds,
      String targetIncarnation) {
    throw new UnsupportedOperationException(
        "pushed account assignment is not supported by the lifecycle-drain policy");
  }

  @Override
  public Status status() {
    List<AccountStatus> statuses = new ArrayList<>();
    for (var entry : accounts.entrySet()) {
      statuses.add(status(entry.getKey(), entry.getValue()));
    }
    statuses.sort(Comparator.comparing(AccountStatus::accountId));
    return new Status(
        memberId,
        incarnation,
        0L,
        AssignmentPhase.SERVING,
        false,
        processDraining,
        List.copyOf(statuses));
  }

  public AccountStatus status(String accountId) {
    if (mode == Mode.STANDALONE) {
      return new AccountStatus(accountId, AccountMode.SERVING, true, 0L, 0L, 0L, "");
    }
    AccountState state = accounts.get(accountId);
    if (state == null) {
      return new AccountStatus(
          accountId,
          processDraining ? AccountMode.DRAINING : AccountMode.SERVING,
          !processDraining && mode == Mode.MANAGED,
          0L,
          0L,
          0L,
          "");
    }
    return status(accountId, state);
  }

  /** Stops admitting pins, mutations and GC. Irreversible for the life of the process. */
  public Status beginProcessDrain() {
    processDraining = true;
    accounts.values().forEach(state -> state.generation++);
    return status();
  }

  private AccountStatus status(String accountId, AccountState state) {
    synchronized (state) {
      return new AccountStatus(
          accountId,
          processDraining ? AccountMode.DRAINING : AccountMode.SERVING,
          !processDraining && mode == Mode.MANAGED,
          state.activeResolutions,
          state.activeMutations,
          state.activeGc,
          "");
    }
  }

  private AccountState state(String accountId) {
    return accounts.computeIfAbsent(accountId, ignored -> new AccountState());
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

  private final class CountedPermit implements Permit {
    private final AccountState state;
    private final Activity activity;
    private final AtomicBoolean closed = new AtomicBoolean();

    private CountedPermit(AccountState state, Activity activity) {
      this.state = state;
      this.activity = activity;
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      synchronized (state) {
        if (activity == Activity.RESOLUTION) {
          state.activeResolutions--;
        } else {
          state.activeMutations--;
        }
      }
    }
  }

  private static final class RuntimeGcPermit implements GcPermit {
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
      if (state == null) {
        return true;
      }
      synchronized (state) {
        return state.generation == generation;
      }
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true) || state == null) {
        return;
      }
      synchronized (state) {
        state.activeGc--;
      }
    }
  }
}
