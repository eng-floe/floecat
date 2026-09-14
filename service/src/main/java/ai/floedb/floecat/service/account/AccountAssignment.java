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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.cache.DurablePointerReads;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

/**
 * Which accounts this process serves, and the permits gating pins, mutations and GC on them.
 *
 * <p>Core pushes complete assignments through {@link #apply}; nothing here calls out. The only KV
 * writes are taking an account's fence and recording the member index, both when it enters {@code
 * SERVING} and both off the request path. {@link AssignmentFence} carries the fence version on
 * every account-scoped write, so the store is what enforces exclusivity.
 *
 * <p>{@code standalone} behaves exactly like {@link PlanningPointerIndex.Ownership#ALWAYS_OWNED}.
 */
@ApplicationScoped
public class AccountAssignment implements PlanningPointerIndex.Ownership {

  private static final Logger LOG = Logger.getLogger(AccountAssignment.class);
  private static final int CONSISTENT_READ_BATCH = 100;
  private static final int MEMBER_INDEX_WRITE_ATTEMPTS = 4;
  static final String OWNED_MARKER_PREFIX = "owned/";

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
                "floecat.account-ownership.mode must be standalone, managed or none: "
                    + configured);
      };
    }
  }

  public enum AccountMode {
    UNASSIGNED,
    SERVING,
    DRAINING
  }

  public enum AssignmentPhase {
    DRAINING,
    SERVING
  }

  public record AccountStatus(
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

  public record Status(
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

  public interface Permit extends AutoCloseable {
    @Override
    void close();
  }

  public interface GcPermit extends Permit {
    /** Valid until closed; the permit of a run outside any assignment (standalone, tests). */
    static GcPermit unfenced(String accountId) {
      return new StandaloneGcPermit(accountId);
    }

    String accountId();

    /** Changes whenever this process's ownership of the account starts or ends. */
    long generation();

    boolean valid();

    default void requireValid() {
      if (!valid()) {
        throw new GcPermitRevokedException(accountId());
      }
    }
  }

  /** Control-flow signal: ownership of the account ended while a collector held its permit. */
  public static final class GcPermitRevokedException extends RuntimeException {
    private final String accountId;

    public GcPermitRevokedException(String accountId) {
      super("GC permit revoked for account " + accountId);
      this.accountId = accountId;
    }

    public String accountId() {
      return accountId;
    }
  }

  /** The two index calls the module makes, plus the readiness word it reports. */
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
  private final DurablePointerReads durable;
  private final PartitionHooks hooks;
  private final Executor background;
  private final ExecutorService ownedExecutor;
  private final Observability observability;
  private final Tag[] baseTags = {
    Tag.of(TagKey.COMPONENT, "service"), Tag.of(TagKey.OPERATION, "account_assignment")
  };

  private final Object lock = new Object();
  private final ConcurrentHashMap<String, AccountState> accounts = new ConcurrentHashMap<>();
  private boolean applied;
  private long epoch;
  private AssignmentPhase phase = AssignmentPhase.SERVING;
  private Set<String> assignedAccounts = Set.of();
  private Set<String> gcAllowedAccounts = Set.of();
  private boolean recoveredFromStore;
  private volatile boolean processDraining;
  private volatile boolean storeUnreachable;

  @Inject
  public AccountAssignment(
      Config config,
      DurablePointerReads durable,
      Instance<PlanningPointerIndex> index,
      Observability observability) {
    this(
        Mode.parse(
            config
                .getOptionalValue("floecat.account-ownership.mode", String.class)
                .orElse("standalone")),
        config
            .getOptionalValue("floecat.account-ownership.member-id", String.class)
            .map(String::trim)
            .orElse(""),
        null,
        durable,
        indexHooks(index),
        null,
        observability);
  }

  AccountAssignment(
      Mode mode,
      String memberId,
      String incarnation,
      DurablePointerReads durable,
      PartitionHooks hooks,
      Executor background,
      Observability observability) {
    this.mode = Objects.requireNonNull(mode, "mode");
    this.memberId = memberId == null ? "" : memberId;
    if (mode == Mode.MANAGED && this.memberId.isBlank()) {
      throw new IllegalArgumentException(
          "floecat.account-ownership.member-id is required in managed mode");
    }
    this.incarnation =
        incarnation != null
            ? incarnation
            : (this.memberId.isBlank() ? "local" : this.memberId) + "/" + UUID.randomUUID();
    this.durable = Objects.requireNonNull(durable, "durable");
    this.hooks = Objects.requireNonNull(hooks, "hooks");
    if (background == null) {
      this.ownedExecutor =
          Executors.newSingleThreadExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "floecat-account-assignment");
                thread.setDaemon(true);
                return thread;
              });
      this.background = ownedExecutor;
    } else {
      this.ownedExecutor = null;
      this.background = background;
    }
    this.observability = Objects.requireNonNull(observability, "observability");
    registerGauges();
  }

  /** Standalone module for tests that wire collaborators by hand. */
  public static AccountAssignment standaloneForTesting(
      PointerStore raw, Observability observability) {
    return new AccountAssignment(
        Mode.STANDALONE,
        "",
        "local/standalone",
        new DurablePointerReads(raw),
        PartitionHooks.NONE,
        Runnable::run,
        observability);
  }

  /** Managed module with a synchronous background and no index, for tests outside this package. */
  public static AccountAssignment managedForTesting(
      String memberId, String incarnation, PointerStore raw, Observability observability) {
    return new AccountAssignment(
        Mode.MANAGED,
        memberId,
        incarnation,
        new DurablePointerReads(raw),
        PartitionHooks.NONE,
        Runnable::run,
        observability);
  }

  static AccountAssignment forTesting(
      Mode mode,
      String memberId,
      String incarnation,
      PointerStore raw,
      PartitionHooks hooks,
      Executor background,
      Observability observability) {
    return new AccountAssignment(
        mode,
        memberId,
        incarnation,
        new DurablePointerReads(raw),
        hooks,
        background,
        observability);
  }

  private static PartitionHooks indexHooks(Instance<PlanningPointerIndex> index) {
    return new PartitionHooks() {
      @Override
      public void ownershipGained(String accountId) {
        index.get().ownershipGained(accountId);
      }

      @Override
      public void ownershipLost(String accountId) {
        index.get().ownershipLost(accountId);
      }

      @Override
      public String partitionState(String accountId) {
        return index.get().partitionState(accountId);
      }
    };
  }

  @PreDestroy
  void shutdownExecutor() {
    if (ownedExecutor != null) {
      ownedExecutor.shutdownNow();
    }
  }

  public Mode mode() {
    return mode;
  }

  public boolean managed() {
    return mode == Mode.MANAGED;
  }

  public String memberId() {
    return memberId;
  }

  public String incarnation() {
    return incarnation;
  }

  // ---------------------------------------------------------------------------------------------
  // Permits
  // ---------------------------------------------------------------------------------------------

  @Override
  public Optional<PlanningPointerIndex.Ownership.Permit> acquire(String accountId, Access access) {
    if (mode == Mode.STANDALONE || PlanningPointerIndex.isAccountDirectoryPartition(accountId)) {
      return Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP);
    }
    AccountState state = accounts.get(accountId);
    if (state == null) {
      return Optional.empty();
    }
    if (access == Access.READ) {
      return state.mode != AccountMode.UNASSIGNED
          ? Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP)
          : Optional.empty();
    }
    synchronized (state) {
      if (!admits(state)) {
        return Optional.empty();
      }
      state.activeMutations++;
    }
    return Optional.of(new CountedPermit(accountId, state, Activity.MUTATION)::close);
  }

  /** Admits one account mutation; refused unless this process serves the account. */
  public Permit admitMutation(String accountId) {
    return acquire(accountId, Access.WRITE)
        .map(permit -> (Permit) permit::close)
        .orElseThrow(() -> new PlanningPointerIndex.Ownership.NotOwnedException(accountId));
  }

  /** Admits one pin resolution; released when the pin is rooted or the resolution is abandoned. */
  public Permit admitResolution(String accountId) {
    if (mode == Mode.STANDALONE) {
      return () -> {};
    }
    AccountState state = accountId == null || accountId.isBlank() ? null : accounts.get(accountId);
    if (state == null) {
      throw new PlanningPointerIndex.Ownership.NotOwnedException(accountId);
    }
    synchronized (state) {
      if (!admits(state)) {
        throw new PlanningPointerIndex.Ownership.NotOwnedException(accountId);
      }
      state.activeResolutions++;
    }
    return new CountedPermit(accountId, state, Activity.RESOLUTION);
  }

  /**
   * Grants a GC permit when the account is served, GC-allowed, and one consistent read of the fence
   * still shows the remembered version.
   */
  public Optional<GcPermit> tryAcquireGc(String accountId) {
    if (mode == Mode.STANDALONE) {
      return Optional.of(new StandaloneGcPermit(accountId));
    }
    AccountState state = accounts.get(accountId);
    if (state == null) {
      return Optional.empty();
    }
    long generation;
    synchronized (state) {
      if (!admitsGc(state)) {
        return Optional.empty();
      }
      generation = state.generation;
    }
    if (!selfCheckOne(accountId, state)) {
      return Optional.empty();
    }
    synchronized (state) {
      if (!admitsGc(state) || state.generation != generation) {
        return Optional.empty();
      }
      state.activeGc++;
    }
    return Optional.of(new FencedGcPermit(accountId, state, generation));
  }

  private boolean admits(AccountState state) {
    return state.mode == AccountMode.SERVING && !processDraining && !storeUnreachable;
  }

  private boolean admitsGc(AccountState state) {
    return admits(state) && state.gcAllowed;
  }

  // ---------------------------------------------------------------------------------------------
  // Apply
  // ---------------------------------------------------------------------------------------------

  /**
   * Applies one complete assignment from Core. Rejects a wrong incarnation and a lower epoch; an
   * equal epoch is accepted only when it repeats the current account set and does not move {@code
   * SERVING -> DRAINING}. Fencing joining accounts and writing the member index run in the
   * background; the returned status shows joining accounts as {@code UNASSIGNED} until the fence
   * commits.
   */
  public Status apply(
      long epoch,
      AssignmentPhase phase,
      Collection<String> accountIds,
      Collection<String> gcAllowedAccountIds,
      String targetIncarnation) {
    requireManaged();
    Objects.requireNonNull(phase, "phase");
    if (!incarnation.equals(targetIncarnation)) {
      throw new IllegalArgumentException(
          "target_incarnation " + targetIncarnation + " does not match " + incarnation);
    }
    Set<String> ids = new TreeSet<>(accountIds == null ? List.of() : accountIds);
    ids.removeIf(id -> id == null || id.isBlank());
    Set<String> gcIds =
        new TreeSet<>(gcAllowedAccountIds == null ? List.of() : gcAllowedAccountIds);
    gcIds.retainAll(ids);

    List<Runnable> afterLock = new ArrayList<>();
    List<String> joining = new ArrayList<>();
    synchronized (lock) {
      if (processDraining) {
        throw new IllegalStateException("process is draining");
      }
      if (epoch < this.epoch) {
        throw new IllegalArgumentException(
            "epoch " + epoch + " is older than the applied epoch " + this.epoch);
      }
      if (applied && epoch == this.epoch) {
        if (!ids.equals(assignedAccounts)) {
          throw new IllegalArgumentException(
              "epoch " + epoch + " was already applied with a different account set");
        }
        if (phase == AssignmentPhase.DRAINING && this.phase == AssignmentPhase.SERVING) {
          throw new IllegalArgumentException("epoch " + epoch + " cannot move SERVING -> DRAINING");
        }
      }
      this.applied = true;
      this.epoch = epoch;
      this.phase = phase;
      this.assignedAccounts = Set.copyOf(ids);
      this.gcAllowedAccounts = Set.copyOf(gcIds);
      this.recoveredFromStore = false;

      for (Map.Entry<String, AccountState> entry : accounts.entrySet()) {
        String accountId = entry.getKey();
        AccountState state = entry.getValue();
        if (ids.contains(accountId)) {
          continue;
        }
        synchronized (state) {
          state.pending = false;
          state.gcAllowed = false;
          switch (state.mode) {
            case SERVING -> {
              state.mode = AccountMode.DRAINING;
              state.leaving = true;
              state.leftAtEpoch = epoch;
              state.generation++;
              if (state.drained()) {
                unassignLocked(accountId, state, afterLock);
              }
            }
            case DRAINING -> {
              state.leaving = true;
              if (state.drained()) {
                unassignLocked(accountId, state, afterLock);
              }
            }
            case UNASSIGNED -> {
              if (state.leftAtEpoch < epoch) {
                accounts.remove(accountId, state);
              }
            }
          }
        }
      }
      for (String accountId : ids) {
        AccountState state = accounts.computeIfAbsent(accountId, ignored -> new AccountState());
        synchronized (state) {
          switch (state.mode) {
            case SERVING -> {
              state.gcAllowed = gcIds.contains(accountId);
              state.pending = false;
            }
            case DRAINING -> {
              if (phase == AssignmentPhase.SERVING) {
                // Still fenced by this process; nothing to re-take from the store.
                state.resumeServing(gcIds.contains(accountId));
              } else {
                state.pending = true;
              }
            }
            case UNASSIGNED -> {
              state.pending = true;
              state.leftAtEpoch = -1L;
              if (phase == AssignmentPhase.SERVING) {
                joining.add(accountId);
              }
            }
          }
        }
      }
    }
    afterLock.forEach(Runnable::run);
    if (phase == AssignmentPhase.SERVING) {
      List<String> indexIds = List.copyOf(ids);
      background.execute(
          () -> {
            for (String accountId : joining) {
              fenceAndServe(accountId, epoch);
            }
            if (isCurrentServing(epoch, indexIds)) {
              writeMemberIndex(epoch, indexIds);
            }
          });
    }
    return status();
  }

  public Status status() {
    List<String> accountIds;
    long currentEpoch;
    AssignmentPhase currentPhase;
    boolean currentRecovered;
    boolean currentDraining;
    synchronized (lock) {
      // Snapshot the process-wide state before taking any per-account locks. Mutation paths use
      // the opposite nesting (process lock, then account state), so status must never acquire the
      // global lock after a state lock.
      accountIds = new ArrayList<>(new TreeSet<>(accounts.keySet()));
      currentEpoch = epoch;
      currentPhase = phase;
      currentRecovered = recoveredFromStore;
      currentDraining = processDraining;
    }
    List<AccountStatus> statuses = new ArrayList<>();
    for (String accountId : accountIds) {
      AccountState state = accounts.get(accountId);
      if (state != null) {
        statuses.add(status(accountId, state));
      }
    }
    return new Status(
        memberId,
        incarnation,
        currentEpoch,
        currentPhase,
        currentRecovered,
        currentDraining,
        List.copyOf(statuses));
  }

  public AccountStatus status(String accountId) {
    if (mode == Mode.STANDALONE) {
      return new AccountStatus(
          accountId, AccountMode.SERVING, true, 0L, 0L, 0L, hooks.partitionState(accountId));
    }
    AccountState state = accounts.get(accountId);
    if (state == null) {
      return new AccountStatus(
          accountId, AccountMode.UNASSIGNED, false, 0L, 0L, 0L, hooks.partitionState(accountId));
    }
    return status(accountId, state);
  }

  private AccountStatus status(String accountId, AccountState state) {
    synchronized (state) {
      AccountMode reported =
          processDraining && state.mode == AccountMode.SERVING ? AccountMode.DRAINING : state.mode;
      return new AccountStatus(
          accountId,
          reported,
          admitsGc(state),
          state.activeResolutions,
          state.activeMutations,
          state.activeGc,
          hooks.partitionState(accountId));
    }
  }

  /**
   * Version of the account's fence pointer that every write by this process must still see; empty
   * unless this process owns the account. Read by {@link AssignmentFence}.
   */
  public OptionalLong fenceVersion(String accountId) {
    if (mode != Mode.MANAGED) {
      return OptionalLong.empty();
    }
    AccountState state = accounts.get(accountId);
    if (state == null) {
      return OptionalLong.empty();
    }
    synchronized (state) {
      return state.mode == AccountMode.UNASSIGNED || state.fenceVersion == 0L
          ? OptionalLong.empty()
          : OptionalLong.of(state.fenceVersion);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Process drain
  // ---------------------------------------------------------------------------------------------

  /** Stops admitting pins, mutations and GC on every account. Irreversible; touches no KV. */
  public Status beginProcessDrain() {
    processDraining = true;
    return status();
  }

  // ---------------------------------------------------------------------------------------------
  // Store fence
  // ---------------------------------------------------------------------------------------------

  private void fenceAndServe(String accountId, long epoch) {
    AccountState state = accounts.get(accountId);
    if (state == null) {
      return;
    }
    synchronized (lock) {
      if (this.epoch != epoch
          || phase != AssignmentPhase.SERVING
          || !assignedAccounts.contains(accountId)) {
        return;
      }
      synchronized (state) {
        if (!state.pending || state.mode != AccountMode.UNASSIGNED) {
          return;
        }
      }
    }
    String key = Keys.accountAssignmentFence(accountId);
    String payload = ownedPayload(epoch);
    long version;
    try {
      long current = durable.read(key).map(Pointer::getVersion).orElse(0L);
      // The store assigns the next version, so the taken fence is the one the CAS just wrote.
      if (!durable.compareAndSet(
          key, current, PointerReferences.opaqueMarkerPointer(key, payload, current + 1L))) {
        LOG.warnf(
            "account_assignment_fence_conflict account_id=%s epoch=%d member=%s",
            accountId, epoch, memberId);
        fenceBump("conflict");
        return;
      }
      version = current + 1L;
    } catch (RuntimeException failure) {
      LOG.warnf(
          failure, "account_assignment_fence_failed account_id=%s member=%s", accountId, memberId);
      fenceBump("error");
      return;
    }
    boolean serving = false;
    synchronized (lock) {
      synchronized (state) {
        if (this.epoch == epoch
            && phase == AssignmentPhase.SERVING
            && assignedAccounts.contains(accountId)
            && state.pending
            && state.mode == AccountMode.UNASSIGNED) {
          state.startServing(version, gcAllowedAccounts.contains(accountId));
          serving = true;
        }
      }
    }
    fenceBump("ok");
    if (serving) {
      hooks.ownershipGained(accountId);
    }
  }

  private boolean isCurrentServing(long expectedEpoch, List<String> expectedAccounts) {
    synchronized (lock) {
      return !processDraining
          && phase == AssignmentPhase.SERVING
          && epoch == expectedEpoch
          && assignedAccounts.equals(Set.copyOf(expectedAccounts));
    }
  }

  String ownedPayload(long epoch) {
    return OWNED_MARKER_PREFIX + epoch + "/" + Keys.encodeSegment(memberId);
  }

  /** Member named by an {@code owned} fence marker, or empty for any other payload. */
  static Optional<String> ownedMarkerMember(String payload) {
    if (payload == null || !payload.startsWith(OWNED_MARKER_PREFIX)) {
      return Optional.empty();
    }
    String[] parts = payload.split("/", 3);
    if (parts.length != 3 || parts[2].isBlank()) {
      return Optional.empty();
    }
    return Optional.of(Keys.decodeSegment(parts[2]));
  }

  private void writeMemberIndex(long epoch, List<String> accountIds) {
    String key = Keys.memberAssignmentIndex(memberId);
    String payload =
        epoch + ";" + String.join(",", accountIds.stream().map(Keys::encodeSegment).toList());
    try {
      for (int attempt = 0; attempt < MEMBER_INDEX_WRITE_ATTEMPTS; attempt++) {
        Optional<Pointer> currentPointer = durable.read(key);
        long current = currentPointer.map(Pointer::getVersion).orElse(0L);
        Optional<MemberIndex> currentIndex =
            currentPointer.flatMap(pointer -> MemberIndex.parse(pointer.getBlobUri()));
        if (currentIndex.filter(existing -> existing.epoch() > epoch).isPresent()) {
          return;
        }
        Pointer next = PointerReferences.opaqueMarkerPointer(key, payload, current + 1L);
        if (durable.compareAndSet(key, current, next)) {
          return;
        }
      }
      LOG.warnf("account_assignment_member_index_conflict member=%s epoch=%d", memberId, epoch);
    } catch (RuntimeException failure) {
      LOG.warnf(
          failure, "account_assignment_member_index_failed member=%s epoch=%d", memberId, epoch);
    }
  }

  /** Parsed member index: the epoch and account ids of the member's last applied SERVING. */
  record MemberIndex(long epoch, List<String> accountIds) {
    static Optional<MemberIndex> parse(String payload) {
      if (payload == null || payload.isBlank()) {
        return Optional.empty();
      }
      int separator = payload.indexOf(';');
      if (separator < 0) {
        return Optional.empty();
      }
      try {
        long epoch = Long.parseLong(payload.substring(0, separator));
        String ids = payload.substring(separator + 1);
        List<String> accountIds = new ArrayList<>();
        if (!ids.isBlank()) {
          for (String encoded : ids.split(",")) {
            if (!encoded.isBlank()) {
              accountIds.add(Keys.decodeSegment(encoded));
            }
          }
        }
        return Optional.of(new MemberIndex(epoch, List.copyOf(accountIds)));
      } catch (RuntimeException malformed) {
        return Optional.empty();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------------------------

  /**
   * Restores the accounts whose fence pointer still names this member, from the member index
   * written at the last {@code SERVING}. Recovered accounts are served but never GC-allowed.
   */
  public Status recoverFromStore() {
    if (mode != Mode.MANAGED) {
      return status();
    }
    MemberIndex index;
    try {
      index =
          durable
              .read(Keys.memberAssignmentIndex(memberId))
              .flatMap(pointer -> MemberIndex.parse(pointer.getBlobUri()))
              .orElse(null);
    } catch (RuntimeException failure) {
      LOG.warnf(failure, "account_assignment_recovery_failed member=%s", memberId);
      return status();
    }
    if (index == null) {
      return status();
    }
    List<String> recovered = new ArrayList<>();
    for (String accountId : index.accountIds()) {
      long version;
      try {
        Pointer fence = durable.read(Keys.accountAssignmentFence(accountId)).orElse(null);
        if (fence == null
            || ownedMarkerMember(fence.getBlobUri()).filter(memberId::equals).isEmpty()) {
          continue;
        }
        version = fence.getVersion();
      } catch (RuntimeException failure) {
        LOG.warnf(
            failure,
            "account_assignment_recovery_read_failed member=%s account_id=%s",
            memberId,
            accountId);
        continue;
      }
      AccountState state = accounts.computeIfAbsent(accountId, ignored -> new AccountState());
      synchronized (lock) {
        synchronized (state) {
          if (state.mode != AccountMode.UNASSIGNED) {
            continue;
          }
          state.startServing(version, false);
        }
      }
      recovered.add(accountId);
    }
    if (!recovered.isEmpty()) {
      synchronized (lock) {
        if (!applied) {
          this.epoch = index.epoch();
          this.phase = AssignmentPhase.SERVING;
          this.assignedAccounts = Set.copyOf(recovered);
          this.gcAllowedAccounts = Set.of();
          this.recoveredFromStore = true;
        }
      }
      recovered.forEach(hooks::ownershipGained);
      LOG.infof(
          "account_assignment_recovered member=%s epoch=%d accounts=%d",
          memberId, index.epoch(), recovered.size());
    }
    return status();
  }

  // ---------------------------------------------------------------------------------------------
  // Self-check
  // ---------------------------------------------------------------------------------------------

  /**
   * One consistent read of the fence pointer per owned account. A version other than the remembered
   * one means another process took the account: it becomes {@code UNASSIGNED}. A sweep that cannot
   * reach the store fences pins and writes on every owned account until a sweep succeeds. Pending
   * joins whose fence failed earlier are retried here.
   */
  public void selfCheck() {
    if (mode != Mode.MANAGED) {
      return;
    }
    Map<String, Long> expectedByAccount = new LinkedHashMap<>();
    for (Map.Entry<String, AccountState> entry : accounts.entrySet()) {
      AccountState state = entry.getValue();
      synchronized (state) {
        if (state.mode != AccountMode.UNASSIGNED && state.fenceVersion != 0L) {
          expectedByAccount.put(entry.getKey(), state.fenceVersion);
        }
      }
    }
    Map<String, Pointer> found = new HashMap<>();
    try {
      List<String> keys =
          expectedByAccount.keySet().stream().map(Keys::accountAssignmentFence).toList();
      for (int start = 0; start < keys.size(); start += CONSISTENT_READ_BATCH) {
        List<String> chunk =
            keys.subList(start, Math.min(keys.size(), start + CONSISTENT_READ_BATCH));
        found.putAll(durable.readBatch(chunk));
      }
    } catch (RuntimeException failure) {
      if (!storeUnreachable) {
        LOG.warnf(failure, "account_assignment_self_check_failed member=%s", memberId);
      }
      storeUnreachable = true;
      selfCheckResult("error");
      return;
    }
    boolean mismatch = false;
    for (Map.Entry<String, Long> entry : expectedByAccount.entrySet()) {
      Pointer fence = found.get(Keys.accountAssignmentFence(entry.getKey()));
      if (fence == null || fence.getVersion() != entry.getValue()) {
        mismatch = true;
        revoke(entry.getKey(), "self-check mismatch");
      }
    }
    if (storeUnreachable) {
      LOG.infof("account_assignment_self_check_recovered member=%s", memberId);
    }
    storeUnreachable = false;
    selfCheckResult(mismatch ? "mismatch" : "ok");
    retryPendingFences();
  }

  private void retryPendingFences() {
    long currentEpoch;
    List<String> pending = new ArrayList<>();
    synchronized (lock) {
      if (phase != AssignmentPhase.SERVING || processDraining) {
        return;
      }
      currentEpoch = epoch;
      for (String accountId : assignedAccounts) {
        AccountState state = accounts.get(accountId);
        if (state == null) {
          continue;
        }
        synchronized (state) {
          if (state.pending && state.mode == AccountMode.UNASSIGNED) {
            pending.add(accountId);
          }
        }
      }
    }
    for (String accountId : pending) {
      fenceAndServe(accountId, currentEpoch);
    }
  }

  private boolean selfCheckOne(String accountId, AccountState state) {
    long expected;
    synchronized (state) {
      if (state.fenceVersion == 0L) {
        return false;
      }
      expected = state.fenceVersion;
    }
    Optional<Pointer> fence;
    try {
      fence = durable.read(Keys.accountAssignmentFence(accountId));
    } catch (RuntimeException failure) {
      selfCheckResult("error");
      return false;
    }
    if (fence.isEmpty() || fence.get().getVersion() != expected) {
      selfCheckResult("mismatch");
      revoke(accountId, "gc permit self-check mismatch");
      return false;
    }
    selfCheckResult("ok");
    return true;
  }

  // ---------------------------------------------------------------------------------------------
  // Fence callbacks
  // ---------------------------------------------------------------------------------------------

  /**
   * A fenced write failed its condition. One consistent read tells a lost fence apart from an
   * ordinary CAS conflict on the payload keys.
   */
  void fenceRejected(String accountId, long expectedVersion) {
    boolean lost;
    try {
      Optional<Pointer> fence = durable.read(Keys.accountAssignmentFence(accountId));
      lost = fence.isEmpty() || fence.get().getVersion() != expectedVersion;
    } catch (RuntimeException failure) {
      observability.counter(
          ServiceMetrics.Assignment.FENCE_REJECTIONS,
          1,
          append(baseTags, Tag.of(TagKey.RESULT, "error")));
      return;
    }
    observability.counter(
        ServiceMetrics.Assignment.FENCE_REJECTIONS,
        1,
        append(baseTags, Tag.of(TagKey.RESULT, lost ? "revoked" : "conflict")));
    if (lost) {
      revoke(accountId, "fence rejected");
    }
  }

  private void revoke(String accountId, String reason) {
    AccountState state = accounts.get(accountId);
    if (state == null) {
      return;
    }
    boolean revoked = false;
    synchronized (lock) {
      synchronized (state) {
        if (state.mode != AccountMode.UNASSIGNED) {
          LOG.warnf(
              "account_assignment_revoked account_id=%s member=%s reason=%s",
              accountId, memberId, reason);
          state.clearOwnership();
          revoked = true;
        }
      }
    }
    if (revoked) {
      hooks.ownershipLost(accountId);
    }
  }

  private void unassignLocked(String accountId, AccountState state, List<Runnable> afterLock) {
    state.clearOwnership();
    afterLock.add(() -> hooks.ownershipLost(accountId));
  }

  private void finishDrainIfIdle(String accountId, AccountState state) {
    List<Runnable> afterLock = new ArrayList<>();
    synchronized (lock) {
      synchronized (state) {
        if (state.mode == AccountMode.DRAINING && state.leaving && state.drained()) {
          unassignLocked(accountId, state, afterLock);
        }
      }
    }
    afterLock.forEach(Runnable::run);
  }

  private void requireManaged() {
    if (mode != Mode.MANAGED) {
      throw new IllegalStateException(
          "account assignment control requires floecat.account-ownership.mode=managed");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------------------------

  private void registerGauges() {
    for (AccountMode accountMode : AccountMode.values()) {
      observability.gauge(
          ServiceMetrics.Assignment.ACCOUNTS,
          () -> accountCount(accountMode),
          "Accounts tracked by this process in the given assignment state",
          append(baseTags, Tag.of(TagKey.MODE, accountMode.name().toLowerCase(Locale.ROOT))));
    }
    observability.gauge(
        ServiceMetrics.Assignment.GC_ALLOWED_ACCOUNTS,
        this::gcAllowedAccountCount,
        "Accounts this process may currently collect garbage for",
        baseTags);
  }

  private long accountCount(AccountMode accountMode) {
    long count = 0;
    for (AccountState state : accounts.values()) {
      if (state.mode == accountMode) {
        count++;
      }
    }
    return count;
  }

  private long gcAllowedAccountCount() {
    long count = 0;
    for (AccountState state : accounts.values()) {
      if (state.mode == AccountMode.SERVING && state.gcAllowed && !processDraining) {
        count++;
      }
    }
    return count;
  }

  private void selfCheckResult(String result) {
    observability.counter(
        ServiceMetrics.Assignment.SELF_CHECKS, 1, append(baseTags, Tag.of(TagKey.RESULT, result)));
  }

  private void fenceBump(String result) {
    observability.counter(
        ServiceMetrics.Assignment.FENCE_BUMPS, 1, append(baseTags, Tag.of(TagKey.RESULT, result)));
  }

  private static Tag[] append(Tag[] base, Tag... extra) {
    Tag[] result = Arrays.copyOf(base, base.length + extra.length);
    System.arraycopy(extra, 0, result, base.length, extra.length);
    return result;
  }

  // ---------------------------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------------------------

  private enum Activity {
    RESOLUTION,
    MUTATION
  }

  private static final class AccountState {
    private volatile AccountMode mode = AccountMode.UNASSIGNED;
    private volatile boolean gcAllowed;
    private boolean pending;
    private boolean leaving;
    private long leftAtEpoch = -1L;
    private long generation;
    private long activeResolutions;
    private long activeMutations;
    private long activeGc;
    private long fenceVersion;

    /** Resume a fence that is still valid; this is not a new ownership generation. */
    private void resumeServing(boolean gcAllowed) {
      mode = AccountMode.SERVING;
      leaving = false;
      pending = false;
      this.gcAllowed = gcAllowed;
    }

    /** Publish a newly acquired or recovered fence as a new local ownership generation. */
    private void startServing(long fenceVersion, boolean gcAllowed) {
      mode = AccountMode.SERVING;
      leaving = false;
      pending = false;
      this.gcAllowed = gcAllowed;
      this.fenceVersion = fenceVersion;
      generation++;
    }

    /** Clear every local capability associated with the account. */
    private void clearOwnership() {
      mode = AccountMode.UNASSIGNED;
      gcAllowed = false;
      pending = false;
      leaving = false;
      fenceVersion = 0L;
      generation++;
    }

    private boolean drained() {
      return activeResolutions == 0L && activeMutations == 0L;
    }
  }

  private final class CountedPermit implements Permit {
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
      finishDrainIfIdle(accountId, state);
    }
  }

  private static final class StandaloneGcPermit implements GcPermit {
    private final String accountId;
    private volatile boolean closed;

    private StandaloneGcPermit(String accountId) {
      this.accountId = accountId;
    }

    @Override
    public String accountId() {
      return accountId;
    }

    @Override
    public long generation() {
      return 0L;
    }

    @Override
    public boolean valid() {
      return !closed;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private final class FencedGcPermit implements GcPermit {
    private final String accountId;
    private final AccountState state;
    private final long generation;
    private final AtomicBoolean closed = new AtomicBoolean();

    private FencedGcPermit(String accountId, AccountState state, long generation) {
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
      synchronized (state) {
        return admitsGc(state) && state.generation == generation;
      }
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      synchronized (state) {
        state.activeGc--;
      }
    }
  }
}
