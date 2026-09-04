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

import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.repo.cache.PointerCache;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.eclipse.microprofile.config.Config;

/**
 * Process-local account ownership, admission and GC fencing.
 *
 * <p>Core is the durable authority and applies monotonic, incarnation-targeted commands through the
 * control RPC. Normal requests consult only this local module: they never read or write an
 * assignment store. Standalone deployments use the same implementation with every account
 * implicitly owned, so the ownership plumbing adds no external dependency to OSS deployments.
 *
 * <p>The interface is deliberately small. Resolution holds a permit until its pin is registered as
 * a transient root; pointer publication holds a mutation permit through durable mutation and local
 * cache publication; collectors hold a cancellable GC permit. Applying {@link AccountMode#DRAINING}
 * first closes all three admission doors, then status exposes when already-admitted work and local
 * query roots have retired.
 */
@ApplicationScoped
public class AccountGcAuthority {

  public enum AccountMode {
    UNASSIGNED,
    SERVING,
    DRAINING
  }

  public record Status(
      String accountId,
      long assignmentVersion,
      String processIncarnation,
      AccountMode mode,
      boolean gcAllowed,
      long activeResolutions,
      long activeMutations,
      long activeGc,
      long referencedRoots,
      String pointerCacheState) {

    public boolean drained() {
      return mode == AccountMode.DRAINING
          && activeResolutions == 0L
          && activeMutations == 0L
          && activeGc == 0L
          && referencedRoots == 0L;
    }
  }

  public interface Permit extends AutoCloseable {
    @Override
    void close();
  }

  public interface GcPermit extends Permit {
    String accountId();

    long assignmentVersion();

    String processIncarnation();

    boolean valid();

    default void requireValid() {
      if (!valid()) {
        throw new GcPermitRevokedException(accountId(), assignmentVersion());
      }
    }
  }

  /** Expected control-flow signal when a handoff revokes a collector between destructive pages. */
  public static final class GcPermitRevokedException extends RuntimeException {
    public GcPermitRevokedException(String accountId, long assignmentVersion) {
      super(
          "GC permit revoked for account "
              + accountId
              + " at assignment version "
              + assignmentVersion);
    }
  }

  private enum DeploymentMode {
    STANDALONE,
    MANAGED;

    private static DeploymentMode parse(String configured) {
      try {
        return valueOf(
            Objects.requireNonNull(configured, "ownership mode").trim().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException invalid) {
        throw new IllegalArgumentException(
            "floecat.account-ownership.mode must be standalone or managed", invalid);
      }
    }
  }

  private static final Permit NOOP_PERMIT = () -> {};

  private final DeploymentMode deploymentMode;
  private final String processIncarnation;
  private final ToLongFunction<String> referencedRoots;
  private final Consumer<String> prepareCache;
  private final Function<String, String> cacheState;
  private final ConcurrentHashMap<String, AccountState> accounts = new ConcurrentHashMap<>();

  @Inject Observability observability;

  @Inject
  public AccountGcAuthority(Config config, QueryContextStore queryContexts, PointerCache pointers) {
    this(
        DeploymentMode.parse(
            config
                .getOptionalValue("floecat.account-ownership.mode", String.class)
                .orElse("standalone")),
        processIncarnation(config),
        queryContexts::referencedPinBlobCount,
        config.getOptionalValue("floecat.cache.pointer.enabled", Boolean.class).orElse(true)
            ? pointers::resetAndWarm
            : ignored -> {},
        config.getOptionalValue("floecat.cache.pointer.enabled", Boolean.class).orElse(true)
            ? pointers::accountReadiness
            : ignored -> "DISABLED");
  }

  private AccountGcAuthority(
      DeploymentMode deploymentMode,
      String processIncarnation,
      ToLongFunction<String> referencedRoots,
      Consumer<String> prepareCache,
      Function<String, String> cacheState) {
    this.deploymentMode = Objects.requireNonNull(deploymentMode, "deploymentMode");
    this.processIncarnation = requireText(processIncarnation, "processIncarnation");
    this.referencedRoots = Objects.requireNonNull(referencedRoots, "referencedRoots");
    this.prepareCache = Objects.requireNonNull(prepareCache, "prepareCache");
    this.cacheState = Objects.requireNonNull(cacheState, "cacheState");
  }

  public Permit admitResolution(String accountId) {
    return admit(accountId, Activity.RESOLUTION);
  }

  public Permit admitMutation(String accountId) {
    return admit(accountId, Activity.MUTATION);
  }

  public Optional<GcPermit> tryAcquireGc(String accountId) {
    String account = requireText(accountId, "accountId");
    if (deploymentMode == DeploymentMode.STANDALONE) {
      return Optional.of(new UnrestrictedGcPermit(account, processIncarnation));
    }
    AccountState state = accounts.get(account);
    if (state == null) {
      return Optional.empty();
    }
    synchronized (state) {
      if (state.mode != AccountMode.SERVING || !state.gcAllowed) {
        return Optional.empty();
      }
      state.activeGc++;
      return Optional.of(
          new FencedGcPermit(account, state, state.assignmentVersion, processIncarnation));
    }
  }

  /**
   * Applies Core's complete desired local state. Equal commands are idempotent; equal-version
   * conflicts and older commands are rejected so retries and network reordering cannot reopen a
   * drained account.
   */
  public Status apply(
      String accountId,
      long assignmentVersion,
      String targetIncarnation,
      AccountMode mode,
      boolean gcAllowed) {
    if (deploymentMode != DeploymentMode.MANAGED) {
      throw new IllegalStateException("account ownership control requires managed mode");
    }
    String account = requireText(accountId, "accountId");
    if (assignmentVersion <= 0L) {
      throw new IllegalArgumentException("assignmentVersion must be positive");
    }
    if (!processIncarnation.equals(requireText(targetIncarnation, "targetIncarnation"))) {
      throw new IllegalArgumentException("command targets a different process incarnation");
    }
    AccountMode desiredMode = Objects.requireNonNull(mode, "mode");
    boolean desiredGc = gcAllowed && desiredMode == AccountMode.SERVING;
    AccountState state = stateForApply(account);
    synchronized (state) {
      if (assignmentVersion < state.assignmentVersion) {
        throw new IllegalArgumentException("stale account ownership command");
      }
      if (assignmentVersion == state.assignmentVersion) {
        if (state.mode != desiredMode || state.gcAllowed != desiredGc) {
          throw new IllegalArgumentException("conflicting account ownership command version");
        }
        return status(account, state);
      }

      boolean enteringServing =
          desiredMode == AccountMode.SERVING && state.mode != AccountMode.SERVING;
      if (enteringServing) {
        // Clear mutable completeness before opening the admission gate. The asynchronous warm may
        // continue after this method returns; until promotion, misses safely use metadata KV.
        prepareCache.accept(account);
      }
      state.assignmentVersion = assignmentVersion;
      state.mode = desiredMode;
      state.gcAllowed = desiredGc;
      return status(account, state);
    }
  }

  public Status status(String accountId) {
    String account = requireText(accountId, "accountId");
    if (deploymentMode == DeploymentMode.STANDALONE) {
      return new Status(
          account,
          0L,
          processIncarnation,
          AccountMode.SERVING,
          true,
          0L,
          0L,
          0L,
          referencedRoots.applyAsLong(account),
          cacheState.apply(account));
    }
    AccountState state = accounts.get(account);
    if (state == null) {
      return new Status(
          account,
          0L,
          processIncarnation,
          AccountMode.UNASSIGNED,
          false,
          0L,
          0L,
          0L,
          referencedRoots.applyAsLong(account),
          cacheState.apply(account));
    }
    synchronized (state) {
      return status(account, state);
    }
  }

  public String processIncarnation() {
    return processIncarnation;
  }

  public boolean managed() {
    return deploymentMode == DeploymentMode.MANAGED;
  }

  /** Global account-directory GC needs separate leadership in managed multi-replica deployments. */
  public boolean ownsGlobalGc() {
    return deploymentMode == DeploymentMode.STANDALONE;
  }

  void registerGauges(@Observes StartupEvent startup) {
    Tag component = Tag.of(TagKey.COMPONENT, "service");
    Tag operation = Tag.of(TagKey.OPERATION, "account_ownership");
    for (AccountMode mode : AccountMode.values()) {
      observability.gauge(
          ServiceMetrics.Gc.ACCOUNT_OWNERSHIP_STATES,
          () -> accountCount(mode),
          "Managed accounts held by this process in the given local ownership mode",
          component,
          operation,
          Tag.of(TagKey.MODE, mode.name().toLowerCase(Locale.ROOT)));
    }
    observability.gauge(
        ServiceMetrics.Gc.ACCOUNT_GC_ALLOWED,
        this::gcAllowedAccountCount,
        "Managed accounts for which this exact process incarnation may collect garbage",
        component,
        operation);
  }

  static AccountGcAuthority standaloneForTesting() {
    return new AccountGcAuthority(
        DeploymentMode.STANDALONE,
        "standalone/test",
        ignored -> 0L,
        ignored -> {},
        ignored -> "UNLOADED");
  }

  static AccountGcAuthority managedForTesting(
      String incarnation,
      ToLongFunction<String> referencedRoots,
      Consumer<String> prepareCache,
      Function<String, String> cacheState) {
    return new AccountGcAuthority(
        DeploymentMode.MANAGED, incarnation, referencedRoots, prepareCache, cacheState);
  }

  private Permit admit(String accountId, Activity activity) {
    String account = requireText(accountId, "accountId");
    if (deploymentMode == DeploymentMode.STANDALONE) {
      return NOOP_PERMIT;
    }
    AccountState state = accounts.get(account);
    if (state == null) {
      throw notServing(account);
    }
    synchronized (state) {
      if (state.mode != AccountMode.SERVING) {
        throw notServing(account);
      }
      if (activity == Activity.RESOLUTION) {
        state.activeResolutions++;
      } else {
        state.activeMutations++;
      }
      return new CountedPermit(state, activity);
    }
  }

  private Status status(String accountId, AccountState state) {
    return new Status(
        accountId,
        state.assignmentVersion,
        processIncarnation,
        state.mode,
        state.gcAllowed,
        state.activeResolutions,
        state.activeMutations,
        state.activeGc,
        referencedRoots.applyAsLong(accountId),
        cacheState.apply(accountId));
  }

  private AccountState stateForApply(String accountId) {
    return accounts.computeIfAbsent(accountId, ignored -> new AccountState());
  }

  private static StorageAbortRetryableException notServing(String accountId) {
    return new StorageAbortRetryableException(
        "account is not served by this Floecat incarnation: " + accountId);
  }

  private long accountCount(AccountMode mode) {
    return accounts.values().stream()
        .filter(
            state -> {
              synchronized (state) {
                return state.mode == mode;
              }
            })
        .count();
  }

  private long gcAllowedAccountCount() {
    return accounts.values().stream()
        .filter(
            state -> {
              synchronized (state) {
                return state.mode == AccountMode.SERVING && state.gcAllowed;
              }
            })
        .count();
  }

  private static String processIncarnation(Config config) {
    String pod =
        config
            .getOptionalValue("floecat.account-ownership.pod-uid", String.class)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .orElse("local");
    return pod + "/" + UUID.randomUUID();
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must be non-blank");
    }
    return value;
  }

  private enum Activity {
    RESOLUTION,
    MUTATION
  }

  private static final class AccountState {
    private long assignmentVersion;
    private AccountMode mode = AccountMode.UNASSIGNED;
    private boolean gcAllowed;
    private long activeResolutions;
    private long activeMutations;
    private long activeGc;
  }

  private static final class CountedPermit implements Permit {
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

  private final class FencedGcPermit implements GcPermit {
    private final String accountId;
    private final AccountState state;
    private final long assignmentVersion;
    private final String incarnation;
    private final AtomicBoolean closed = new AtomicBoolean();

    private FencedGcPermit(
        String accountId, AccountState state, long assignmentVersion, String incarnation) {
      this.accountId = accountId;
      this.state = state;
      this.assignmentVersion = assignmentVersion;
      this.incarnation = incarnation;
    }

    @Override
    public String accountId() {
      return accountId;
    }

    @Override
    public long assignmentVersion() {
      return assignmentVersion;
    }

    @Override
    public String processIncarnation() {
      return incarnation;
    }

    @Override
    public boolean valid() {
      if (closed.get()) {
        return false;
      }
      synchronized (state) {
        return !closed.get()
            && state.assignmentVersion == assignmentVersion
            && state.mode == AccountMode.SERVING
            && state.gcAllowed;
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

  private static final class UnrestrictedGcPermit implements GcPermit {
    private final String accountId;
    private final String incarnation;
    private final AtomicBoolean closed = new AtomicBoolean();

    private UnrestrictedGcPermit(String accountId, String incarnation) {
      this.accountId = accountId;
      this.incarnation = incarnation;
    }

    @Override
    public String accountId() {
      return accountId;
    }

    @Override
    public long assignmentVersion() {
      return 0L;
    }

    @Override
    public String processIncarnation() {
      return incarnation;
    }

    @Override
    public boolean valid() {
      return !closed.get();
    }

    @Override
    public void close() {
      closed.set(true);
    }
  }
}
