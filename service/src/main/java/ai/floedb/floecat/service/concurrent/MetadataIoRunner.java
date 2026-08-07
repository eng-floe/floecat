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
package ai.floedb.floecat.service.concurrent;

import ai.floedb.floecat.service.concurrent.CancellableCallRunner.FailureMessages;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.ConfigValue;
import org.jboss.logging.Logger;

/**
 * Application-wide admission and platform-worker ownership for blocking metadata I/O.
 *
 * <p>Every production/default overlay caller shares one runtime, so the configured capacity is a
 * process ceiling rather than a per-service multiplier. Admission remains held until the downstream
 * call exits, even when its waiting request has already cancelled. Explicit-capacity instances are
 * isolated for focused tests.
 *
 * <p><b>Every downstream store client routed through here must have a request or socket
 * timeout.</b> A permit is held until the callable returns, and there is no deadline in this tier:
 * a client that hangs rather than fails pins its permit for the life of the process. With one
 * process-wide semaphore, {@code capacity} such hangs wedge all metadata I/O — the gauges show
 * {@code in_use == capacity} but nothing here can name the stuck call. Verify the timeout for each
 * client as it is wired in.
 */
@ApplicationScoped
public class MetadataIoRunner {
  private static final Logger LOG = Logger.getLogger(MetadataIoRunner.class);
  static final String MAX_CONCURRENCY_PROPERTY = "floecat.query.metadata-io.max-concurrency";
  private static final int DEFAULT_CAPACITY = 64;
  private static final int MAX_CAPACITY = 256;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  /**
   * The runtime this facade owns, or null when it follows the process-wide one.
   *
   * <p>Never cache the shared runtime in a field: a facade outlives it. The
   * {@code @ApplicationScoped} bean and any long-held reference would keep serving the instance
   * captured at construction, which a shutdown closes for good — {@code closed} is sticky, and
   * {@link #reopenSharedRuntime()} lowers a flag; it cannot revive a closed runtime. Resolve per
   * use instead.
   */
  private final RuntimeState ownedRuntime;

  // Only an isolated, explicit-capacity runtime is owned (and torn down) by this facade. The
  // process-wide runtime is shared and daemon-backed, so no single bean's @PreDestroy may close it.
  private final boolean ownsRuntime;

  // Set on whichever thread is running an admitted operation, so a nested admission on the SAME
  // thread reuses the permit already held instead of acquiring a second one. A store call reached
  // from within an outer admitted scope — one repository method calling another, or an explicit
  // admit wrapping a call the repository interceptor also admits — therefore runs inline under the
  // one permit: no double-count against the ceiling, no self-deadlock. Being thread-local, it does
  // not span a fan-out's thread hop — units dispatched to other threads acquire their own permits
  // while the caller still holds one, which is what would wedge the ceiling. That case is refused
  // outright by rejectFanOutFromAdmittedOperation rather than avoided by convention.
  private static final ThreadLocal<RuntimeState> IN_ADMITTED_OP = new ThreadLocal<>();

  // Notified when an admission has to wait for a permit. Telemetry installs a sink that increments
  // a counter; the default is a no-op so the acquire path works outside CDI.
  private static volatile Runnable saturationSink = () -> {};

  /**
   * Set while a shutdown is in progress, so no replacement runtime is built after it.
   *
   * <p>A flag beside {@link #SHARED} rather than a sentinel installed into it. A sentinel has to
   * displace the runtime being closed, and every resolution during the drain window then has
   * nothing to return but a rejection — including the re-entrancy check and the gauges, neither of
   * which is submitting anything. Leaving the closed runtime in place lets each caller answer for
   * itself: a nested call reports cancellation, a new submission reports rejection, and the gauges
   * keep reading real numbers.
   */
  private static final java.util.concurrent.atomic.AtomicBoolean SHUTDOWN_LATCHED =
      new java.util.concurrent.atomic.AtomicBoolean();

  /**
   * True when the calling thread is currently inside an admitted operation. Drives re-entrant
   * permit reuse (a nested admission on this thread runs inline under the held permit) and lets a
   * caller detect whether its store access is already bounded.
   */
  public static boolean isRunningAdmittedOperation() {
    return IN_ADMITTED_OP.get() != null;
  }

  /**
   * Wrap {@code operation} so its execution thread is marked admitted for its duration, so a nested
   * admission on that thread reuses this permit rather than acquiring another.
   */
  private <T> Supplier<T> guarded(RuntimeState granting, Supplier<T> operation) {
    return () -> {
      // The remove() below is load-bearing but has no test: nothing non-admitted runs on these
      // workers today, so a leaked marker is unobservable from here. It becomes observable the
      // moment a fan-out runs on one — isRunningAdmittedOperation would read true on an idle worker
      // and trip rejectFanOutFromAdmittedOperation. Keep the finally.
      IN_ADMITTED_OP.set(granting);
      try {
        return operation.get();
      } finally {
        IN_ADMITTED_OP.remove();
      }
    };
  }

  /**
   * Whether this thread's held permit was granted by <em>this</em> runtime. Only then may a nested
   * admission run inline: a permit from another runtime says nothing about this one's ceiling, so
   * reusing it would let the nested call bypass this runtime's semaphore entirely and run on the
   * other's workers. Production has a single shared runtime, so this only separates explicit-
   * capacity runtimes — which is exactly what that constructor (test-only) exists to isolate.
   *
   * <p>A thread holding another runtime's permit therefore waits for one of ours rather than
   * running inline, so nesting admission across two runtimes can block. The single shared runtime
   * makes that unreachable in production.
   */
  private static boolean holdsPermit(RuntimeState resolved) {
    return IN_ADMITTED_OP.get() == resolved;
  }

  /**
   * Create a facade over the process-wide production runtime. Direct and CDI construction share the
   * same admission semaphore and bounded daemon worker pool.
   */
  public MetadataIoRunner() {
    this(null, false);
  }

  /** Return the process-wide runner for compatibility constructors outside CDI. */
  public static MetadataIoRunner shared() {
    return new MetadataIoRunner();
  }

  /**
   * Create an isolated runner with a caller-selected capacity, for focused same-package tests only.
   * Package-private so no production call site can spin up a second runtime and silently multiply
   * the process-wide capacity ceiling — every public construction path is backed by the shared
   * runtime.
   */
  MetadataIoRunner(int capacity) {
    this(new RuntimeState(capacity), true);
  }

  /** Clamp deployment input before it can determine semaphore, worker, and queue sizes. */
  static int clampConfiguredCapacity(int configured) {
    return Math.max(1, Math.min(MAX_CAPACITY, configured));
  }

  /**
   * Read the process-wide capacity once per application lifecycle when the shared runtime is first
   * requested.
   */
  private static int configuredCapacity() {
    int configured;
    try {
      configured =
          ConfigProvider.getConfig()
              .getOptionalValue(MAX_CONCURRENCY_PROPERTY, Integer.class)
              .orElse(DEFAULT_CAPACITY);
    } catch (RuntimeException unusable) {
      // Covers both an unparseable value and a missing config provider. Reading the ceiling is a
      // startup concern, not a per-call one, so a config failure must not turn every later
      // admission into a hard failure: fall back and let validateConfiguredCapacity reject a bad
      // value at StartupEvent, where the message can name the property and the offending value.
      LOG.warnf(
          unusable, "cannot read %s; using %d permits", MAX_CONCURRENCY_PROPERTY, DEFAULT_CAPACITY);
      return DEFAULT_CAPACITY;
    }
    int clamped = clampConfiguredCapacity(configured);
    if (configured != clamped) {
      LOG.warnf(
          "%s must be between 1 and %d; using %d instead of %d",
          MAX_CONCURRENCY_PROPERTY, MAX_CAPACITY, clamped, configured);
    }
    return clamped;
  }

  /**
   * Resolve the runtime per call rather than at construction. A shared-runtime facade outlives the
   * runtime it was built against — a CDI bean is built once and held for the container's life — and
   * {@code closed} is sticky, so a captured RuntimeState would keep serving the one a shutdown
   * closed. Resolving here also lets construction succeed while the shutdown sentinel is installed.
   */
  private RuntimeState runtime() {
    return ownsRuntime ? ownedRuntime : sharedRuntime();
  }

  private MetadataIoRunner(RuntimeState runtime, boolean ownsRuntime) {
    this.ownedRuntime = ownsRuntime ? java.util.Objects.requireNonNull(runtime, "runtime") : null;
    this.ownsRuntime = ownsRuntime;
    // Shared-runtime facades hold nothing; see runtime(). An owned runtime creates its pool lazily
    // in executor(), so construction succeeds even after shutdown and a closed runtime reports
    // itself as a RejectedExecutionException at the point of use.
  }

  /**
   * Create the pool now instead of on first use. Not a CDI callback: {@code closed} is sticky, so
   * an eager start after shutdown throws. {@link #call} starts the pool lazily on its own.
   */
  public void start() {
    runtime().start();
  }

  /**
   * Permits configured for this runtime — the process-wide store-concurrency ceiling.
   *
   * <p>Public so the metrics bean in the telemetry package can read them. Observations only — no
   * control path.
   *
   * <p>These three, with the saturated-wait counter that {@code MetadataIoMetrics} publishes as
   * {@code floecat.service.metadata_io.admission.saturated_waits.total}, are what make the
   * "admission is retained until the downstream call truly exits" trade observable: a stalled store
   * pins permits long after its callers have given up, and without them that is indistinguishable
   * from store latency.
   */
  public int capacity() {
    return runtime().capacity;
  }

  /** Permits currently held by in-flight calls. */
  public int permitsInUse() {
    RuntimeState current = runtime();
    return current.capacity - current.permits.availablePermits();
  }

  /** Threads currently parked waiting for admission. */
  public int admissionWaiters() {
    return runtime().permits.getQueueLength();
  }

  /**
   * Refuse to fan out from inside an admitted operation.
   *
   * <p>Re-entrant permit reuse is thread-local, so units dispatched to other threads acquire their
   * own permits while the outer operation still holds one. With {@code capacity} such operations in
   * flight every permit is held by a thread waiting on its children, and admission has no deadline:
   * all metadata I/O in the process wedges until each request's cancellation fires. Propagating the
   * marker instead would let units inherit a permit and stop bounding store concurrency, which is
   * the whole point of the tier.
   *
   * <p><b>Guarded today: {@code BoundedFanout}'s dispatch only.</b> Every other way an admitted
   * operation can hand work to another thread — {@code CompletableFuture.supplyAsync}, an injected
   * {@code ManagedExecutor}, a Mutiny hop, a store client's own worker pool — is unguarded, because
   * the marker is thread-local and this check only fires where it is called. Call it from any new
   * dispatch point that can run under admission; the alternative, if these grow, is to enforce at
   * the executor boundary rather than per call site.
   */
  public static void rejectFanOutFromAdmittedOperation(String dispatchSite) {
    if (isRunningAdmittedOperation()) {
      throw new IllegalStateException(
          dispatchSite
              + " ran inside an admitted metadata-I/O operation. Its units would each acquire their"
              + " own permit while this thread holds one, deadlocking the process-wide ceiling."
              + " Fan out first, then admit each unit.");
    }
  }

  /**
   * Apply the gates a fresh admission would, before reusing a held permit inline. A closed runtime
   * or an already-interrupted caller must stop a nested read too: otherwise re-entry starts store
   * I/O in exactly the states where {@code acquire} refuses to.
   */
  private static void rejectIfUnusable(
      RuntimeState granting, BooleanSupplier cancelled, FailureMessages failureMessages) {
    if (granting.isClosed()) {
      // In flight by construction — this only runs on a thread already holding a permit — so
      // closure is a cancellation here, not a rejected submission. Throwing the rejection type
      // would put a spurious INTERNAL next to the clean cancellations an identical non-nested call
      // produces at the same instant.
      throw new CancellationException(CancellableCallRunner.RUNTIME_CLOSED);
    }
    if (Thread.currentThread().isInterrupted()) {
      throw new CancellationException(failureMessages.interruption());
    }
    if (cancelled != null && cancelled.getAsBoolean()) {
      throw new CancellationException(failureMessages.cancellation());
    }
  }

  /** Report that an admission could not be granted immediately. */
  static void recordSaturatedWait() {
    saturationSink.run();
  }

  /**
   * Install the sink notified on each saturated admission. Called once by telemetry at startup; the
   * sink runs on the waiting thread, so it must not block.
   */
  public static void setSaturationSink(Runnable sink) {
    saturationSink = java.util.Objects.requireNonNull(sink, "sink");
  }

  /**
   * Drop the installed sink. The sink closes over CDI beans, so leaving it in place across a
   * dev-mode reload or {@code @QuarkusTest} restart pins the previous container and sends later
   * increments to a dead bean.
   */
  public static void clearSaturationSink() {
    saturationSink = () -> {};
  }

  /**
   * Reject a malformed capacity value at startup.
   *
   * <p>{@link #configuredCapacity()} falls back rather than throwing, so a config failure cannot
   * turn every later admission into a hard failure. That fallback would otherwise let a service
   * start on the default ceiling when the operator asked for fewer, so the value is validated here,
   * once at startup, where the failure names the property and the offending value.
   */
  public static void validateConfiguredCapacity() {
    // No catch here: SmallRyeConfigProviderResolver builds and registers a Config on demand, so
    // there is no "no provider" case to swallow, and swallowing every other IllegalStateException
    // it can raise would skip validation silently.
    ConfigValue configured = ConfigProvider.getConfig().getConfigValue(MAX_CONCURRENCY_PROPERTY);
    if (configured == null || configured.getRawValue() == null) {
      return;
    }
    String raw = configured.getValue();
    if (raw == null) {
      // Declared, but the expression did not expand — an unresolvable ${ENV_VAR}, typically.
      // getOptionalValue reports this as an empty Optional rather than throwing, so reading through
      // it here would accept the property and let configuredCapacity() fall back: an operator who
      // lowered the ceiling would silently get the default back.
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY
              + " is set to \""
              + configured.getRawValue()
              + "\" but could not be resolved");
    }
    if (raw.isBlank()) {
      // Declared and resolved, just empty. Only a missing raw value means absent; reaching here
      // means an operator set the key, and configuredCapacity() would read it as empty and hand
      // back the default ceiling they were trying to change.
      throw new IllegalStateException(MAX_CONCURRENCY_PROPERTY + " is set to a blank value");
    }
    int parsed;
    try {
      parsed = Integer.parseInt(raw.trim());
    } catch (NumberFormatException badValue) {
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY + " must be an integer; got \"" + raw + "\"", badValue);
    }
    // Out of range is rejected, not clamped. Clamping is silent, and 0 or a negative value clamps
    // to 1 — serialising every metadata round trip in the process behind a single permit, with one
    // WARN line as the only trace.
    if (parsed != clampConfiguredCapacity(parsed)) {
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY + " must be between 1 and " + MAX_CAPACITY + "; got " + parsed);
    }
  }

  /** True when two facades share the same process or test runtime. */
  boolean sharesRuntimeWith(MetadataIoRunner other) {
    return other != null && runtime() == other.runtime();
  }

  /** Run one blocking call with cancellation polling and application-wide admission. */
  <T> T call(
      BooleanSupplier cancelled,
      Supplier<T> operation,
      CancellableCallRunner.FailureMessages failureMessages) {
    // Resolved once, and for the same reason on both paths: two calls to runtime() can straddle a
    // restart. Non-re-entrant that hands one call the executor and the semaphore of different
    // runtimes; re-entrant, rejectIfUnusable would read a replacement's open `closed` flag and let
    // the nested read proceed on a permit from the torn-down runtime it exists to stop.
    RuntimeState current = runtime();
    if (holdsPermit(current)) {
      // Re-entrant: reuse this thread's permit and run inline. Apply the same gates a fresh
      // admission would, or a nested read starts store I/O in states where an outer call could not.
      rejectIfUnusable(current, cancelled, failureMessages);
      return operation.get();
    }
    return CancellableCallRunner.call(
        current.executor(),
        current.permits,
        cancelled,
        current::isClosed,
        guarded(current, operation),
        failureMessages,
        MetadataIoRunner::recordSaturatedWait);
  }

  /** Run one blocking call off-thread without imposing cancellation or a new deadline. */
  <T> T callWithoutCancellation(
      Supplier<T> operation, CancellableCallRunner.FailureMessages failureMessages) {
    RuntimeState current = runtime();
    if (holdsPermit(current)) {
      // Re-entrant, and gated like a fresh admission minus request cancellation, which this entry
      // point deliberately does not impose.
      rejectIfUnusable(current, null, failureMessages);
      return operation.get();
    }
    return CancellableCallRunner.callWithoutCancellation(
        current.executor(),
        current.permits,
        current::isClosed,
        guarded(current, operation),
        failureMessages,
        MetadataIoRunner::recordSaturatedWait);
  }

  /**
   * Stop this runtime's workers.
   *
   * <p>Acceptance stops first, under the monitor: latching {@code closed} alone would leave the
   * pool running until the drain below, and a caller holding an executor reference from before the
   * latch could still submit into that gap.
   *
   * <p>{@code shutdownNow} then interrupts workers and returns only queued tasks, so a store call
   * that ignores its interrupt keeps running. Closure is visible to every wait in this tier, so
   * callers and permit waiters abandon it rather than parking behind that call: an admission wait
   * is rejected, an in-flight wait is cancelled, and the abandoned task releases its own permit
   * when it eventually returns.
   */
  @PreDestroy
  public void close() {
    // The process-wide runtime is shared by the CDI bean, shared(), and every default-constructed
    // instance, so an instance @PreDestroy must not tear it down: CDI destroy order among instances
    // is unspecified, and a runner constructed after teardown would get a closed runtime. Only an
    // isolated, explicit-capacity runtime is closed here.
    //
    // The shared one is closed by the ShutdownEvent observer instead — which fires at the START of
    // shutdown, BEFORE Arc runs any @PreDestroy, so it closes the runtime earlier than this hook
    // would, not later. A bean performing metadata I/O in its own @PreDestroy would therefore see
    // RejectedExecutionException. None does today; if one ever needs to, move the observer.
    if (!ownsRuntime) {
      return;
    }
    if (!ownedRuntime.close()) {
      LOG.warn("metadata I/O executor did not terminate during shutdown");
    }
  }

  /**
   * Close the process-wide runtime once at application shutdown, reclaiming its platform-worker
   * pool. The shared pool is a daemon-backed static with no per-instance owner, so no
   * {@code @PreDestroy} may close it; the application {@code ShutdownEvent} fires once per
   * lifecycle — including dev-mode live reload and {@code @QuarkusTest} restarts — so closing there
   * reclaims the pool (and its context class loader) that would otherwise leak across in-JVM
   * restarts, while production still relies on daemon status for JVM exit.
   *
   * <p>Driven by {@link MetadataIoLifecycle} rather than an observer on this bean: the shared
   * runtime can be started without any CDI instance existing ({@link #shared()} and the default
   * constructor are supported outside CDI), and an {@code IF_EXISTS} observer would then never fire
   * — leaking exactly the pool this reclaims. That always-present observer can call this
   * unconditionally: a never-started runtime leaves the reference null, so nothing is touched and
   * an unused runner still never starts a pool just to close it.
   */
  static void closeSharedRuntimeIfStarted() {
    // Gated on the reference alone: a separate "started" flag is a second fact that can disagree
    // with it, and did — a runtime installed during the shutdown window outlived it with the flag
    // already cleared, so no later shutdown reclaimed its pool. getAndSet on a never-started
    // runtime returns null and touches nothing, so an unused runner still never starts a pool just
    // to close it.
    //
    // Latch before reading, so a creator that has not yet checked cannot get past it. One that
    // already did still loses: it re-checks after winning the CAS and closes what it built.
    SHUTDOWN_LATCHED.set(true);
    // Leave something readable behind even if nothing ever resolved a runtime. The gauges are
    // registered at startup and no caller routes store I/O through this tier yet, so a first scrape
    // during the drain is exactly the likely case; with nothing installed it would publish NaN.
    // Never started, so this closes without touching a pool.
    SHARED.compareAndSet(null, createSharedRuntime());
    RuntimeState closing = SHARED.get();
    if (closing != null && !closing.close()) {
      LOG.warn("metadata I/O executor did not terminate during shutdown");
    }
  }

  /**
   * Re-arm the shared runtime for a new application lifecycle.
   *
   * <p>A no-op under Quarkus: this is an application class, so a dev-mode reload or
   * {@code @QuarkusTest} restart mints a fresh {@code Class} with fresh statics and {@code SHARED}
   * is already null. It is what lets a plain JUnit fork — where the statics really are shared —
   * recover, and it keeps the latch from being a one-way door if that ever stops holding.
   *
   * <p>Clearing the latch is enough: {@link #sharedRuntime()} replaces a closed runtime once it is
   * allowed to build one, so the next caller gets a fresh one without this having to null the
   * reference and race a concurrent resolution.
   */
  static void reopenSharedRuntime() {
    SHUTDOWN_LATCHED.set(false);
    // Drop the dead reference too. sharedRuntime() would replace it on the next resolution anyway,
    // so this changes no outcome; it just stops a closed runtime being pinned until someone happens
    // to ask, and leaves the next lifecycle in the same state a fresh process starts in.
    SHARED.updateAndGet(current -> current != null && current.isClosed() ? null : current);
  }

  private static RuntimeState createSharedRuntime() {
    return new RuntimeState(configuredCapacity());
  }

  /**
   * The process-wide runtime, replaceable across application lifecycles.
   *
   * <p>Not a {@code static final} holder: {@code closed} is sticky, and Quarkus reuses the runtime
   * classloader across dev-mode reloads and {@code @QuarkusTest} restarts. A one-shot holder would
   * hand every call in the restarted application a closed runtime. Not cleared on shutdown — the
   * closed instance stays installed so the drain window still has something to read — and replaced
   * by {@link #sharedRuntime()} once {@link #reopenSharedRuntime()} lowers the latch. Still lazy,
   * so an application that never reads metadata never starts a pool.
   */
  private static final java.util.concurrent.atomic.AtomicReference<RuntimeState> SHARED =
      new java.util.concurrent.atomic.AtomicReference<>();

  private static RuntimeState sharedRuntime() {
    while (true) {
      RuntimeState current = SHARED.get();
      if (current != null && !current.isClosed()) {
        return current;
      }
      if (SHUTDOWN_LATCHED.get()) {
        if (current != null) {
          // Closed, and no replacement may be built. Returned rather than rejected here so the
          // caller decides: acquire() reports the rejection, a nested call reports cancellation,
          // and an observation accessor just reads it.
          return current;
        }
        throw new RejectedExecutionException("metadata I/O executor is closed");
      }
      RuntimeState fresh = createSharedRuntime();
      if (SHARED.compareAndSet(current, fresh)) {
        if (SHUTDOWN_LATCHED.get()) {
          // Latched between the check above and this CAS. Undo rather than hand back a runtime the
          // ShutdownEvent will never see: close() on a never-started runtime touches nothing, since
          // the pool is built lazily by executor().
          fresh.close();
          throw new RejectedExecutionException("metadata I/O executor is closed");
        }
        return fresh;
      }
    }
  }

  /** Shared lifecycle and admission state behind one or more runner facades. */
  private static final class RuntimeState {
    private final int capacity;
    private final Semaphore permits;
    private volatile ThreadPoolExecutor executor;
    // Read by waiting callers outside this object's monitor, so closure is visible to them
    // promptly. One-way: there is no reopen path.
    private volatile boolean closed;

    private RuntimeState(int capacity) {
      if (capacity < 1) {
        throw new IllegalArgumentException("metadata I/O capacity must be positive");
      }
      this.capacity = capacity;
      // Deliberately NOT a fair semaphore. Every waiter here polls with a timed tryAcquire so it
      // can abandon the wait on cancellation or closure, and each timeout cancels its AQS queue
      // node and re-enqueues at the tail. Fairness would therefore buy no FIFO bound while still
      // forbidding barging, so a polling waiter could lose a permit that is free at the moment it
      // wakes. Prompt abandonment is the property worth keeping; strict FIFO would need an untimed
      // acquire plus a watchdog to interrupt waiters.
      this.permits = new Semaphore(capacity);
    }

    /**
     * Create the pool on first use. {@code closed} is sticky and there is no reopen path, so this
     * is start-once-then-throw, never a restart: a closed runtime stays closed for the life of the
     * process.
     */
    private synchronized ThreadPoolExecutor start() {
      if (closed) {
        throw new RejectedExecutionException("metadata I/O executor is closed");
      }
      if (executor == null) {
        executor =
            MetadataIoExecutors.newBoundedDaemonPool(capacity, capacity, "floecat-metadata-io-");
      }
      return executor;
    }

    private ThreadPoolExecutor executor() {
      ThreadPoolExecutor current = executor;
      if (current != null) {
        return current;
      }
      // Take what start() returns rather than re-reading the field: close() holds this monitor and
      // nulls it, so a re-read could hand back null to a caller that already passed the closed
      // check — a bare NPE from execute() instead of the
      // RejectedExecutionException this runtime documents. Losing the race now means running on a
      // pool being shut down, which the executor rejects properly.
      return start();
    }

    private boolean isClosed() {
      return closed;
    }

    private boolean close() {
      ThreadPoolExecutor closing;
      // Latch and detach under the monitor, then release it before waiting. start() contends on
      // this same monitor, so awaiting termination while holding it would park every concurrent
      // caller for the shutdown timeout before they could read `closed` and be rejected — the
      // opposite of the prompt rejection this runtime promises.
      synchronized (this) {
        closed = true;
        closing = executor;
        executor = null;
        if (closing != null) {
          // Stop acceptance as early as possible: latching `closed` alone leaves the pool RUNNING
          // until shutdownNow runs below. This narrows the submit-after-latch window but does not
          // close it — a caller that already resolved the executor can still reach execute() first.
          // The guards that actually reject such a call are in acquire()'s fast path and the task's
          // own closure check.
          closing.shutdown();
        }
      }
      if (closing == null) {
        return true;
      }
      // A queued call already holds its permit but will never run its finally, so releasing the
      // discarded tasks is this runtime's job, not the pool factory's.
      return MetadataIoExecutors.shutdownNowAndAwait(
          closing,
          CancellableCallRunner::cancelDiscardedTasks,
          SHUTDOWN_TIMEOUT_SECONDS,
          TimeUnit.SECONDS);
    }
  }
}
