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

import ai.floedb.floecat.engine.concurrent.ProcessWideAdmission;
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
 * client as it is wired in. The hung task also necessarily retains its application generation until
 * it returns; no Java lifecycle callback can forcibly reclaim a running thread safely.
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

  /** The runtime and permit lease the current worker owns, for same-runtime nesting only. */
  private static final ThreadLocal<HeldAdmission> HELD_ADMISSION = new ThreadLocal<>();

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

  /**
   * Read and validate the process-wide capacity when the shared runtime is first requested.
   *
   * <p>Default construction is a supported non-CDI API, so this cannot defer validation to the CDI
   * startup observer. Otherwise an embedding that never starts CDI would silently use the default
   * ceiling for a malformed value that a service deployment correctly refuses.
   */
  private static int configuredCapacity() {
    return parseConfiguredCapacity(
        ConfigProvider.getConfig().getConfigValue(MAX_CONCURRENCY_PROPERTY));
  }

  /** Convert the raw deployment value to a valid admission capacity. */
  private static int parseConfiguredCapacity(ConfigValue configured) {
    if (configured == null || configured.getRawValue() == null) {
      return DEFAULT_CAPACITY;
    }
    String raw = configured.getValue();
    if (raw == null) {
      // Declared, but the expression did not expand — an unresolvable ${ENV_VAR}, typically.
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY
              + " is set to \""
              + configured.getRawValue()
              + "\" but could not be resolved");
    }
    if (raw.isBlank()) {
      throw new IllegalStateException(MAX_CONCURRENCY_PROPERTY + " is set to a blank value");
    }
    int parsed;
    try {
      parsed = Integer.parseInt(raw.trim());
    } catch (NumberFormatException badValue) {
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY + " must be an integer; got \"" + raw + "\"", badValue);
    }
    if (parsed < 1 || parsed > MAX_CAPACITY) {
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY + " must be between 1 and " + MAX_CAPACITY + "; got " + parsed);
    }
    return parsed;
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

  /** Report that an admission could not be granted immediately. */
  static void recordSaturatedWait() {
    saturationSink.run();
  }

  /**
   * Run application code with the submitting generation's loader, then leave the pooled worker
   * neutral.
   */
  private static <T> Supplier<T> withApplicationClassLoader(
      RuntimeState runtime, Supplier<T> operation) {
    ClassLoader callerClassLoader = Thread.currentThread().getContextClassLoader();
    return () -> {
      Thread worker = Thread.currentThread();
      worker.setContextClassLoader(callerClassLoader);
      HeldAdmission previous = HELD_ADMISSION.get();
      CancellableCallRunner.AdmissionLease admission = CancellableCallRunner.currentAdmission();
      HELD_ADMISSION.set(new HeldAdmission(runtime, admission));
      try {
        return operation.get();
      } finally {
        if (previous == null) {
          HELD_ADMISSION.remove();
        } else {
          HELD_ADMISSION.set(previous);
        }
        worker.setContextClassLoader(ClassLoader.getPlatformClassLoader());
      }
    };
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
   * <p>Default construction validates by the same path, so this observer fails a deployment before
   * it accepts traffic and establishes the process-wide gate before any consumer starts.
   */
  public static void validateConfiguredCapacity() {
    ProcessWideAdmission.resolve(configuredCapacity());
  }

  /** True when two facades share the same process or test runtime. */
  boolean sharesRuntimeWith(MetadataIoRunner other) {
    return other != null && runtime() == other.runtime();
  }

  private static void rejectNestedIfUnusable(
      RuntimeState runtime,
      BooleanSupplier cancelled,
      CancellableCallRunner.FailureMessages failureMessages) {
    if (runtime.isClosed()) {
      throw new CancellationException(CancellableCallRunner.RUNTIME_CLOSED);
    }
    if (Thread.currentThread().isInterrupted()) {
      throw new CancellationException(failureMessages.interruption());
    }
    if (cancelled != null && cancelled.getAsBoolean()) {
      throw new CancellationException(failureMessages.cancellation());
    }
  }

  /** Run one blocking call with cancellation polling and application-wide admission. */
  <T> T call(
      BooleanSupplier cancelled,
      Supplier<T> operation,
      CancellableCallRunner.FailureMessages failureMessages) {
    RuntimeState current = runtime();
    HeldAdmission held = HELD_ADMISSION.get();
    if (held != null && held.runtime() == current && held.admission().isReusable()) {
      rejectNestedIfUnusable(current, cancelled, failureMessages);
      return CancellableCallRunner.callAlreadyAdmitted(
          held.admission(),
          cancelled,
          current::isClosed,
          withApplicationClassLoader(current, operation),
          failureMessages);
    }
    return CancellableCallRunner.call(
        current.executor(),
        current.permits,
        cancelled,
        current::isClosed,
        withApplicationClassLoader(current, operation),
        failureMessages,
        MetadataIoRunner::recordSaturatedWait);
  }

  /** Run one blocking call off-thread without imposing cancellation or a new deadline. */
  <T> T callWithoutCancellation(
      Supplier<T> operation, CancellableCallRunner.FailureMessages failureMessages) {
    RuntimeState current = runtime();
    HeldAdmission held = HELD_ADMISSION.get();
    if (held != null && held.runtime() == current && held.admission().isReusable()) {
      rejectNestedIfUnusable(current, null, failureMessages);
      return operation.get();
    }
    return CancellableCallRunner.callWithoutCancellation(
        current.executor(),
        current.permits,
        current::isClosed,
        withApplicationClassLoader(current, operation),
        failureMessages,
        MetadataIoRunner::recordSaturatedWait);
  }

  private record HeldAdmission(
      RuntimeState runtime, CancellableCallRunner.AdmissionLease admission) {}

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
    // The shared runtime remains open through CDI teardown. No single bean owns its shutdown:
    // other application or singleton beans can still use metadata I/O from @PreDestroy.
    if (!ownsRuntime) {
      return;
    }
    if (!ownedRuntime.close()) {
      LOG.warn("metadata I/O executor did not terminate during shutdown");
    }
  }

  /**
   * Close the shared runtime for a controlled, non-CDI lifecycle transition.
   *
   * <p>CDI must not call this during shutdown: its event and destruction phases precede potential
   * teardown consumers. This hook instead supports isolated tests and embedding code that has
   * already stopped every metadata-I/O consumer.
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
    // registered at startup and a first scrape can arrive before any admitted call, so with nothing
    // installed it would publish NaN during the drain.
    // Never started, so this closes without touching a pool.
    if (SHARED.get() == null) {
      // Guarded because the argument would be evaluated on every shutdown: createSharedRuntime()
      // reads and validates config. Avoid that entirely when a runtime already exists.
      SHARED.compareAndSet(null, createSharedRuntime());
    }
    RuntimeState closing = SHARED.get();
    if (closing != null) {
      if (!closing.close()) {
        LOG.warn("metadata I/O executor did not terminate during shutdown");
      }
    }
  }

  /**
   * Re-arm the shared runtime for a new application lifecycle.
   *
   * <p>Clearing the latch never revives or discards a closed runtime. {@link #sharedRuntime()}
   * replaces it only after its executor has actually terminated, so a restart cannot overlap a
   * timed-out predecessor. This hook exists for the explicit, non-CDI lifecycle used by embedding
   * code and tests; CDI deliberately leaves the shared runtime available through bean teardown.
   */
  static void reopenSharedRuntime() {
    SHUTDOWN_LATCHED.set(false);
  }

  private static RuntimeState createSharedRuntime() {
    int configuredCapacity = configuredCapacity();
    ProcessWideAdmission.State admission = ProcessWideAdmission.resolve(configuredCapacity);
    if (admission.capacity() != configuredCapacity) {
      LOG.warnf(
          "metadata I/O capacity is fixed at %d until the JVM restarts; ignoring configured %d",
          admission.capacity(), configuredCapacity);
    }
    return new RuntimeState(admission.capacity(), admission.permits());
  }

  /**
   * The process-wide runtime, replaceable across application lifecycles.
   *
   * <p>Not a {@code static final} holder because {@code closed} is sticky. The closed instance
   * stays installed through its drain so every caller observes the same rejection; after a
   * controlled reopen and actual executor termination, {@link #sharedRuntime()} replaces it. Still
   * lazy, so an application that never reads metadata never starts a pool.
   */
  private static final java.util.concurrent.atomic.AtomicReference<RuntimeState> SHARED =
      new java.util.concurrent.atomic.AtomicReference<>();

  private static RuntimeState sharedRuntime() {
    while (true) {
      RuntimeState current = SHARED.get();
      if (current != null && !current.isClosed()) {
        return current;
      }
      // A shutdown may time out while a store client ignores interruption. That call still owns a
      // permit (and its application generation) until it returns, so the next lifecycle must not
      // build another executor alongside it. Keep returning the closed runtime until termination;
      // its normal admission path rejects every new call.
      if (current != null && current.isTerminationPending()) {
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
          // Latched between the check above and this CAS. Undo rather than hand back a runtime an
          // explicit close will never see: close() on a never-started runtime touches nothing,
          // since the pool is built lazily by executor().
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
    // Retained after close only while an interruption-insensitive call is still running. It lets a
    // future lifecycle distinguish a fully stopped generation from one that must continue
    // rejecting work.
    private volatile ThreadPoolExecutor closingExecutor;
    // Read by waiting callers outside this object's monitor, so closure is visible to them
    // promptly. One-way: there is no reopen path.
    private volatile boolean closed;

    private RuntimeState(int capacity) {
      this(capacity, null);
    }

    private RuntimeState(int capacity, Semaphore sharedPermits) {
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
      this.permits = sharedPermits != null ? sharedPermits : new Semaphore(capacity);
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

    private boolean isTerminationPending() {
      ThreadPoolExecutor closing = closingExecutor;
      return closing != null && !closing.isTerminated();
    }

    private boolean close() {
      ThreadPoolExecutor closing;
      // Latch and detach under the monitor, then release it before waiting. start() contends on
      // this same monitor, so awaiting termination while holding it would park every concurrent
      // caller for the shutdown timeout before they could read `closed` and be rejected — the
      // opposite of the prompt rejection this runtime promises.
      synchronized (this) {
        closed = true;
        closing = executor != null ? executor : closingExecutor;
        executor = null;
        if (closing != null) {
          closingExecutor = closing;
        }
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
        return closingExecutor == null || closingExecutor.isTerminated();
      }
      // A queued call already holds its permit but will never run its finally, so releasing the
      // discarded tasks is this runtime's job, not the pool factory's.
      boolean terminated =
          MetadataIoExecutors.shutdownNowAndAwait(
              closing,
              CancellableCallRunner::cancelDiscardedTasks,
              SHUTDOWN_TIMEOUT_SECONDS,
              TimeUnit.SECONDS);
      if (terminated) {
        closingExecutor = null;
      }
      return terminated;
    }
  }
}
