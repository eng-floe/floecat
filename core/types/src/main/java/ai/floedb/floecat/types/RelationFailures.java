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

package ai.floedb.floecat.types;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The rule for a call that builds or scans relations one by one: an infrastructure or control-flow
 * failure fails the whole call; every other failure is that relation's own error.
 *
 * <p>Per-relation code applies it by outcome. A failure reported as the relation's error, in a
 * response row or a per-item result, goes through {@code GrpcErrors.relationFailure}, which also
 * gives that error. A failure that leaves the relation out goes through {@link #isolate} directly,
 * or through {@code SystemObjectScanContext.forRelation} in system scans. Both name the relation
 * with a {@link #label}.
 */
public final class RelationFailures {
  private static final Logger LOG = Logger.getLogger(RelationFailures.class.getName());
  private static final long WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(5);
  static final int MAX_WARNED_RELATIONS = 4096;
  static final ConcurrentMap<String, Long> LAST_WARNED = new ConcurrentHashMap<>();
  // When the oldest entry of a full limiter expires; it is not swept for expired entries before.
  static volatile long nextSweepNanos = System.nanoTime();

  private RelationFailures() {}

  /**
   * Whether {@code failure}, or any exception in its cause chain, means the system failed rather
   * than the relation being built: an {@link InfrastructureFailure}, cancellation, a timeout, an
   * interrupt, a rejected task, a JVM {@link Error}, or a gRPC status whose code no single relation
   * raises.
   */
  public static boolean isInfrastructure(Throwable failure) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable current = failure; current != null && seen.add(current); ) {
      if (current instanceof InfrastructureFailure
          || current instanceof CancellationException
          || current instanceof TimeoutException
          || current instanceof InterruptedException
          || current instanceof RejectedExecutionException
          || current instanceof Error
          || endsTheCall(statusCode(current))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /** A relation known by its id, as a log line and the warning limiter name it. */
  public static String label(ResourceId relation) {
    return "account=" + relation.getAccountId() + " " + relation.getKind() + "=" + relation.getId();
  }

  /** A relation known by its name, as a log line and the warning limiter name it. */
  public static String label(String accountId, NameRef relation) {
    List<String> parts = new ArrayList<>();
    parts.add(relation.getCatalog());
    parts.addAll(relation.getPathList());
    parts.add(relation.getName());
    parts.removeIf(String::isBlank);
    return "account=" + accountId + " relation=" + String.join(".", parts);
  }

  /**
   * Keeps {@code failure} to the relation {@code label} names: rethrows it when it is
   * infrastructure, otherwise logs it. A NOT_FOUND, PERMISSION_DENIED, FAILED_PRECONDITION or
   * INVALID_ARGUMENT status logs at DEBUG; anything else logs at WARN, at most once per relation
   * every five minutes and while fewer than {@value #MAX_WARNED_RELATIONS} relations were warned
   * about in that time, and at DEBUG past that.
   */
  public static void isolate(String label, RuntimeException failure) {
    if (isInfrastructure(failure)) {
      throw failure;
    }
    String message = "Relation failed on its own, " + label;
    if (isExpected(failure)) {
      LOG.log(Level.FINE, message, failure);
      return;
    }
    long now = System.nanoTime();
    Long last = LAST_WARNED.get(label);
    if (last != null && now - last < WARN_INTERVAL_NANOS) {
      return;
    }
    if (LAST_WARNED.size() >= MAX_WARNED_RELATIONS) {
      if (now - nextSweepNanos >= 0) {
        sweep(now);
      }
      if (LAST_WARNED.size() >= MAX_WARNED_RELATIONS) {
        LOG.log(Level.FINE, message, failure);
        return;
      }
    }
    LAST_WARNED.put(label, now);
    LOG.log(Level.WARNING, message, failure);
  }

  /** Drops expired entries and notes when the oldest remaining one expires. */
  private static void sweep(long now) {
    long oldest = now;
    for (var entries = LAST_WARNED.values().iterator(); entries.hasNext(); ) {
      long at = entries.next();
      if (now - at >= WARN_INTERVAL_NANOS) {
        entries.remove();
      } else if (at - oldest < 0) {
        oldest = at;
      }
    }
    nextSweepNanos = oldest + WARN_INTERVAL_NANOS;
  }

  private static boolean isExpected(Throwable failure) {
    Status.Code code = statusCode(failure);
    return code == Status.Code.NOT_FOUND
        || code == Status.Code.PERMISSION_DENIED
        || code == Status.Code.FAILED_PRECONDITION
        || code == Status.Code.INVALID_ARGUMENT;
  }

  private static Status.Code statusCode(Throwable failure) {
    if (failure instanceof StatusRuntimeException status) {
      return status.getStatus().getCode();
    }
    if (failure instanceof StatusException status) {
      return status.getStatus().getCode();
    }
    return null;
  }

  private static boolean endsTheCall(Status.Code code) {
    return code != null
        && switch (code) {
          case UNAVAILABLE,
              DEADLINE_EXCEEDED,
              CANCELLED,
              ABORTED,
              RESOURCE_EXHAUSTED,
              UNAUTHENTICATED,
              DATA_LOSS ->
              true;
          default -> false;
        };
  }
}
