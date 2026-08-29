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

package ai.floedb.floecat.reconciler.jobs;

import java.util.Map;
import java.util.TreeMap;

/**
 * Deployment-cohort affinity carried by a reconcile job's execution policy.
 *
 * <p>A cohort is a property of the deployment, not of any individual producer or executor. Both
 * sides of the durable queue are therefore stamped at the job-store boundary: the enqueue path
 * applies {@link #applyTo} so every job records its owning cohort, and the lease path forces that
 * cohort onto every {@link ReconcileJobStore.LeaseRequest}. Two deployments configured with
 * different cohorts can then share one durable queue without leasing each other's job trees.
 *
 * <p>Set a cohort to the job-tree contract version rather than the build version: versions that can
 * safely process each other's trees should share a cohort, and only a breaking change to the tree
 * should fork it. Coexisting deployments may share accounts and tables, so two cohorts will
 * independently capture the same table -- duplicated work is the deliberate price of neither
 * version touching the other's trees.
 *
 * <p>A cohort that goes away leaves its queued jobs claimable by nobody. The surviving cohort's
 * planner re-enqueues the equivalent work, so nothing is lost, but the abandoned rows are not
 * reclaimed by lease expiry or job GC (both of which only act on jobs that reached a terminal
 * state). Draining in-flight trees before retiring a cohort avoids the leak.
 */
public record ReconcileWorkerAffinity(String value) {
  public static final String ATTRIBUTE = "floecat.worker-affinity";
  public static final String DISABLED_STORAGE_KEY_SEGMENT = "_none_";
  public static final ReconcileWorkerAffinity DISABLED = new ReconcileWorkerAffinity("");

  /**
   * Separates the cohort from the underlying value in a ready-index filter. A control character
   * cannot collide with an operator-configured execution lane, and ready-index filter values are
   * percent-encoded into a single key segment, so it round-trips unchanged.
   */
  private static final char INDEX_FILTER_SEPARATOR = '\u001f';

  public ReconcileWorkerAffinity {
    value = normalize(value);
  }

  public static ReconcileWorkerAffinity of(String value) {
    String normalized = normalize(value);
    return normalized.isEmpty() ? DISABLED : new ReconcileWorkerAffinity(normalized);
  }

  /** Applies the server-owned affinity, removing any caller-supplied value when it is disabled. */
  public ReconcileExecutionPolicy applyTo(ReconcileExecutionPolicy policy) {
    ReconcileExecutionPolicy effective =
        policy == null ? ReconcileExecutionPolicy.defaults() : policy;
    Map<String, String> attributes = new TreeMap<>(effective.attributes());
    if (!enabled()) {
      attributes.remove(ATTRIBUTE);
    } else {
      attributes.put(ATTRIBUTE, value);
    }
    return ReconcileExecutionPolicy.of(effective.executionClass(), effective.lane(), attributes);
  }

  /** The cohort that owns a job, or empty when the job predates or disables affinity. */
  public static ReconcileWorkerAffinity fromPolicy(ReconcileExecutionPolicy policy) {
    if (policy == null) {
      return DISABLED;
    }
    return of(policy.attributes().get(ATTRIBUTE));
  }

  public boolean enabled() {
    return !value.isEmpty();
  }

  /** A non-blank key segment for affinity-qualified storage indexes. */
  public String storageKeySegment() {
    return enabled() ? value : DISABLED_STORAGE_KEY_SEGMENT;
  }

  public boolean matches(ReconcileExecutionPolicy policy) {
    return equals(fromPolicy(policy));
  }

  /**
   * Qualifies a ready-index filter value with the owning cohort so each cohort scans its own slice
   * of the execution-class, execution-lane, pinned-executor and job-kind indexes instead of one
   * shared slice.
   */
  public String indexFilterValue(String filterValue) {
    // Strip the separator from both halves so an operator-configured lane or cohort cannot compose
    // a value that collides with another cohort's slice.
    String normalizedCohort = stripSeparator(value);
    String normalizedFilterValue = stripSeparator(filterValue == null ? "" : filterValue);
    return normalizedCohort.isEmpty()
        ? normalizedFilterValue
        : normalizedCohort + INDEX_FILTER_SEPARATOR + normalizedFilterValue;
  }

  private static String stripSeparator(String value) {
    return value.indexOf(INDEX_FILTER_SEPARATOR) < 0
        ? value
        : value.replace(String.valueOf(INDEX_FILTER_SEPARATOR), "");
  }

  private static String normalize(String value) {
    String normalized = value == null ? "" : value.trim();
    if (DISABLED_STORAGE_KEY_SEGMENT.equals(normalized)) {
      throw new IllegalArgumentException(
          "worker affinity '" + DISABLED_STORAGE_KEY_SEGMENT + "' is reserved");
    }
    return normalized;
  }
}
