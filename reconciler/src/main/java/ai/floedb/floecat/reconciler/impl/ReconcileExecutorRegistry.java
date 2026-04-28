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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Resolves the executor that should execute a leased reconcile job. */
@ApplicationScoped
public class ReconcileExecutorRegistry {
  private final List<ReconcileExecutor> executors;

  @Inject
  public ReconcileExecutorRegistry(Instance<ReconcileExecutor> executors) {
    this(executors == null ? List.of() : executors.stream().toList());
  }

  ReconcileExecutorRegistry(List<ReconcileExecutor> executors) {
    this.executors =
        (executors == null ? List.<ReconcileExecutor>of() : executors)
            .stream()
                .filter(ReconcileExecutor::enabled)
                .sorted(
                    Comparator.comparingInt(ReconcileExecutor::priority)
                        .thenComparing(ReconcileExecutor::id))
                .toList();
  }

  public Optional<ReconcileExecutor> executorFor(ReconcileJobStore.LeasedJob lease) {
    if (lease != null && lease.pinnedExecutorId != null && !lease.pinnedExecutorId.isBlank()) {
      return executors.stream()
          .filter(executor -> lease.pinnedExecutorId.equals(executor.id()))
          .filter(executor -> executor.supportsJobKind(lease.jobKind))
          .filter(
              executor -> executor.supportsExecutionClass(lease.executionPolicy.executionClass()))
          .filter(executor -> executor.supportsLane(lease.executionPolicy.lane()))
          .filter(executor -> executor.supports(lease))
          .findFirst();
    }
    return executors.stream()
        .filter(executor -> executor.supportsJobKind(lease.jobKind))
        .filter(executor -> executor.supportsExecutionClass(lease.executionPolicy.executionClass()))
        .filter(executor -> executor.supportsLane(lease.executionPolicy.lane()))
        .filter(executor -> executor.supports(lease))
        .findFirst();
  }

  public List<ReconcileExecutor> orderedExecutors() {
    return executors;
  }

  public boolean hasExecutorForJobKind(ReconcileJobKind jobKind) {
    ReconcileJobKind effectiveJobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
    return executors.stream().anyMatch(executor -> executor.supportsJobKind(effectiveJobKind));
  }

  public ReconcileJobStore.LeaseRequest leaseRequest() {
    Set<ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass> executionClasses =
        executors.stream()
            .flatMap(executor -> executor.supportedExecutionClasses().stream())
            .collect(
                java.util.stream.Collectors.toCollection(
                    () ->
                        EnumSet.noneOf(
                            ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass.class)));
    boolean wildcardLane =
        executors.stream().anyMatch(executor -> executor.supportedLanes().isEmpty());
    Set<String> lanes =
        wildcardLane
            ? Set.of(ReconcileJobStore.LeaseRequest.anyLaneToken())
            : executors.stream()
                .flatMap(executor -> executor.supportedLanes().stream())
                .map(lane -> lane == null ? "" : lane.trim())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    Set<String> executorIds =
        executors.stream()
            .map(ReconcileExecutor::id)
            .map(executorId -> executorId == null ? "" : executorId.trim())
            .filter(executorId -> !executorId.isEmpty())
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    Set<ReconcileJobKind> jobKinds =
        executors.stream()
            .flatMap(executor -> executor.supportedJobKinds().stream())
            .collect(
                java.util.stream.Collectors.toCollection(
                    () -> EnumSet.noneOf(ReconcileJobKind.class)));
    return ReconcileJobStore.LeaseRequest.of(executionClasses, lanes, executorIds, jobKinds);
  }
}
