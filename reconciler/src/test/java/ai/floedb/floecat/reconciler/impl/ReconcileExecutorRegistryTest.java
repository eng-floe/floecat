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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReconcileExecutorRegistryTest {

  @Test
  void selectsLowestPrioritySupportingExecutor() {
    ReconcileJobStore.LeasedJob lease = defaultLease(ReconcileExecutionPolicy.defaults(), "");

    ReconcileExecutor fallback = new TestExecutor("fallback", 100, true);
    ReconcileExecutor preferred = new TestExecutor("preferred", 10, true);

    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(fallback, preferred));

    assertThat(registry.executorFor(lease)).isPresent();
    assertThat(registry.executorFor(lease).orElseThrow().id()).isEqualTo("preferred");
  }

  @Test
  void skipsUnsupportedExecutor() {
    ReconcileJobStore.LeasedJob lease = defaultLease(ReconcileExecutionPolicy.defaults(), "");

    ReconcileExecutor unsupported = new TestExecutor("unsupported", 1, false);
    ReconcileExecutor supported = new TestExecutor("supported", 2, true);

    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(unsupported, supported));

    assertThat(registry.executorFor(lease)).isPresent();
    assertThat(registry.executorFor(lease).orElseThrow().id()).isEqualTo("supported");
  }

  @Test
  void returnsEmptyWhenNoExecutorSupportsLease() {
    ReconcileJobStore.LeasedJob lease = defaultLease(ReconcileExecutionPolicy.defaults(), "");

    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(new TestExecutor("unsupported", 1, false)));

    assertThat(registry.executorFor(lease)).isEmpty();
  }

  @Test
  void explicitExecutorIdWinsOverPriorityFallback() {
    ReconcileJobStore.LeasedJob lease = defaultLease(ReconcileExecutionPolicy.defaults(), "named");

    ReconcileExecutor preferredByPriority = new TestExecutor("preferred", 1, true);
    ReconcileExecutor explicitlyNamed = new TestExecutor("named", 100, true);

    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(preferredByPriority, explicitlyNamed));

    assertThat(registry.executorFor(lease)).isPresent();
    assertThat(registry.executorFor(lease).orElseThrow().id()).isEqualTo("named");
  }

  @Test
  void explicitExecutorIdDoesNotSilentlyFallBackWhenMissing() {
    ReconcileJobStore.LeasedJob lease = defaultLease(ReconcileExecutionPolicy.defaults(), "named");

    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(new TestExecutor("other", 1, true)));

    assertThat(registry.executorFor(lease)).isEmpty();
  }

  @Test
  void executionPolicyClassAndLaneConstrainSelection() {
    ReconcileJobStore.LeasedJob lease =
        defaultLease(
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "remote", Map.of()), "");

    ReconcileExecutor wrongLane = new TestExecutor("wrong-lane", 1, true, true, false, true);
    ReconcileExecutor matching = new TestExecutor("matching", 2, true, true, true, true);

    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(wrongLane, matching));

    assertThat(registry.executorFor(lease)).isPresent();
    assertThat(registry.executorFor(lease).orElseThrow().id()).isEqualTo("matching");
  }

  @Test
  void leaseRequestAggregatesSupportedClassesAndLanes() {
    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(
            List.of(
                new TestExecutor("default", 1, true, true, false, true),
                new TestExecutor("remote", 2, true, false, true, true)));

    var request = registry.leaseRequest();

    assertThat(request.executionClasses)
        .contains(
            ReconcileExecutionClass.DEFAULT,
            ReconcileExecutionClass.INTERACTIVE,
            ReconcileExecutionClass.BATCH,
            ReconcileExecutionClass.HEAVY);
    assertThat(request.lanes).contains("", "remote");
    assertThat(request.executorIds).contains("default", "remote");
  }

  @Test
  void leaseRequestTreatsEmptySupportedLanesAsWildcard() {
    ReconcileExecutor wildcardLaneExecutor =
        new ReconcileExecutor() {
          @Override
          public String id() {
            return "planner";
          }

          @Override
          public java.util.Set<String> supportedLanes() {
            return java.util.Set.of();
          }

          @Override
          public ExecutionResult execute(ExecutionContext context) {
            throw new UnsupportedOperationException();
          }
        };

    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(wildcardLaneExecutor));

    var request = registry.leaseRequest();

    assertThat(request.lanes).containsExactly(ReconcileJobStore.LeaseRequest.anyLaneToken());
    assertThat(
            request.matches(
                ReconcileExecutionPolicy.of(
                    ReconcileExecutionClass.HEAVY, "planner-lane", Map.of()),
                "",
                ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.PLAN_CONNECTOR))
        .isTrue();
  }

  @Test
  void reportsWhetherJobKindHasEnabledExecutor() {
    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(List.of(new TestExecutor("default", 1, true)));

    assertThat(
            registry.hasExecutorForJobKind(
                ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.PLAN_CONNECTOR))
        .isTrue();
  }

  @Test
  void disabledExecutorsAreExcludedFromRoutingAndLeaseAggregation() {
    ReconcileExecutorRegistry registry =
        new ReconcileExecutorRegistry(
            List.of(
                new TestExecutor("disabled", 1, true, true, true, false),
                new TestExecutor("enabled", 2, true, true, true, true)));

    assertThat(registry.orderedExecutors())
        .extracting(ReconcileExecutor::id)
        .containsExactly("enabled");
    assertThat(registry.leaseRequest().executorIds).containsExactly("enabled");
  }

  private static ReconcileJobStore.LeasedJob defaultLease(
      ReconcileExecutionPolicy executionPolicy, String pinnedExecutorId) {
    return new ReconcileJobStore.LeasedJob(
        "job-1",
        "acct",
        "connector",
        false,
        CaptureMode.METADATA_AND_STATS,
        ReconcileScope.empty(),
        executionPolicy,
        "lease-1",
        pinnedExecutorId,
        "");
  }

  private record TestExecutor(
      String id,
      int priority,
      boolean supports,
      boolean supportsHeavy,
      boolean supportsRemoteLane,
      boolean enabled)
      implements ReconcileExecutor {

    private TestExecutor(String id, int priority, boolean supports) {
      this(id, priority, supports, true, true, true);
    }

    @Override
    public boolean supports(ReconcileJobStore.LeasedJob lease) {
      return supports;
    }

    @Override
    public boolean enabled() {
      return enabled;
    }

    @Override
    public boolean supportsExecutionClass(ReconcileExecutionClass executionClass) {
      return executionClass != ReconcileExecutionClass.HEAVY || supportsHeavy;
    }

    @Override
    public boolean supportsLane(String lane) {
      return lane == null || lane.isBlank() || (supportsRemoteLane && "remote".equals(lane));
    }

    @Override
    public java.util.Set<ReconcileExecutionClass> supportedExecutionClasses() {
      java.util.EnumSet<ReconcileExecutionClass> classes =
          java.util.EnumSet.of(
              ReconcileExecutionClass.DEFAULT,
              ReconcileExecutionClass.INTERACTIVE,
              ReconcileExecutionClass.BATCH);
      if (supportsHeavy) {
        classes.add(ReconcileExecutionClass.HEAVY);
      }
      return classes;
    }

    @Override
    public java.util.Set<String> supportedLanes() {
      return supportsRemoteLane ? java.util.Set.of("", "remote") : java.util.Set.of("");
    }

    @Override
    public ExecutionResult execute(ExecutionContext context) {
      throw new UnsupportedOperationException();
    }
  }
}
