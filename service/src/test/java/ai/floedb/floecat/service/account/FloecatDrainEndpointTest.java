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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.service.account.AccountAssignment.AssignmentPhase;
import ai.floedb.floecat.service.account.AccountAssignment.Mode;
import ai.floedb.floecat.service.account.FloecatDrainEndpoint.Request;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.telemetry.TestObservability;
import io.quarkus.runtime.ShutdownEvent;
import java.util.List;
import org.junit.jupiter.api.Test;

class FloecatDrainEndpointTest {
  private static final String INCARNATION = "m/inc";
  private static final String A = "acct-a";

  private final InMemoryPointerStore raw = new InMemoryPointerStore();
  private final TestObservability observability = new TestObservability();

  private AccountAssignment managed() {
    return AccountAssignment.forTesting(
        Mode.MANAGED,
        "m",
        INCARNATION,
        raw,
        AccountAssignment.PartitionHooks.NONE,
        Runnable::run,
        observability);
  }

  private FloecatDrainEndpoint endpoint(AccountAssignment assignment) {
    FloecatDrainEndpoint endpoint = new FloecatDrainEndpoint();
    endpoint.assignment = assignment;
    endpoint.defaultTimeoutMs = 0L;
    return endpoint;
  }

  private static Request request(String method, boolean wait, String timeoutMs) {
    return new Request(method, wait, timeoutMs);
  }

  @Test
  void timeoutParameterIsValidatedAndClamped() {
    FloecatDrainEndpoint endpoint = endpoint(managed());
    endpoint.defaultTimeoutMs = 5_000L;

    assertThat(endpoint.timeoutMs(null)).isEqualTo(5_000L);
    assertThat(endpoint.timeoutMs(" ")).isEqualTo(5_000L);
    assertThat(endpoint.timeoutMs("250")).isEqualTo(250L);
    assertThat(endpoint.timeoutMs("-1")).isNull();
    assertThat(endpoint.timeoutMs("soon")).isNull();
    assertThat(endpoint.timeoutMs("99999999999999999"))
        .isEqualTo(FloecatDrainEndpoint.MAX_TIMEOUT_MS);
    assertThat(endpoint.timeoutMs("99999999999999999999")).as("overflow").isNull();
    assertThat(FloecatDrainEndpoint.clampTimeout(Long.MAX_VALUE))
        .isEqualTo(FloecatDrainEndpoint.MAX_TIMEOUT_MS);

    var rejected = endpoint.handle(request("GET", true, "-1"));
    assertThat(rejected.status()).isEqualTo(400);
    assertThat(endpoint.assignment.status().processDraining()).as("nothing drained").isFalse();
  }

  @Test
  void statusIsReportedWithoutDraining() {
    var response = endpoint(managed()).handle(request("GET", false, null));

    assertThat(response.status()).isEqualTo(200);
    assertThat(response.body()).contains("\"draining\":false", "\"memberId\":\"m\"");
  }

  @Test
  void drainRefusesNewWorkAndReturnsOnceInFlightWorkFinishes() {
    AccountAssignment assignment = managed();
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    FloecatDrainEndpoint endpoint = endpoint(assignment);
    var resolution = assignment.admitResolution(A);

    var begun = endpoint.handle(request("POST", false, null));
    assertThat(begun.status()).isEqualTo(202);
    assertThat(begun.body())
        .contains("\"draining\":true", "\"drained\":false", "\"activeResolutions\":1");
    assertThatThrownBy(() -> assignment.admitResolution(A))
        .isInstanceOf(PlanningPointerIndex.Ownership.NotOwnedException.class);

    var timedOut = endpoint.handle(request("GET", true, "0"));
    assertThat(timedOut.status()).isEqualTo(202);

    resolution.close();
    var drained = endpoint.handle(request("GET", true, "1000"));
    assertThat(drained.status()).isEqualTo(200);
    assertThat(drained.body()).contains("\"drained\":true", "\"drainingAccounts\":1");
    assertThat(raw.get(ai.floedb.floecat.service.repo.model.Keys.memberAssignmentIndex("m")))
        .map(pointer -> pointer.getVersion())
        .as("drain writes nothing")
        .contains(1L);
  }

  @Test
  void shutdownObserverDrainsWhenTheHookNeverArrived() {
    AccountAssignment assignment = managed();
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    FloecatDrainEndpoint endpoint = endpoint(assignment);

    endpoint.onShutdown(new ShutdownEvent());

    assertThat(assignment.status().processDraining()).isTrue();
    assertThatThrownBy(() -> assignment.admitResolution(A))
        .isInstanceOf(PlanningPointerIndex.Ownership.NotOwnedException.class);
  }
}
