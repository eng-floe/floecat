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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class StandaloneJavaFileGroupExecutionRunnerTest {

  @Test
  void executePassesWorkerAuthorizationToCaptureEngine() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider = () -> Optional.of("Bearer worker-token");
    when(runner.captureEngineRegistry.capture(any())).thenReturn(CaptureEngineResult.empty());

    runner.execute(payload());

    ArgumentCaptor<CaptureEngineRequest> request =
        ArgumentCaptor.forClass(CaptureEngineRequest.class);
    org.mockito.Mockito.verify(runner.captureEngineRegistry).capture(request.capture());
    assertThat(request.getValue().authorizationToken()).contains("Bearer worker-token");
  }

  @Test
  void executeAllowsMissingWorkerAuthorization() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider = Optional::<String>empty;
    when(runner.captureEngineRegistry.capture(any())).thenReturn(CaptureEngineResult.empty());

    runner.execute(payload());

    ArgumentCaptor<CaptureEngineRequest> request =
        ArgumentCaptor.forClass(CaptureEngineRequest.class);
    org.mockito.Mockito.verify(runner.captureEngineRegistry).capture(request.capture());
    assertThat(request.getValue().authorizationToken()).isEmpty();
  }

  private static StandaloneFileGroupExecutionPayload payload() {
    return new StandaloneFileGroupExecutionPayload(
        "job-1",
        "lease-1",
        "parent-1",
        Connector.newBuilder().setKind(ConnectorKind.CK_ICEBERG).build(),
        "ns",
        "table",
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-id")
            .build(),
        1L,
        "plan-1",
        "group-1",
        List.of("s3://bucket/path/file.parquet"),
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
  }
}
