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

package ai.floedb.floecat.reconciler.spi.capture;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CaptureEngineRegistryTest {
  @Test
  void selectReturnsLowestPrioritySupportedEngine() {
    var fallback =
        new TestCaptureEngine(
            "fallback",
            100,
            CaptureEngineCapabilities.of(Set.of(FloecatConnector.StatsTargetKind.FILE), true),
            Optional.of(CaptureEngineResult.empty()));
    var preferred =
        new TestCaptureEngine(
            "preferred",
            1,
            CaptureEngineCapabilities.of(Set.of(FloecatConnector.StatsTargetKind.FILE), true),
            Optional.of(CaptureEngineResult.empty()));
    var registry = new CaptureEngineRegistry(List.of(fallback, preferred));

    assertThat(registry.select(request(true, Set.of(FloecatConnector.StatsTargetKind.FILE))))
        .hasValue(preferred);
  }

  @Test
  void captureFallsBackWhenPreferredEngineDeclinesRequest() {
    var declining =
        new TestCaptureEngine(
            "declining",
            1,
            CaptureEngineCapabilities.of(Set.of(FloecatConnector.StatsTargetKind.FILE), true),
            Optional.empty());
    var capturing =
        new TestCaptureEngine(
            "capturing",
            2,
            CaptureEngineCapabilities.of(Set.of(FloecatConnector.StatsTargetKind.FILE), true),
            Optional.of(
                CaptureEngineResult.of(
                    List.of(TargetStatsRecord.getDefaultInstance()), List.of(), List.of())));
    var registry = new CaptureEngineRegistry(List.of(declining, capturing));

    assertThat(registry.capture(request(false, Set.of(FloecatConnector.StatsTargetKind.FILE))))
        .extracting(CaptureEngineResult::statsRecords)
        .asList()
        .hasSize(1);
  }

  @Test
  void candidatesFilterOnRequestedOutputs() {
    var statsOnly =
        new TestCaptureEngine(
            "stats",
            1,
            CaptureEngineCapabilities.of(Set.of(FloecatConnector.StatsTargetKind.FILE), false),
            Optional.of(CaptureEngineResult.empty()));
    var mixed =
        new TestCaptureEngine(
            "mixed",
            2,
            CaptureEngineCapabilities.of(Set.of(FloecatConnector.StatsTargetKind.FILE), true),
            Optional.of(CaptureEngineResult.empty()));
    var registry = new CaptureEngineRegistry(List.of(statsOnly, mixed));

    assertThat(registry.candidates(request(true, Set.of(FloecatConnector.StatsTargetKind.FILE))))
        .extracting(CaptureEngine::id)
        .containsExactly("mixed");
  }

  @Test
  void capabilitiesExposeExecutionContractForRoutingAndReplacement() {
    CaptureEngineCapabilities capabilities =
        CaptureEngineCapabilities.of(
            Set.of(FloecatConnector.StatsTargetKind.TABLE, FloecatConnector.StatsTargetKind.FILE),
            true,
            false,
            true,
            CaptureEngineCapabilities.ExecutionScope.FILE_GROUP_ONLY,
            CaptureEngineCapabilities.ResultContract.COMPLETE_FILE_GROUP_OUTPUTS,
            CaptureEngineCapabilities.ExecutionRuntime.LOCAL_OR_REMOTE);

    assertThat(capabilities.supportsExpressionTargets()).isFalse();
    assertThat(capabilities.columnSelectors()).isTrue();
    assertThat(capabilities.executionScope())
        .isEqualTo(CaptureEngineCapabilities.ExecutionScope.FILE_GROUP_ONLY);
    assertThat(capabilities.resultContract())
        .isEqualTo(CaptureEngineCapabilities.ResultContract.COMPLETE_FILE_GROUP_OUTPUTS);
    assertThat(capabilities.executionRuntime())
        .isEqualTo(CaptureEngineCapabilities.ExecutionRuntime.LOCAL_OR_REMOTE);
  }

  @Test
  void capabilitiesRejectRequestsOutsideAdvertisedFileGroupContract() {
    CaptureEngineCapabilities capabilities =
        CaptureEngineCapabilities.of(
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            true,
            false,
            false,
            CaptureEngineCapabilities.ExecutionScope.FILE_GROUP_ONLY,
            CaptureEngineCapabilities.ResultContract.COMPLETE_FILE_GROUP_OUTPUTS,
            CaptureEngineCapabilities.ExecutionRuntime.LOCAL_OR_REMOTE);

    CaptureEngineRequest missingPlannedFiles =
        new CaptureEngineRequest(
            request(false, Set.of(FloecatConnector.StatsTargetKind.FILE)).sourceConnector(),
            "db",
            "orders",
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1")
                .build(),
            0L,
            "plan-1",
            "group-1",
            List.of(),
            Set.of(),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            false);
    CaptureEngineRequest columnSelectors =
        new CaptureEngineRequest(
            request(false, Set.of(FloecatConnector.StatsTargetKind.FILE)).sourceConnector(),
            "db",
            "orders",
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1")
                .build(),
            0L,
            "plan-1",
            "group-1",
            List.of("s3://bucket/file.parquet"),
            Set.of("c1"),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            false);

    assertThat(capabilities.supports(missingPlannedFiles)).isFalse();
    assertThat(capabilities.supports(columnSelectors)).isFalse();
    assertThat(capabilities.firstValidationError(missingPlannedFiles))
        .hasValue("request does not satisfy file-group execution scope");
    assertThat(capabilities.validationErrors(columnSelectors))
        .contains("column selectors are not supported");
  }

  @Test
  void requestNormalizesColumnSelectorsAndAdvertisesFileGroupExpectation() {
    CaptureEngineRequest request =
        new CaptureEngineRequest(
            Connector.newBuilder()
                .setResourceId(
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_CONNECTOR)
                        .setId("conn-1")
                        .build())
                .build(),
            "db",
            "orders",
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1")
                .build(),
            0L,
            "plan-1",
            "group-1",
            List.of("s3://bucket/file.parquet"),
            Set.of(" c1 ", "", "c1", "  "),
            Set.of(" idx ", "idx", ""),
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            false);

    assertThat(request.statsColumns()).containsExactly("c1");
    assertThat(request.indexColumns()).containsExactly("idx");
    assertThat(request.isFileGroupScoped()).isTrue();
    assertThat(request.expectsCompleteFileGroupOutputs()).isTrue();
  }

  private static CaptureEngineRequest request(
      boolean capturePageIndex, Set<FloecatConnector.StatsTargetKind> statsTargetKinds) {
    return new CaptureEngineRequest(
        Connector.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .setId("conn-1")
                    .build())
            .build(),
        "db",
        "orders",
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build(),
        0L,
        "plan-1",
        "group-1",
        List.of("s3://bucket/file.parquet"),
        Set.of(),
        Set.of(),
        statsTargetKinds,
        capturePageIndex);
  }

  private record TestCaptureEngine(
      String id,
      int priority,
      CaptureEngineCapabilities capabilities,
      Optional<CaptureEngineResult> result)
      implements CaptureEngine {
    @Override
    public Optional<CaptureEngineResult> capture(CaptureEngineRequest request) {
      return result;
    }
  }
}
