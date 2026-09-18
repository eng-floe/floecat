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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FloecatConnectorCompatibilityTest {

  @Test
  void defaultSnapshotConstraintsMethodsAreBackwardCompatible() {
    FloecatConnector connector = new LegacyConnector();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();

    assertTrue(connector.snapshotConstraints("ns", "tbl", tableId, 10L).isEmpty());

    FloecatConnector.SnapshotBundle bundle =
        new FloecatConnector.SnapshotBundle(
            10L, 0L, 0L, "", null, 0L, null, java.util.Map.of(), 0, null);
    Optional<?> fromBundle = connector.snapshotConstraints("ns", "tbl", tableId, bundle);
    assertTrue(fromBundle.isEmpty());
    assertEquals(ColumnIdentityMap.getDefaultInstance(), bundle.columnIdentityMap());

    FloecatConnector.SnapshotEnumerationOptions options =
        new FloecatConnector.SnapshotEnumerationOptions(
            false,
            Set.of(9L),
            Set.of(10L),
            FloecatConnector.SnapshotSelectionKind.CURRENT,
            Set.of(),
            0);
    assertEquals(ColumnIdentityMap.getDefaultInstance(), options.previousColumnIdentityMap());
  }

  @Test
  void legacyConnectorReconcilesThroughTheLegacyCaptureMethod() {
    LegacyConnector connector = new LegacyConnector();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    CaptureEngineRequest request =
        new CaptureEngineRequest(
            ai.floedb.floecat.connector.rpc.Connector.getDefaultInstance(),
            "ns",
            "tbl",
            tableId,
            10L,
            "plan",
            "group",
            List.of("s3://bucket/file.parquet"),
            Set.of(),
            Set.of(),
            FloecatConnector.ColumnSelectorPolicy.defaults(),
            Set.of(),
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            () -> false,
            ColumnIdentityMap.newBuilder().setFingerprint("must-not-be-supplied").build());

    new JavaConnectorFileGroupCaptureAdapter()
        .capture(connector, request, (fileStats, pageIndexes) -> {});

    assertTrue(connector.legacyCaptureCalled);
  }

  private static final class LegacyConnector implements FloecatConnector {
    private boolean legacyCaptureCalled;

    @Override
    public String id() {
      return "legacy";
    }

    @Override
    public ConnectorFormat format() {
      return ConnectorFormat.CF_DELTA;
    }

    @Override
    public List<String> listNamespaces() {
      return List.of();
    }

    @Override
    public List<String> listTables(String namespaceFq) {
      return List.of();
    }

    @Override
    public TableDescriptor describe(String namespaceFq, String tableName) {
      return new TableDescriptor(
          namespaceFq, tableName, "", "{}", List.of(), null, java.util.Map.of());
    }

    @Override
    public List<SnapshotBundle> enumerateSnapshots(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        SnapshotEnumerationOptions options) {
      return List.of();
    }

    @Override
    public List<ai.floedb.floecat.catalog.rpc.TargetStatsRecord> captureSnapshotTargetStats(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        long snapshotId,
        Set<String> includeColumns) {
      return List.of();
    }

    @Override
    public FileGroupCaptureResult capturePlannedFileGroup(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        long snapshotId,
        Set<String> plannedFilePaths,
        Set<String> includeColumns,
        Set<String> indexColumns,
      Set<StatsTargetKind> includeTargetKinds,
      boolean captureIndexes,
      ColumnSelectorPolicy columnSelectorPolicy) {
      legacyCaptureCalled = true;
      return FileGroupCaptureResult.empty();
    }

    @Override
    public void close() {}
  }
}
