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

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
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
            10L, 0L, 0L, List.of(), "", null, 0L, null, java.util.Map.of(), 0, java.util.Map.of());
    Optional<?> fromBundle = connector.snapshotConstraints("ns", "tbl", tableId, bundle);
    assertTrue(fromBundle.isEmpty());
  }

  private static final class LegacyConnector implements FloecatConnector {

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
    public List<SnapshotBundle> enumerateSnapshotsWithStats(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        Set<String> includeColumns) {
      return List.of();
    }

    @Override
    public void close() {}
  }
}
