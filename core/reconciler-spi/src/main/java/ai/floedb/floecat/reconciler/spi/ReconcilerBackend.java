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
package ai.floedb.floecat.reconciler.spi;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Front door used by the reconciler regardless of deployment mode. */
public interface ReconcilerBackend {

  ResourceId ensureNamespace(ReconcileContext ctx, ResourceId catalogId, NameRef namespace);

  ResourceId ensureTable(
      ReconcileContext ctx, ResourceId namespaceId, NameRef table, TableSpecDescriptor descriptor);

  Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table);

  SnapshotPin snapshotPinFor(
      ReconcileContext ctx, ResourceId tableId, SnapshotRef ref, Optional<Timestamp> asOf);

  Optional<Snapshot> fetchSnapshot(ReconcileContext ctx, ResourceId tableId, long snapshotId);

  void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot);

  boolean statsAlreadyCaptured(ReconcileContext ctx, ResourceId tableId, long snapshotId);

  void putTableStats(ReconcileContext ctx, ResourceId tableId, TableStats stats);

  void putColumnStats(ReconcileContext ctx, List<ColumnStats> stats);

  void putFileColumnStats(ReconcileContext ctx, List<FileColumnStats> stats);

  String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId);

  String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId);

  Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId);

  void updateConnectorDestination(
      ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination);

  record TableSpecDescriptor(
      String namespaceFq,
      String displayName,
      String schemaJson,
      Map<String, String> properties,
      List<String> partitionKeys,
      ColumnIdAlgorithm columnIdAlgorithm,
      ConnectorFormat connectorFormat,
      ResourceId connectorId,
      String connectorUri,
      String sourceNamespace,
      String sourceTable) {}
}
