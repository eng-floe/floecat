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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedSnapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@ApplicationScoped
public class SnapshotMetadataService {

  @Inject SnapshotClient snapshotClient;
  @Inject SnapshotUpdateService updateService;

  public Response applySnapshotUpdates(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates,
      String idempotencyKey) {
    return updateService.applySnapshotUpdates(
        tableSupport, tableId, namespacePath, tableName, tableSupplier, updates, idempotencyKey);
  }

  public Response validateSnapshotUpdates(List<Map<String, Object>> updates) {
    return updateService.validateSnapshotUpdates(updates);
  }

  Response ensureSnapshotExists(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      ImportedSnapshot snapshot,
      String schemaJson,
      IcebergMetadata importedIcebergMetadata,
      String idempotencyKey) {
    return updateService.ensureSnapshotExists(
        tableSupport,
        tableId,
        namespacePath,
        tableName,
        tableSupplier,
        snapshot,
        schemaJson,
        importedIcebergMetadata,
        idempotencyKey);
  }

  public void deleteSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    updateService.deleteSnapshots(tableId, snapshotIds);
  }
}
