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

package ai.floedb.floecat.gateway.iceberg.minimal.services.table;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public interface TableBackend {
  ListTablesResponse list(
      String prefix, List<String> namespacePath, Integer pageSize, String pageToken);

  Table get(String prefix, List<String> namespacePath, String tableName);

  void exists(String prefix, List<String> namespacePath, String tableName);

  void delete(String prefix, List<String> namespacePath, String tableName);

  Snapshot currentSnapshot(String prefix, List<String> namespacePath, String tableName);

  ListSnapshotsResponse listSnapshots(String prefix, List<String> namespacePath, String tableName);

  Table create(
      String prefix,
      List<String> namespacePath,
      String tableName,
      JsonNode schema,
      String location,
      Map<String, String> properties,
      String idempotencyKey);

  void rename(
      String prefix,
      List<String> sourceNamespacePath,
      String sourceTableName,
      List<String> destinationNamespacePath,
      String destinationTableName);

  Table commitProperties(
      String prefix,
      List<String> namespacePath,
      String tableName,
      List<Map<String, Object>> updates);
}
