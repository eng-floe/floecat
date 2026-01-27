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

package ai.floedb.floecat.systemcatalog.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Planner-facing contract for builtin system tables. */
public sealed interface SystemTableNode extends TableNode
    permits SystemTableNode.FloeCatSystemTableNode,
        SystemTableNode.StorageSystemTableNode,
        SystemTableNode.EngineSystemTableNode,
        SystemTableNode.GenericSystemTableNode {

  List<SchemaColumn> columns();

  Map<String, Map<EngineHintKey, EngineHint>> columnHints();

  Map<EngineHintKey, EngineHint> engineHints();

  TableBackendKind backendKind();

  record FloeCatSystemTableNode(
      ResourceId id,
      long version,
      Instant metadataUpdatedAt,
      String engineVersion,
      String displayName,
      ResourceId namespaceId,
      List<SchemaColumn> columns,
      Map<String, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints,
      String scannerId)
      implements SystemTableNode {

    public FloeCatSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = normalizeColumnHints(columnHints);
      engineHints = normalizeEngineHints(engineHints);
      displayName = displayName == null ? "" : displayName;
      scannerId = scannerId == null ? "" : scannerId;
    }

    @Override
    public TableBackendKind backendKind() {
      return TableBackendKind.TABLE_BACKEND_KIND_FLOECAT;
    }

    @Override
    public GraphNodeOrigin origin() {
      return GraphNodeOrigin.SYSTEM;
    }
  }

  record StorageSystemTableNode(
      ResourceId id,
      long version,
      Instant metadataUpdatedAt,
      String engineVersion,
      String displayName,
      ResourceId namespaceId,
      List<SchemaColumn> columns,
      Map<String, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints,
      String storagePath)
      implements SystemTableNode {

    public StorageSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = normalizeColumnHints(columnHints);
      engineHints = normalizeEngineHints(engineHints);
      displayName = displayName == null ? "" : displayName;
      storagePath = storagePath == null ? "" : storagePath;
    }

    @Override
    public TableBackendKind backendKind() {
      return TableBackendKind.TABLE_BACKEND_KIND_STORAGE;
    }

    @Override
    public GraphNodeOrigin origin() {
      return GraphNodeOrigin.SYSTEM;
    }
  }

  record EngineSystemTableNode(
      ResourceId id,
      long version,
      Instant metadataUpdatedAt,
      String engineVersion,
      String displayName,
      ResourceId namespaceId,
      List<SchemaColumn> columns,
      Map<String, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints,
      String engineSpecificLabel)
      implements SystemTableNode {

    public EngineSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = normalizeColumnHints(columnHints);
      engineHints = normalizeEngineHints(engineHints);
      displayName = displayName == null ? "" : displayName;
      engineSpecificLabel = engineSpecificLabel == null ? "" : engineSpecificLabel;
    }

    @Override
    public TableBackendKind backendKind() {
      return TableBackendKind.TABLE_BACKEND_KIND_ENGINE;
    }

    @Override
    public GraphNodeOrigin origin() {
      return GraphNodeOrigin.SYSTEM;
    }
  }

  record GenericSystemTableNode(
      ResourceId id,
      long version,
      Instant metadataUpdatedAt,
      String engineVersion,
      String displayName,
      ResourceId namespaceId,
      List<SchemaColumn> columns,
      Map<String, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints,
      TableBackendKind backendKind)
      implements SystemTableNode {

    public GenericSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = normalizeColumnHints(columnHints);
      engineHints = normalizeEngineHints(engineHints);
      displayName = displayName == null ? "" : displayName;
    }

    @Override
    public GraphNodeOrigin origin() {
      return GraphNodeOrigin.SYSTEM;
    }
  }

  static Map<String, Map<EngineHintKey, EngineHint>> normalizeColumnHints(
      Map<String, Map<EngineHintKey, EngineHint>> hints) {
    if (hints == null || hints.isEmpty()) {
      return Map.of();
    }
    Map<String, Map<EngineHintKey, EngineHint>> normalized = new LinkedHashMap<>();
    for (Map.Entry<String, Map<EngineHintKey, EngineHint>> entry : hints.entrySet()) {
      Map<EngineHintKey, EngineHint> value =
          entry.getValue() == null ? Map.of() : Map.copyOf(entry.getValue());
      normalized.put(entry.getKey(), value);
    }
    return Map.copyOf(normalized);
  }

  static Map<EngineHintKey, EngineHint> normalizeEngineHints(Map<EngineHintKey, EngineHint> hints) {
    if (hints == null || hints.isEmpty()) {
      return Map.of();
    }
    return Map.copyOf(hints);
  }
}
