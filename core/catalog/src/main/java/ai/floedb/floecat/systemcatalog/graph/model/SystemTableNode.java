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
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Planner-facing contract for builtin system tables. */
public sealed interface SystemTableNode extends TableNode
    permits SystemTableNode.FloeCatSystemTableNode,
        SystemTableNode.StorageSystemTableNode,
        SystemTableNode.EngineSystemTableNode,
        SystemTableNode.GenericSystemTableNode {

  List<SchemaColumn> columns();

  Map<Long, Map<EngineHintKey, EngineHint>> columnHints();

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
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints,
      String scannerId)
      implements SystemTableNode {

    public FloeCatSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = RelationNode.normalizeColumnHints(columnHints);
      engineHints = GraphNode.normalizeEngineHints(engineHints);
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
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints,
      String storagePath,
      FlightEndpointRef flightEndpoint)
      implements SystemTableNode {

    public StorageSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = RelationNode.normalizeColumnHints(columnHints);
      engineHints = GraphNode.normalizeEngineHints(engineHints);
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
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints)
      implements SystemTableNode {

    public EngineSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = RelationNode.normalizeColumnHints(columnHints);
      engineHints = GraphNode.normalizeEngineHints(engineHints);
      displayName = displayName == null ? "" : displayName;
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
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> engineHints,
      TableBackendKind backendKind)
      implements SystemTableNode {

    public GenericSystemTableNode {
      columns = List.copyOf(columns);
      columnHints = RelationNode.normalizeColumnHints(columnHints);
      engineHints = GraphNode.normalizeEngineHints(engineHints);
      displayName = displayName == null ? "" : displayName;
    }

    @Override
    public GraphNodeOrigin origin() {
      return GraphNodeOrigin.SYSTEM;
    }
  }
}
