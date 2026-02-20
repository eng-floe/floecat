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

package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.NAMESPACE;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class PgCatalogTestSupport {

  static final EngineContext ENGINE_CTX = EngineContext.of("floedb", "1.0");

  private PgCatalogTestSupport() {}

  static SystemObjectScanContext contextWithNamespaces(NamespaceNode... namespaces) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    boolean hasUser = false;

    for (NamespaceNode ns : namespaces) {
      overlay.addNode(ns);
      if (ns != null && ns.id() != null && !SystemResourceIdGenerator.isSystemId(ns.id())) {
        hasUser = true;
      }
    }

    return new SystemObjectScanContext(
        overlay, null, hasUser ? userCatalogId() : catalogId(), ENGINE_CTX);
  }

  static SystemObjectScanContext contextWithRelations(RelationNode... nodes) {
    boolean hasUser =
        Arrays.stream(nodes)
            .map(RelationNode::id)
            .anyMatch(id -> id != null && !SystemResourceIdGenerator.isSystemId(id));

    NamespaceNode ns = hasUser ? userPgCatalogNamespace() : systemPgCatalogNamespace();
    return contextWithRelations(ns, nodes);
  }

  static SystemObjectScanContext contextWithRelations(
      NamespaceNode namespace, RelationNode... nodes) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    overlay.addNode(namespace);
    for (RelationNode node : nodes) {
      overlay.addRelation(namespace.id(), node);
    }
    return new SystemObjectScanContext(overlay, null, namespace.catalogId(), ENGINE_CTX);
  }

  static SystemObjectScanContext contextWithSystemNodes(GraphNode... nodes) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    for (GraphNode node : nodes) {
      overlay.addNode(node);
    }
    return new SystemObjectScanContext(overlay, null, catalogId(), ENGINE_CTX);
  }

  static SystemObjectScanContext systemCatalogContext(
      NamespaceNode namespace,
      List<TypeNode> types,
      List<TableNode> tables,
      List<SchemaColumn> schema) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    overlay.addNode(namespace);
    for (TypeNode type : types) {
      overlay.addType(namespace.id(), type);
    }
    for (TableNode table : tables) {
      overlay.addRelation(namespace.id(), table);
      overlay.setTableSchema(table.id(), schema);
    }
    return new SystemObjectScanContext(overlay, null, namespace.catalogId(), ENGINE_CTX);
  }

  static NamespaceNode systemNamespace(String name, Map<EngineHintKey, EngineHint> hints) {
    return new NamespaceNode(
        PgCatalogTestIds.namespace(name),
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        hints);
  }

  static NamespaceNode userNamespace(String name, Map<EngineHintKey, EngineHint> hints) {
    return new NamespaceNode(
        userNamespaceId(name),
        1,
        Instant.EPOCH,
        userCatalogId(),
        List.of(),
        name,
        GraphNodeOrigin.USER,
        Map.of(),
        hints);
  }

  static NamespaceNode systemPgCatalogNamespace() {
    return new NamespaceNode(
        PgCatalogTestIds.namespace("pg_catalog"),
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        "pg_catalog",
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Map.of(
            new EngineHintKey("floedb", "1.0", NAMESPACE.type()),
            new EngineHint(
                NAMESPACE.type(),
                FloeNamespaceSpecific.newBuilder()
                    .setOid(11)
                    .setNspname("pg_catalog")
                    .build()
                    .toByteArray())));
  }

  static NamespaceNode userPgCatalogNamespace() {
    ResourceId userCatalogId = userCatalogId();
    ResourceId userNsId = userNamespaceId("pg_catalog");
    return new NamespaceNode(
        userNsId,
        1,
        Instant.EPOCH,
        userCatalogId,
        List.of(),
        "pg_catalog",
        GraphNodeOrigin.USER,
        Map.of(),
        Map.of());
  }

  static TableNode systemTable(
      ResourceId namespaceId,
      String name,
      List<SchemaColumn> columns,
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> hints) {
    return new SystemTableNode.FloeCatSystemTableNode(
        PgCatalogTestIds.table(name),
        1,
        Instant.EPOCH,
        "15",
        name,
        namespaceId,
        columns,
        columnHints,
        hints,
        "scanner");
  }

  static TableNode userTable(
      ResourceId namespaceId,
      String name,
      List<SchemaColumn> columns,
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints,
      Map<EngineHintKey, EngineHint> hints) {
    return new UserTableNode(
        PgCatalogTestIds.userTable(name),
        1,
        Instant.EPOCH,
        userCatalogId(),
        namespaceId,
        name,
        TableFormat.TF_ICEBERG,
        ColumnIdAlgorithm.CID_FIELD_ID,
        "",
        Map.of(),
        List.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        hints,
        columnHints);
  }

  static FunctionNode systemFunction(
      ResourceId namespaceId,
      String name,
      boolean aggregate,
      boolean window,
      Map<EngineHintKey, EngineHint> hints) {
    return new FunctionNode(
        PgCatalogTestIds.function(name),
        1,
        Instant.EPOCH,
        "15",
        namespaceId,
        name,
        List.of(),
        null,
        aggregate,
        window,
        hints);
  }

  static SystemObjectScanContext contextWithFunctions(
      NamespaceNode namespace, FunctionNode... functions) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    overlay.addNode(namespace);
    for (FunctionNode function : functions) {
      overlay.addFunction(namespace.id(), function);
    }
    return new SystemObjectScanContext(
        overlay, NameRef.getDefaultInstance(), namespace.catalogId(), ENGINE_CTX);
  }

  static TypeNode systemType(String name, Map<EngineHintKey, EngineHint> hints) {
    return new TypeNode(
        PgCatalogTestIds.type(nameRef(name)),
        1,
        Instant.EPOCH,
        "15",
        name,
        "U",
        false,
        null,
        hints);
  }

  static NameRef nameRef(String qualified) {
    if (qualified == null || qualified.isBlank()) {
      return NameRef.getDefaultInstance();
    }
    String[] parts = qualified.split("\\.");
    return NameRefUtil.name(parts);
  }

  static ResourceId catalogId() {
    return PgCatalogTestIds.catalog();
  }

  static ResourceId userCatalogId() {
    return PgCatalogTestIds.userCatalog();
  }

  static ResourceId userNamespaceId(String name) {
    return PgCatalogTestIds.userNamespace(name);
  }
}
