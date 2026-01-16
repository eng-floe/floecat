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

package ai.floedb.floecat.service.metagraph.loader;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Responsible for materialising immutable relation nodes from repository metadata.
 *
 * <p>MetadataGraph relies on this helper for both pointer metadata (`metaForSafe`) and the actual
 * protobuf → node conversions.
 */
@ApplicationScoped
public class NodeLoader {

  private static final Map<EngineKey, EngineHint> NO_ENGINE_HINTS = Map.of();

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;

  @Inject
  public NodeLoader(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository) {
    this.catalogRepository = catalogRepository;
    this.namespaceRepository = namespaceRepository;
    this.tableRepository = tableRepository;
    this.viewRepository = viewRepository;
  }

  public List<ResourceId> listCatalogIds(String accountId) {
    return catalogRepository.listIds(accountId);
  }

  public Optional<NamespaceNode> namespace(ResourceId id) {
    if (id.getKind() != ResourceKind.RK_NAMESPACE) return Optional.empty();
    return namespaceRepository
        .getById(id)
        .map(ns -> toNamespaceNode(ns, namespaceRepository.metaForSafe(id)));
  }

  public Optional<UserTableNode> table(ResourceId id) {
    if (id.getKind() != ResourceKind.RK_TABLE) return Optional.empty();
    return tableRepository.getById(id).map(t -> toTableNode(t, tableRepository.metaForSafe(id)));
  }

  public Optional<ViewNode> view(ResourceId id) {
    if (id.getKind() != ResourceKind.RK_VIEW) return Optional.empty();
    return viewRepository.getById(id).map(v -> toViewNode(v, viewRepository.metaForSafe(id)));
  }

  /** Loads the mutation metadata for the provided resource. */
  public Optional<MutationMeta> mutationMeta(ResourceId id) {
    try {
      ResourceKind kind = id.getKind();
      return switch (kind) {
        case RK_CATALOG -> Optional.of(catalogRepository.metaForSafe(id));
        case RK_NAMESPACE -> Optional.of(namespaceRepository.metaForSafe(id));
        case RK_TABLE -> Optional.of(tableRepository.metaForSafe(id));
        case RK_VIEW -> Optional.of(viewRepository.metaForSafe(id));
        default -> Optional.empty();
      };
    } catch (StorageNotFoundException snf) {
      return Optional.empty();
    }
  }

  /** Rehydrates the relation node for the provided metadata snapshot. */
  public Optional<GraphNode> load(ResourceId id, MutationMeta meta) {
    return switch (id.getKind()) {
      case RK_CATALOG -> catalogRepository.getById(id).map(catalog -> toCatalogNode(catalog, meta));
      case RK_NAMESPACE ->
          namespaceRepository.getById(id).map(namespace -> toNamespaceNode(namespace, meta));
      case RK_TABLE -> tableRepository.getById(id).map(table -> toTableNode(table, meta));
      case RK_VIEW -> viewRepository.getById(id).map(view -> toViewNode(view, meta));
      default -> Optional.empty();
    };
  }

  private CatalogNode toCatalogNode(Catalog catalog, MutationMeta meta) {
    return new CatalogNode(
        catalog.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        catalog.getDisplayName(),
        catalog.getPropertiesMap(),
        catalog.hasConnectorRef() ? Optional.of(catalog.getConnectorRef()) : Optional.empty(),
        catalog.hasPolicyRef() ? Optional.of(catalog.getPolicyRef()) : Optional.empty(),
        Optional.empty(),
        NO_ENGINE_HINTS);
  }

  private NamespaceNode toNamespaceNode(Namespace namespace, MutationMeta meta) {
    return new NamespaceNode(
        namespace.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        namespace.getCatalogId(),
        namespace.getParentsList(),
        namespace.getDisplayName(),
        GraphNodeOrigin.USER,
        namespace.getPropertiesMap(),
        Optional.empty(),
        NO_ENGINE_HINTS);
  }

  private UserTableNode toTableNode(Table table, MutationMeta meta) {
    UpstreamRef upstream =
        table.hasUpstream() ? table.getUpstream() : UpstreamRef.getDefaultInstance();
    TableFormat format = upstream.getFormat();
    return new UserTableNode(
        table.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        table.getCatalogId(),
        table.getNamespaceId(),
        table.getDisplayName(),
        format,
        upstream.getColumnIdAlgorithm(),
        table.getSchemaJson(),
        table.getPropertiesMap(),
        upstream.getPartitionKeysList(),
        Optional.<SnapshotRef>empty(),
        Optional.<SnapshotRef>empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        NO_ENGINE_HINTS);
  }

  private ViewNode toViewNode(View view, MutationMeta meta) {
    return new ViewNode(
        view.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        view.getCatalogId(),
        view.getNamespaceId(),
        view.getDisplayName(),
        view.getSql(),
        "",
        List.<SchemaColumn>of(),
        List.<ResourceId>of(),
        List.of(),
        view.getPropertiesMap(),
        Optional.empty(),
        NO_ENGINE_HINTS);
  }

  private static Instant toInstant(Timestamp ts) {
    if (ts == null) {
      return Instant.EPOCH;
    }
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
  }
}
