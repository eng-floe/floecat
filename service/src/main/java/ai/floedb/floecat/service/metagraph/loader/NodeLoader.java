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
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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

  // These fetch the pointer-only meta once and hydrate from the blob it names (via load), rather
  // than reading the canonical pointer twice — once in getById and again in pointerMetaForSafe.
  // Called per-item in listing loops, so the extra pointer read added up.

  public Optional<NamespaceNode> namespace(ResourceId id) {
    if (id.getKind() != ResourceKind.RK_NAMESPACE) return Optional.empty();
    return mutationMeta(id).flatMap(meta -> load(id, meta)).map(NamespaceNode.class::cast);
  }

  public Optional<UserTableNode> table(ResourceId id) {
    if (id.getKind() != ResourceKind.RK_TABLE) return Optional.empty();
    return mutationMeta(id).flatMap(meta -> load(id, meta)).map(UserTableNode.class::cast);
  }

  public Optional<ViewNode> view(ResourceId id) {
    if (id.getKind() != ResourceKind.RK_VIEW) return Optional.empty();
    return mutationMeta(id).flatMap(meta -> load(id, meta)).map(ViewNode.class::cast);
  }

  /**
   * Loads a table node from a specific immutable blob rather than the current pointer. Unlike
   * {@link #load}, there is no pointer fallback: a query pinned to this blob must read that exact
   * blob, so a miss returns empty (the caller fails hard) instead of silently drifting to current
   * state.
   */
  public Optional<UserTableNode> tableFromBlob(ResourceId id, String blobUri) {
    if (id.getKind() != ResourceKind.RK_TABLE || blobUri == null || blobUri.isEmpty()) {
      return Optional.empty();
    }
    MutationMeta meta = MutationMeta.newBuilder().setBlobUri(blobUri).build();
    // LIVE: this read's emptiness is the pin-integrity detector (the caller wraps it in
    // requirePinnedTableBlob); a still-resident decode must not mask a swept pinned blob.
    return tableRepository.getByBlobUriLive(blobUri).map(table -> toTableNode(table, meta));
  }

  /**
   * Loads the mutation metadata for the provided resource. Graph consumers only use the pointer
   * version (cache key) and timestamps, so this is a pointer-only read — no blob HEAD, blank etag.
   */
  public Optional<MutationMeta> mutationMeta(ResourceId id) {
    try {
      ResourceKind kind = id.getKind();
      return switch (kind) {
        case RK_CATALOG -> Optional.of(catalogRepository.pointerMetaForSafe(id));
        case RK_NAMESPACE -> Optional.of(namespaceRepository.pointerMetaForSafe(id));
        case RK_TABLE -> Optional.of(tableRepository.pointerMetaForSafe(id));
        case RK_VIEW -> Optional.of(viewRepository.pointerMetaForSafe(id));
        default -> Optional.empty();
      };
    } catch (StorageNotFoundException snf) {
      return Optional.empty();
    }
  }

  /**
   * Rehydrates the relation node for the provided metadata snapshot. The metadata already names the
   * blob, so the blob is fetched directly — skipping the pointer re-read {@code getById} would do —
   * and the node content stays consistent with the metadata's pointer version. Falls back to a
   * pointer-based read when the blob has moved (e.g. updated and garbage-collected in between).
   */
  public Optional<GraphNode> load(ResourceId id, MutationMeta meta) {
    return loadAt(id, meta, false)
        .or(() -> mutationMeta(id).flatMap(live -> reload(id, meta, live)));
  }

  /**
   * The blob {@code stale} named is gone; the LIVE pointer decides what that means. Moved → benign
   * supersede race: build from the blob the live pointer names (never {@code getById} with the
   * stale meta — stamping fresh content with a superseded URI poisons the content-keyed node
   * cache). Blank → the resource was dropped: genuine absence. Unchanged → the pointer dangles; one
   * live re-probe absorbs a content-addressed revert legitimately reusing the URI, then this fails
   * LOUD: a current pointer whose blob is lost is corruption, and reporting it as absence would
   * mask data loss and invite re-creating over the corrupt resource.
   */
  private Optional<GraphNode> reload(ResourceId id, MutationMeta stale, MutationMeta live) {
    if (live.getBlobUri().isBlank()) {
      return Optional.empty();
    }
    if (!live.getBlobUri().equals(stale.getBlobUri())) {
      return loadAt(id, live, false);
    }
    Optional<GraphNode> reread = loadAt(id, live, true);
    if (reread.isEmpty()) {
      throw new BaseResourceRepository.CorruptionException(
          "dangling pointer, missing blob: " + live.getBlobUri(), null);
    }
    return reread;
  }

  /**
   * One coherent (content, identity) load: the node is built from the blob {@code meta} names.
   * {@code live} bypasses the blob cache for the read whose emptiness is load-bearing — the
   * dangling-pointer verdict in {@link #reload}.
   */
  private Optional<GraphNode> loadAt(ResourceId id, MutationMeta meta, boolean live) {
    String blobUri = meta.getBlobUri();
    return switch (id.getKind()) {
      case RK_CATALOG ->
          (live
                  ? catalogRepository.getByBlobUriLive(blobUri)
                  : catalogRepository.getByBlobUri(blobUri))
              .map(catalog -> toCatalogNode(catalog, meta));
      case RK_NAMESPACE ->
          (live
                  ? namespaceRepository.getByBlobUriLive(blobUri)
                  : namespaceRepository.getByBlobUri(blobUri))
              .map(namespace -> toNamespaceNode(namespace, meta));
      case RK_TABLE ->
          (live ? tableRepository.getByBlobUriLive(blobUri) : tableRepository.getByBlobUri(blobUri))
              .map(table -> toTableNode(table, meta));
      case RK_VIEW ->
          (live ? viewRepository.getByBlobUriLive(blobUri) : viewRepository.getByBlobUri(blobUri))
              .map(view -> toViewNode(view, meta));
      default -> Optional.empty();
    };
  }

  private CatalogNode toCatalogNode(Catalog catalog, MutationMeta meta) {
    return new CatalogNode(
        catalog.getResourceId(),
        meta.getBlobUri(),
        catalog.getDisplayName(),
        catalog.getPropertiesMap(),
        catalog.hasConnectorRef() ? Optional.of(catalog.getConnectorRef()) : Optional.empty(),
        catalog.hasPolicyRef() ? Optional.of(catalog.getPolicyRef()) : Optional.empty(),
        Optional.empty(),
        Map.of());
  }

  private NamespaceNode toNamespaceNode(Namespace namespace, MutationMeta meta) {
    return new NamespaceNode(
        namespace.getResourceId(),
        meta.getBlobUri(),
        namespace.getCatalogId(),
        namespace.getParentsList(),
        namespace.getDisplayName(),
        GraphNodeOrigin.USER,
        namespace.getPropertiesMap(),
        Map.of());
  }

  private UserTableNode toTableNode(Table table, MutationMeta meta) {
    UpstreamRef upstream =
        table.hasUpstream() ? table.getUpstream() : UpstreamRef.getDefaultInstance();
    TableFormat format = upstream.getFormat();
    RelationHints hints = relationHints(table.getPropertiesMap());
    return new UserTableNode(
        table.getResourceId(),
        meta.getBlobUri(),
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
        List.of(),
        hints.engineHints(),
        hints.columnHints());
  }

  private ViewNode toViewNode(View view, MutationMeta meta) {
    RelationHints hints = relationHints(view.getPropertiesMap());
    return new ViewNode(
        view.getResourceId(),
        meta.getBlobUri(),
        view.getCatalogId(),
        view.getNamespaceId(),
        view.getDisplayName(),
        view.getSqlDefinitionsList(),
        view.getOutputColumnsList(),
        parseBaseRelations(view.getBaseRelationsList()),
        view.getCreationSearchPathList(),
        GraphNodeOrigin.USER,
        view.getPropertiesMap(),
        Optional.empty(),
        hints.columnHints(),
        hints.engineHints());
  }

  private static List<NameRef> parseBaseRelations(List<String> fqns) {
    return fqns.stream().map(NodeLoader::parseFqn).toList();
  }

  /**
   * Splits a fully-qualified name {@code "catalog[.path]*.name"} into a {@link NameRef}.
   *
   * <ul>
   *   <li>1 segment → {@code name} only
   *   <li>2 segments → {@code catalog} + {@code name}
   *   <li>3+ segments → {@code catalog}, middle segments as {@code path}, {@code name}
   * </ul>
   *
   * <p>Package-private for testing.
   */
  static NameRef parseFqn(String fqn) {
    String[] parts = fqn.split("\\.", -1);
    NameRef.Builder b = NameRef.newBuilder();
    if (parts.length == 1) {
      return b.setName(parts[0]).build();
    }
    b.setCatalog(parts[0]).setName(parts[parts.length - 1]);
    for (int i = 1; i < parts.length - 1; i++) {
      b.addPath(parts[i]);
    }
    return b.build();
  }

  static RelationHints relationHints(Map<String, String> properties) {
    Map<EngineHintKey, EngineHint> engineHints =
        containsHintKey(properties, "engine.hint.")
            ? EngineHintMetadata.hintsFromProperties(properties)
            : Map.of();
    Map<Long, Map<EngineHintKey, EngineHint>> columnHints =
        containsHintKey(properties, "engine.hint.column.")
            ? EngineHintMetadata.columnHints(properties)
            : Map.of();
    return new RelationHints(engineHints, columnHints);
  }

  static record RelationHints(
      Map<EngineHintKey, EngineHint> engineHints,
      Map<Long, Map<EngineHintKey, EngineHint>> columnHints) {}

  private static boolean containsHintKey(Map<String, String> properties, String prefix) {
    if (properties == null || properties.isEmpty()) {
      return false;
    }
    for (String key : properties.keySet()) {
      if (key != null && key.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
