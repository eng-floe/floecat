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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TableKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class TableRepository {

  private final GenericResourceRepository<Table, TableKey> repo;

  @Inject
  public TableRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TABLE,
            Table::parseFrom,
            Table::toByteArray,
            "application/x-protobuf");
  }

  public void create(Table table) {
    repo.create(table);
  }

  public boolean update(Table table, long expectedPointerVersion) {
    return repo.update(table, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableResourceId) {
    return repo.delete(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId tableResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Table> getById(ResourceId tableResourceId) {
    return repo.getByKey(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  public Optional<Table> getByName(
      String accountId, String catalogId, String namespaceId, String tableName) {
    return repo.get(Keys.tablePointerByName(accountId, catalogId, namespaceId, tableName));
  }

  public List<Table> list(
      String accountId,
      String catalogId,
      String namespaceId,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.tablePointerByNamePrefix(accountId, catalogId, namespaceId);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String accountId, String catalogId, String namespaceId) {
    return repo.countByPrefix(Keys.tablePointerByNamePrefix(accountId, catalogId, namespaceId));
  }

  /**
   * Scans the by-name pointer prefix for a namespace and returns lightweight refs without loading
   * blobs from S3. Falls back to key/blobUri parsing for legacy pointers that predate
   * Pointer.resource_id / display_name.
   */
  public List<RelationRef> listRefs(String accountId, String catalogId, String namespaceId) {
    String prefix = Keys.tablePointerByNamePrefix(accountId, catalogId, namespaceId);
    var pointers = repo.listRefsByPrefix(prefix);
    var refs = new ArrayList<RelationRef>(pointers.size());
    for (var p : pointers) {
      toRelationRef(accountId, ResourceKind.RK_TABLE, p).ifPresent(refs::add);
    }
    return refs;
  }

  /**
   * Reads the shared, kind-agnostic relation-name claim ({@link Keys#relationPointerByName}) and
   * returns the owning relation's id — kind {@code RK_TABLE} or {@code RK_VIEW} — in one pointer
   * read, with no blob fetch. Tables and views both reserve this claim on create/rename, so a hit
   * answers kind-agnostic name resolution outright. Empty when the claim is absent (rows created
   * before the claim existed) or carries no owner; callers must then fall back to the kind-specific
   * by-name probes.
   */
  public Optional<ResourceId> relationNameClaim(
      String accountId, String catalogId, String namespaceId, String name) {
    return repo.refByPointer(Keys.relationPointerByName(accountId, catalogId, namespaceId, name))
        .map(p -> p.getResourceId())
        .filter(rid -> !rid.getId().isEmpty())
        .filter(
            rid -> rid.getKind() == ResourceKind.RK_TABLE || rid.getKind() == ResourceKind.RK_VIEW);
  }

  /** Reads exact by-name table pointers and returns refs without fetching blobs from S3. */
  public List<RelationRef> listRefsByName(
      String accountId, String catalogId, String namespaceId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    List<RelationRef> refs = new ArrayList<>(names.size());
    for (String name : names) {
      if (name == null || name.isBlank()) {
        continue;
      }
      repo.refByPointer(Keys.tablePointerByName(accountId, catalogId, namespaceId, name))
          .flatMap(p -> toRelationRef(accountId, ResourceKind.RK_TABLE, p))
          .ifPresent(refs::add);
    }
    return refs;
  }

  static Optional<RelationRef> toRelationRef(
      String accountId, ResourceKind kind, ai.floedb.floecat.common.rpc.Pointer p) {
    String name =
        !p.getDisplayName().isEmpty() ? p.getDisplayName() : Keys.extractLastSegment(p.getKey());
    ResourceId rid = p.getResourceId();
    if (rid.getId().isEmpty()) {
      String rawId = Keys.extractResourceIdFromBlobUri(p.getBlobUri());
      if (rawId.isEmpty()) {
        return Optional.empty();
      }
      rid = ResourceId.newBuilder().setAccountId(accountId).setId(rawId).setKind(kind).build();
    }
    return Optional.of(new RelationRef(rid, name, kind));
  }

  public MutationMeta metaFor(ResourceId tableResourceId) {
    return repo.metaFor(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId tableResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableResourceId) {
    return repo.metaForSafe(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  /** Pointer-only meta (no blob HEAD, blank etag) for metadata-graph consumers. */
  public MutationMeta pointerMetaForSafe(ResourceId tableResourceId) {
    return repo.pointerMetaForSafe(
        new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  /** Blob-direct read for graph hydration from resolved metadata; empty if the blob moved. */
  public Optional<Table> getByBlobUri(String blobUri) {
    return repo.getByBlobUri(blobUri);
  }
}
