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

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.NamespaceKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class NamespaceRepository {

  private final GenericResourceRepository<Namespace, NamespaceKey> repo;

  @Inject
  public NamespaceRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.NAMESPACE,
            Namespace::parseFrom,
            Namespace::toByteArray,
            "application/x-protobuf");
  }

  public void create(Namespace namespace) {
    repo.create(namespace);
  }

  public boolean update(Namespace namespace, long expectedPointerVersion) {
    return repo.update(namespace, expectedPointerVersion);
  }

  public boolean delete(ResourceId namespaceResourceId) {
    return repo.delete(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public boolean deleteWithPrecondition(
      ResourceId namespaceResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Namespace> getById(ResourceId namespaceResourceId) {
    return repo.getByKey(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public Optional<Namespace> getByPath(
      String accountId, String catalogId, List<String> pathSegments) {
    return repo.get(Keys.namespacePointerByPath(accountId, catalogId, pathSegments));
  }

  public List<Namespace> list(
      String accountId,
      String catalogId,
      List<String> parentSegmentsOrEmpty,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String accountId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty);
    return repo.countByPrefix(prefix);
  }

  public List<ResourceId> listIds(String accountId, String catalogId) {
    // empty parent path -> all namespaces in catalog
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, List.of());
    List<Namespace> namespaces =
        repo.listByPrefix(prefix, Integer.MAX_VALUE, "", new StringBuilder());
    List<ResourceId> ids = new java.util.ArrayList<>(namespaces.size());
    for (Namespace ns : namespaces) {
      ids.add(ns.getResourceId());
    }
    return ids;
  }

  public MutationMeta metaFor(ResourceId namespaceResourceId) {
    return repo.metaFor(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId namespaceResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId namespaceResourceId) {
    return repo.metaForSafe(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }
}
