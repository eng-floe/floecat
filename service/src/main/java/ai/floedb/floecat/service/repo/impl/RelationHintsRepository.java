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

import ai.floedb.floecat.catalog.rpc.RelationHintsResource;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.RelationHintsKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.MetadataRepositoryFactory;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Durable current-value repository for one relation's hints under one exact engine version. */
@ApplicationScoped
public final class RelationHintsRepository {

  private final GenericResourceRepository<RelationHintsResource, RelationHintsKey> repo;
  private final PointerStore pointers;

  public RelationHintsRepository(PointerStore pointers, BlobStore blobs) {
    this(
        new GenericResourceRepository<>(
            pointers,
            blobs,
            Schemas.RELATION_HINTS,
            RelationHintsResource::parseFrom,
            RelationHintsResource::toByteArray,
            "application/x-protobuf"),
        pointers);
  }

  @Inject
  public RelationHintsRepository(MetadataRepositoryFactory repositories, PointerStore pointers) {
    this(
        repositories.create(
            Schemas.RELATION_HINTS,
            RelationHintsResource::parseFrom,
            RelationHintsResource::toByteArray,
            "application/x-protobuf"),
        pointers);
  }

  private RelationHintsRepository(
      GenericResourceRepository<RelationHintsResource, RelationHintsKey> repo,
      PointerStore pointers) {
    this.repo = repo;
    this.pointers = Objects.requireNonNull(pointers, "pointers");
  }

  public Optional<ResourceWithMeta<RelationHintsResource>> getForMutation(
      ResourceId relationId, String engineKind, String engineVersion) {
    return repo.getByKeyWithMetaForMutation(key(relationId, engineKind, engineVersion));
  }

  /** Resolve the current pointer while a caller-owned decoded cache reads the immutable body. */
  public Optional<RelationHintsResource> getThrough(
      ResourceId relationId,
      String engineKind,
      String engineVersion,
      Function<String, Optional<RelationHintsResource>> bodyReader) {
    return repo.getByKeyThrough(key(relationId, engineKind, engineVersion), bodyReader);
  }

  public Optional<RelationHintsResource> getByBlobUri(String blobUri) {
    return repo.getByBlobUriDecodedFresh(blobUri);
  }

  public Optional<ResourceWithMeta<RelationHintsResource>> create(
      RelationHintsResource hints, MutationMeta relation) {
    return repo.createWithMeta(hints, relationFence(relation), null);
  }

  public Optional<MutationMeta> update(
      RelationHintsResource hints, long expectedPointerVersion, MutationMeta relation) {
    return repo.updateWithMetaWhilePointersMatchAndBumpMarkers(
        hints, expectedPointerVersion, relationFence(relation));
  }

  public int deleteAll(ResourceId relationId) {
    return pointers.deleteByPrefix(
        Keys.relationHintsPointerPrefix(relationId.getAccountId(), relationId.getId()));
  }

  private static PointerConditions relationFence(MutationMeta relation) {
    if (relation == null
        || relation.getPointerKey().isBlank()
        || relation.getPointerVersion() <= 0L
        || relation.getBlobUri().isBlank()) {
      throw new IllegalArgumentException("relation mutation metadata must name a live version");
    }
    return new PointerConditions(
        Map.of(relation.getPointerKey(), relation.getPointerVersion()), Set.of(), Map.of());
  }

  private static RelationHintsKey key(
      ResourceId relationId, String engineKind, String engineVersion) {
    return new RelationHintsKey(
        relationId.getAccountId(),
        relationId.getId(),
        relationId.getKind(),
        engineKind,
        engineVersion);
  }
}
