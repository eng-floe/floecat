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

package ai.floedb.floecat.service.cache;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CacheFamily;
import ai.floedb.floecat.cache.CaffeineMemoryCache;
import ai.floedb.floecat.cache.MemoryCache;
import ai.floedb.floecat.cache.WeightedValue;
import ai.floedb.floecat.catalog.rpc.ColumnEngineHints;
import ai.floedb.floecat.catalog.rpc.EngineHintPayload;
import ai.floedb.floecat.catalog.rpc.RelationHintsResource;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.repo.impl.RelationHintsRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Process-wide cache and durable merge point for user-relation engine hints.
 *
 * <p>Callers provide a relation and an engine; this module owns storage keys, immutable-body cache
 * keys, legacy fallback, conversion to the runtime model, and optimistic merge retries. A hint body
 * is keyed by its blob URI as well as the relation's DDL identity, so another replica's write moves
 * the pointer to a new cache key without invalidation.
 */
public final class HintCache {

  private static final long KEY_OBJECT_BYTES = 64L;

  private final RelationHintsRepository repository;
  private final MemoryCache<Key, CachedHints> entries;
  private final boolean enabled;

  HintCache(
      RelationHintsRepository repository, long maxBytes, CacheEvents events, boolean enabled) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.entries =
        new CaffeineMemoryCache<>(CacheFamily.HINT, maxBytes, HintCache::estimatedKeyBytes, events);
    this.enabled = enabled;
  }

  public static HintCache forTesting(RelationHintsRepository repository) {
    return new HintCache(repository, 64L * 1024L * 1024L, CacheEvents.none(), true);
  }

  /** Attach the requested engine's persisted hints without exposing cache or storage to callers. */
  public RelationNode attach(RelationNode node, EngineContext context) {
    Objects.requireNonNull(node, "node");
    if (node.origin() != GraphNodeOrigin.USER || context == null || !context.hasEngineKind()) {
      return node;
    }
    String engineKind = context.normalizedKind();
    String engineVersion = context.normalizedVersion();
    Optional<CachedHints> stored =
        repository.getThrough(
            node.id(),
            engineKind,
            engineVersion,
            blobUri -> readBody(node.id(), engineKind, engineVersion, blobUri));
    Hints hints =
        stored
            .filter(cached -> node.cacheIdentity().equals(cached.relationIdentity()))
            .map(CachedHints::hints)
            .orElseGet(() -> readLegacy(node, engineKind, engineVersion));
    return withHints(node, hints);
  }

  /**
   * Whether the current resource already contains every supplied hint for this relation identity.
   *
   * <p>This is the warm-path guard for the runtime's best-effort persistence callback. The runtime
   * may submit hints that it just reused from the relation node; proving they are already current
   * through the query-serving pointer and decoded cache avoids entering the authoritative mutation
   * path merely to discover a no-op. A false answer is only a hint to attempt the durable merge
   * below; mutation correctness never relies on this read.
   */
  public boolean containsAll(
      ResourceId relationId,
      String relationIdentity,
      String engineKind,
      String engineVersion,
      String relationPayloadType,
      byte[] relationPayload,
      List<EngineHintPersistence.ColumnHint> columnHints) {
    Objects.requireNonNull(relationId, "relationId");
    if (relationIdentity == null || relationIdentity.isBlank()) {
      return false;
    }
    Optional<CachedHints> current =
        repository.getThrough(
            relationId,
            engineKind,
            engineVersion,
            blobUri -> readBody(relationId, engineKind, engineVersion, blobUri));
    return current
        .filter(cached -> relationIdentity.equals(cached.relationIdentity()))
        .map(
            cached ->
                containsAll(
                    cached.hints(),
                    relationPayloadType,
                    relationPayload,
                    columnHints,
                    engineKind,
                    engineVersion))
        .orElse(false);
  }

  /** Merge one decorator result and commit the immutable body durably. */
  public void persist(
      ResourceId relationId,
      MutationMeta relation,
      String engineKind,
      String engineVersion,
      String relationPayloadType,
      byte[] relationPayload,
      List<EngineHintPersistence.ColumnHint> columnHints) {
    Objects.requireNonNull(relationId, "relationId");
    Objects.requireNonNull(relation, "relation");
    String relationIdentity = requireText(relation.getBlobUri(), "relation blob identity");
    requireText(engineKind, "engineKind");
    Objects.requireNonNull(engineVersion, "engineVersion");

    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Optional<
              ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta<
                  RelationHintsResource>>
          current = repository.getForMutation(relationId, engineKind, engineVersion);
      RelationHintsResource base =
          current
              .map(
                  found ->
                      relationIdentity.equals(found.value().getRelationIdentity())
                          ? found.value()
                          : empty(relationId, relationIdentity, engineKind, engineVersion))
              .orElseGet(() -> empty(relationId, relationIdentity, engineKind, engineVersion));
      RelationHintsResource merged = merge(base, relationPayloadType, relationPayload, columnHints);
      if (merged.equals(base) && current.isPresent()) {
        return;
      }
      if (current.isEmpty()) {
        try {
          Optional<
                  ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta<
                      RelationHintsResource>>
              created = repository.create(merged, relation);
          // An empty result means the relation fence changed while we were creating the hint
          // resource. Re-read the relation and merge against the new version instead of silently
          // dropping an advisory hint produced for a live relation.
          if (created.isEmpty()) {
            continue;
          }
          return;
        } catch (BaseResourceRepository.NameConflictException
            | BaseResourceRepository.AbortRetryableException raced) {
          continue;
        }
      }
      Optional<MutationMeta> committed =
          repository.update(merged, current.get().meta().getPointerVersion(), relation);
      if (committed.isPresent()) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "relation hints changed during optimistic merge");
  }

  public CacheFamily family() {
    return entries.family();
  }

  public long bytes() {
    return entries.bytes();
  }

  public long entryCount() {
    return entries.entryCount();
  }

  public boolean enabled() {
    return enabled;
  }

  public void evictAccount(String accountId) {
    entries.evictPartition(key -> key.accountId().equals(accountId));
  }

  /** Remove every persisted and resident engine version after the relation itself is gone. */
  public void deleteRelation(ResourceId relationId) {
    Objects.requireNonNull(relationId, "relationId");
    repository.deleteAll(relationId);
    entries.evictPartition(
        key ->
            key.accountId().equals(relationId.getAccountId())
                && key.relationId().equals(relationId.getId()));
  }

  private Optional<CachedHints> readBody(
      ResourceId relationId, String engineKind, String engineVersion, String blobUri) {
    if (!enabled) {
      return repository.getByBlobUri(blobUri).map(HintCache::decoded);
    }
    Key key =
        new Key(relationId.getAccountId(), relationId.getId(), engineKind, engineVersion, blobUri);
    return Optional.ofNullable(
        entries.get(
            key, ignored -> repository.getByBlobUri(blobUri).map(HintCache::decoded).orElse(null)));
  }

  private Hints readLegacy(RelationNode node, String engineKind, String engineVersion) {
    if (!enabled) {
      return legacyHints(node, engineKind, engineVersion);
    }
    Key key =
        new Key(
            node.id().getAccountId(),
            node.id().getId(),
            engineKind,
            engineVersion,
            node.cacheIdentity());
    return entries
        .get(
            key,
            ignored ->
                new CachedHints(node.cacheIdentity(), legacyHints(node, engineKind, engineVersion)))
        .hints();
  }

  private static RelationHintsResource merge(
      RelationHintsResource base,
      String relationPayloadType,
      byte[] relationPayload,
      List<EngineHintPersistence.ColumnHint> columnHints) {
    RelationHintsResource.Builder builder = base.toBuilder();
    if (relationPayload != null) {
      builder.putRelationHints(
          requireText(relationPayloadType, "relationPayloadType"), payload(relationPayload));
    }
    if (columnHints != null) {
      for (var hint : columnHints) {
        if (hint == null || hint.payload() == null || hint.columnId() <= 0L) {
          continue;
        }
        long columnId = hint.columnId();
        ColumnEngineHints.Builder column =
            builder
                .getColumnHintsOrDefault(columnId, ColumnEngineHints.getDefaultInstance())
                .toBuilder();
        column.putHints(
            requireText(hint.payloadType(), "column payloadType"), payload(hint.payload()));
        builder.putColumnHints(columnId, column.build());
      }
    }
    return builder.build();
  }

  private static RelationHintsResource empty(
      ResourceId relationId, String relationIdentity, String engineKind, String engineVersion) {
    return RelationHintsResource.newBuilder()
        .setRelationId(relationId)
        .setRelationIdentity(relationIdentity)
        .setEngineKind(engineKind)
        .setEngineVersion(engineVersion)
        .build();
  }

  private static EngineHintPayload payload(byte[] bytes) {
    return EngineHintPayload.newBuilder().setPayload(ByteString.copyFrom(bytes)).build();
  }

  private static Hints decode(RelationHintsResource resource) {
    String engineKind = resource.getEngineKind();
    String engineVersion = resource.getEngineVersion();
    Map<EngineHintKey, EngineHint> relation = new LinkedHashMap<>();
    resource
        .getRelationHintsMap()
        .forEach(
            (payloadType, payload) ->
                relation.put(
                    new EngineHintKey(engineKind, engineVersion, payloadType),
                    hint(payloadType, payload)));
    Map<Long, Map<EngineHintKey, EngineHint>> columns = new LinkedHashMap<>();
    resource
        .getColumnHintsMap()
        .forEach(
            (columnId, values) -> {
              Map<EngineHintKey, EngineHint> decoded = new LinkedHashMap<>();
              values
                  .getHintsMap()
                  .forEach(
                      (payloadType, payload) ->
                          decoded.put(
                              new EngineHintKey(engineKind, engineVersion, payloadType),
                              hint(payloadType, payload)));
              columns.put(columnId, Map.copyOf(decoded));
            });
    return new Hints(Map.copyOf(relation), Map.copyOf(columns));
  }

  private static CachedHints decoded(RelationHintsResource resource) {
    return new CachedHints(resource.getRelationIdentity(), decode(resource));
  }

  private static EngineHint hint(String payloadType, EngineHintPayload payload) {
    byte[] bytes = payload.getPayload().toByteArray();
    return new EngineHint(payloadType, bytes, bytes.length, payload.getMetadataMap());
  }

  private static Hints legacyHints(RelationNode node, String engineKind, String engineVersion) {
    Map<String, String> properties =
        node instanceof UserTableNode table
            ? table.properties()
            : node instanceof ViewNode view ? view.properties() : Map.of();
    Map<EngineHintKey, EngineHint> relation = new LinkedHashMap<>();
    EngineHintMetadata.hintsFromProperties(properties)
        .forEach(
            (key, value) -> {
              if (matches(key, engineKind, engineVersion)) {
                relation.put(key, value);
              }
            });
    Map<Long, Map<EngineHintKey, EngineHint>> columns = new LinkedHashMap<>();
    EngineHintMetadata.columnHints(properties)
        .forEach(
            (columnId, values) -> {
              Map<EngineHintKey, EngineHint> selected = new LinkedHashMap<>();
              values.forEach(
                  (key, value) -> {
                    if (matches(key, engineKind, engineVersion)) {
                      selected.put(key, value);
                    }
                  });
              if (!selected.isEmpty()) {
                columns.put(columnId, Map.copyOf(selected));
              }
            });
    return new Hints(Map.copyOf(relation), Map.copyOf(columns));
  }

  private static boolean matches(EngineHintKey key, String engineKind, String engineVersion) {
    return key.engineKind().equals(engineKind) && key.engineVersion().equals(engineVersion);
  }

  private static boolean containsAll(
      Hints current,
      String relationPayloadType,
      byte[] relationPayload,
      List<EngineHintPersistence.ColumnHint> columnHints,
      String engineKind,
      String engineVersion) {
    if (relationPayload != null
        && !contains(
            current.relation(),
            new EngineHintKey(engineKind, engineVersion, relationPayloadType),
            relationPayload)) {
      return false;
    }
    if (columnHints == null) {
      return true;
    }
    for (var hint : columnHints) {
      if (hint == null || hint.payload() == null || hint.columnId() <= 0L) {
        continue;
      }
      if (!contains(
          current.columns().getOrDefault(hint.columnId(), Map.of()),
          new EngineHintKey(engineKind, engineVersion, hint.payloadType()),
          hint.payload())) {
        return false;
      }
    }
    return true;
  }

  private static boolean contains(
      Map<EngineHintKey, EngineHint> current, EngineHintKey key, byte[] payload) {
    EngineHint hint = current.get(key);
    return hint != null && Arrays.equals(hint.payload(), payload);
  }

  private static RelationNode withHints(RelationNode node, Hints hints) {
    if (hints.relation().isEmpty() && hints.columns().isEmpty()) {
      return node;
    }
    if (node instanceof UserTableNode table) {
      return table.withEngineHints(hints.relation(), hints.columns());
    }
    if (node instanceof ViewNode view) {
      return view.withEngineHints(hints.relation(), hints.columns());
    }
    return node;
  }

  private static long estimatedKeyBytes(Key key) {
    return KEY_OBJECT_BYTES
        + 2L
            * (key.accountId().length()
                + key.relationId().length()
                + key.engineKind().length()
                + key.engineVersion().length()
                + key.blobUri().length());
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private record CachedHints(String relationIdentity, Hints hints) implements WeightedValue {
    @Override
    public long estimatedWeightBytes() {
      return 32L + 2L * relationIdentity.length() + hints.estimatedWeightBytes();
    }
  }

  private record Hints(
      Map<EngineHintKey, EngineHint> relation, Map<Long, Map<EngineHintKey, EngineHint>> columns) {
    private long estimatedWeightBytes() {
      long bytes = 96L;
      for (Map.Entry<EngineHintKey, EngineHint> entry : relation.entrySet()) {
        bytes += estimatedHintBytes(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<Long, Map<EngineHintKey, EngineHint>> column : columns.entrySet()) {
        bytes += 64L;
        for (Map.Entry<EngineHintKey, EngineHint> entry : column.getValue().entrySet()) {
          bytes += estimatedHintBytes(entry.getKey(), entry.getValue());
        }
      }
      return bytes;
    }
  }

  private static long estimatedHintBytes(EngineHintKey key, EngineHint hint) {
    long metadataBytes = 0L;
    for (Map.Entry<String, String> entry : hint.metadata().entrySet()) {
      metadataBytes += 32L + 2L * (entry.getKey().length() + entry.getValue().length());
    }
    return 128L
        + 2L
            * (key.engineKind().length()
                + key.engineVersion().length()
                + key.payloadType().length()
                + hint.payloadType().length())
        + hint.payload().length
        + metadataBytes;
  }

  private record Key(
      String accountId,
      String relationId,
      String engineKind,
      String engineVersion,
      String blobUri) {}
}
