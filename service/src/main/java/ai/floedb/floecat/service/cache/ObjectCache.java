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
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.MessageLite;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The process-wide cache of decoded, engine-neutral SQL metadata.
 *
 * <p>Callers supply domain objects and immutable content identities; this module owns key layout,
 * weighing, single-flight loading and account eviction. Names, projections, engine decoration and
 * absence are deliberately not retained. Content-versioned entries need no mutation invalidation: a
 * writer publishes a new reachable identity and old entries remain useful to existing pins until
 * ordinary capacity eviction.
 *
 * <p>One {@link MemoryCache} and one byte budget serve every object kind. The private typed key
 * keeps schemas, relations, constraints and snapshot facts from colliding without creating a
 * separately tuned cache for each Java type.
 */
public final class ObjectCache {

  private static final long RETAINED_PROTO_FACTOR = 3L;
  private static final long RETAINED_SCHEMA_FACTOR_NUMERATOR = 677L;
  private static final long RETAINED_SCHEMA_FACTOR_DENOMINATOR = 100L;
  private static final long VALUE_WRAPPER_BYTES = 24L;
  private static final long KEY_OBJECT_BYTES = 48L;

  private final MemoryCache<Key, Value> entries;
  private final LogicalSchemaMapper schemaMapper;
  private final boolean enabled;

  ObjectCache(long maxBytes, CacheEvents events, boolean enabled) {
    this(maxBytes, events, new LogicalSchemaMapper(), enabled);
  }

  ObjectCache(
      long maxBytes, CacheEvents events, LogicalSchemaMapper schemaMapper, boolean enabled) {
    this.entries =
        new CaffeineMemoryCache<>(
            CacheFamily.OBJECT, maxBytes, ObjectCache::estimatedKeyBytes, events);
    this.schemaMapper = Objects.requireNonNull(schemaMapper, "schemaMapper");
    this.enabled = enabled;
  }

  /** An ordinary enabled cache for focused tests that exercise the production implementation. */
  public static ObjectCache forTesting() {
    return new ObjectCache(64L * 1024L * 1024L, CacheEvents.none(), true);
  }

  /** The DDL-shaped relation answer and the mapped schema needed to project and decorate it. */
  public record RelationObject(RelationInfo relation, SchemaDescriptor schema)
      implements WeightedValue {
    public RelationObject {
      Objects.requireNonNull(relation, "relation");
      Objects.requireNonNull(schema, "schema");
    }

    @Override
    public long estimatedWeightBytes() {
      // Conservative while the same descriptor is also reachable through the schema entry: if
      // that entry is evicted first, the relation still owns this reference and remains fully
      // charged. Shared descriptors may therefore be over-counted, never under-counted.
      return retainedMappedMetadataBytes(relation) + retainedMappedMetadataBytes(schema);
    }
  }

  /** The two small ingest-shaped values relation assembly needs from table-level statistics. */
  public record SnapshotFacts(OptionalLong rowCount, OptionalLong totalSizeBytes)
      implements WeightedValue {
    public SnapshotFacts {
      Objects.requireNonNull(rowCount, "rowCount");
      Objects.requireNonNull(totalSizeBytes, "totalSizeBytes");
    }

    @Override
    public long estimatedWeightBytes() {
      return 64L;
    }
  }

  /** Map a table schema once for its real mapping inputs, not once per snapshot or caller. */
  public SchemaDescriptor mappedSchema(UserTableNode table, String schemaJson) {
    Objects.requireNonNull(table, "table");
    String effectiveSchema =
        schemaJson == null || schemaJson.isBlank() ? table.schemaJson() : schemaJson;
    String identity = schemaIdentity(table, effectiveSchema);
    Key key = new Key(table.id().getAccountId(), Kind.SCHEMA, identity);
    return get(key, SchemaDescriptor.class, () -> schemaMapper.map(table, effectiveSchema));
  }

  /**
   * Resolve a pinned schema by the immutable identities already carried on the pin. The backing
   * table and snapshot are read only on a miss, which keeps callers from resolving cache
   * ingredients before asking Objects for the answer.
   */
  public SchemaDescriptor pinnedSchema(
      String correlationId, TablePin pin, CatalogGraphView graphView) {
    Objects.requireNonNull(correlationId, "correlationId");
    Objects.requireNonNull(pin, "pin");
    Objects.requireNonNull(graphView, "graphView");
    String schemaScope = pinnedSchemaScope(pin);
    String identity =
        Hashing.sha256Hex(
            requireIdentity(pin.getTableBlobUri(), "pinned table identity")
                + '\0'
                + requireIdentity(schemaScope, "pinned schema identity"));
    Key key = new Key(account(pin.getTableId()), Kind.SCHEMA, identity);
    return get(
        key,
        SchemaDescriptor.class,
        () -> {
          SnapshotRef snapshotRef =
              SnapshotRef.newBuilder().setSnapshotId(pin.getSnapshotId()).build();
          CatalogGraphView.SchemaResolution resolved =
              Objects.requireNonNull(
                  graphView.schemaFor(
                      correlationId,
                      pin.getTableId(),
                      snapshotRef,
                      pin.getTableBlobUri(),
                      pin.getSnapshotBlobUri()),
                  "pinned schema resolution returned null");
          if (!resolved.table().id().equals(pin.getTableId())) {
            throw new IllegalArgumentException("resolved schema does not match the pinned table");
          }
          String schemaJson = resolved.schemaJson();
          String effectiveSchema =
              schemaJson == null || schemaJson.isBlank()
                  ? resolved.table().schemaJson()
                  : schemaJson;
          return schemaMapper.map(resolved.table(), effectiveSchema);
        });
  }

  /** Load the full engine-neutral relation for one immutable user-table DDL identity. */
  public RelationObject tableRelation(
      UserTableNode table, Optional<TablePin> pin, Supplier<RelationObject> loader) {
    Objects.requireNonNull(table, "table");
    Optional<TablePin> effectivePin = Objects.requireNonNull(pin, "pin");
    String definitionIdentity =
        effectivePin
            .map(TablePin::getTableBlobUri)
            .filter(identity -> !identity.isBlank())
            .orElse(table.cacheIdentity());
    String schemaIdentity =
        effectivePin
            .map(ObjectCache::pinnedSchemaScope)
            .orElseGet(() -> schemaIdentity(table, table.schemaJson()));
    String identity =
        Hashing.sha256Hex(
            requireIdentity(definitionIdentity, "definition identity")
                + '\0'
                + requireIdentity(schemaIdentity, "schema identity"));
    return relation(table.id(), identity, loader);
  }

  /** Load the full engine-neutral relation for one immutable user-view definition. */
  public RelationObject viewRelation(ViewNode view, Supplier<RelationObject> loader) {
    Objects.requireNonNull(view, "view");
    return relation(view.id(), view.cacheIdentity(), loader);
  }

  private RelationObject relation(
      ResourceId relationId, String identity, Supplier<RelationObject> loader) {
    requireIdentity(identity, "relation identity");
    Key key =
        new Key(
            account(relationId),
            Kind.RELATION,
            requireIdentity(relationId.getId(), "relation id") + '\0' + identity);
    return get(key, RelationObject.class, loader);
  }

  /**
   * Load a normalized constraints bundle by immutable blob identity. Request-specific pruning is
   * intentionally performed after this lookup.
   */
  public Optional<SnapshotConstraints> constraints(
      ResourceId tableId, String contentIdentity, Supplier<Optional<SnapshotConstraints>> loader) {
    requireIdentity(contentIdentity, "constraints content identity");
    Key key =
        new Key(
            account(tableId),
            Kind.CONSTRAINTS,
            requireIdentity(tableId.getId(), "table id") + '\0' + contentIdentity);
    return getOptional(key, SnapshotConstraints.class, loader);
  }

  /**
   * Load the two small ingest-shaped facts for one table snapshot and stats-generation view.
   *
   * <p>A blank generation identity denotes the mutable live view. A non-blank identity denotes an
   * immutable generation frozen on a query pin; keeping it in the key preserves the existing
   * query-consistent stats policy when generations overlap for the same snapshot.
   */
  public Optional<SnapshotFacts> snapshotFacts(
      ResourceId tableId,
      long snapshotId,
      String generationIdentity,
      Supplier<Optional<SnapshotFacts>> loader) {
    if (generationIdentity == null || generationIdentity.isBlank()) {
      // Live statistics are mutable. Read them through without retaining them under a process-wide
      // key; only a frozen generation has an immutable identity suitable for ObjectCache.
      return Objects.requireNonNull(loader, "loader").get();
    }
    return getOptional(
        snapshotFactsKey(tableId, snapshotId, generationIdentity), SnapshotFacts.class, loader);
  }

  /** Publish facts already held by a successful writer under an immutable generation token. */
  public void publishSnapshotFacts(
      ResourceId tableId, long snapshotId, String generationIdentity, SnapshotFacts facts) {
    if (!enabled) {
      return;
    }
    String generation = requireIdentity(generationIdentity, "generation identity");
    entries.put(
        snapshotFactsKey(tableId, snapshotId, generation),
        Value.of(Objects.requireNonNull(facts, "facts")));
  }

  /** Load one immutable target-stat record under its generation and target identity. */
  public Optional<TargetStatsRecord> targetStats(
      ResourceId tableId,
      long snapshotId,
      String generationIdentity,
      String storageId,
      Supplier<Optional<TargetStatsRecord>> loader) {
    return getOptional(
        targetStatsKey(tableId, snapshotId, generationIdentity, storageId),
        TargetStatsRecord.class,
        loader);
  }

  /** Publish a target-stat record after its immutable generation is durably committed. */
  public void publishTargetStats(
      ResourceId tableId,
      long snapshotId,
      String generationIdentity,
      String storageId,
      TargetStatsRecord record) {
    if (!enabled) {
      return;
    }
    String generation = requireIdentity(generationIdentity, "generation identity");
    String target = requireIdentity(storageId, "stats target identity");
    entries.put(
        targetStatsKey(tableId, snapshotId, generation, target),
        Value.of(Objects.requireNonNull(record, "record")));
  }

  /** Evict all target-stat records for one table snapshot after a successful mutation. */
  public void evictTargetStats(ResourceId tableId, long snapshotId) {
    entries.evictPartition(
        key ->
            key.accountId().equals(account(tableId))
                && key.kind() == Kind.TARGET_STATS
                && key.identity().startsWith(tableId.getId() + "\0" + snapshotId + "\0"));
  }

  /** Evict one target-stat record for one table snapshot after a successful mutation. */
  public void evictTargetStats(ResourceId tableId, long snapshotId, String storageId) {
    evictTargetStats(tableId, snapshotId, Set.of(storageId));
  }

  /** Evict several target-stat records in one resident-key scan. */
  public void evictTargetStats(ResourceId tableId, long snapshotId, Set<String> storageIds) {
    Objects.requireNonNull(storageIds, "storageIds");
    if (storageIds.isEmpty()) {
      return;
    }
    String prefix = tableId.getId() + "\0" + snapshotId + "\0";
    Set<String> targets =
        storageIds.stream()
            .map(id -> requireIdentity(id, "stats target identity"))
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    entries.evictPartition(
        key ->
            key.accountId().equals(account(tableId))
                && key.kind() == Kind.TARGET_STATS
                && key.identity().startsWith(prefix)
                && targets.stream().anyMatch(target -> key.identity().endsWith("\0" + target)));
  }

  private static Key snapshotFactsKey(
      ResourceId tableId, long snapshotId, String generationIdentity) {
    if (snapshotId < 0L) {
      throw new IllegalArgumentException("snapshotId must be non-negative");
    }
    String generation = generationIdentity == null ? "" : generationIdentity.trim();
    return new Key(
        account(tableId),
        Kind.SNAPSHOT_FACTS,
        requireIdentity(tableId.getId(), "table id") + '\0' + snapshotId + '\0' + generation);
  }

  private static Key targetStatsKey(
      ResourceId tableId, long snapshotId, String generationIdentity, String storageId) {
    if (snapshotId < 0L) {
      throw new IllegalArgumentException("snapshotId must be non-negative");
    }
    return new Key(
        account(tableId),
        Kind.TARGET_STATS,
        requireIdentity(tableId.getId(), "table id")
            + '\0'
            + snapshotId
            + '\0'
            + requireIdentity(generationIdentity, "generation identity")
            + '\0'
            + requireIdentity(storageId, "stats target identity"));
  }

  /** Drop every decoded object belonging to an account. */
  public void evictAccount(String accountId) {
    requireIdentity(accountId, "accountId");
    entries.evictPartition(key -> accountId.equals(key.accountId()));
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

  private <T> T get(Key key, Class<T> type, Supplier<T> loader) {
    Objects.requireNonNull(loader, "loader");
    if (!enabled) {
      return Objects.requireNonNull(loader.get(), "an object-cache loader returned null");
    }
    Value cached =
        entries.get(
            key,
            ignored ->
                Value.of(
                    Objects.requireNonNull(loader.get(), "an object-cache loader returned null")));
    return type.cast(cached.value());
  }

  private <T> Optional<T> getOptional(Key key, Class<T> type, Supplier<Optional<T>> loader) {
    Objects.requireNonNull(loader, "loader");
    if (!enabled) {
      return Objects.requireNonNull(loader.get(), "an object-cache loader returned null");
    }
    Value cached =
        entries.get(
            key,
            ignored -> {
              Optional<T> loaded =
                  Objects.requireNonNull(loader.get(), "an object-cache loader returned null");
              return loaded.map(Value::of).orElse(null);
            });
    return Optional.ofNullable(cached).map(Value::value).map(type::cast);
  }

  static String schemaIdentity(UserTableNode table, String schemaJson) {
    Objects.requireNonNull(table, "table");
    String effectiveSchema = schemaJson == null ? "" : schemaJson;
    List<String> partitionKeys = new ArrayList<>(table.partitionKeys());
    partitionKeys.sort(Comparator.naturalOrder());
    String material =
        table.format().getNumber()
            + "\0"
            + table.columnIdAlgorithm().getNumber()
            + "\0"
            + String.join("\0", partitionKeys)
            + "\0"
            + effectiveSchema;
    return Hashing.sha256Hex(material);
  }

  private static String pinnedSchemaScope(TablePin pin) {
    String scope = QueryPins.schemaScope(pin);
    return requireIdentity(
        scope.isBlank() ? pin.getSnapshotBlobUri() : scope, "pinned schema identity");
  }

  private static String account(ResourceId id) {
    Objects.requireNonNull(id, "resourceId");
    return requireIdentity(id.getAccountId(), "resource accountId");
  }

  private static String requireIdentity(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static long estimatedKeyBytes(Key key) {
    return KEY_OBJECT_BYTES + 2L * key.accountId().length() + 2L * key.identity().length();
  }

  private static long retainedProtoBytes(MessageLite message) {
    return RETAINED_PROTO_FACTOR * message.getSerializedSize();
  }

  private static long retainedMappedMetadataBytes(MessageLite message) {
    return Math.multiplyExact(RETAINED_SCHEMA_FACTOR_NUMERATOR, (long) message.getSerializedSize())
        / RETAINED_SCHEMA_FACTOR_DENOMINATOR;
  }

  private enum Kind {
    SCHEMA,
    RELATION,
    CONSTRAINTS,
    SNAPSHOT_FACTS,
    TARGET_STATS
  }

  private record Key(String accountId, Kind kind, String identity) {
    private Key {
      requireIdentity(accountId, "accountId");
      Objects.requireNonNull(kind, "kind");
      requireIdentity(identity, "identity");
    }
  }

  private record Value(Object value, long estimatedWeightBytes) implements WeightedValue {
    private Value {
      Objects.requireNonNull(value, "value");
      if (estimatedWeightBytes < 0L) {
        throw new IllegalArgumentException("estimatedWeightBytes must be non-negative");
      }
    }

    private static Value of(Object value) {
      long retained =
          value instanceof WeightedValue weighted
              ? weighted.estimatedWeightBytes()
              : value instanceof SchemaDescriptor schema
                  ? retainedMappedMetadataBytes(schema)
                  : value instanceof MessageLite message ? retainedProtoBytes(message) : 0L;
      if (!(value instanceof WeightedValue) && !(value instanceof MessageLite)) {
        throw new IllegalArgumentException("unsupported object-cache value: " + value.getClass());
      }
      return new Value(value, VALUE_WRAPPER_BYTES + retained);
    }
  }
}
