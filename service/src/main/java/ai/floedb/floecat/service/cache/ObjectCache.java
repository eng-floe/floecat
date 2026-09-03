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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationStats;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.MessageLite;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
  private static final long VALUE_WRAPPER_BYTES = 24L;
  private static final long KEY_OBJECT_BYTES = 48L;

  private final MemoryCache<Key, Value> entries;
  private final LogicalSchemaMapper schemaMapper;
  private final boolean enabled;

  ObjectCache(long maxBytes, CacheEvents events, boolean enabled) {
    this(
        new CaffeineMemoryCache<>(
            CacheFamily.OBJECT, maxBytes, ObjectCache::estimatedKeyBytes, events),
        new LogicalSchemaMapper(),
        enabled);
  }

  ObjectCache(MemoryCache<Key, Value> entries, LogicalSchemaMapper schemaMapper, boolean enabled) {
    this.entries = Objects.requireNonNull(entries, "entries");
    this.schemaMapper = Objects.requireNonNull(schemaMapper, "schemaMapper");
    this.enabled = enabled;
  }

  /** An ordinary enabled cache for focused tests that exercise the production implementation. */
  public static ObjectCache forTesting() {
    return new ObjectCache(64L * 1024L * 1024L, CacheEvents.none(), true);
  }

  /** The mapped schema together with the exact identity used by relation-template entries. */
  public record MappedSchema(String identity, SchemaDescriptor descriptor) {
    public MappedSchema {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(descriptor, "descriptor");
    }
  }

  /**
   * Engine-neutral relation content. The schema is retained because request-time projection and
   * engine decoration require the physical column tree even though the assembled relation already
   * carries planner-facing columns.
   */
  public record RelationTemplate(RelationInfo relation, SchemaDescriptor schema)
      implements WeightedValue {
    public RelationTemplate {
      Objects.requireNonNull(relation, "relation");
      Objects.requireNonNull(schema, "schema");
    }

    @Override
    public long estimatedWeightBytes() {
      return retainedProtoBytes(relation) + retainedProtoBytes(schema);
    }
  }

  /** Map a table schema once for its real mapping inputs, not once per snapshot or caller. */
  public MappedSchema mappedSchema(UserTableNode table, String schemaJson) {
    Objects.requireNonNull(table, "table");
    String effectiveSchema =
        schemaJson == null || schemaJson.isBlank() ? table.schemaJson() : schemaJson;
    String identity = schemaIdentity(table, effectiveSchema);
    Key key = new Key(table.id().getAccountId(), Kind.SCHEMA, identity);
    SchemaDescriptor descriptor =
        get(key, SchemaDescriptor.class, () -> schemaMapper.map(table, effectiveSchema));
    return new MappedSchema(identity, descriptor);
  }

  /** Load the full engine-neutral template for one immutable user-table definition. */
  public RelationTemplate tableRelation(
      UserTableNode table, MappedSchema schema, Supplier<RelationTemplate> loader) {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(schema, "schema");
    return relation(table.id(), table.cacheIdentity() + '\0' + schema.identity(), loader);
  }

  /** Load the full engine-neutral template for one immutable user-view definition. */
  public RelationTemplate viewRelation(ViewNode view, Supplier<RelationTemplate> loader) {
    Objects.requireNonNull(view, "view");
    return relation(view.id(), view.cacheIdentity(), loader);
  }

  private RelationTemplate relation(
      ResourceId relationId, String identity, Supplier<RelationTemplate> loader) {
    requireIdentity(identity, "relation identity");
    Key key = new Key(account(relationId), Kind.RELATION, identity);
    return get(key, RelationTemplate.class, loader);
  }

  /**
   * Load a normalized constraints bundle by immutable blob identity. Request-specific pruning is
   * intentionally performed after this lookup.
   */
  public Optional<SnapshotConstraints> constraints(
      ResourceId tableId, String contentIdentity, Supplier<Optional<SnapshotConstraints>> loader) {
    requireIdentity(contentIdentity, "constraints content identity");
    Key key = new Key(account(tableId), Kind.CONSTRAINTS, contentIdentity);
    return getOptional(key, SnapshotConstraints.class, loader);
  }

  /** Load the two small ingest-shaped facts for an immutable stats-generation identity. */
  public Optional<RelationStats> snapshotFacts(
      ResourceId tableId,
      long snapshotId,
      String generationIdentity,
      Supplier<Optional<RelationStats>> loader) {
    if (snapshotId < 0L) {
      throw new IllegalArgumentException("snapshotId must be non-negative");
    }
    requireIdentity(generationIdentity, "stats generation identity");
    String identity = tableId.getId() + '\0' + snapshotId + '\0' + generationIdentity;
    Key key = new Key(account(tableId), Kind.SNAPSHOT_FACTS, identity);
    return getOptional(key, RelationStats.class, loader);
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

  private enum Kind {
    SCHEMA,
    RELATION,
    CONSTRAINTS,
    SNAPSHOT_FACTS
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
              : value instanceof MessageLite message ? retainedProtoBytes(message) : 0L;
      if (!(value instanceof WeightedValue) && !(value instanceof MessageLite)) {
        throw new IllegalArgumentException("unsupported object-cache value: " + value.getClass());
      }
      return new Value(value, VALUE_WRAPPER_BYTES + retained);
    }
  }
}
