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

package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.ResourceKey;
import ai.floedb.floecat.service.repo.model.ResourceSchema;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.jboss.logging.Logger;

public class GenericResourceRepository<T, K extends ResourceKey> extends BaseResourceRepository<T> {

  private static final Logger log = Logger.getLogger(GenericResourceRepository.class);

  private final ResourceSchema<T, K> schema;

  /** Build a direct-loading repository over the supplied stores. */
  public record ResourceWithMeta<T>(T value, MutationMeta meta) {}

  public record PointerConditions(
      Map<String, Long> requiredVersions,
      Set<String> requiredAbsent,
      Map<String, Long> markerVersions) {
    public PointerConditions {
      requiredVersions = Map.copyOf(requiredVersions);
      requiredAbsent = Set.copyOf(requiredAbsent);
      markerVersions = Map.copyOf(markerVersions);
    }

    public static PointerConditions none() {
      return new PointerConditions(Map.of(), Set.of(), Map.of());
    }
  }

  public GenericResourceRepository(
      PointerStore mutationPointerStore,
      BlobStore mutationBlobStore,
      ResourceSchema<T, K> schema,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType) {
    this(mutationPointerStore, mutationBlobStore, schema, parser, toBytes, contentType, null);
  }

  /** Build a cached repository that reads directly from the supplied stores. */
  public GenericResourceRepository(
      PointerStore mutationPointerStore,
      BlobStore mutationBlobStore,
      ResourceSchema<T, K> schema,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType,
      ImmutableBlobCache blobCache) {
    super(mutationPointerStore, mutationBlobStore, parser, toBytes, contentType, blobCache);
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  /** Build a repository with separate raw mutation stores and read-only storage adapters. */
  public GenericResourceRepository(
      PointerStore mutationPointerStore,
      BlobStore mutationBlobStore,
      ResourceSchema<T, K> schema,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType,
      ImmutableBlobCache blobCache,
      RepositoryReads reads) {
    super(mutationPointerStore, mutationBlobStore, parser, toBytes, contentType, blobCache, reads);
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  @Override
  protected boolean blobsImmutable() {
    // CAS schemas write a NEW content-addressed URI per content; the bytes at a URI never change,
    // so decoded values are safe to serve from the process-wide immutable cache. Non-CAS schemas
    // overwrite a stable URI in place and must never be cached by URI.
    return schema.casBlobs;
  }

  public Optional<T> getByKey(K key) {
    return observeRepository("get_by_key", () -> getByKeyUnobserved(key));
  }

  /** Returns a body and metadata resolved from the same canonical pointer version. */
  public Optional<ResourceWithMeta<T>> getByKeyWithMeta(K key) {
    return observeRepository(
        "get_by_key_with_meta",
        () -> {
          String pointerKey = schema.canonicalPointerForKey.apply(key);
          Optional<Pointer> pointer = pointerReads.get(pointerKey);
          if (pointer.isEmpty()) return Optional.empty();
          String blobUri = requireBlobReference(pointer.get(), pointerKey);
          T value =
              getByBlobUri(blobUri)
                  .orElseThrow(() -> new CorruptionException("blob missing: " + blobUri, null));
          MutationMeta meta =
              readMetaOrDefault(
                  pointer, pointerKey, blobUri, Timestamps.fromMillis(clock.millis()));
          return Optional.of(new ResourceWithMeta<>(value, meta));
        });
  }

  /**
   * Graph-hydration primitive: fetches and parses a resource directly by blob URI, skipping the
   * pointer read, for callers that already resolved a <em>fresh</em> pointer (see {@code
   * NodeLoader#load}). Returns empty when the blob is absent so the caller can fall back to a
   * pointer read.
   *
   * <p>This reads by content and ignores the current pointer, so it is <b>only</b> safe for
   * hydration and only exposed by the relation/scope repositories NodeLoader uses (catalog,
   * namespace, table, view). It deliberately lives here rather than on {@link
   * BaseResourceRepository} so repositories with GC or lifecycle semantics tied to pointer state do
   * not inherit a "read regardless of the pointer" primitive.
   */
  public Optional<T> getByBlobUri(String blobUri) {
    if (blobUri == null || blobUri.isBlank()) {
      return Optional.empty();
    }
    Function<String, Optional<T>> cacheMissLoader = this::loadAndParseBlob;
    if (blobCacheable()) {
      // CONTENT-only read: a resident decode may outlive the durable blob, so an empty result
      // means absent but a present result does NOT prove the blob still exists. Callers whose
      // emptiness doubles as a liveness/integrity check must use getByBlobUriLive.
      return blobCache.get(blobUri, cacheMissLoader);
    }
    return cacheMissLoader.apply(blobUri);
  }

  /**
   * Cache-bypassing variant of {@link #getByBlobUri} for reads whose EMPTINESS is load-bearing —
   * integrity detectors like {@code PinValidator.requirePinned*}, where a missing pinned blob must
   * fail loudly rather than be masked by a still-resident decode.
   */
  public Optional<T> getByBlobUriLive(String blobUri) {
    if (blobUri == null || blobUri.isBlank()) {
      return Optional.empty();
    }
    return loadAndParseBlob(blobUri);
  }

  /**
   * Lists resources together with metadata from the exact pointers used to select their blobs. This
   * avoids pairing a listed body with metadata from a later concurrent mutation.
   */
  public List<ResourceWithMeta<T>> listByPrefixWithMeta(
      String prefix, int limit, String token, StringBuilder nextOut) {
    return observeRepository(
        "list_by_prefix_with_meta",
        () -> {
          List<Pointer> pointers = pointerReads.list(prefix, Math.max(1, limit), token, nextOut);
          List<ResourceWithMeta<T>> values = new ArrayList<>(pointers.size());
          Timestamp now = Timestamps.fromMillis(clock.millis());
          for (Pointer selectedPointer : pointers) {
            String selectedBlobUri =
                requireBlobReference(selectedPointer, selectedPointer.getKey());
            T selectedValue =
                getByBlobUri(selectedBlobUri)
                    .orElseThrow(
                        () -> new CorruptionException("blob missing: " + selectedBlobUri, null));
            String canonicalKey =
                schema.canonicalPointerForKey.apply(schema.keyFromValue.apply(selectedValue));
            Optional<Pointer> canonicalPointer = pointerReads.get(canonicalKey);
            if (canonicalPointer.isEmpty()) {
              // The resource was deleted after the secondary-index page was selected.
              continue;
            }
            String canonicalBlobUri = requireBlobReference(canonicalPointer.get(), canonicalKey);
            if (!canonicalBlobUri.equals(selectedBlobUri)) {
              // The resource changed after the secondary-index page was selected. Returning the
              // new canonical body would violate both page membership and body/meta coherence.
              continue;
            }
            MutationMeta meta =
                readMetaOrDefault(canonicalPointer, canonicalKey, canonicalBlobUri, now);
            values.add(new ResourceWithMeta<>(selectedValue, meta));
          }
          return List.copyOf(values);
        });
  }

  public boolean existsByKey(K key) {
    return observeRepository("exists_by_key", () -> existsByKeyUnobserved(key));
  }

  /**
   * The version (etag) of the immutable blob at {@code blobUri}, or {@code null} if no blob is
   * there, using a HEAD (no body fetch, no parse). Lets a validator confirm a pinned blob is both
   * present and the exact version captured at pin time in a single O(1) probe.
   */
  public String blobEtag(String blobUri) {
    if (blobUri == null || blobUri.isBlank()) {
      return null;
    }
    return observeRepository(
        "blob_etag", () -> blobReads.head(blobUri).map(BlobHeader::getEtag).orElse(null));
  }

  private Optional<T> getByKeyUnobserved(K key) {
    return read(schema.canonicalPointerForKey.apply(key));
  }

  /** Load the current value through the raw stores that participate in a mutation transaction. */
  private Optional<T> getByKeyForMutation(K key) {
    return readForMutation(schema.canonicalPointerForKey.apply(key));
  }

  private boolean existsByKeyUnobserved(K key) {
    return pointerReads.get(schema.canonicalPointerForKey.apply(key)).isPresent();
  }

  /**
   * Structural backstop for system-object immutability: rejects any create/update/delete whose
   * target resolves to a system-owned id, for schemas that opt in via {@link
   * ResourceSchema#withSystemGuard}. The surface-layer {@code CatalogSurfaceWritePolicy} is the
   * primary gate (with user-facing errors and overlay-based writability checks); this ensures that
   * a write path which skips or forgets that policy still cannot persist a mutation against a
   * system object. Schemas that can never be system-owned leave the hook null and pay nothing.
   */
  private void guardSystemObject(K key) {
    if (schema.resourceIdFromKey == null) {
      return;
    }
    ResourceId id = schema.resourceIdFromKey.apply(key);
    if (SystemResourceIdGenerator.isSystemId(id)) {
      throw new SystemObjectImmutableException(
          "refusing to mutate system " + schema.resourceName + " " + id.getId());
    }
  }

  /**
   * Atomically creates a resource: the canonical (by-id) pointer and every secondary (by-name, …)
   * pointer are reserved in a single {@link PointerStore#compareAndSetBatch} transaction. Because
   * the batch is all-or-nothing on both backends, a mid-create storage error (or process death)
   * leaves <b>zero</b> partial pointer state — there is nothing to roll back and no orphan can be
   * stranded to poison later creates.
   *
   * <p>The blob is written first; it is content-addressed (SHA-256), so a dangling blob after a
   * failed batch is harmless and deduped on retry.
   *
   * <p><b>Idempotency &amp; conflict contract:</b> re-creating a byte-identical resource (every
   * pointer already resolves to our blob) is a no-op; a collision against a pointer bound to a
   * different blob throws {@link NameConflictException}. A pre-existing <em>partial</em> state —
   * some of this resource's pointers present (bound to our blob) and some absent, which an atomic
   * create can never itself produce — is a stored inconsistency left by a legacy or non-atomic
   * writer; it is surfaced as a (non-retryable) {@link CorruptionException} rather than silently
   * repaired or spun on. A batch that conflicts but whose read-back finds <em>no</em> pointer at
   * all is a transient transaction conflict and is signalled as retryable.
   */
  public void create(T value) {
    createWithMeta(value, PointerConditions.none(), null)
        .orElseThrow(() -> new AbortRetryableException("create conditions changed"));
  }

  /** Creates a resource and returns metadata for the exact canonical pointer that committed. */
  public ResourceWithMeta<T> createWithMeta(T value) {
    return createWithMeta(value, PointerConditions.none(), null)
        .orElseThrow(() -> new AbortRetryableException("create conditions changed"));
  }

  /**
   * Creates a resource while publishing caller-supplied companion pointer operations in the same
   * atomic pointer transaction. The factory receives the exact body/meta pair that will commit, so
   * a companion record can embed the committed metadata. Used both for idempotency receipts and to
   * co-create a resource of another kind that must never exist without this one.
   */
  public ResourceWithMeta<T> createWithMeta(
      T value, Function<ResourceWithMeta<T>, List<PointerStore.CasOp>> completionFactory) {
    return createWithMeta(
            value, PointerConditions.none(), Objects.requireNonNull(completionFactory))
        .orElseThrow(() -> new AbortRetryableException("create conditions changed"));
  }

  /**
   * Creates a resource in one atomic pointer transaction that also asserts every supplied external
   * pointer condition, advances every supplied lifecycle marker, and — when {@code
   * completionFactory} is non-null — publishes the caller's companion operations alongside the
   * resource. The factory receives the exact body/meta pair that will commit.
   *
   * <p>Returns empty when a required version, a required absence, or a marker version no longer
   * holds; the caller decides whether that is a conflict or a lost race. A create that commits (or
   * replays a byte-identical prior create) always returns a value.
   */
  public Optional<ResourceWithMeta<T>> createWithMeta(
      T value,
      PointerConditions conditions,
      Function<ResourceWithMeta<T>, List<PointerStore.CasOp>> completionFactory) {
    CreateCommit commit = createInternal(value, conditions, completionFactory);
    if (!commit.conditionsMatched()) {
      return Optional.empty();
    }
    K key = schema.keyFromValue.apply(value);
    String canonicalKey = schema.canonicalPointerForKey.apply(key);
    String blobUri = requireBlobReference(commit.canonicalPointer(), canonicalKey);
    if (!blobUri.equals(schema.blobUriForKey.apply(key))) {
      throw new NameConflictException("canonical pointer bound to different blob: " + canonicalKey);
    }
    return Optional.of(new ResourceWithMeta<>(value, commit.meta()));
  }

  /** Resolves a secondary pointer and returns the body with its exact canonical metadata. */
  public Optional<ResourceWithMeta<T>> getWithMeta(String pointerKey) {
    Optional<Pointer> secondary = pointerReads.get(pointerKey);
    if (secondary.isEmpty()) return Optional.empty();
    String selectedBlobUri = requireBlobReference(secondary.get(), pointerKey);
    T selected =
        getByBlobUri(selectedBlobUri)
            .orElseThrow(() -> new CorruptionException("blob missing: " + selectedBlobUri, null));
    String canonicalKey = schema.canonicalPointerForKey.apply(schema.keyFromValue.apply(selected));
    Optional<Pointer> canonical = pointerReads.get(canonicalKey);
    if (canonical.isEmpty()) return Optional.empty();
    String canonicalBlobUri = requireBlobReference(canonical.get(), canonicalKey);
    if (!canonicalBlobUri.equals(selectedBlobUri)) {
      return Optional.empty();
    }
    return Optional.of(
        new ResourceWithMeta<>(
            selected,
            readMetaOrDefault(
                canonical, canonicalKey, canonicalBlobUri, Timestamps.fromMillis(clock.millis()))));
  }

  private CreateCommit createInternal(
      T value,
      PointerConditions conditions,
      Function<ResourceWithMeta<T>, List<PointerStore.CasOp>> completionFactory) {
    Map<String, Long> requiredPointerVersions = conditions.requiredVersions();
    Map<String, Long> markerVersions = conditions.markerVersions();
    return observeRepository(
        "create",
        () -> {
          PreparedCreate prepared = prepareCreate(value);
          Set<String> effectiveRequiredAbsent = new HashSet<>(conditions.requiredAbsent());
          List<PointerStore.CasOp> ops =
              new ArrayList<>(
                  prepared.ops.size()
                      + requiredPointerVersions.size()
                      + effectiveRequiredAbsent.size()
                      + markerVersions.size());
          ops.addAll(prepared.ops);
          Set<String> batchedKeys = new HashSet<>(prepared.pointerKeys);
          addPointerConditions(requiredPointerVersions, effectiveRequiredAbsent, batchedKeys, ops);
          addMarkerAdvances(markerVersions, batchedKeys, ops, "create");
          MutationMeta committedMeta =
              committedMeta(
                  Optional.of(prepared.committedCanonical),
                  prepared.canonicalPointerKey,
                  prepared.blobUri,
                  Timestamps.fromMillis(clock.millis()));
          List<PointerStore.CasOp> companionOps = List.of();
          if (completionFactory != null) {
            ResourceWithMeta<T> committed = new ResourceWithMeta<>(value, committedMeta);
            companionOps = completionFactory.apply(committed);
            for (PointerStore.CasOp companion : companionOps) {
              if (!batchedKeys.add(companion.key())) {
                throw new IllegalArgumentException(
                    "duplicate companion pointer in atomic create: " + companion.key());
              }
              ops.add(companion);
            }
          }

          if (mutationPointerStore.compareAndSetBatch(ops)) {
            healCanonicalBlobIfMissing(prepared.blobUri, value);
            return new CreateCommit(true, prepared.committedCanonical, committedMeta);
          }

          if (!pointerConditionsStillMatch(requiredPointerVersions, effectiveRequiredAbsent)) {
            return new CreateCommit(false, null, null);
          }
          if (!markerVersionsStillMatch(markerVersions)) {
            return new CreateCommit(false, null, null);
          }

          // A companion create that lost its own name reservation is a name conflict, not a
          // transient one: the caller asked to publish a resource whose name another resource
          // already owns. Classify it before this resource's own pointers, because the companion
          // is the reason the batch could never have committed.
          classifyCompanionConflict(companionOps);

          // The batch committed nothing (atomic) because at least one pointer already existed. Read
          // back
          // and classify, walking canonical-then-secondary order so a conflict reports the same
          // key/message as before.
          classifyCreateConflict(prepared.blobUri, prepared.pointerKeys);
          Pointer canonical =
              mutationPointerStore
                  .get(prepared.canonicalPointerKey)
                  .orElseThrow(
                      () ->
                          new CorruptionException(
                              "created canonical pointer missing: " + prepared.canonicalPointerKey,
                              null));
          MutationMeta replayMeta =
              committedMeta(
                  Optional.of(canonical),
                  prepared.canonicalPointerKey,
                  requireBlobReference(canonical, prepared.canonicalPointerKey),
                  Timestamps.fromMillis(clock.millis()));
          return new CreateCommit(true, canonical, replayMeta);
        });
  }

  private record CreateCommit(
      boolean conditionsMatched, Pointer canonicalPointer, MutationMeta meta) {}

  private static void addPointerConditions(
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      Set<String> batchedKeys,
      List<PointerStore.CasOp> ops) {
    for (var required : requiredPointerVersions.entrySet()) {
      if (batchedKeys.add(required.getKey())) {
        ops.add(new PointerStore.CasCheck(required.getKey(), required.getValue()));
      }
    }
    for (String pointerKey : requiredAbsentPointers) {
      if (batchedKeys.add(pointerKey)) {
        ops.add(new PointerStore.CasCheckAbsent(pointerKey));
      }
    }
  }

  /**
   * Advances each supplied lifecycle marker from its expected version in the same batch. A marker
   * that collides with a pointer this mutation already writes is a caller bug: the batch is atomic
   * and a store rejects two operations on one key, so it is rejected before anything is attempted.
   */
  private static void addMarkerAdvances(
      Map<String, Long> markerVersions,
      Set<String> batchedKeys,
      List<PointerStore.CasOp> ops,
      String operation) {
    for (var marker : markerVersions.entrySet()) {
      String markerKey = marker.getKey();
      long expectedVersion = marker.getValue();
      if (!batchedKeys.add(markerKey)) {
        throw new IllegalArgumentException(
            "duplicate pointer in atomic " + operation + ": " + markerKey);
      }
      ops.add(
          new PointerStore.CasUpsert(
              markerKey,
              expectedVersion,
              PointerReferences.opaqueMarkerPointer(markerKey, markerKey, expectedVersion + 1L)));
    }
  }

  private boolean markerVersionsStillMatch(Map<String, Long> markerVersions) {
    for (var marker : markerVersions.entrySet()) {
      if (!pointerVersionMatches(marker.getKey(), marker.getValue())) {
        return false;
      }
    }
    return true;
  }

  private boolean pointerConditionsStillMatch(
      Map<String, Long> requiredPointerVersions, Set<String> requiredAbsentPointers) {
    for (var required : requiredPointerVersions.entrySet()) {
      if (!pointerVersionMatches(required.getKey(), required.getValue())) {
        return false;
      }
    }
    for (String pointerKey : requiredAbsentPointers) {
      if (mutationPointerStore.get(pointerKey).isPresent()) {
        return false;
      }
    }
    return true;
  }

  private boolean pointerVersionMatches(String pointerKey, long expectedVersion) {
    Pointer current = mutationPointerStore.get(pointerKey).orElse(null);
    return expectedVersion == 0L
        ? current == null
        : current != null && current.getVersion() == expectedVersion;
  }

  private PreparedCreate prepareCreate(T value) {
    K key = schema.keyFromValue.apply(value);
    guardSystemObject(key);
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    String blobUri = schema.blobUriForKey.apply(key);
    int blobBytes = writeBlobAndGetSize(blobUri, value);

    Map<String, String> secondaries = schema.secondaryPointersFromValue.apply(value);
    LinkedHashSet<String> uniqueKeys = new LinkedHashSet<>(1 + secondaries.size());
    uniqueKeys.add(canonicalPointer);
    uniqueKeys.addAll(secondaries.values());
    List<String> pointerKeys = new ArrayList<>(uniqueKeys);
    List<PointerStore.CasOp> ops = new ArrayList<>(pointerKeys.size());
    Pointer committedCanonical = null;
    for (String pointerKey : pointerKeys) {
      Pointer reserved = reserve(pointerKey, blobUri, value, blobBytes);
      ops.add(new PointerStore.CasUpsert(pointerKey, 0L, reserved));
      if (pointerKey.equals(canonicalPointer)) {
        committedCanonical = reserved.toBuilder().setVersion(1L).build();
      }
    }
    return new PreparedCreate(blobUri, canonicalPointer, committedCanonical, pointerKeys, ops);
  }

  private record PreparedCreate(
      String blobUri,
      String canonicalPointerKey,
      Pointer committedCanonical,
      List<String> pointerKeys,
      List<PointerStore.CasOp> ops) {}

  /**
   * Raises a name conflict when a companion operation that reserves a fresh pointer (expected
   * version 0) finds that pointer already taken. Without this, a collision on a co-created
   * resource's name would surface as a retryable abort and spin until the caller gave up.
   */
  private void classifyCompanionConflict(List<PointerStore.CasOp> companionOps) {
    for (PointerStore.CasOp companion : companionOps) {
      if (companion instanceof PointerStore.CasUpsert upsert
          && upsert.expectedVersion() == 0L
          && mutationPointerStore.get(upsert.key()).isPresent()) {
        throw new NameConflictException("companion pointer already reserved: " + upsert.key());
      }
    }
  }

  private void classifyCreateConflict(String blobUri, List<String> pointerKeys) {
    int present = 0;
    int absent = 0;
    for (String pointerKey : pointerKeys) {
      Pointer pointer = mutationPointerStore.get(pointerKey).orElse(null);
      if (pointer == null) {
        absent++;
        continue;
      }
      if (!blobUri.equals(pointer.getBlobUri())) {
        throw new NameConflictException("pointer bound to different blob: " + pointerKey);
      }
      present++;
    }
    if (absent == 0) {
      // Every pointer already resolves to our blob: a byte-identical re-create is a no-op.
      return;
    }
    if (present == 0) {
      // The batch reported a conflict yet read-back finds no pointer at all: a transient batch
      // conflict (e.g. a DynamoDB TransactionConflict) or a concurrent delete, not a stable state.
      // Re-running the atomic batch can still make progress, so signal a retry.
      throw new AbortRetryableException(
          "create conflict, no pointer present: " + pointerKeys.get(0));
    }
    // Mixed: some pointers present (bound to our blob), some absent. An atomic create cannot
    // produce this, so it is a stored inconsistency (a legacy orphan, or a non-atomic
    // createIfAbsent / update that died mid-flight). Strict no-repair semantics: surface it
    // terminally instead of healing it or spinning on a retry that can never converge.
    throw partialStateAnomaly(
        "create",
        "partial create state ("
            + present
            + " present, "
            + absent
            + " absent) for: "
            + pointerKeys);
  }

  /**
   * Writes this resource's blob and returns the pointer operations that would create it, without
   * committing anything. The caller folds them into another repository's atomic create so the two
   * resources become visible together or not at all — see {@link #createWithMeta(Object,
   * PointerConditions, Function)}.
   *
   * <p>The blob is content-addressed, so preparing operations that are never committed wastes space
   * but cannot corrupt state or make the resource reachable: reachability comes from the pointers,
   * and those are the caller's to commit. Every returned operation expects version 0, so a
   * concurrent creator of the same resource loses the batch rather than being overwritten.
   */
  public List<PointerStore.CasOp> prepareCreateOps(T value) {
    return prepareCreate(value).ops;
  }

  /**
   * Creates a resource only when it does not already exist, <b>atomically</b>.
   *
   * <p>The canonical (by-id) pointer and every secondary pointer are reserved in a single {@link
   * PointerStore#compareAndSetBatch} transaction, so — exactly like {@link #create} — a failure
   * leaves zero partial pointer state and there is no intermediate window in which the resource is
   * visible by id but not yet by name.
   *
   * <p>Returns {@code true} only when this call committed the batch (it won the create). Returns
   * {@code false} when the canonical pointer already exists — some other writer owns the resource —
   * <em>regardless</em> of which blob that pointer is bound to; the canonical pointer is the
   * authoritative "already created" marker. A new secondary name owned by a different blob throws
   * {@link NameConflictException}. A stored inconsistency (canonical absent but a secondary
   * present, which an atomic path can never produce) is surfaced as a non-retryable {@link
   * CorruptionException}; a transient batch conflict with nothing present is signalled as
   * retryable.
   *
   * <p><b>Blob cleanup:</b> the blob is written before the batch. When the batch does not commit, a
   * best-effort cleanup is attempted. For {@code casBlobs} schemas the blob URI is
   * content-addressed (SHA256), so a cleanup failure only wastes space; it has no correctness
   * impact.
   */
  public boolean createIfAbsent(T value) {
    return observeRepository(
        "create_if_absent",
        () -> {
          K key = schema.keyFromValue.apply(value);
          guardSystemObject(key);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = schema.blobUriForKey.apply(key);
          boolean blobExistedBefore = mutationBlobStore.head(blobUri).isPresent();

          int blobBytes = writeBlobAndGetSize(blobUri, value);

          Map<String, String> secondaries = schema.secondaryPointersFromValue.apply(value);
          // Canonical first, then secondaries, de-duplicated (a schema may expose the canonical
          // pointer
          // as a secondary too, and a transactional batch must not repeat a key). See create().
          LinkedHashSet<String> uniqueKeys = new LinkedHashSet<>(1 + secondaries.size());
          uniqueKeys.add(canonicalPointer);
          uniqueKeys.addAll(secondaries.values());
          List<String> pointerKeys = new ArrayList<>(uniqueKeys);

          List<PointerStore.CasOp> ops = new ArrayList<>(pointerKeys.size());
          for (String pointerKey : pointerKeys) {
            ops.add(
                new PointerStore.CasUpsert(
                    pointerKey, 0L, reserve(pointerKey, blobUri, value, blobBytes)));
          }
          if (mutationPointerStore.compareAndSetBatch(ops)) {
            healCanonicalBlobIfMissing(blobUri, value);
            return true;
          }
          return classifyCreateIfAbsentConflict(
              canonicalPointer, blobUri, pointerKeys, blobExistedBefore);
        });
  }

  private boolean classifyCreateIfAbsentConflict(
      String canonicalPointer,
      String blobUri,
      List<String> pointerKeys,
      boolean blobExistedBefore) {
    // The batch committed nothing, so this call did not create the resource. Reclaim the blob we
    // optimistically wrote (best-effort, content-addressed) before classifying the outcome.
    cleanupCreateIfAbsentBlobOnCasMiss(canonicalPointer, blobUri, blobExistedBefore);

    Pointer canonical = mutationPointerStore.get(canonicalPointer).orElse(null);
    if (canonical != null) {
      // Canonical already taken — another writer owns the create; report a lost race regardless of
      // which blob it points to.
      return false;
    }

    // Canonical is absent. If a secondary nonetheless exists, classify by what it is bound to.
    boolean anySecondaryPresent = false;
    for (String pointerKey : pointerKeys) {
      if (pointerKey.equals(canonicalPointer)) {
        continue;
      }
      Pointer secondary = mutationPointerStore.get(pointerKey).orElse(null);
      if (secondary == null) {
        continue;
      }
      anySecondaryPresent = true;
      if (!blobUri.equals(secondary.getBlobUri())) {
        throw new NameConflictException("pointer bound to different blob: " + pointerKey);
      }
    }
    if (anySecondaryPresent) {
      // Canonical absent but a secondary (bound to our blob) exists: a stored partial-create
      // inconsistency an atomic path can never produce. Surface it terminally, do not repair.
      throw partialStateAnomaly(
          "createIfAbsent",
          "partial create state (canonical absent, secondary present) for: " + canonicalPointer);
    }
    // Nothing present: a transient batch conflict (or a concurrent delete). A retry re-attempts
    // the atomic batch.
    throw new AbortRetryableException(
        "createIfAbsent conflict, no pointer present: " + canonicalPointer);
  }

  private Pointer reserve(String key, String blobUri, T value) {
    return reserve(key, blobUri, value, -1L);
  }

  private Pointer reserve(String key, String blobUri, T value, long blobBytes) {
    if (schema.resourceIdFromValue != null && value != null) {
      var rid = schema.resourceIdFromValue.apply(value);
      var dn = schema.displayNameFromValue.apply(value);
      if (rid != null && !rid.getId().isEmpty()) {
        return blobBytes >= 0L
            ? PointerReferences.blobPointer(key, blobUri, 1L, rid, dn != null ? dn : "", blobBytes)
            : PointerReferences.blobPointer(key, blobUri, 1L, rid, dn != null ? dn : "");
      }
    }
    return blobBytes >= 0L
        ? PointerReferences.blobPointer(key, blobUri, 1L, blobBytes)
        : PointerReferences.blobPointer(key, blobUri, 1L);
  }

  /**
   * A stable partial-pointer state that an atomic create/createIfAbsent can never itself produce (a
   * legacy orphan, or a non-atomic writer that died mid-flight). Under the strict no-repair
   * contract we surface it as a non-retryable {@link CorruptionException}. Log it and bump a
   * counter so the (rare) anomaly is visible and can be reconciled out of band rather than
   * vanishing into a generic 500.
   */
  private CorruptionException partialStateAnomaly(String operation, String message) {
    log.errorf("partial pointer state in %s.%s: %s", schema.resourceName, operation, message);
    observability()
        .counter(
            ServiceMetrics.Storage.PARTIAL_STATE,
            1.0,
            Tag.of(TagKey.OPERATION, operation),
            Tag.of(TagKey.RESOURCE, schema.resourceName));
    return new CorruptionException(message);
  }

  private void cleanupCreateIfAbsentBlobOnCasMiss(
      String canonicalPointer, String blobUri, boolean blobExistedBefore) {
    // For casBlobs schemas the URI is content-addressed (SHA256): concurrent writers with
    // identical content share a URI and no cleanup is needed. For distinct content, deleteQuietly
    // is best-effort — a silent failure leaves an orphaned blob (space cost, no correctness
    // impact).
    if (blobExistedBefore || !schema.casBlobs || blobUri.isBlank()) {
      return;
    }
    Pointer pointer = mutationPointerStore.get(canonicalPointer).orElse(null);
    if (pointer != null && blobUri.equals(pointer.getBlobUri())) {
      return;
    }
    deleteQuietly(() -> mutationBlobStore.delete(blobUri));
  }

  /**
   * Atomically updates a resource: the canonical pointer is advanced, new secondary pointers are
   * reserved, kept secondaries are moved onto the new (content-addressed) blob when it changed, and
   * removed secondaries are deleted — all in a single {@link PointerStore#compareAndSetBatch}
   * transaction. Because the batch is all-or-nothing, a mid-update storage error (or process death)
   * commits nothing and leaves <b>zero</b> partial pointer state.
   *
   * <p>Returns {@code true} on commit. Returns {@code false} when the canonical pointer is not at
   * {@code expectedCanonicalVersion} (an optimistic-concurrency miss — it moved or was deleted
   * under us). A new secondary name already owned by a different blob throws {@link
   * NameConflictException}. A conflict the read-back cannot attribute to either case (a concurrent
   * version shift) is signalled as retryable.
   */
  public boolean update(T updatedValue, long expectedCanonicalVersion) {
    return updateWithMeta(updatedValue, expectedCanonicalVersion).isPresent();
  }

  /**
   * Atomically updates a resource and returns metadata for the exact committed pointer version. The
   * returned metadata is assembled from the committed pointer rather than a post-commit pointer
   * reread, so a concurrent later writer cannot change the response.
   */
  public Optional<MutationMeta> updateWithMeta(T updatedValue, long expectedCanonicalVersion) {
    return updateWithMetaWhilePointersMatchAndBumpMarkers(
        updatedValue, expectedCanonicalVersion, PointerConditions.none());
  }

  private record PreparedUpdate(
      String blobUri,
      Pointer committedCanonical,
      Set<String> addedSecondaries,
      Set<String> batchedKeys,
      List<PointerStore.CasOp> ops) {}

  /**
   * Pointer operations that would update this resource from {@code expectedCanonicalVersion},
   * without committing anything, so a caller can fold them into another repository's atomic
   * mutation. Renaming an overlay uses this to rename the catalog it owns in the same transaction.
   */
  public List<PointerStore.CasOp> prepareUpdateOps(T updatedValue, long expectedCanonicalVersion) {
    return prepareUpdate(updatedValue, expectedCanonicalVersion).ops;
  }

  private PreparedUpdate prepareUpdate(T updatedValue, long expectedCanonicalVersion) {
    K key = schema.keyFromValue.apply(updatedValue);
    guardSystemObject(key);
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    String blobUri = schema.blobUriForKey.apply(key);

    T currentValue =
        getByKeyForMutation(key)
            .orElseThrow(
                () ->
                    new NotFoundException(
                        schema.resourceName + " not found for canonical: " + canonicalPointer));
    String currentBlobUri = schema.blobUriForKey.apply(schema.keyFromValue.apply(currentValue));

    Set<String> currentSecondary =
        new HashSet<>(schema.secondaryPointersFromValue.apply(currentValue).values());
    Set<String> nextSecondary =
        new HashSet<>(schema.secondaryPointersFromValue.apply(updatedValue).values());

    Set<String> toAdd = new HashSet<>(nextSecondary);
    toAdd.removeAll(currentSecondary);
    Set<String> toDelete = new HashSet<>(currentSecondary);
    toDelete.removeAll(nextSecondary);
    Set<String> kept = new HashSet<>(nextSecondary);
    kept.removeAll(toAdd);

    int blobBytes = writeBlobAndGetSize(blobUri, updatedValue);

    boolean blobChanged = schema.casBlobs && !Objects.equals(currentBlobUri, blobUri);

    // Build a single all-or-nothing batch covering every pointer mutation this update
    // implies. Ops
    // are de-duplicated by key (a schema may expose the canonical pointer as a secondary too)
    // with
    // the canonical advance taking precedence. Because the batch is atomic the update can
    // never
    // leave partial pointer state; a conflict commits nothing and is classified below.
    Set<String> batchedKeys = new HashSet<>();
    List<PointerStore.CasOp> ops = new ArrayList<>();

    batchedKeys.add(canonicalPointer);
    Pointer committedCanonical =
        reserve(canonicalPointer, blobUri, updatedValue, blobBytes).toBuilder()
            .setVersion(expectedCanonicalVersion + 1L)
            .build();
    ops.add(
        new PointerStore.CasUpsert(canonicalPointer, expectedCanonicalVersion, committedCanonical));

    for (String p : toAdd) {
      if (!batchedKeys.add(p)) {
        continue;
      }
      Pointer existing = mutationPointerStore.get(p).orElse(null);
      if (existing == null) {
        ops.add(new PointerStore.CasUpsert(p, 0L, reserve(p, blobUri, updatedValue, blobBytes)));
      } else if (!blobUri.equals(existing.getBlobUri())) {
        // The new name already belongs to a different blob. Nothing has been committed, so
        // failing
        // fast here leaves no partial state.
        throw new NameConflictException("pointer bound to different blob: " + p);
      }
      // else: already reserved to our blob — idempotent, no op needed.
    }

    if (blobChanged) {
      // Kept secondaries still point at the old content-addressed blob; advance each onto the
      // new
      // one (or reserve it if a legacy gap left it absent).
      for (String p : kept) {
        if (!batchedKeys.add(p)) {
          continue;
        }
        Pointer existing = mutationPointerStore.get(p).orElse(null);
        if (existing == null) {
          ops.add(new PointerStore.CasUpsert(p, 0L, reserve(p, blobUri, updatedValue, blobBytes)));
        } else if (!blobUri.equals(existing.getBlobUri())) {
          ops.add(
              new PointerStore.CasUpsert(
                  p, existing.getVersion(), reserve(p, blobUri, updatedValue, blobBytes)));
        }
        // else: already on the new blob — no op needed.
      }
    }

    for (String p : toDelete) {
      if (!batchedKeys.add(p)) {
        continue;
      }
      Pointer existing = mutationPointerStore.get(p).orElse(null);
      if (existing != null) {
        ops.add(new PointerStore.CasDelete(p, existing.getVersion()));
      }
    }

    return new PreparedUpdate(
        blobUri, committedCanonical, Set.copyOf(toAdd), Set.copyOf(batchedKeys), List.copyOf(ops));
  }

  /**
   * Atomically updates a resource while external pointers retain their versions and advances the
   * supplied lifecycle markers in the same transaction.
   */
  public Optional<MutationMeta> updateWithMetaWhilePointersMatchAndBumpMarkers(
      T updatedValue, long expectedCanonicalVersion, PointerConditions conditions) {
    return updateWithMetaWhilePointersMatchAndBumpMarkers(
        updatedValue, expectedCanonicalVersion, conditions, List.of());
  }

  /**
   * Updates a resource and publishes the caller's companion operations in the same atomic pointer
   * transaction, so a resource this one owns is renamed or retargeted with it rather than after it.
   */
  public Optional<MutationMeta> updateWithMetaWhilePointersMatchAndBumpMarkers(
      T updatedValue,
      long expectedCanonicalVersion,
      PointerConditions conditions,
      List<PointerStore.CasOp> companions) {
    Map<String, Long> requiredPointerVersions = conditions.requiredVersions();
    Set<String> requiredAbsentPointers = conditions.requiredAbsent();
    Map<String, Long> markerVersions = conditions.markerVersions();
    return observeRepository(
        "update_with_meta",
        () -> {
          K key = schema.keyFromValue.apply(updatedValue);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          PreparedUpdate prepared = prepareUpdate(updatedValue, expectedCanonicalVersion);
          String blobUri = prepared.blobUri;
          Set<String> toAdd = prepared.addedSecondaries;
          Set<String> batchedKeys = new HashSet<>(prepared.batchedKeys);
          List<PointerStore.CasOp> ops = new ArrayList<>(prepared.ops);
          Pointer committedCanonical = prepared.committedCanonical;
          addPointerConditions(requiredPointerVersions, requiredAbsentPointers, batchedKeys, ops);
          addMarkerAdvances(markerVersions, batchedKeys, ops, "update");
          for (PointerStore.CasOp companion : companions) {
            if (!batchedKeys.add(companion.key())) {
              throw new IllegalArgumentException(
                  "duplicate companion pointer in atomic update: " + companion.key());
            }
            ops.add(companion);
          }

          if (mutationPointerStore.compareAndSetBatch(ops)) {
            healCanonicalBlobIfMissing(blobUri, updatedValue);
            return Optional.of(
                committedMeta(
                    Optional.of(committedCanonical),
                    canonicalPointer,
                    blobUri,
                    Timestamps.fromMillis(clock.millis())));
          }
          if (!pointerConditionsStillMatch(requiredPointerVersions, requiredAbsentPointers))
            return Optional.empty();
          if (!markerVersionsStillMatch(markerVersions)) return Optional.empty();
          classifyUpdateConflict(canonicalPointer, expectedCanonicalVersion, blobUri, toAdd);
          return Optional.empty();
        });
  }

  /**
   * Post-commit backstop for the CAS-GC mark/CAS race (eng-floe/core#1904): if a concurrent sweep
   * deleted the (re-referenced) canonical blob between this call's writeBlob and its pointer batch
   * commit, re-PUT it — the writer still holds the bytes, so the residual race becomes a transient
   * blip instead of a permanent dangling pointer, and a pre-existing dangling heals on the next
   * write. One cheap HEAD per committed write. Best-effort: the pointers HAVE committed, so a heal
   * failure must not fail the call; the warn is the GC-race detection signal.
   */
  private void healCanonicalBlobIfMissing(String blobUri, T value) {
    try {
      if (mutationBlobStore.head(blobUri).isPresent()) {
        return;
      }
      log.warnf(
          "canonical %s blob %s missing after pointer commit; re-writing (gc-race heal)",
          schema.resourceName, blobUri);
      writeBlob(blobUri, value);
    } catch (RuntimeException e) {
      log.errorf(e, "failed to heal canonical blob %s after pointer commit", blobUri);
    }
  }

  private boolean classifyUpdateConflict(
      String canonicalPointer, long expectedCanonicalVersion, String blobUri, Set<String> toAdd) {
    Pointer canonical = mutationPointerStore.get(canonicalPointer).orElse(null);
    if (canonical == null || canonical.getVersion() != expectedCanonicalVersion) {
      // Optimistic-concurrency miss: the canonical pointer moved or vanished under us. Same
      // observable result as the previous advancePointer -> PreconditionFailed path — the caller
      // retries with a fresh expected version.
      return false;
    }
    // Canonical is exactly where we expected, so a secondary op lost the race. A new name now owned
    // by a different blob is a terminal collision; otherwise a concurrent writer shifted a
    // secondary's version between our read and the commit and a retry re-reads fresh versions.
    for (String p : toAdd) {
      Pointer secondary = mutationPointerStore.get(p).orElse(null);
      if (secondary != null && !blobUri.equals(secondary.getBlobUri())) {
        throw new NameConflictException("pointer bound to different blob: " + p);
      }
    }
    throw new AbortRetryableException("update conflict for: " + canonicalPointer);
  }

  /**
   * Atomically replaces one resource identity with another. The old canonical pointer is deleted,
   * the new canonical pointer is created, shared secondary pointers are swapped to the new body,
   * and non-shared secondaries are removed/created in the same pointer transaction.
   */
  public Optional<ResourceWithMeta<T>> replaceIdentityWithMeta(
      T currentValue,
      long expectedCanonicalVersion,
      T replacementValue,
      PointerConditions conditions,
      Map<String, Long> pointerVersionsToDelete) {
    Map<String, Long> requiredPointerVersions = conditions.requiredVersions();
    Set<String> requiredAbsentPointers = conditions.requiredAbsent();
    Map<String, Long> markerVersions = conditions.markerVersions();
    return observeRepository(
        "replace_identity",
        () -> {
          K currentKey = schema.keyFromValue.apply(currentValue);
          K replacementKey = schema.keyFromValue.apply(replacementValue);
          guardSystemObject(currentKey);
          guardSystemObject(replacementKey);
          if (!currentKey.accountId().equals(replacementKey.accountId())) {
            throw new IllegalArgumentException("replacement cannot cross accounts");
          }

          String currentCanonical = schema.canonicalPointerForKey.apply(currentKey);
          String replacementCanonical = schema.canonicalPointerForKey.apply(replacementKey);
          if (currentCanonical.equals(replacementCanonical)) {
            throw new IllegalArgumentException("replacement must use a new resource identity");
          }
          Pointer currentPointer = mutationPointerStore.get(currentCanonical).orElse(null);
          String currentBlobUri = schema.blobUriForKey.apply(currentKey);
          if (currentPointer == null
              || currentPointer.getVersion() != expectedCanonicalVersion
              || !currentBlobUri.equals(requireBlobReference(currentPointer, currentCanonical))) {
            return Optional.empty();
          }

          String replacementBlobUri = schema.blobUriForKey.apply(replacementKey);
          int replacementBlobBytes = writeBlobAndGetSize(replacementBlobUri, replacementValue);
          Map<String, String> currentSecondaries =
              schema.secondaryPointersFromValue.apply(currentValue);
          Map<String, String> replacementSecondaries =
              schema.secondaryPointersFromValue.apply(replacementValue);
          Set<String> currentSecondaryKeys = new HashSet<>(currentSecondaries.values());
          Set<String> replacementSecondaryKeys = new HashSet<>(replacementSecondaries.values());

          List<PointerStore.CasOp> ops = new ArrayList<>();
          Set<String> batchedKeys = new HashSet<>();
          batchedKeys.add(currentCanonical);
          ops.add(new PointerStore.CasDelete(currentCanonical, expectedCanonicalVersion));

          if (!batchedKeys.add(replacementCanonical)) {
            throw new IllegalArgumentException("duplicate replacement canonical pointer");
          }
          Pointer replacementReserved =
              reserve(
                  replacementCanonical, replacementBlobUri, replacementValue, replacementBlobBytes);
          ops.add(new PointerStore.CasUpsert(replacementCanonical, 0L, replacementReserved));

          Set<String> allSecondaries = new HashSet<>(currentSecondaryKeys);
          allSecondaries.addAll(replacementSecondaryKeys);
          for (String pointerKey : allSecondaries) {
            boolean inCurrent = currentSecondaryKeys.contains(pointerKey);
            boolean inReplacement = replacementSecondaryKeys.contains(pointerKey);
            if ((pointerKey.equals(currentCanonical) && inCurrent && !inReplacement)
                || (pointerKey.equals(replacementCanonical) && inReplacement && !inCurrent)) {
              continue;
            }
            if (!batchedKeys.add(pointerKey)) {
              throw new IllegalArgumentException(
                  "secondary collides with canonical pointer: " + pointerKey);
            }
            Pointer existing = mutationPointerStore.get(pointerKey).orElse(null);
            if (inCurrent) {
              if (existing == null
                  || !currentBlobUri.equals(requireBlobReference(existing, pointerKey))) {
                throw new CorruptionException(
                    "current secondary is missing or references another blob: " + pointerKey, null);
              }
              if (inReplacement) {
                ops.add(
                    new PointerStore.CasUpsert(
                        pointerKey,
                        existing.getVersion(),
                        reserve(
                            pointerKey,
                            replacementBlobUri,
                            replacementValue,
                            replacementBlobBytes)));
              } else {
                ops.add(new PointerStore.CasDelete(pointerKey, existing.getVersion()));
              }
            } else {
              if (existing != null) {
                throw new NameConflictException("pointer already exists: " + pointerKey);
              }
              ops.add(
                  new PointerStore.CasUpsert(
                      pointerKey,
                      0L,
                      reserve(
                          pointerKey, replacementBlobUri, replacementValue, replacementBlobBytes)));
            }
          }

          addPointerDeletes(pointerVersionsToDelete, batchedKeys, ops);
          addPointerConditions(requiredPointerVersions, requiredAbsentPointers, batchedKeys, ops);
          addMarkerAdvances(markerVersions, batchedKeys, ops, "replacement");

          if (!mutationPointerStore.compareAndSetBatch(ops)) {
            return Optional.empty();
          }
          healCanonicalBlobIfMissing(replacementBlobUri, replacementValue);
          Pointer committedCanonical = replacementReserved.toBuilder().setVersion(1L).build();
          return Optional.of(
              new ResourceWithMeta<>(
                  replacementValue,
                  committedMeta(
                      Optional.of(committedCanonical),
                      replacementCanonical,
                      replacementBlobUri,
                      Timestamps.fromMillis(clock.millis()))));
        });
  }

  public boolean delete(K key) {
    return observeRepository(
        "delete",
        () -> {
          guardSystemObject(key);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          var canonicalPtr = mutationPointerStore.get(canonicalPointer).orElse(null);
          if (canonicalPtr == null) {
            return false;
          }
          String blobUri = resolveBlobUriForDelete(key, canonicalPointer);
          Optional<T> current;
          try {
            current = getByKeyForMutation(key);
          } catch (CorruptionException e) {
            if (!deleteCanonicalPointer(canonicalPointer, canonicalPtr.getVersion())) {
              return false;
            }
            if (!schema.casBlobs && !blobUri.isBlank()) {
              deleteQuietly(() -> mutationBlobStore.delete(blobUri));
            }
            return true;
          }
          if (current.isEmpty()) {
            return false;
          }
          T currentValue = current.get();

          if (!deleteAtomically(
              canonicalPointer,
              canonicalPtr.getVersion(),
              new HashSet<>(schema.secondaryPointersFromValue.apply(currentValue).values()))) {
            return false;
          }

          if (!schema.casBlobs && !blobUri.isBlank()) {
            deleteQuietly(() -> mutationBlobStore.delete(blobUri));
          }
          return true;
        });
  }

  public boolean deleteWithPrecondition(K key, long expectedCanonicalVersion) {
    return deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        key, expectedCanonicalVersion, PointerConditions.none(), Map.of());
  }

  /**
   * Deletes the resource and supplied external pointers atomically while all remaining external
   * pointer conditions hold.
   */
  public boolean deleteWithPreconditionWhilePointersMatchAndDeletePointers(
      K key,
      long expectedCanonicalVersion,
      PointerConditions conditions,
      Map<String, Long> pointerVersionsToDelete) {
    return deleteWithPrecondition(
        key, expectedCanonicalVersion, conditions, pointerVersionsToDelete, List.of());
  }

  /**
   * Deletes the resource and the caller's companion operations in one atomic pointer transaction.
   * Used to cascade into a resource this one owns, so neither can outlive the other.
   */
  public boolean deleteWithPreconditionAndCompanions(
      K key, long expectedCanonicalVersion, List<PointerStore.CasOp> companions) {
    return deleteWithPrecondition(
        key, expectedCanonicalVersion, PointerConditions.none(), Map.of(), companions);
  }

  private boolean deleteWithPrecondition(
      K key,
      long expectedCanonicalVersion,
      PointerConditions conditions,
      Map<String, Long> pointerVersionsToDelete,
      List<PointerStore.CasOp> companions) {
    Map<String, Long> requiredPointerVersions = conditions.requiredVersions();
    Set<String> requiredAbsentPointers = conditions.requiredAbsent();
    return observeRepository(
        "delete_with_precondition",
        () -> {
          guardSystemObject(key);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = resolveBlobUriForDelete(key, canonicalPointer);
          Optional<T> current;
          try {
            current = getByKeyForMutation(key);
          } catch (CorruptionException e) {
            if (!deleteAtomically(
                canonicalPointer,
                expectedCanonicalVersion,
                Set.of(),
                requiredPointerVersions,
                requiredAbsentPointers,
                pointerVersionsToDelete,
                companions)) {
              return false;
            }
            if (!schema.casBlobs && !blobUri.isBlank()) {
              deleteQuietly(() -> mutationBlobStore.delete(blobUri));
            }
            return true;
          }
          if (current.isEmpty()) {
            return false;
          }
          T currentValue = current.get();

          if (!deleteAtomically(
              canonicalPointer,
              expectedCanonicalVersion,
              new HashSet<>(schema.secondaryPointersFromValue.apply(currentValue).values()),
              requiredPointerVersions,
              requiredAbsentPointers,
              pointerVersionsToDelete,
              companions)) {
            return false;
          }

          if (!schema.casBlobs && !blobUri.isBlank()) {
            deleteQuietly(() -> mutationBlobStore.delete(blobUri));
          }
          return true;
        });
  }

  private boolean deleteAtomically(
      String canonicalPointer, long expectedCanonicalVersion, Set<String> currentSecondary) {
    return deleteAtomically(
        canonicalPointer,
        expectedCanonicalVersion,
        currentSecondary,
        Map.of(),
        Set.of(),
        Map.of(),
        List.of());
  }

  /**
   * Returns the pointer operations that would delete this resource at its current versions, without
   * committing anything, so a caller can fold them into another repository's atomic mutation. The
   * counterpart to {@link #prepareCreateOps}.
   *
   * <p>Returns empty when the resource is already absent, which lets a cascading caller treat a
   * partially completed earlier attempt as work already done rather than as an error.
   */
  public List<PointerStore.CasOp> prepareDeleteOps(K key) {
    guardSystemObject(key);
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    Pointer canonical = mutationPointerStore.get(canonicalPointer).orElse(null);
    if (canonical == null) {
      return List.of();
    }
    Optional<T> current = getByKeyForMutation(key);
    if (current.isEmpty()) {
      return List.of(new PointerStore.CasDelete(canonicalPointer, canonical.getVersion()));
    }
    Set<String> batchedKeys = new HashSet<>();
    List<PointerStore.CasOp> ops = new ArrayList<>();
    batchedKeys.add(canonicalPointer);
    ops.add(new PointerStore.CasDelete(canonicalPointer, canonical.getVersion()));
    for (String pointerKey : schema.secondaryPointersFromValue.apply(current.get()).values()) {
      if (!batchedKeys.add(pointerKey)) {
        continue;
      }
      Pointer secondary = mutationPointerStore.get(pointerKey).orElse(null);
      ops.add(
          secondary != null
              ? new PointerStore.CasDelete(pointerKey, secondary.getVersion())
              : new PointerStore.CasCheckAbsent(pointerKey));
    }
    return List.copyOf(ops);
  }

  private boolean deleteAtomically(
      String canonicalPointer,
      long expectedCanonicalVersion,
      Set<String> currentSecondary,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      Map<String, Long> pointerVersionsToDelete,
      List<PointerStore.CasOp> companions) {
    Set<String> batchedKeys = new HashSet<>();
    List<PointerStore.CasOp> ops = new ArrayList<>();

    batchedKeys.add(canonicalPointer);
    ops.add(new PointerStore.CasDelete(canonicalPointer, expectedCanonicalVersion));

    for (String pointerKey : currentSecondary) {
      if (!batchedKeys.add(pointerKey)) {
        continue;
      }
      Pointer secondary = mutationPointerStore.get(pointerKey).orElse(null);
      if (secondary != null) {
        ops.add(new PointerStore.CasDelete(pointerKey, secondary.getVersion()));
      } else {
        ops.add(new PointerStore.CasCheckAbsent(pointerKey));
      }
    }

    addPointerDeletes(pointerVersionsToDelete, batchedKeys, ops);
    addPointerConditions(requiredPointerVersions, requiredAbsentPointers, batchedKeys, ops);
    for (PointerStore.CasOp companion : companions) {
      if (!batchedKeys.add(companion.key())) {
        throw new IllegalArgumentException(
            "duplicate companion pointer in atomic delete: " + companion.key());
      }
      ops.add(companion);
    }

    return mutationPointerStore.compareAndSetBatch(ops);
  }

  private boolean deleteCanonicalPointer(String canonicalPointer, long expectedCanonicalVersion) {
    return deleteCanonicalPointer(
        canonicalPointer, expectedCanonicalVersion, Map.of(), Set.of(), Map.of());
  }

  private boolean deleteCanonicalPointer(
      String canonicalPointer,
      long expectedCanonicalVersion,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      Map<String, Long> pointerVersionsToDelete) {
    List<PointerStore.CasOp> ops = new ArrayList<>();
    Set<String> batchedKeys = new HashSet<>();
    batchedKeys.add(canonicalPointer);
    ops.add(new PointerStore.CasDelete(canonicalPointer, expectedCanonicalVersion));
    addPointerDeletes(pointerVersionsToDelete, batchedKeys, ops);
    addPointerConditions(requiredPointerVersions, requiredAbsentPointers, batchedKeys, ops);
    return mutationPointerStore.compareAndSetBatch(ops);
  }

  private static void addPointerDeletes(
      Map<String, Long> pointerVersionsToDelete,
      Set<String> batchedKeys,
      List<PointerStore.CasOp> ops) {
    for (var entry : pointerVersionsToDelete.entrySet()) {
      if (!batchedKeys.add(entry.getKey())) {
        throw new IllegalArgumentException("duplicate pointer in atomic delete: " + entry.getKey());
      }
      ops.add(new PointerStore.CasDelete(entry.getKey(), entry.getValue()));
    }
  }

  public MutationMeta metaFor(K key) {
    return metaFor(key, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaFor(K key, Timestamp nowTs) {
    return observeRepository(
        "meta_for",
        () -> {
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          var pointer =
              pointerReads
                  .get(canonicalPointer)
                  .orElseThrow(
                      () ->
                          new NotFoundException(
                              "Pointer missing for "
                                  + schema.resourceName
                                  + ": "
                                  + canonicalPointer));
          return readMetaOrDefault(
              Optional.of(pointer), canonicalPointer, pointer.getBlobUri(), nowTs);
        });
  }

  public MutationMeta metaForSafe(K key) {
    return metaForSafe(key, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaForSafe(K key, Timestamp nowTs) {
    return observeRepository(
        "meta_for_safe",
        () -> {
          String canonical = schema.canonicalPointerForKey.apply(key);
          var ptrOpt = pointerReads.get(canonical);
          if (schema.casBlobs && ptrOpt.isEmpty()) {
            return MutationMeta.newBuilder()
                .setPointerKey(canonical)
                .setBlobUri("")
                .setPointerVersion(0L)
                .setEtag("")
                .setUpdatedAt(nowTs)
                .build();
          }
          String blobUri = blobUriFor(key, ptrOpt);
          return readMetaOrDefault(ptrOpt, canonical, blobUri, nowTs);
        });
  }

  /**
   * Pointer-only mutation meta: one pointer read, no blob HEAD, blank etag. For consumers that only
   * need the pointer version and key (e.g. metadata-graph cache keys and node hydration) — callers
   * returning meta to RPC clients must keep using {@link #metaForSafe}, whose etag feeds
   * precondition checks.
   */
  public MutationMeta pointerMetaForSafe(K key) {
    return observeRepository(
        "pointer_meta_for_safe",
        () -> {
          Timestamp nowTs = Timestamps.fromMillis(clock.millis());
          String canonical = schema.canonicalPointerForKey.apply(key);
          var ptrOpt = pointerReads.get(canonical);
          String blobUri = blobUriFor(key, ptrOpt);
          return MutationMeta.newBuilder()
              .setPointerKey(canonical)
              .setBlobUri(blobUri)
              .setPointerVersion(ptrOpt.map(Pointer::getVersion).orElse(0L))
              .setEtag("")
              .setUpdatedAt(nowTs)
              .build();
        });
  }

  private String blobUriFor(K key, Optional<Pointer> ptrOpt) {
    if (schema.casBlobs) {
      return (ptrOpt.isPresent() && ptrOpt.get().getBlobUri() != null)
          ? ptrOpt.get().getBlobUri()
          : "";
    }
    return schema.blobUriForKey.apply(key);
  }

  private String resolveBlobUriForDelete(K key, String canonicalPointer) {
    if (schema.casBlobs) {
      var ptrOpt = mutationPointerStore.get(canonicalPointer);
      if (ptrOpt.isPresent() && ptrOpt.get().getBlobUri() != null) {
        return ptrOpt.get().getBlobUri();
      }
      return "";
    }
    return schema.blobUriForKey.apply(key);
  }

  @Override
  protected String resourceName() {
    return schema.resourceName;
  }
}
