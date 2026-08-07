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

  public GenericResourceRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      ResourceSchema<T, K> schema,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType) {
    this(pointerStore, blobStore, schema, parser, toBytes, contentType, null);
  }

  public GenericResourceRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      ResourceSchema<T, K> schema,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType,
      ImmutableBlobCache blobCache) {
    super(pointerStore, blobStore, parser, toBytes, contentType, blobCache);
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
    if (blobCacheable()) {
      // CONTENT-only read: a resident decode may outlive the durable blob, so an empty result
      // means absent but a present result does NOT prove the blob still exists. Callers whose
      // emptiness doubles as a liveness/integrity check must use getByBlobUriLive.
      return blobCache.get(blobUri, this::loadAndParseBlob);
    }
    return loadAndParseBlob(blobUri);
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
        "blob_etag", () -> blobStore.head(blobUri).map(BlobHeader::getEtag).orElse(null));
  }

  private Optional<T> getByKeyUnobserved(K key) {
    return read(schema.canonicalPointerForKey.apply(key));
  }

  private boolean existsByKeyUnobserved(K key) {
    return pointerStore.get(schema.canonicalPointerForKey.apply(key)).isPresent();
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
    create(value, BatchGuard.NONE);
  }

  /**
   * Guarded {@link #create(Object)}: {@code guard}'s preconditions join the same all-or-nothing
   * batch, so the resource becomes visible only while the guarded state still holds. Used to
   * publish a child into a parent namespace atomically with respect to that namespace's deletion —
   * see {@link BatchGuard}.
   *
   * <p>Benign guard contention (a sibling create advancing the same children marker) is absorbed
   * here by re-reading the guard and re-running the batch, keeping its cost equivalent to the
   * marker's own CAS loop rather than escalating to an RPC retry. A guard that is {@link
   * BatchGuard.Outcome#BROKEN} — the parent is gone — raises {@link
   * BaseResourceRepository.BatchGuardFailedException}.
   */
  public void create(T value, BatchGuard guard) {
    observeRepository(
        "create",
        () -> {
          K key = schema.keyFromValue.apply(value);
          guardSystemObject(key);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = schema.blobUriForKey.apply(key);

          int blobBytes = writeBlobAndGetSize(blobUri, value);

          Map<String, String> secondaries = schema.secondaryPointersFromValue.apply(value);
          // Canonical first, then secondaries, de-duplicated: some schemas (e.g. snapshots) expose
          // the
          // canonical by-id pointer as a secondary too, and a transactional batch must not contain
          // two
          // operations on the same key (DynamoDB rejects duplicate items within a transaction). All
          // ops
          // for a given key target the same blob, so dropping the duplicate is loss-free.
          LinkedHashSet<String> uniqueKeys = new LinkedHashSet<>(1 + secondaries.size());
          uniqueKeys.add(canonicalPointer);
          uniqueKeys.addAll(secondaries.values());
          List<String> pointerKeys = new ArrayList<>(uniqueKeys);

          for (int attempt = 0; ; attempt++) {
            List<PointerStore.CasOp> ops = new ArrayList<>(pointerKeys.size());
            for (String pointerKey : pointerKeys) {
              ops.add(
                  new PointerStore.CasUpsert(
                      pointerKey, 0L, reserve(pointerKey, blobUri, value, blobBytes)));
            }
            appendGuardOps(ops, uniqueKeys, guard);

            if (pointerStore.compareAndSetBatch(ops)) {
              healCanonicalBlobIfMissing(blobUri, value);
              return;
            }

            // Re-read the guard once — NamespaceChildGuard re-captures its marker on RETRY, so a
            // second reevaluate would report HOLDS and lose the contention it just absorbed.
            BatchGuard.Outcome verdict = guard.reevaluate();
            requireGuardIntact(verdict, guard, "create");

            // Classify this mutation's own conflict BEFORE honouring a guard retry. A name
            // collision
            // is terminal no matter how busy the guarded marker is; deciding retry first meant that
            // under sustained sibling contention the retries ran out and the collision surfaced as
            // a
            // retryable abort instead of ALREADY_EXISTS.
            if (classifyCreateConflict(blobUri, pointerKeys)) {
              return;
            }

            // Nothing of ours is present, so the batch failed on the guard or on a transient store
            // conflict. Absorbing a moved marker here keeps concurrent sibling creates as cheap as
            // the marker's own CAS loop rather than escalating to an RPC retry.
            if (verdict == BatchGuard.Outcome.RETRY) {
              requireRetryBudget(guard, "create", attempt);
              continue;
            }
            throw new AbortRetryableException(
                "create conflict, no pointer present: " + pointerKeys.get(0));
          }
        });
  }

  /**
   * Appends a guard's preconditions to a batch, skipping any check whose key the mutation already
   * constrains. A transactional batch must not carry two operations on one key — DynamoDB rejects
   * duplicate items outright — and this can legitimately happen when a resource is republished
   * under a parent that resolves to the resource itself (a namespace moved beneath its own path).
   * Dropping such a check is loss-free only when the mutation actually emitted an operation for the
   * key. Update planning can record an idempotent secondary in {@code batchedKeys} without an op,
   * so that case is rejected rather than silently erasing the guard.
   *
   * <p>A guard <em>write</em> that collides is a different matter — silently dropping it would
   * weaken the fence to nothing — so it is refused rather than merged. No key layout produces that
   * today (a children marker is never a resource pointer); the check exists so a future schema
   * cannot introduce it quietly.
   */
  static void appendGuardOps(
      List<PointerStore.CasOp> ops, Set<String> batchedKeys, BatchGuard guard) {
    for (PointerStore.CasOp op : guard.ops()) {
      String key = op.key();
      if (batchedKeys.contains(key)) {
        if (op instanceof PointerStore.CasCheck || op instanceof PointerStore.CasCheckAbsent) {
          boolean constrained = ops.stream().anyMatch(batchOp -> batchOp.key().equals(key));
          if (constrained) {
            continue;
          }
          throw new IllegalStateException(
              "batch guard check was shadowed by an unconstrained mutation key: " + key);
        }
        throw new IllegalStateException(
            "batch guard writes a key the mutation already mutates: " + key);
      }
      ops.add(op);
    }
  }

  /**
   * Refuses to go any further when the guarded parent is gone or has changed. Checked before the
   * mutation classifies its own conflict: the parent's fate is more fundamental than anything
   * happening inside it, and a retry re-resolves the parent and reports the accurate NOT_FOUND.
   */
  private void requireGuardIntact(BatchGuard.Outcome verdict, BatchGuard guard, String operation) {
    if (verdict == BatchGuard.Outcome.BROKEN) {
      throw new BatchGuardFailedException(
          operation + " lost the race against deletion of " + guard.describe());
    }
  }

  /** Bounds in-repository absorption of guard contention, so a hot marker cannot spin forever. */
  private void requireRetryBudget(BatchGuard guard, String operation, int attempt) {
    // attempt is zero-based and this check follows a failed batch. Stop after CAS_MAX total batch
    // attempts, rather than allowing attempts 0..CAS_MAX (CAS_MAX + 1 writes).
    if (attempt >= CAS_MAX - 1) {
      throw new AbortRetryableException(
          operation
              + " contended on guard for "
              + guard.describe()
              + " after "
              + CAS_MAX
              + " attempts");
    }
  }

  /**
   * Reads back the batch's own pointers, walking canonical-then-secondary order so a conflict
   * reports the same key and message it always has.
   *
   * @return true when the read-back settled the outcome — every pointer already resolves to this
   *     blob, so the create is a byte-identical no-op. False when no pointer is present at all,
   *     which means the failure was not this mutation's own conflict and the caller decides what it
   *     was. A genuine collision or a stored inconsistency throws.
   */
  private boolean classifyCreateConflict(String blobUri, List<String> pointerKeys) {
    int present = 0;
    int absent = 0;
    for (String pointerKey : pointerKeys) {
      Pointer pointer = pointerStore.get(pointerKey).orElse(null);
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
      return true;
    }
    if (present == 0) {
      // Read-back finds no pointer at all, so nothing here conflicted with us — the guard moved, or
      // the store reported a transient batch conflict. Either way it is the caller's call.
      return false;
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
          boolean blobExistedBefore = blobStore.head(blobUri).isPresent();

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

          if (pointerStore.compareAndSetBatch(ops)) {
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

    Pointer canonical = pointerStore.get(canonicalPointer).orElse(null);
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
      Pointer secondary = pointerStore.get(pointerKey).orElse(null);
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
    Pointer pointer = pointerStore.get(canonicalPointer).orElse(null);
    if (pointer != null && blobUri.equals(pointer.getBlobUri())) {
      return;
    }
    deleteQuietly(() -> blobStore.delete(blobUri));
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
    return update(updatedValue, expectedCanonicalVersion, BatchGuard.NONE);
  }

  /**
   * Guarded {@link #update(Object, long)}: {@code guard}'s preconditions join the same
   * all-or-nothing batch. Used when an update republishes a resource under a <em>different</em>
   * parent (a relation or namespace reparent), which is a child-publishing write into the
   * destination and must be atomic with respect to that destination's deletion — see {@link
   * BatchGuard}.
   */
  public boolean update(T updatedValue, long expectedCanonicalVersion, BatchGuard guard) {
    return observeRepository(
        "update",
        () -> {
          K key = schema.keyFromValue.apply(updatedValue);
          guardSystemObject(key);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = schema.blobUriForKey.apply(key);

          T currentValue =
              getByKeyUnobserved(key)
                  .orElseThrow(
                      () ->
                          new NotFoundException(
                              schema.resourceName
                                  + " not found for canonical: "
                                  + canonicalPointer));
          String currentBlobUri =
              schema.blobUriForKey.apply(schema.keyFromValue.apply(currentValue));

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
          for (int attempt = 0; ; attempt++) {
            Set<String> batchedKeys = new HashSet<>();
            List<PointerStore.CasOp> ops = new ArrayList<>();

            batchedKeys.add(canonicalPointer);
            ops.add(
                new PointerStore.CasUpsert(
                    canonicalPointer,
                    expectedCanonicalVersion,
                    reserve(canonicalPointer, blobUri, updatedValue, blobBytes)));

            for (String p : toAdd) {
              if (!batchedKeys.add(p)) {
                continue;
              }
              Pointer existing = pointerStore.get(p).orElse(null);
              if (existing == null) {
                ops.add(
                    new PointerStore.CasUpsert(
                        p, 0L, reserve(p, blobUri, updatedValue, blobBytes)));
              } else if (!blobUri.equals(existing.getBlobUri())) {
                // The new name already belongs to a different blob. Nothing has been committed, so
                // failing
                // fast here leaves no partial state.
                throw new NameConflictException("pointer bound to different blob: " + p);
              }
              // else: already reserved to our blob — idempotent, no op needed.
            }

            if (blobChanged) {
              // Kept secondaries still point at the old content-addressed blob; advance each onto
              // the new
              // one (or reserve it if a legacy gap left it absent).
              for (String p : kept) {
                if (!batchedKeys.add(p)) {
                  continue;
                }
                Pointer existing = pointerStore.get(p).orElse(null);
                if (existing == null) {
                  ops.add(
                      new PointerStore.CasUpsert(
                          p, 0L, reserve(p, blobUri, updatedValue, blobBytes)));
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
              Pointer existing = pointerStore.get(p).orElse(null);
              if (existing != null) {
                ops.add(new PointerStore.CasDelete(p, existing.getVersion()));
              }
            }

            appendGuardOps(ops, batchedKeys, guard);

            if (pointerStore.compareAndSetBatch(ops)) {
              healCanonicalBlobIfMissing(blobUri, updatedValue);
              return true;
            }

            // One reevaluate per failed batch (see create), and the same precedence: a lost
            // optimistic-concurrency check or a name bound elsewhere is this mutation's own answer,
            // and must not be re-labelled as guard contention just because a sibling is publishing
            // into the same namespace.
            BatchGuard.Outcome verdict = guard.reevaluate();
            requireGuardIntact(verdict, guard, "update");

            var ownConflict =
                classifyUpdateConflict(
                    canonicalPointer, expectedCanonicalVersion, blobUri, toAdd, verdict);
            if (ownConflict != null) {
              return ownConflict;
            }

            requireRetryBudget(guard, "update", attempt);
          }
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
      if (blobStore.head(blobUri).isPresent()) {
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

  /**
   * Classifies a failed update against its own pointers.
   *
   * @return {@code false} when the canonical pointer moved — an optimistic-concurrency miss, which
   *     is the caller's answer — or {@code null} when nothing here explains the failure and {@code
   *     guardVerdict} says the guarded marker moved, so the batch is worth re-running. A name owned
   *     by a different blob throws, as does an unexplained failure with the guard holding.
   */
  private Boolean classifyUpdateConflict(
      String canonicalPointer,
      long expectedCanonicalVersion,
      String blobUri,
      Set<String> toAdd,
      BatchGuard.Outcome guardVerdict) {
    Pointer canonical = pointerStore.get(canonicalPointer).orElse(null);
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
      Pointer secondary = pointerStore.get(p).orElse(null);
      if (secondary != null && !blobUri.equals(secondary.getBlobUri())) {
        throw new NameConflictException("pointer bound to different blob: " + p);
      }
    }
    if (guardVerdict == BatchGuard.Outcome.RETRY) {
      return null;
    }
    throw new AbortRetryableException("update conflict for: " + canonicalPointer);
  }

  public boolean delete(K key) {
    return delete(key, BatchGuard.NONE);
  }

  /**
   * Guarded {@link #delete(Object)}: {@code guard}'s preconditions join the same all-or-nothing
   * batch, so the resource disappears only while the guarded state still holds. Used to remove a
   * parent namespace atomically with respect to any child being published into it — see {@link
   * BatchGuard}.
   */
  public boolean delete(K key, BatchGuard guard) {
    return observeRepository(
        "delete",
        () -> {
          guardSystemObject(key);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          var canonicalPtr = pointerStore.get(canonicalPointer).orElse(null);
          if (canonicalPtr == null) {
            return false;
          }
          String blobUri = resolveBlobUriForDelete(key, canonicalPointer);
          Optional<T> current;
          try {
            current = getByKeyUnobserved(key);
          } catch (CorruptionException e) {
            if (!deleteCanonicalPointer(canonicalPointer, canonicalPtr.getVersion(), guard)) {
              return false;
            }
            if (!schema.casBlobs && !blobUri.isBlank()) {
              deleteQuietly(() -> blobStore.delete(blobUri));
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
              new HashSet<>(schema.secondaryPointersFromValue.apply(currentValue).values()),
              guard)) {
            return false;
          }

          if (!schema.casBlobs && !blobUri.isBlank()) {
            deleteQuietly(() -> blobStore.delete(blobUri));
          }
          return true;
        });
  }

  public boolean deleteWithPrecondition(K key, long expectedCanonicalVersion) {
    return deleteWithPrecondition(key, expectedCanonicalVersion, BatchGuard.NONE);
  }

  /**
   * Guarded {@link #deleteWithPrecondition(ResourceKey, long)}; see {@link #delete(Object,
   * BatchGuard)}.
   */
  public boolean deleteWithPrecondition(K key, long expectedCanonicalVersion, BatchGuard guard) {
    return observeRepository(
        "delete_with_precondition",
        () -> {
          guardSystemObject(key);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = resolveBlobUriForDelete(key, canonicalPointer);
          Optional<T> current;
          try {
            current = getByKeyUnobserved(key);
          } catch (CorruptionException e) {
            if (!deleteCanonicalPointer(canonicalPointer, expectedCanonicalVersion, guard)) {
              return false;
            }
            if (!schema.casBlobs && !blobUri.isBlank()) {
              deleteQuietly(() -> blobStore.delete(blobUri));
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
              guard)) {
            return false;
          }

          if (!schema.casBlobs && !blobUri.isBlank()) {
            deleteQuietly(() -> blobStore.delete(blobUri));
          }
          return true;
        });
  }

  private boolean deleteAtomically(
      String canonicalPointer,
      long expectedCanonicalVersion,
      Set<String> currentSecondary,
      BatchGuard guard) {
    Set<String> batchedKeys = new HashSet<>();
    List<PointerStore.CasOp> ops = new ArrayList<>();

    batchedKeys.add(canonicalPointer);
    ops.add(new PointerStore.CasDelete(canonicalPointer, expectedCanonicalVersion));

    for (String pointerKey : currentSecondary) {
      if (!batchedKeys.add(pointerKey)) {
        continue;
      }
      Pointer secondary = pointerStore.get(pointerKey).orElse(null);
      if (secondary != null) {
        ops.add(new PointerStore.CasDelete(pointerKey, secondary.getVersion()));
      } else {
        ops.add(new PointerStore.CasCheckAbsent(pointerKey));
      }
    }

    appendGuardOps(ops, batchedKeys, guard);

    return commitGuardedDelete(ops, guard);
  }

  private boolean deleteCanonicalPointer(
      String canonicalPointer, long expectedCanonicalVersion, BatchGuard guard) {
    List<PointerStore.CasOp> ops = new ArrayList<>();
    ops.add(new PointerStore.CasDelete(canonicalPointer, expectedCanonicalVersion));
    appendGuardOps(ops, Set.of(canonicalPointer), guard);
    return commitGuardedDelete(ops, guard);
  }

  /**
   * Commits a delete batch, distinguishing a guard failure from an ordinary precondition miss. A
   * broken guard is never retried here: it means a child may have been published, and only the
   * caller's emptiness scan can decide whether the delete is still legal, so it is surfaced for the
   * caller to re-run that scan.
   */
  private boolean commitGuardedDelete(List<PointerStore.CasOp> ops, BatchGuard guard) {
    if (pointerStore.compareAndSetBatch(ops)) {
      return true;
    }
    BatchGuard.Outcome verdict = guard.reevaluate();
    if (verdict == BatchGuard.Outcome.BROKEN) {
      throw new BatchGuardFailedException(
          "delete lost the race against a child published into " + guard.describe());
    }
    if (verdict == BatchGuard.Outcome.RETRY) {
      // The guard refreshed its own preconditions. The operations above were built against the
      // previous guard state, so the safe retry boundary is the caller's whole delete attempt,
      // which re-reads both the resource and guard ops.
      throw new AbortRetryableException(
          "delete guard moved while deleting from " + guard.describe());
    }
    return false;
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
              pointerStore
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
          var ptrOpt = pointerStore.get(canonical);
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
          var ptrOpt = pointerStore.get(canonical);
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
      var ptrOpt = pointerStore.get(canonicalPointer);
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
