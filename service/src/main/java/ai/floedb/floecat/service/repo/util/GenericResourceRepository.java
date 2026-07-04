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

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.ResourceKey;
import ai.floedb.floecat.service.repo.model.ResourceSchema;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
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
    super(pointerStore, blobStore, parser, toBytes, contentType);
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  public Optional<T> getByKey(K key) {
    return observeRepository("get_by_key", () -> getByKeyUnobserved(key));
  }

  private Optional<T> getByKeyUnobserved(K key) {
    return read(schema.canonicalPointerForKey.apply(key));
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
    observeRepository(
        "create",
        () -> {
          K key = schema.keyFromValue.apply(value);
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = schema.blobUriForKey.apply(key);

          writeBlob(blobUri, value);

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

          List<PointerStore.CasOp> ops = new ArrayList<>(pointerKeys.size());
          for (String pointerKey : pointerKeys) {
            ops.add(
                new PointerStore.CasUpsert(pointerKey, 0L, reserve(pointerKey, blobUri, value)));
          }

          if (pointerStore.compareAndSetBatch(ops)) {
            return;
          }

          // The batch committed nothing (atomic) because at least one pointer already existed. Read
          // back
          // and classify, walking canonical-then-secondary order so a conflict reports the same
          // key/message as before.
          classifyCreateConflict(blobUri, pointerKeys);
        });
  }

  private void classifyCreateConflict(String blobUri, List<String> pointerKeys) {
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
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = schema.blobUriForKey.apply(key);
          boolean blobExistedBefore = blobStore.head(blobUri).isPresent();

          writeBlob(blobUri, value);

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
                new PointerStore.CasUpsert(pointerKey, 0L, reserve(pointerKey, blobUri, value)));
          }

          if (pointerStore.compareAndSetBatch(ops)) {
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
    if (schema.resourceIdFromValue != null && value != null) {
      var rid = schema.resourceIdFromValue.apply(value);
      var dn = schema.displayNameFromValue.apply(value);
      if (rid != null && !rid.getId().isEmpty()) {
        return PointerReferences.blobPointer(key, blobUri, 1L, rid, dn != null ? dn : "");
      }
    }
    return PointerReferences.blobPointer(key, blobUri, 1L);
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
    return observeRepository(
        "update",
        () -> {
          K key = schema.keyFromValue.apply(updatedValue);
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

          writeBlob(blobUri, updatedValue);

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
          ops.add(
              new PointerStore.CasUpsert(
                  canonicalPointer,
                  expectedCanonicalVersion,
                  reserve(canonicalPointer, blobUri, updatedValue)));

          for (String p : toAdd) {
            if (!batchedKeys.add(p)) {
              continue;
            }
            Pointer existing = pointerStore.get(p).orElse(null);
            if (existing == null) {
              ops.add(new PointerStore.CasUpsert(p, 0L, reserve(p, blobUri, updatedValue)));
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
              Pointer existing = pointerStore.get(p).orElse(null);
              if (existing == null) {
                ops.add(new PointerStore.CasUpsert(p, 0L, reserve(p, blobUri, updatedValue)));
              } else if (!blobUri.equals(existing.getBlobUri())) {
                ops.add(
                    new PointerStore.CasUpsert(
                        p, existing.getVersion(), reserve(p, blobUri, updatedValue)));
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

          if (pointerStore.compareAndSetBatch(ops)) {
            return true;
          }
          return classifyUpdateConflict(canonicalPointer, expectedCanonicalVersion, blobUri, toAdd);
        });
  }

  private boolean classifyUpdateConflict(
      String canonicalPointer, long expectedCanonicalVersion, String blobUri, Set<String> toAdd) {
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
    throw new AbortRetryableException("update conflict for: " + canonicalPointer);
  }

  public boolean delete(K key) {
    return observeRepository(
        "delete",
        () -> {
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
            if (!deleteCanonicalPointer(canonicalPointer, canonicalPtr.getVersion())) {
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
              new HashSet<>(schema.secondaryPointersFromValue.apply(currentValue).values()))) {
            return false;
          }

          if (!schema.casBlobs && !blobUri.isBlank()) {
            deleteQuietly(() -> blobStore.delete(blobUri));
          }
          return true;
        });
  }

  public boolean deleteWithPrecondition(K key, long expectedCanonicalVersion) {
    return observeRepository(
        "delete_with_precondition",
        () -> {
          String canonicalPointer = schema.canonicalPointerForKey.apply(key);
          String blobUri = resolveBlobUriForDelete(key, canonicalPointer);
          Optional<T> current;
          try {
            current = getByKeyUnobserved(key);
          } catch (CorruptionException e) {
            if (!deleteCanonicalPointer(canonicalPointer, expectedCanonicalVersion)) {
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
              new HashSet<>(schema.secondaryPointersFromValue.apply(currentValue).values()))) {
            return false;
          }

          if (!schema.casBlobs && !blobUri.isBlank()) {
            deleteQuietly(() -> blobStore.delete(blobUri));
          }
          return true;
        });
  }

  private boolean deleteAtomically(
      String canonicalPointer, long expectedCanonicalVersion, Set<String> currentSecondary) {
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

    return pointerStore.compareAndSetBatch(ops);
  }

  private boolean deleteCanonicalPointer(String canonicalPointer, long expectedCanonicalVersion) {
    return pointerStore.compareAndSetBatch(
        List.of(new PointerStore.CasDelete(canonicalPointer, expectedCanonicalVersion)));
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
          return readMetaOrDefault(canonicalPointer, pointer.getBlobUri(), nowTs);
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
          String blobUri;
          if (schema.casBlobs) {
            blobUri =
                (ptrOpt.isPresent() && ptrOpt.get().getBlobUri() != null)
                    ? ptrOpt.get().getBlobUri()
                    : "";
          } else {
            blobUri = schema.blobUriForKey.apply(key);
          }
          return readMetaOrDefault(canonical, blobUri, nowTs);
        });
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
