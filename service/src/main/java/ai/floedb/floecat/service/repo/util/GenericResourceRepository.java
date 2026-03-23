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
import ai.floedb.floecat.service.repo.model.ResourceKey;
import ai.floedb.floecat.service.repo.model.ResourceSchema;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class GenericResourceRepository<T, K extends ResourceKey> extends BaseResourceRepository<T> {

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
    return get(schema.canonicalPointerForKey.apply(key));
  }

  public void create(T value) {
    K key = schema.keyFromValue.apply(value);
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    String blobUri = schema.blobUriForKey.apply(key);

    putBlob(blobUri, value);

    Map<String, String> secondaries = schema.secondaryPointersFromValue.apply(value);
    List<String> kvPairs = new ArrayList<>(2 + 2 * secondaries.size());
    kvPairs.add(canonicalPointer);
    kvPairs.add(blobUri);
    for (String secondaryPtr : secondaries.values()) {
      kvPairs.add(secondaryPtr);
      kvPairs.add(blobUri);
    }
    reserveAllOrRollback(kvPairs.toArray(String[]::new));
  }

  /**
   * Creates a resource only when the canonical pointer is currently absent.
   *
   * <p>Returns {@code true} only when this call won the canonical pointer CAS from version 0 to 1.
   * This provides distributed create-if-absent semantics across repository instances.
   *
   * <p><b>Visibility ordering:</b> the canonical pointer is published atomically via CAS before
   * secondary pointers are created. During secondary creation a concurrent reader may find the
   * resource via the canonical key but not yet via secondary keys. This is an inherent trade-off of
   * the non-transactional storage model; the canonical pointer is the authoritative source of
   * truth.
   *
   * <p><b>Blob cleanup:</b> the blob is written before the canonical CAS attempt. On any failure
   * (CAS miss or secondary creation error), a best-effort cleanup is attempted. For {@code
   * casBlobs} schemas the blob URI is content-addressed (SHA256), so a cleanup failure only wastes
   * space; it has no correctness impact.
   */
  public boolean createIfAbsent(T value) {
    K key = schema.keyFromValue.apply(value);
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    String blobUri = schema.blobUriForKey.apply(key);
    boolean blobExistedBefore = blobStore.head(blobUri).isPresent();

    putBlob(blobUri, value);

    Pointer reserveCanonical =
        Pointer.newBuilder().setKey(canonicalPointer).setBlobUri(blobUri).setVersion(1L).build();
    if (!pointerStore.compareAndSet(canonicalPointer, 0L, reserveCanonical)) {
      cleanupCreateIfAbsentBlobOnCasMiss(canonicalPointer, blobUri, blobExistedBefore);
      return false;
    }

    Map<String, String> secondaries = schema.secondaryPointersFromValue.apply(value);
    final var createdSecondary = new ArrayList<String>(secondaries.size());
    try {
      for (String secondaryPtr : secondaries.values()) {
        var reserve =
            Pointer.newBuilder().setKey(secondaryPtr).setBlobUri(blobUri).setVersion(1L).build();
        if (pointerStore.compareAndSet(secondaryPtr, 0L, reserve)) {
          createdSecondary.add(secondaryPtr);
          continue;
        }
        var pointer = pointerStore.get(secondaryPtr).orElse(null);
        if (pointer == null) {
          throw new AbortRetryableException("pointer suddenly vanished: " + secondaryPtr);
        }
        if (!blobUri.equals(pointer.getBlobUri())) {
          throw new NameConflictException("pointer bound to different blob: " + secondaryPtr);
        }
      }
      return true;
    } catch (Throwable e) {
      for (int i = createdSecondary.size() - 1; i >= 0; i--) {
        compareAndDeleteOrFalse(createdSecondary.get(i), 1L);
      }
      compareAndDeleteOrFalse(canonicalPointer, 1L);
      // Blob was written before the try block; attempt cleanup now that all pointers are gone.
      cleanupCreateIfAbsentBlobOnCasMiss(canonicalPointer, blobUri, blobExistedBefore);
      throw e;
    }
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

  public boolean update(T updatedValue, long expectedCanonicalVersion) {
    K key = schema.keyFromValue.apply(updatedValue);
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    String blobUri = schema.blobUriForKey.apply(key);

    T currentValue =
        getByKey(key)
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

    putBlob(blobUri, updatedValue);

    if (!toAdd.isEmpty()) {
      List<String> kvPairs = new ArrayList<>(2 * toAdd.size());
      for (String p : toAdd) {
        kvPairs.add(p);
        kvPairs.add(blobUri);
      }
      reserveAllOrRollback(kvPairs.toArray(String[]::new));
    }

    try {
      advancePointer(canonicalPointer, blobUri, expectedCanonicalVersion);
    } catch (PreconditionFailedException e) {
      for (String p : toAdd) {
        pointerStore.get(p).ifPresent(ptr -> compareAndDeleteOrFalse(p, ptr.getVersion()));
      }
      return false;
    }

    if (schema.casBlobs && !Objects.equals(currentBlobUri, blobUri)) {
      for (String p : nextSecondary) {
        refreshSecondaryPointer(p, blobUri);
      }
    }

    for (String p : toDelete) {
      pointerStore.get(p).ifPresent(ptr -> compareAndDeleteOrFalse(p, ptr.getVersion()));
    }

    return true;
  }

  private void refreshSecondaryPointer(String key, String blobUri) {
    for (int i = 0; i < CAS_MAX; i++) {
      var ptr = pointerStore.get(key).orElse(null);
      if (ptr == null) {
        Pointer reserve =
            Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
        if (pointerStore.compareAndSet(key, 0L, reserve)) {
          return;
        }
        continue;
      }
      if (Objects.equals(ptr.getBlobUri(), blobUri)) {
        return;
      }
      try {
        advancePointer(key, blobUri, ptr.getVersion());
        return;
      } catch (PreconditionFailedException ignore) {
        // retry on concurrent update
      }
    }
  }

  public boolean delete(K key) {
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    var canonicalPtr = pointerStore.get(canonicalPointer).orElse(null);
    if (canonicalPtr == null) {
      return false;
    }
    String blobUri = resolveBlobUriForDelete(key, canonicalPointer);

    Optional<T> currentValue;
    try {
      currentValue = getByKey(key);
    } catch (CorruptionException e) {
      currentValue = Optional.empty();
    }
    Set<String> currentSecondary =
        currentValue
            .map(schema.secondaryPointersFromValue)
            .map(m -> new HashSet<>(m.values()))
            .orElseGet(HashSet::new);

    if (!compareAndDeleteOrFalse(canonicalPointer, canonicalPtr.getVersion())) {
      return false;
    }
    for (String p : currentSecondary) {
      pointerStore.get(p).ifPresent(ptr -> compareAndDeleteOrFalse(p, ptr.getVersion()));
    }

    if (!schema.casBlobs && !blobUri.isBlank()) {
      deleteQuietly(() -> blobStore.delete(blobUri));
    }
    return true;
  }

  public boolean deleteWithPrecondition(K key, long expectedCanonicalVersion) {
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    String blobUri = resolveBlobUriForDelete(key, canonicalPointer);

    Optional<T> currentValue;
    try {
      currentValue = getByKey(key);
    } catch (CorruptionException e) {
      currentValue = Optional.empty();
    }
    Set<String> currentSecondary =
        currentValue
            .map(schema.secondaryPointersFromValue)
            .map(m -> new HashSet<>(m.values()))
            .orElseGet(HashSet::new);

    if (!compareAndDeleteOrFalse(canonicalPointer, expectedCanonicalVersion)) {
      return false;
    }

    for (String p : currentSecondary) {
      pointerStore.get(p).ifPresent(ptr -> compareAndDeleteOrFalse(p, ptr.getVersion()));
    }

    if (!schema.casBlobs && !blobUri.isBlank()) {
      deleteQuietly(() -> blobStore.delete(blobUri));
    }
    return true;
  }

  public MutationMeta metaFor(K key) {
    return metaFor(key, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaFor(K key, Timestamp nowTs) {
    String canonicalPointer = schema.canonicalPointerForKey.apply(key);
    var pointer =
        pointerStore
            .get(canonicalPointer)
            .orElseThrow(
                () ->
                    new NotFoundException(
                        "Pointer missing for " + schema.resourceName + ": " + canonicalPointer));
    return safeMetaOrDefault(canonicalPointer, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(K key) {
    return metaForSafe(key, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaForSafe(K key, Timestamp nowTs) {
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
    return safeMetaOrDefault(canonical, blobUri, nowTs);
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
}
