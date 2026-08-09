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
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleUris;
import ai.floedb.floecat.service.repo.ResourceRepository;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.NoopObservability;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.helpers.StoreMetrics;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.quarkus.arc.Arc;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class BaseResourceRepository<T> implements ResourceRepository<T> {
  private static final Observability NOOP_OBSERVABILITY = new NoopObservability();
  private static volatile Observability cachedObservability;

  protected PointerStore mutationPointerStore;
  protected BlobStore mutationBlobStore;
  protected RepositoryReads mutationReads;
  protected RepositoryReads.Pointers pointerReads;
  protected RepositoryReads.Blobs blobReads;
  protected ProtoParser<T> parser;
  protected Function<T, byte[]> toBytes;
  protected String contentType;
  // Optional decoded-content cache for immutable (content-addressed) blobs; null = no caching.
  protected ImmutableBlobCache blobCache;

  public static final int CAS_MAX = 10;

  protected static final Clock clock = Clock.systemUTC();

  public BaseResourceRepository() {
    super();
  }

  public static class RepoException extends RuntimeException {
    public RepoException(String msg) {
      super(msg);
    }

    public RepoException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  public static class NameConflictException extends RepoException {
    public NameConflictException(String msg) {
      super(msg);
    }
  }

  /**
   * Thrown by the repository write path when a mutation targets a system-owned resource id. System
   * objects are immutable; the surface-layer {@code CatalogSurfaceWritePolicy} is the primary gate
   * (with user-facing errors), and this is the structural backstop so that no write path — a
   * service that forgot the policy, a future caller — can persist a system-object mutation.
   */
  public static class SystemObjectImmutableException extends RepoException {
    public SystemObjectImmutableException(String msg) {
      super(msg);
    }
  }

  public static class PreconditionFailedException extends RepoException {
    public PreconditionFailedException(String msg) {
      super(msg);
    }
  }

  public static class AbortRetryableException extends RepoException {
    public AbortRetryableException(String msg) {
      super(msg);
    }
  }

  public static class NotFoundException extends RepoException {
    public NotFoundException(String msg) {
      super(msg);
    }
  }

  public static class CorruptionException extends RepoException {
    public CorruptionException(String msg) {
      super(msg);
    }

    public CorruptionException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  protected BaseResourceRepository(
      PointerStore mutationPointerStore,
      BlobStore mutationBlobStore,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType) {
    this(mutationPointerStore, mutationBlobStore, parser, toBytes, contentType, null);
  }

  protected BaseResourceRepository(
      PointerStore mutationPointerStore,
      BlobStore mutationBlobStore,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType,
      ImmutableBlobCache blobCache) {
    this(
        mutationPointerStore,
        mutationBlobStore,
        parser,
        toBytes,
        contentType,
        blobCache,
        RepositoryReads.direct(mutationPointerStore, mutationBlobStore));
  }

  /**
   * Compose raw stores for mutation transactions with the read capabilities selected for this
   * repository family. Reads performed as part of a write protocol stay on the raw stores so the
   * protocol retains one direct consistency boundary.
   */
  protected BaseResourceRepository(
      PointerStore mutationPointerStore,
      BlobStore mutationBlobStore,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType,
      ImmutableBlobCache blobCache,
      RepositoryReads reads) {
    this.mutationPointerStore =
        Objects.requireNonNull(mutationPointerStore, "mutationPointerStore");
    this.mutationBlobStore = Objects.requireNonNull(mutationBlobStore, "blobs");
    this.mutationReads = RepositoryReads.direct(mutationPointerStore, mutationBlobStore);
    this.pointerReads = Objects.requireNonNull(reads, "reads").pointers();
    this.blobReads = reads.blobs();
    this.parser = Objects.requireNonNull(parser, "parser");
    this.toBytes = Objects.requireNonNull(toBytes, "toBytes");
    this.contentType = Objects.requireNonNull(contentType, "contentType");
    this.blobCache = blobCache;
  }

  /**
   * Whether this repository's blobs are immutable once written (content-addressed) and may be
   * served from {@link ImmutableBlobCache}. Subclasses that know their schema override (see {@code
   * ResourceSchema.casBlobs}); the default is the safe "never cache".
   */
  protected boolean blobsImmutable() {
    return false;
  }

  protected final boolean blobCacheable() {
    return blobCache != null && blobCache.enabled() && blobsImmutable();
  }

  @Override
  public Optional<T> get(String key) {
    return observeRepository("get", () -> read(key));
  }

  protected Optional<T> read(String key) {
    return read(key, pointerReads, this::loadAndParseReferencedBlob);
  }

  /** Read one mutation prerequisite through the raw stores that own the write protocol. */
  protected final Optional<T> readForMutation(String key) {
    return read(
        key,
        mutationReads.pointers(),
        (pointerKey, blobUri) ->
            loadAndParseReferencedBlob(pointerKey, blobUri, mutationReads.blobs()));
  }

  /**
   * Resolve one pointer with the selected read capability, loading its referenced blob through
   * {@code blobLoader}. Cache hits preserve the selected boundary by avoiding the loader; a stable
   * dangling pointer remains corruption, while a concurrently changed pointer reads as absent.
   */
  private Optional<T> read(
      String key,
      RepositoryReads.Pointers pointers,
      BiFunction<String, String, Optional<T>> blobLoader) {
    var pointerStoreOpt = pointers.get(key);
    if (pointerStoreOpt.isEmpty()) {
      return Optional.empty();
    }

    var pointer = pointerStoreOpt.get();
    String blobUri = requireBlobReference(pointer, key);
    Optional<T> loaded =
        blobCacheable()
            ? ReusableArtifactBundleUris.isBundleUri(blobUri)
                ? blobCache.getProjection(blobUri, key, ignored -> blobLoader.apply(key, blobUri))
                : blobCache.get(blobUri, ignored -> blobLoader.apply(key, blobUri))
            : blobLoader.apply(key, blobUri);
    if (loaded.isPresent()) {
      return loaded;
    }
    // The pointed-at blob is absent: either the pointer moved/vanished under us (benign race) or
    // it genuinely dangles (corruption). Absence is never cached, so this re-check stays live.
    if (pointerChangedOrDeleted(pointers, key, pointer)) {
      return Optional.empty();
    }
    throw new CorruptionException("dangling pointer, missing blob: " + blobUri, null);
  }

  /**
   * Fetch-and-decode one blob: empty ONLY for a genuinely absent blob; parse failures and retryable
   * store faults throw. This is the loader {@link ImmutableBlobCache} single-flights — decode
   * semantics live here, once, for both pointer-resolved and blob-direct reads.
   */
  protected final Optional<T> loadAndParseBlob(String blobUri) {
    byte[] bytes = loadBlob(blobReads, blobUri);
    if (bytes == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(parser.parse(bytes));
    } catch (InvalidProtocolBufferException ipbe) {
      throw new CorruptionException("parse failed: " + blobUri, ipbe);
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + blobUri, e);
    }
  }

  /** Decodes a pointer-resolved blob. Subclasses may select one record from a shared bundle. */
  protected Optional<T> loadAndParseReferencedBlob(String pointerKey, String blobUri) {
    return loadAndParseReferencedBlob(pointerKey, blobUri, blobReads);
  }

  /**
   * Load and decode a referenced blob through the selected read capability. Missing blobs are
   * returned as empty so the caller can distinguish a concurrent pointer change from corruption;
   * storage and parse failures propagate with their existing repository semantics.
   */
  private Optional<T> loadAndParseReferencedBlob(
      String pointerKey, String blobUri, RepositoryReads.Blobs blobs) {
    byte[] bytes = loadBlob(blobs, blobUri);
    if (bytes == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(parseReferencedBlob(pointerKey, blobUri, bytes));
    } catch (InvalidProtocolBufferException ipbe) {
      throw new CorruptionException("parse failed: " + blobUri, ipbe);
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + blobUri, e);
    }
  }

  /** Read one blob while preserving policy failures and normalizing the storage API's outcomes. */
  private static byte[] loadBlob(RepositoryReads.Blobs blobs, String blobUri) {
    try {
      return blobs.get(blobUri);
    } catch (StorageNotFoundException notFound) {
      return null;
    } catch (StorageAbortRetryableException retryable) {
      throw new AbortRetryableException("blob read retryable: " + blobUri);
    }
  }

  protected T parseReferencedBlob(String pointerKey, String blobUri, byte[] bytes)
      throws Exception {
    return parser.parse(bytes);
  }

  private boolean pointerChangedOrDeleted(
      RepositoryReads.Pointers pointers, String key, Pointer before) {
    var after = pointers.get(key).orElse(null);
    return after == null || !Objects.equals(after.getBlobUri(), before.getBlobUri());
  }

  private boolean reserveIndexOrIdempotent(String key, String blobUri) {
    return reserveIndexOrIdempotent(key, blobUri, -1L);
  }

  private boolean reserveIndexOrIdempotent(String key, String blobUri, long referencedBytes) {
    var reserve =
        referencedBytes >= 0L
            ? PointerReferences.blobPointer(key, blobUri, 1L, referencedBytes)
            : PointerReferences.blobPointer(key, blobUri, 1L);

    if (mutationPointerStore.compareAndSet(key, 0L, reserve)) {
      return true;
    }

    var pointer = mutationPointerStore.get(key).orElse(null);

    if (pointer == null) {
      throw new AbortRetryableException("pointer suddenly vanished: " + key);
    }

    if (!blobUri.equals(pointer.getBlobUri())) {
      throw new NameConflictException("pointer bound to different blob: " + key);
    }

    return false;
  }

  protected void reserveAllOrRollback(String... keyBlobPairs) {
    reserveAllOrRollback(-1L, keyBlobPairs);
  }

  protected void reserveAllOrRollback(long referencedBytes, String... keyBlobPairs) {
    final var createdKeys = new ArrayList<String>(keyBlobPairs.length / 2);
    try {
      for (int i = 0; i < keyBlobPairs.length; i += 2) {
        final var key = keyBlobPairs[i];
        final var blobUri = keyBlobPairs[i + 1];
        if (reserveIndexOrIdempotent(key, blobUri, referencedBytes)) {
          createdKeys.add(key);
        }
      }
    } catch (Throwable e) {
      for (int i = createdKeys.size() - 1; i >= 0; i--) {
        final var k = createdKeys.get(i);
        try {
          compareAndDeleteOrFalse(k, 1L);
        } catch (Throwable ignore) {
        }
      }
      throw e;
    }
  }

  @Override
  public void putBlob(String blobUri, T value) {
    observeRepository("put_blob", () -> writeBlob(blobUri, value));
  }

  protected void writeBlob(String blobUri, T value) {
    writeBlobAndGetSize(blobUri, value);
  }

  /** Writes and verifies a blob, returning the serialized byte count already available to us. */
  protected int writeBlobAndGetSize(String blobUri, T value) {
    byte[] bytes = toBytes.apply(value);
    String want = sha256B64(bytes);

    // Always PUT, even when an identical blob already exists. Content-addressed URIs recur
    // (rewrites oscillate between content states), and CasBlobGc's min-age fence is anchored on
    // LastModified: skipping the PUT would leave a re-referenced old blob "already old" and thus
    // sweepable in a pass whose mark predates the pointer CAS (eng-floe/core#1904).
    mutationBlobStore.put(blobUri, bytes, contentType);
    var after = mutationBlobStore.head(blobUri);

    if (after.isEmpty() || !want.equals(after.get().getEtag())) {
      throw new AbortRetryableException("blob write verification failed: " + blobUri);
    }
    return bytes.length;
  }

  protected void putBlobStrictBytes(String blobUri, byte[] bytes) {
    observeRepository("put_blob_strict", () -> writeBlobStrictBytes(blobUri, bytes));
  }

  protected void writeBlobStrictBytes(String blobUri, byte[] bytes) {
    final String want = sha256B64(bytes);

    // No exists-with-matching-etag skip: see writeBlob — the PUT refreshes LastModified, which
    // the CAS GC's min-age fence depends on.
    mutationBlobStore.put(blobUri, bytes, contentType);

    if (!mutationBlobStore.head(blobUri).map(h -> want.equals(h.getEtag())).orElse(false)) {
      throw new AbortRetryableException("blob write verification failed: " + blobUri);
    }
  }

  @Override
  public void advancePointer(String key, String blobUri, long expectedVersion) {
    observeRepository(
        "advance_pointer",
        () -> {
          var pointer = mutationPointerStore.get(key).orElse(null);

          if (pointer == null) {
            if (expectedVersion != 0L) {
              throw new PreconditionFailedException(
                  "missing pointer: " + key + " expected=" + expectedVersion);
            }
            var created = PointerReferences.blobPointer(key, blobUri, 1L);
            if (mutationPointerStore.compareAndSet(key, 0L, created)) {
              return;
            }

            var after = mutationPointerStore.get(key).orElse(null);
            if (after == null) {
              throw new AbortRetryableException("pointer vanished during create: " + key);
            }

            throw new PreconditionFailedException(
                "version mismatch: " + key + " expected=0 actual=" + after.getVersion());
          }

          if (pointer.getVersion() != expectedVersion) {
            throw new PreconditionFailedException(
                "version mismatch: "
                    + key
                    + " expected="
                    + expectedVersion
                    + " actual="
                    + pointer.getVersion());
          }

          var next =
              pointer.toBuilder()
                  .setBlobUri(blobUri)
                  .setVersion(pointer.getVersion() + 1)
                  .setReferenceKind(ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_BLOB_URI)
                  .build();
          if (mutationPointerStore.compareAndSet(key, expectedVersion, next)) {
            return;
          }

          var after = mutationPointerStore.get(key).orElse(null);
          if (after == null) {
            throw new AbortRetryableException("pointer vanished during advance: " + key);
          }

          throw new PreconditionFailedException(
              "version mismatch: "
                  + key
                  + " expected="
                  + expectedVersion
                  + " actual="
                  + after.getVersion());
        });
  }

  /**
   * Scans the pointer store by prefix and returns pointers without fetching blobs from S3. Used by
   * topology-ref operations that only need (id, name, kind) — available from pointer metadata.
   *
   * <p>Loops through all DynamoDB pages; a single query returns at most ~1MB.
   */
  private static final int REFS_PAGE_SIZE = 1_000;

  public List<Pointer> listRefsByPrefix(String prefix) {
    return observeRepository(
        "list_refs_by_prefix",
        () -> {
          List<Pointer> out = new ArrayList<>();
          String token = "";
          do {
            var next = new StringBuilder();
            out.addAll(pointerReads.list(prefix, REFS_PAGE_SIZE, token, next));
            token = next.toString();
          } while (!token.isBlank());
          return out;
        });
  }

  public Optional<Pointer> refByPointer(String key) {
    return observeRepository("ref_by_pointer", () -> pointerReads.get(key));
  }

  public record KeyedValue<T>(String key, T value) {}

  @Override
  public List<T> listByPrefix(String prefix, int limit, String token, StringBuilder nextOut) {
    return listByPrefixWithKeys(prefix, limit, token, nextOut).stream()
        .map(KeyedValue::value)
        .toList();
  }

  protected List<KeyedValue<T>> listByPrefixWithKeys(
      String prefix, int limit, String token, StringBuilder nextOut) {
    return observeRepository(
        "list_by_prefix",
        () -> {
          var rows = pointerReads.list(prefix, Math.max(1, limit), token, nextOut);
          var ordinaryUris = new ArrayList<String>(rows.size());
          var projectionKeys = new ArrayList<ImmutableBlobCache.ProjectionKey>(rows.size());
          for (var row : rows) {
            String blobUri = requireBlobReference(row, row.getKey());
            if (ReusableArtifactBundleUris.isBundleUri(blobUri)) {
              projectionKeys.add(new ImmutableBlobCache.ProjectionKey(blobUri, row.getKey()));
            } else {
              ordinaryUris.add(blobUri);
            }
          }

          // Serve immutable blobs from the decoded cache and batch-fetch only the misses; fetched
          // misses are decoded once and populated back for the next page/scan of this data.
          Map<String, T> cachedBlobs =
              blobCacheable() ? blobCache.getAllPresent(ordinaryUris) : Map.<String, T>of();
          Map<ImmutableBlobCache.ProjectionKey, T> cachedProjections =
              blobCacheable()
                  ? blobCache.getAllProjectionsPresent(projectionKeys)
                  : Map.<ImmutableBlobCache.ProjectionKey, T>of();
          var missUris = new LinkedHashSet<String>();
          for (var row : rows) {
            String blobUri = requireBlobReference(row, row.getKey());
            boolean cached =
                ReusableArtifactBundleUris.isBundleUri(blobUri)
                    ? cachedProjections.containsKey(
                        new ImmutableBlobCache.ProjectionKey(blobUri, row.getKey()))
                    : cachedBlobs.containsKey(blobUri);
            if (!cached) {
              missUris.add(blobUri);
            }
          }
          var blobsMap =
              missUris.isEmpty()
                  ? Map.<String, byte[]>of()
                  : blobReads.getBatch(new ArrayList<>(missUris));
          var blobs = new ArrayList<KeyedValue<T>>(rows.size());
          for (var row : rows) {
            String blobUri = requireBlobReference(row, row.getKey());
            boolean projected = ReusableArtifactBundleUris.isBundleUri(blobUri);
            T hit =
                projected
                    ? cachedProjections.get(
                        new ImmutableBlobCache.ProjectionKey(blobUri, row.getKey()))
                    : cachedBlobs.get(blobUri);
            if (hit != null) {
              blobs.add(new KeyedValue<>(row.getKey(), hit));
              continue;
            }
            byte[] bytes = blobsMap.get(blobUri);
            if (bytes == null) {
              var after = pointerReads.get(row.getKey()).orElse(null);
              if (after == null || !Objects.equals(after.getBlobUri(), row.getBlobUri())) {
                continue;
              }
              throw new CorruptionException("dangling pointer, missing blob: " + blobUri, null);
            }

            try {
              T parsed = parseReferencedBlob(row.getKey(), blobUri, bytes);
              if (blobCacheable()) {
                if (projected) {
                  blobCache.putProjection(blobUri, row.getKey(), parsed);
                } else {
                  blobCache.put(blobUri, parsed);
                }
              }
              blobs.add(new KeyedValue<>(row.getKey(), parsed));
            } catch (Exception e) {
              throw new CorruptionException("parse failed: " + blobUri, e);
            }
          }
          return blobs;
        });
  }

  @Override
  public int countByPrefix(String prefix) {
    return observeRepository("count_by_prefix", () -> pointerReads.count(prefix));
  }

  protected static String sha256B64(byte[] data) {
    try {
      var messageDigest = MessageDigest.getInstance("SHA-256");
      return Base64.getEncoder().encodeToString(messageDigest.digest(data));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  protected MutationMeta safeMetaOrDefault(String pointerKey, String blobUri, Timestamp nowTs) {
    return observeRepository("meta", () -> readMetaOrDefault(pointerKey, blobUri, nowTs));
  }

  protected MutationMeta readMetaOrDefault(String pointerKey, String blobUri, Timestamp nowTs) {
    return readMetaOrDefault(pointerReads.get(pointerKey), pointerKey, blobUri, nowTs);
  }

  /**
   * Meta assembly for callers that already hold the canonical pointer — avoids re-reading the
   * pointer that was fetched moments earlier to derive {@code blobUri}.
   */
  protected MutationMeta readMetaOrDefault(
      Optional<Pointer> pointerOpt, String pointerKey, String blobUri, Timestamp nowTs) {
    var header = blobReads.head(blobUri);
    long version = pointerOpt.map(Pointer::getVersion).orElse(0L);
    String etag = header.map(BlobHeader::getEtag).orElse("");

    return MutationMeta.newBuilder()
        .setPointerKey(pointerKey)
        .setBlobUri(blobUri)
        .setPointerVersion(version)
        .setEtag(etag)
        .setUpdatedAt(nowTs)
        .build();
  }

  protected static void deleteQuietly(Runnable runnable) {
    try {
      runnable.run();
    } catch (Throwable ignore) {
      // ignore
    }
  }

  protected void compareAndDeleteOrThrow(String key, long expectedVersion) {
    observeRepository("compare_delete", () -> deletePointerOrThrow(key, expectedVersion));
  }

  protected void deletePointerOrThrow(String key, long expectedVersion) {
    boolean ok = mutationPointerStore.compareAndDelete(key, expectedVersion);
    if (!ok) {
      var cur = mutationPointerStore.get(key).orElse(null);

      if (cur == null) {
        throw new NotFoundException("pointer already deleted: " + key);
      }

      throw new PreconditionFailedException(
          "delete version mismatch for "
              + key
              + " expected="
              + expectedVersion
              + " actual="
              + cur.getVersion());
    }
  }

  protected boolean compareAndDeleteOrFalse(String key, long expectedVersion) {
    return observeRepository(
        "compare_delete", () -> compareAndDeleteOrFalseUnobserved(key, expectedVersion));
  }

  protected boolean compareAndDeleteOrFalseUnobserved(String key, long expectedVersion) {
    try {
      deletePointerOrThrow(key, expectedVersion);
      return true;
    } catch (PreconditionFailedException | NotFoundException e) {
      return false;
    }
  }

  public String dumpByPrefix(String prefix, int pageSize) {
    return observeRepository(
        "dump_by_prefix",
        () -> {
          Objects.requireNonNull(prefix, "prefix");
          final int limit = Math.max(1, pageSize);

          String token = "";
          var stringBuilder = new StringBuilder(512);
          stringBuilder.append("== DUMP prefix=").append(prefix).append(" ==\n");
          stringBuilder.append(
              String.format(
                  "%-5s %-8s %-36s %-24s %-24s  %s -> %s%n",
                  "#", "version", "etag", "created_at", "last_modified", "pointer", "blobUri"));

          int rowNumber = 0;
          do {
            var next = new StringBuilder();
            var rows = pointerReads.list(prefix, limit, token, next);

            for (var r : rows) {
              rowNumber++;
              String blobUri = requireBlobReference(r, r.getKey());
              var blobHeaderOpt = blobReads.head(blobUri);
              String etag = blobHeaderOpt.map(BlobHeader::getEtag).orElse("-");
              String created =
                  blobHeaderOpt
                      .map(header -> Timestamps.toString(header.getCreatedAt()))
                      .orElse("-");
              String modified =
                  blobHeaderOpt
                      .map(header -> Timestamps.toString(header.getLastModifiedAt()))
                      .orElse("-");

              stringBuilder.append(
                  String.format(
                      "%-5d %-8d %-36s %-24s %-24s  %s -> %s%n",
                      rowNumber, r.getVersion(), etag, created, modified, r.getKey(), blobUri));
            }

            token = next.toString();
          } while (!token.isEmpty());

          return stringBuilder.toString();
        });
  }

  public String dumpPointer(String key) {
    return observeRepository(
        "dump_pointer",
        () -> {
          var pointer = pointerReads.get(key).orElse(null);

          if (pointer == null) {
            return "pointer not found: " + key;
          }

          String blobUri = requireBlobReference(pointer, key);
          var blobHeader = blobReads.head(blobUri);
          String etag = blobHeader.map(BlobHeader::getEtag).orElse("-");
          String created = blobHeader.map(h -> Timestamps.toString(h.getCreatedAt())).orElse("-");
          String modified =
              blobHeader.map(h -> Timestamps.toString(h.getLastModifiedAt())).orElse("-");
          String resourceId =
              blobHeader
                  .map(
                      header -> {
                        var id = header.getResourceId();
                        return id.getAccountId() + ":" + id.getId() + ":" + id.getKind().name();
                      })
                  .orElse("-");

          return String.format(
              "version=%d etag=%s created=%s modified=%s rid=%s %s -> %s",
              pointer.getVersion(), etag, created, modified, resourceId, pointer.getKey(), blobUri);
        });
  }

  protected String requireBlobReference(Pointer pointer, String pointerKey) {
    if (PointerReferences.isBlobPointer(pointer)) {
      return pointer.getBlobUri();
    }
    throw new CorruptionException(
        "pointer does not reference a blob: "
            + (pointerKey == null ? "" : pointerKey)
            + " kind="
            + (pointer == null ? "null" : pointer.getReferenceKind().name()));
  }

  protected String resourceName() {
    return "resource";
  }

  protected <R> R observeRepository(String operation, Supplier<R> supplier) {
    StoreMetrics metrics =
        new StoreMetrics(observability(), "repository", resourceName() + "." + operation);
    ObservationScope scope = metrics.observe();
    try {
      R result = supplier.get();
      scope.success();
      return result;
    } catch (RuntimeException | Error e) {
      scope.error(e);
      throw e;
    } finally {
      scope.close();
    }
  }

  protected void observeRepository(String operation, Runnable runnable) {
    observeRepository(
        operation,
        () -> {
          runnable.run();
          return null;
        });
  }

  protected static Observability observability() {
    Observability cached = cachedObservability;
    if (cached != null) {
      return cached;
    }
    try {
      var container = Arc.container();
      if (container != null) {
        var handle = container.instance(Observability.class);
        if (handle.isAvailable()) {
          Observability resolved = handle.get();
          cachedObservability = resolved;
          return resolved;
        }
      }
    } catch (RuntimeException ignore) {
      // Arc not initialised, common in repository unit tests.
    }
    return NOOP_OBSERVABILITY;
  }
}
