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
import ai.floedb.floecat.service.repo.ResourceRepository;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class BaseResourceRepository<T> implements ResourceRepository<T> {
  private static final Observability NOOP_OBSERVABILITY = new NoopObservability();
  private static volatile Observability cachedObservability;

  protected PointerStore pointerStore;
  protected BlobStore blobStore;
  protected ProtoParser<T> parser;
  protected Function<T, byte[]> toBytes;
  protected String contentType;

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
      PointerStore pointerStore,
      BlobStore blobStore,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType) {
    this.pointerStore = Objects.requireNonNull(pointerStore, "pointerStore");
    this.blobStore = Objects.requireNonNull(blobStore, "blobs");
    this.parser = Objects.requireNonNull(parser, "parser");
    this.toBytes = Objects.requireNonNull(toBytes, "toBytes");
    this.contentType = Objects.requireNonNull(contentType, "contentType");
  }

  @Override
  public Optional<T> get(String key) {
    return observeRepository("get", () -> read(key));
  }

  protected Optional<T> read(String key) {
    var pointerStoreOpt = pointerStore.get(key);
    if (pointerStoreOpt.isEmpty()) {
      return Optional.empty();
    }

    var pointer = pointerStoreOpt.get();
    String blobUri = requireBlobReference(pointer, key);
    byte[] bytes;

    try {
      bytes = blobStore.get(blobUri);
      if (bytes == null) {
        if (pointerChangedOrDeleted(key, pointer)) {
          return Optional.empty();
        }
        throw new CorruptionException("dangling pointer, missing blob: " + blobUri, null);
      }
      return Optional.of(parser.parse(bytes));
    } catch (StorageNotFoundException snf) {
      if (pointerChangedOrDeleted(key, pointer)) {
        return Optional.empty();
      }
      throw new CorruptionException("dangling pointer, missing blob: " + blobUri, snf);
    } catch (InvalidProtocolBufferException ipbe) {
      throw new CorruptionException("parse failed: " + blobUri, ipbe);
    } catch (StorageAbortRetryableException sar) {
      throw new AbortRetryableException("blob read retryable: " + blobUri);
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + blobUri, e);
    }
  }

  private boolean pointerChangedOrDeleted(String key, Pointer before) {
    var after = pointerStore.get(key).orElse(null);
    return after == null || !Objects.equals(after.getBlobUri(), before.getBlobUri());
  }

  private boolean reserveIndexOrIdempotent(String key, String blobUri) {
    var reserve = PointerReferences.blobPointer(key, blobUri, 1L);

    if (pointerStore.compareAndSet(key, 0L, reserve)) {
      return true;
    }

    var pointer = pointerStore.get(key).orElse(null);

    if (pointer == null) {
      throw new AbortRetryableException("pointer suddenly vanished: " + key);
    }

    if (!blobUri.equals(pointer.getBlobUri())) {
      throw new NameConflictException("pointer bound to different blob: " + key);
    }

    return false;
  }

  protected void reserveAllOrRollback(String... keyBlobPairs) {
    final var createdKeys = new ArrayList<String>(keyBlobPairs.length / 2);
    try {
      for (int i = 0; i < keyBlobPairs.length; i += 2) {
        final var key = keyBlobPairs[i];
        final var blobUri = keyBlobPairs[i + 1];
        if (reserveIndexOrIdempotent(key, blobUri)) {
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
    byte[] bytes = toBytes.apply(value);
    String want = sha256B64(bytes);
    var before = blobStore.head(blobUri);

    if (before.isPresent() && want.equals(before.get().getEtag())) {
      return;
    }

    blobStore.put(blobUri, bytes, contentType);
    var after = blobStore.head(blobUri);

    if (after.isEmpty() || !want.equals(after.get().getEtag())) {
      throw new AbortRetryableException("blob write verification failed: " + blobUri);
    }
  }

  protected void putBlobStrictBytes(String blobUri, byte[] bytes) {
    observeRepository("put_blob_strict", () -> writeBlobStrictBytes(blobUri, bytes));
  }

  protected void writeBlobStrictBytes(String blobUri, byte[] bytes) {
    final String want = sha256B64(bytes);

    if (blobStore.head(blobUri).map(h -> want.equals(h.getEtag())).orElse(false)) {
      return;
    }

    blobStore.put(blobUri, bytes, contentType);

    if (!blobStore.head(blobUri).map(h -> want.equals(h.getEtag())).orElse(false)) {
      throw new AbortRetryableException("blob write verification failed: " + blobUri);
    }
  }

  @Override
  public void advancePointer(String key, String blobUri, long expectedVersion) {
    observeRepository(
        "advance_pointer",
        () -> {
          var pointer = pointerStore.get(key).orElse(null);

          if (pointer == null) {
            if (expectedVersion != 0L) {
              throw new PreconditionFailedException(
                  "missing pointer: " + key + " expected=" + expectedVersion);
            }
            var created = PointerReferences.blobPointer(key, blobUri, 1L);
            if (pointerStore.compareAndSet(key, 0L, created)) {
              return;
            }

            var after = pointerStore.get(key).orElse(null);
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
          if (pointerStore.compareAndSet(key, expectedVersion, next)) {
            return;
          }

          var after = pointerStore.get(key).orElse(null);
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
            out.addAll(pointerStore.listPointersByPrefix(prefix, REFS_PAGE_SIZE, token, next));
            token = next.toString();
          } while (!token.isBlank());
          return out;
        });
  }

  public Optional<Pointer> refByPointer(String key) {
    return observeRepository("ref_by_pointer", () -> pointerStore.get(key));
  }

  @Override
  public List<T> listByPrefix(String prefix, int limit, String token, StringBuilder nextOut) {
    return observeRepository(
        "list_by_prefix",
        () -> {
          var rows = pointerStore.listPointersByPrefix(prefix, Math.max(1, limit), token, nextOut);
          var uris = new ArrayList<String>(rows.size());
          for (var row : rows) {
            uris.add(requireBlobReference(row, row.getKey()));
          }

          var blobsMap = blobStore.getBatch(uris);
          var blobs = new ArrayList<T>(rows.size());
          for (var row : rows) {
            String blobUri = requireBlobReference(row, row.getKey());
            byte[] bytes = blobsMap.get(blobUri);
            if (bytes == null) {
              var after = pointerStore.get(row.getKey()).orElse(null);
              if (after == null || !Objects.equals(after.getBlobUri(), row.getBlobUri())) {
                continue;
              }
              throw new CorruptionException("dangling pointer, missing blob: " + blobUri, null);
            }

            try {
              blobs.add(parser.parse(bytes));
            } catch (Exception e) {
              throw new CorruptionException("parse failed: " + blobUri, e);
            }
          }
          return blobs;
        });
  }

  @Override
  public int countByPrefix(String prefix) {
    return observeRepository("count_by_prefix", () -> pointerStore.countByPrefix(prefix));
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
    var pointerOpt = pointerStore.get(pointerKey);
    var header = blobStore.head(blobUri);
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
    boolean ok = pointerStore.compareAndDelete(key, expectedVersion);
    if (!ok) {
      var cur = pointerStore.get(key).orElse(null);

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
            var rows = pointerStore.listPointersByPrefix(prefix, limit, token, next);

            for (var r : rows) {
              rowNumber++;
              String blobUri = requireBlobReference(r, r.getKey());
              var blobHeaderOpt = blobStore.head(blobUri);
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
          var pointer = pointerStore.get(key).orElse(null);

          if (pointer == null) {
            return "pointer not found: " + key;
          }

          String blobUri = requireBlobReference(pointer, key);
          var blobHeader = blobStore.head(blobUri);
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
