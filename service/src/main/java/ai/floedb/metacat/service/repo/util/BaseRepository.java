package ai.floedb.metacat.service.repo.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.BlobHeader;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.repo.Repository;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

public abstract class BaseRepository<T> implements Repository<T> {
  protected PointerStore ptr;
  protected BlobStore blobs;
  protected ProtoParser<T> parser;
  protected Function<T, byte[]> toBytes;
  protected String contentType;

  public static final int CAS_MAX = 10;

  public static class RepoException extends RuntimeException {
    public RepoException(String msg) { super(msg); }
    public RepoException(String msg, Throwable cause) { super(msg, cause); }
  }

  public static class NameConflictException extends RepoException {
    public NameConflictException(String msg) { super(msg); }
  }

  public static class PreconditionFailedException extends RepoException {
    public PreconditionFailedException(String msg) { super(msg); }
  }

  public static class AbortRetryableException extends RepoException {
    public AbortRetryableException(String msg) { super(msg); }
  }

  public static class NotFoundException extends RepoException {
    public NotFoundException(String msg) { super(msg); }
  }

  public static class CorruptionException extends RepoException {
    public CorruptionException(String msg, Throwable cause) { super(msg, cause); }
  }

  protected BaseRepository() { }

  protected BaseRepository(
      PointerStore ptr,
      BlobStore blobs,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType) {
    this.ptr = Objects.requireNonNull(ptr, "ptr");
    this.blobs = Objects.requireNonNull(blobs, "blobs");
    this.parser = Objects.requireNonNull(parser, "parser");
    this.toBytes = Objects.requireNonNull(toBytes, "toBytes");
    this.contentType = Objects.requireNonNull(contentType, "contentType");
  }

  @Override
  public Optional<T> get(String key) {
    var pOpt = ptr.get(key);
    if (pOpt.isEmpty()) return Optional.empty();

    var p = pOpt.get();
    if (blobs.head(p.getBlobUri()).isEmpty()) {
      throw new NotFoundException("blob missing: "+p.getBlobUri());
    }

    try {
      return Optional.of(parser.parse(blobs.get(p.getBlobUri())));
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + p.getBlobUri(), e);
    }
  }

  private void reserveIndexOrIdempotent(String key, String blobUri) {
    var reserve = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
    if (ptr.compareAndSet(key, 0L, reserve)) return;

    var ex = ptr.get(key).orElse(null);
    if (ex == null) {
      throw new AbortRetryableException("pointer suddenly vanished: " + key);
    }
    if (!blobUri.equals(ex.getBlobUri())) {
      throw new NameConflictException("pointer bound to different blob: " + key);
    }
  }

  protected void reserveAllOrRollback(String... keyBlobPairs) {
    final var reserved = new ArrayList<String>(keyBlobPairs.length/2);
    try {
      for (int i = 0; i < keyBlobPairs.length; i += 2) {
        final var k = keyBlobPairs[i];
        final var b = keyBlobPairs[i+1];
        reserveIndexOrIdempotent(k, b);
        reserved.add(k);
      }
    } catch (NameConflictException | AbortRetryableException e) {
      for (int i = reserved.size() - 1; i >= 0; i--) {
        final var k = reserved.get(i);
        ptr.get(k).ifPresent(p -> compareAndDeleteOrFalse(k, p.getVersion()));
      }
      throw e;
    }
  }

  @Override
  public void putBlob(String blobUri, T value) {
    byte[] bytes = toBytes.apply(value);
    String want = sha256B64(bytes);
    var before = blobs.head(blobUri);
    if (before.isPresent() && want.equals(before.get().getEtag())) return;

    blobs.put(blobUri, bytes, contentType);
    var after = blobs.head(blobUri);
    if (after.isEmpty() || !want.equals(after.get().getEtag())) {
      throw new AbortRetryableException("blob write verification failed: " + blobUri);
    }
  }

  protected void putBlobStrictBytes(String blobUri, byte[] bytes) {
    String want = sha256B64(bytes);
    var before = blobs.head(blobUri);
    if (before.isPresent() && want.equals(before.get().getEtag())) return;

    blobs.put(blobUri, bytes, contentType);

    var after = blobs.head(blobUri);
    if (after.isEmpty() || !want.equals(after.get().getEtag())) {
      throw new AbortRetryableException("blob write verification failed: " + blobUri);
    }
  }

  @Override
  public void advancePointer(String key, String blobUri, long expectedVersion) {
    for (int i = 0; i < CAS_MAX; i++) {
      var cur = ptr.get(key).orElse(null);
      if (cur == null) {
        if (expectedVersion != 0L) {
          throw new PreconditionFailedException("missing pointer: " + key + " expected=" + expectedVersion);
        }
        var created = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
        if (ptr.compareAndSet(key, 0L, created)) return;
      } else {
        if (cur.getVersion() != expectedVersion) {
          throw new PreconditionFailedException("version mismatch: " + key
              + " expected=" + expectedVersion + " actual=" + cur.getVersion());
        }
        var next = cur.toBuilder().setBlobUri(blobUri).setVersion(cur.getVersion() + 1).build();
        if (ptr.compareAndSet(key, expectedVersion, next)) return;
      }
      backoff(i);
    }
    throw new AbortRetryableException("CAS retries exhausted: " + key);
  }

  private static void backoff(int attempt) {
    try {
      long base = Math.min(5L * (1L << attempt), 50L);
      long jitter = ThreadLocalRandom.current().nextLong(0, 5);
      Thread.sleep(base + jitter);
    } catch (InterruptedException ignored) {}
  }

  @Override
  public List<T> listByPrefix(String prefix, int limit, String token, StringBuilder nextOut) {
    var rows = ptr.listPointersByPrefix(prefix, Math.max(1, limit), token, nextOut);
    var uris = new ArrayList<String>(rows.size());
    for (var r : rows) uris.add(r.blobUri());

    var blobsMap = blobs.getBatch(uris);
    var out = new ArrayList<T>(rows.size());
    for (var r : rows) {
      byte[] bytes = blobsMap.get(r.blobUri());
      if (bytes == null) continue;
      try {
        out.add(parser.parse(bytes));
      }
      catch (Exception e) {
        throw new CorruptionException("parse failed: " + r.blobUri(), e);
      }
    }
    return out;
  }

  @Override
  public int countByPrefix(String prefix) {
    return ptr.countByPrefix(prefix);
  }

  protected static String sha256B64(byte[] data) {
    try {
      var md = MessageDigest.getInstance("SHA-256");
      return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(md.digest(data));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  protected MutationMeta safeMetaOrDefault(String pointerKey, String blobUri, Timestamp nowTs) {
    var pOpt = ptr.get(pointerKey);
    var hdr = blobs.head(blobUri);
    long ver = pOpt.map(Pointer::getVersion).orElse(0L);
    String etag = hdr.map(BlobHeader::getEtag).orElse("");
    return MutationMeta.newBuilder()
        .setPointerKey(pointerKey)
        .setBlobUri(blobUri)
        .setPointerVersion(ver)
        .setEtag(etag)
        .setUpdatedAt(nowTs)
        .build();
  }

  protected static void deleteQuietly(Runnable r) {try { r.run(); } catch (Throwable ignore) {} }

  protected void compareAndDeleteOrThrow(String key, long expectedVersion) {
    boolean ok = ptr.compareAndDelete(key, expectedVersion);
    if (!ok) {
      var cur = ptr.get(key).orElse(null);
      if (cur == null) {
        throw new NotFoundException("pointer already deleted: " + key);
      }
      throw new PreconditionFailedException(
          "delete version mismatch for " + key +
          " expected=" + expectedVersion + " actual=" + cur.getVersion());
    }
  }

  protected boolean compareAndDeleteOrFalse(String key, long expectedVersion) {
    try {
      compareAndDeleteOrThrow(key, expectedVersion);
      return true;
    } catch (PreconditionFailedException | NotFoundException e) {
      return false;
    }
  }

  public String dumpByPrefix(String prefix, int pageSize) {
    Objects.requireNonNull(prefix, "prefix");
    final int limit = Math.max(1, pageSize);

    String token = "";
    var out = new StringBuilder(512);
    out.append("== DUMP prefix=").append(prefix).append(" ==\n");
    out.append(String.format("%-5s %-8s %-36s %-24s %-24s  %s -> %s%n",
        "#", "ver", "etag", "created_at", "last_modified", "pointer", "blobUri"));

    int rowNum = 0;
    do {
      var next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(prefix, limit, token, next);

      for (var r : rows) {
        rowNum++;
        var hdrOpt = blobs.head(r.blobUri());
        String etag = hdrOpt.map(BlobHeader::getEtag).orElse("-");
        String created = hdrOpt.map(h -> Timestamps.toString(h.getCreatedAt())).orElse("-");
        String modified = hdrOpt.map(h -> Timestamps.toString(h.getLastModifiedAt())).orElse("-");

        out.append(String.format("%-5d %-8d %-36s %-24s %-24s  %s -> %s%n",
            rowNum, r.version(), etag, created, modified, r.key(), r.blobUri()));
      }

      token = next.toString();
    } while (!token.isEmpty());

    return out.toString();
  }

  public String dumpPointer(String key) {
    var p = ptr.get(key).orElse(null);
    if (p == null) return "pointer not found: " + key;

    var hdr = blobs.head(p.getBlobUri());
    String etag = hdr.map(BlobHeader::getEtag).orElse("-");
    String created = hdr.map(h -> Timestamps.toString(h.getCreatedAt())).orElse("-");
    String modified = hdr.map(h -> Timestamps.toString(h.getLastModifiedAt())).orElse("-");
    String rid = hdr.map(h -> {
      var id = h.getResourceId();
      return id.getTenantId() + ":" + id.getId() + ":" + id.getKind().name();
    }).orElse("-");

    return String.format("ver=%d etag=%s created=%s modified=%s rid=%s %s -> %s",
        p.getVersion(), etag, created, modified, rid, p.getKey(), p.getBlobUri());
  }
}
