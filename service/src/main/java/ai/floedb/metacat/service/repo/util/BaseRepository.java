package ai.floedb.metacat.service.repo.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import com.google.protobuf.Timestamp;

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
    if (blobs.head(p.getBlobUri()).isEmpty()) return Optional.empty();

    try {
      return Optional.of(parser.parse(blobs.get(p.getBlobUri())));
    } catch (Exception e) {
      throw new RuntimeException("parse failed: " + p.getBlobUri(), e);
    }
  }

  protected void reserveIndexOrIdempotent(String key, String blobUri) {
    var reserve = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
    if (ptr.compareAndSet(key, 0L, reserve)) return;

    var ex = ptr.get(key).orElse(null);
    if (ex == null || !blobUri.equals(ex.getBlobUri())) {
      throw new IllegalStateException("pointer exists with different blob: " + key);
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
      throw new IllegalStateException("blob write verification failed: " + blobUri);
    }
  }

  protected void putBlobStrictBytes(String blobUri, byte[] bytes) {
    String want = sha256B64(bytes);
    var before = blobs.head(blobUri);
    if (before.isPresent() && want.equals(before.get().getEtag())) return;

    blobs.put(blobUri, bytes, contentType);

    var after = blobs.head(blobUri);
    if (after.isEmpty() || !want.equals(after.get().getEtag())) {
      throw new IllegalStateException("blob write verification failed: " + blobUri);
    }
  }

  @Override
  public void advancePointer(String key, String blobUri) {
    for (int i = 0; i < CAS_MAX; i++) {
      var cur = ptr.get(key).orElse(null);
      var next = (cur == null)
          ? Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build()
          : cur.toBuilder().setBlobUri(blobUri).setVersion(cur.getVersion() + 1).build();
      if (ptr.compareAndSet(key, cur == null ? 0L : cur.getVersion(), next)) return;
      backoff(i);
    }
    throw new IllegalStateException("CAS failed after retries: " + key);
  }

  @Override
  public void advancePointer(String key, String blobUri, long expectedVersion) {
    for (int i = 0; i < CAS_MAX; i++) {
      var cur = ptr.get(key).orElse(null);
      if (cur == null) {
        if (expectedVersion != 0L) throw new IllegalStateException("precondition failed: " + key);
        var created = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
        if (ptr.compareAndSet(key, 0L, created)) return;
      } else {
        if (cur.getVersion() != expectedVersion) {
          throw new IllegalStateException("precondition failed: " + key
              + " expected=" + expectedVersion + " actual=" + cur.getVersion());
        }
        var next = cur.toBuilder().setBlobUri(blobUri).setVersion(cur.getVersion() + 1).build();
        if (ptr.compareAndSet(key, expectedVersion, next)) return;
      }
      backoff(i);
    }
    throw new IllegalStateException("CAS failed after retries: " + key);
  }

  private static void backoff(int attempt) {
    try {
      Thread.sleep(Math.min(5L * (1 << attempt), 50L));
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
      try { out.add(parser.parse(bytes)); }
      catch (Exception e) {
        throw new RuntimeException("parse failed: " + r.blobUri(), e);
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

  protected static boolean compareAndDeleteOrFalse(PointerStore ps, String key, long expectedVersion) {
    try { 
      return ps.compareAndDelete(key, expectedVersion); 
    } catch (Throwable ignore) {
      return false; 
    }
  }
}
