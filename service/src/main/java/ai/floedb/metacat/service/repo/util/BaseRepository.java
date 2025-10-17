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

  protected boolean updateCanonical(String key, String blobUri, T value, long expectedVersion) {
    putBlob(blobUri, value);
    var next = Pointer.newBuilder()
        .setKey(key).setBlobUri(blobUri).setVersion(expectedVersion + 1).build();
    return ptr.compareAndSet(key, expectedVersion, next);
  }

  @Override
  public boolean update(String key, String blobUri, T value, long expectedVersion) {
    return updateCanonical(key, blobUri, value, expectedVersion);
  }

  @Override
  public void putBlob(String blobUri, T value) {
    byte[] bytes = toBytes.apply(value);
    String etag = sha256B64(bytes);
    var hdr = blobs.head(blobUri);
    if (hdr.isEmpty() || !etag.equals(hdr.get().getEtag())) {
      blobs.put(blobUri, bytes, contentType);
    }
  }

  enum PointerKind { CREATE_ONLY, ADVANCING }

  private PointerKind classifyPointer(String key) {
    if (key.contains("/snapshots/by-time/")
        || key.contains("/namespaces/by-path/")
        || key.contains("/catalogs/by-name/")
        || key.contains("/tables/by-name/")) {
      return PointerKind.CREATE_ONLY;
    }
    return PointerKind.ADVANCING;
  }

  @Override
  public void putCas(String key, String blobUri) {
    var kind = classifyPointer(key);

    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      var cur = ptr.get(key).orElse(null);

      if (cur == null) {
        var created = Pointer.newBuilder()
            .setKey(key).setBlobUri(blobUri).setVersion(1L).build();
        if (ptr.compareAndSet(key, 0L, created)) return;
        continue;
      }

      if (blobUri.equals(cur.getBlobUri())) return;

      if (kind == PointerKind.CREATE_ONLY) {
        throw new IllegalStateException("pointer exists with different blob: " + key);
      }

      var next = Pointer.newBuilder()
          .setKey(key)
          .setBlobUri(blobUri)
          .setVersion(cur.getVersion() + 1)
          .setExpiresAt(cur.hasExpiresAt() ? cur.getExpiresAt() : Timestamp.getDefaultInstance())
          .build();

      if (ptr.compareAndSet(key, cur.getVersion(), next)) return;
    }
    throw new IllegalStateException("CAS failed: " + key);
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
