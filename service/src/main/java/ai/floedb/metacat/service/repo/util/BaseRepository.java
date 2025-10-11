package ai.floedb.metacat.service.repo.util;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

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

  private static final int CAS_MAX = 10;

  protected BaseRepository() { }

  protected BaseRepository(PointerStore ptr,
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
    return ptr.get(key).map(p -> {
      var bytes = blobs.get(p.getBlobUri());
      try { 
        return parser.parse(bytes); 
      }
      catch (Exception e) {
        throw new RuntimeException("parse failed: " + p.getBlobUri(), e);
      }
    });
  }

  @Override
  public void put(String key, String blobUri, T value) {
    final byte[] bytes = toBytes.apply(value);
    final String newEtag = sha256B64(bytes);

    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      final Optional<Pointer> cur = ptr.get(key);

      if (cur.isPresent() && blobUri.equals(cur.get().getBlobUri())) {
        var hdr = blobs.head(blobUri);
        if (hdr.isPresent() && newEtag.equals(hdr.get().getEtag())) {
          return;
        }
      }

      blobs.put(blobUri, bytes, contentType);

      long expected = cur.map(Pointer::getVersion).orElse(0L);
      var next = Pointer.newBuilder()
        .setKey(key)
        .setBlobUri(blobUri)
        .setVersion(expected + 1)
        .build();

      if (ptr.compareAndSet(key, expected, next)) return;

      try {
        Thread.sleep((1L << attempt) + ThreadLocalRandom.current().nextInt(4));
      }
      catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new IllegalStateException("CAS failed: " + key);
  }

  @Override
  public java.util.List<T> listByPrefix(String prefix, int limit, String token, StringBuilder nextOut) {
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
        throw new RuntimeException("parse failed: " + r.blobUri(), e); 
      }
    }
    return out;
  }

  @Override
  public int countByPrefix(String prefix) {
    return ptr.countByPrefix(prefix);
  }

  private static String sha256B64(byte[] data) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(data);
      return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
