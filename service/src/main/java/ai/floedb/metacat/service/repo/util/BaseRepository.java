package ai.floedb.metacat.service.repo.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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
  protected Clock clock = Clock.systemUTC();

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
    Optional<Pointer> pOpt = ptr.get(key);
    if (pOpt.isEmpty()) {
      return Optional.empty();
    }

    Pointer p = pOpt.get();

    var hdr = blobs.head(p.getBlobUri());
    if (hdr.isEmpty()) {
      return Optional.empty();
    }

    try {
      byte[] bytes = blobs.get(p.getBlobUri());
      return Optional.of(parser.parse(bytes));
    } catch (Exception e) {
        throw new RuntimeException("parse failed: " + p.getBlobUri(), e);
    }
  }

  @Override
  public void put(String key, String blobUri, T value) {
    putAll(List.of(key), blobUri, value);
  }

  @Override
  public boolean update(String key, String blobUri, T value, long expectedVersion) {
    byte[] bytes = toBytes.apply(value);
    String newEtag = sha256B64(bytes);

    var hdr = blobs.head(blobUri);
    if (hdr.isEmpty() || !newEtag.equals(hdr.get().getEtag())) {
      blobs.put(blobUri, bytes, contentType);
    }

    var next = Pointer.newBuilder()
        .setKey(key).setBlobUri(blobUri).setVersion(expectedVersion + 1).build();

    return ptr.compareAndSet(key, expectedVersion, next);
  }

  protected void putAll(Collection<String> keys, String blobUri, T value) {
    byte[] bytes = toBytes.apply(value);
    String newEtag = sha256B64(bytes);
    var hdr = blobs.head(blobUri);

    if (hdr.isEmpty() || !newEtag.equals(hdr.get().getEtag())) {
      blobs.put(blobUri, bytes, contentType);
    }
    for (String key : keys) {
      putCas(key, blobUri);
    }
  }

  private void putCas(String key, String blobUri) {
    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      var next = Pointer.newBuilder()
          .setKey(key).setBlobUri(blobUri).setVersion(expected + 1).build();
      if (ptr.compareAndSet(key, expected, next)) return;
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
      try {
        out.add(parser.parse(bytes)); 
      }
      catch (Exception e) {
        throw new RuntimeException("parse failed: " + r.blobUri(), e); 
      }
    }
    return out;
  }

  protected <R> List<R> listByPrefixParsed(
      String prefix, int limit, String token, StringBuilder nextOut,
      java.util.function.Function<byte[], R> parse) {

    var rows = ptr.listPointersByPrefix(prefix, Math.max(1, limit), token, nextOut);
    var uris = new ArrayList<String>(rows.size());
    for (var r : rows) uris.add(r.blobUri());
    var blobMap = blobs.getBatch(uris);

    var out = new ArrayList<R>(rows.size());
    for (var r : rows) {
      var bytes = blobMap.get(r.blobUri());
      if (bytes == null) continue;
      out.add(parse.apply(bytes));
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
      byte[] digest = md.digest(data);
      return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  protected MutationMeta safeMetaOrDefault(String pointerKey, String blobUri, Clock clock) {
    var pOpt = ptr.get(pointerKey);
    var hdr = blobs.head(blobUri);
    long ver = pOpt.map(Pointer::getVersion).orElse(0L);
    String etag = hdr.map(BlobHeader::getEtag).orElse("");
    return MutationMeta.newBuilder()
        .setPointerKey(pointerKey)
        .setBlobUri(blobUri)
        .setPointerVersion(ver)
        .setEtag(etag)
        .setUpdatedAt(Timestamps.fromMillis(clock.millis()))
        .build();
  }
}
