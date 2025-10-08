package ai.floedb.metacat.service.repo.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import jakarta.inject.Inject;

import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.repo.Repository;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

public abstract class BaseRepository<T> implements Repository<T> {
  @Inject protected PointerStore ptr;
  @Inject protected BlobStore blobs;

  private final ProtoParser<T> parser;
  private final Function<T, byte[]> toBytes;
  private final String contentType;
  private static final int CAS_MAX = 10;

  protected BaseRepository(ProtoParser<T> parser, Function<T, byte[]> toBytes, String contentType) {
    this.parser = parser;
    this.toBytes = toBytes;
    this.contentType = contentType;
  }

  protected static String memUriFor(String pointerKey, String leaf) {
    String base = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    return "mem://" + base + (leaf.startsWith("/") ? leaf : "/" + leaf);
  }

  @Override
  public Optional<T> get(String key) {
    return ptr.get(key).map(p -> {
      var bytes = blobs.get(p.getBlobUri());
      try { 
        return parser.parse(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  @Override
  public void put(String key, String blobUri, T value) {
    blobs.put(blobUri, toBytes.apply(value), contentType);
    for (int i = 0; i < CAS_MAX; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      var next = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(expected + 1).build();
      if (ptr.compareAndSet(key, expected, next)) return;
    }
    throw new IllegalStateException("CAS failed: " + key);
  }

  @Override
  public List<T> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    var keys = ptr.listByPrefix(prefix, Math.max(1, limit), pageToken, nextTokenOut);
    var out = new ArrayList<T>(keys.size());
    for (var k : keys) {
      ptr.get(k).ifPresent(p -> {
        var bytes = blobs.get(p.getBlobUri());
        try { 
          out.add(parser.parse(bytes)); 
        } catch (Exception e) { 
          throw new RuntimeException(e); 
        }
      });
    }
    return out;
  }

  @Override
  public int countByPrefix(String prefix) {
    StringBuilder ignore = new StringBuilder();
    return ptr.listByPrefix(prefix, Integer.MAX_VALUE, "", ignore).size();
  }
}