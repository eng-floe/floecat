package ai.floedb.metacat.service.storage.impl;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.util.Timestamps;

import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.arc.properties.IfBuildProperty;

import ai.floedb.metacat.common.rpc.BlobHeader;
import ai.floedb.metacat.service.storage.BlobStore;

@ApplicationScoped
@IfBuildProperty(name = "metacat.kv", stringValue = "memory")
public class InMemoryBlobStore implements BlobStore {
  
  private final Clock clock = Clock.systemUTC();

  private static final class Blob { 
    final byte[] data; 
    final BlobHeader hdr; 
    
    Blob(byte[] d, BlobHeader h) {
      this.data = d;
      this.hdr = h;
    } 
  }

  private final Map<String, Blob> map = new ConcurrentHashMap<>();

  @Override public void put(String uri, byte[] bytes, String contentType) {
    BlobHeader hdr = BlobHeader.newBuilder()
      .setSchemaVersion("v1")
      .setEtag(UUID.randomUUID().toString())
      .setCreatedAt(Timestamps.fromMillis(clock.millis()))
      .build();
    map.put(uri, new Blob(bytes, hdr));
  }

  @Override public byte[] get(String uri) { 
    return Optional.ofNullable(map.get(uri)).map(b -> b.data).orElseThrow(); 
  }

  @Override public Optional<BlobHeader> head(String uri) { 
    return Optional.ofNullable(map.get(uri)).map(b -> b.hdr); 
  }

  @Override public boolean delete(String key) {
    if (!map.containsKey(key)) {
      return false;
    }
    map.remove(key);
    return true;
  }
}