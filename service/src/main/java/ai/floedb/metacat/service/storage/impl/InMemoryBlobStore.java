package ai.floedb.metacat.service.storage.impl;

import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.arc.Unremovable;

import ai.floedb.metacat.common.rpc.BlobHeader;
import ai.floedb.metacat.service.storage.BlobStore;

@ApplicationScoped
@Unremovable
public class InMemoryBlobStore implements BlobStore {
  
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
      .setCreatedAtMs(System.currentTimeMillis())
      .build();
    map.put(uri, new Blob(bytes, hdr));
  }

  @Override public byte[] get(String uri) { 
    return Optional.ofNullable(map.get(uri)).map(b -> b.data).orElseThrow(); 
  }

  @Override public Optional<BlobHeader> head(String uri) { 
    return Optional.ofNullable(map.get(uri)).map(b -> b.hdr); 
  }
}