package ai.floedb.metacat.service.storage;

import java.util.*;

import ai.floedb.metacat.common.rpc.BlobHeader;

public interface BlobStore {
  byte[] get(String uri);
  void put(String uri, byte[] bytes, String contentType);
  public Optional<BlobHeader> head(String uri);
  boolean delete(String uri);

  default Map<String, byte[]> getBatch(List<String> uris) {
    Map<String, byte[]> out = new HashMap<>(uris.size());
    for (String u : uris) out.put(u, get(u));
    return out;
  }
}
