package ai.floedb.metacat.storage;

import ai.floedb.metacat.common.rpc.BlobHeader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
