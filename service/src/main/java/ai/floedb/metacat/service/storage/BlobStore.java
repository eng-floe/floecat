package ai.floedb.metacat.service.storage;

import java.util.Optional;

public interface BlobStore {
  void put(String uri, byte[] bytes, String contentType);
  byte[] get(String uri);
  Optional<ai.floedb.metacat.common.rpc.BlobHeader> head(String uri);
}