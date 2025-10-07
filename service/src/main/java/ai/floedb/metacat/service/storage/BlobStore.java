package ai.floedb.metacat.service.storage;

import java.util.Optional;

import ai.floedb.metacat.common.rpc.BlobHeader;

public interface BlobStore {
  void put(String uri, byte[] bytes, String contentType);
  byte[] get(String uri);
  Optional<BlobHeader> head(String uri);
}