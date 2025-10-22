package ai.floedb.metacat.service.storage;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.Timestamp;
import java.util.Optional;

public interface IdempotencyStore {
  Optional<IdempotencyRecord> get(String key);

  boolean createPending(
      String key, String opName, String requestHash, Timestamp createdAt, Timestamp expiresAt);

  void finalizeSuccess(
      String key,
      String opName,
      String requestHash,
      ResourceId resourceId,
      MutationMeta meta,
      byte[] payloadBytes,
      Timestamp createdAt,
      Timestamp expiresAt);

  boolean delete(String key);
}
