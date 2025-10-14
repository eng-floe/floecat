package ai.floedb.metacat.service.storage;

import java.util.Optional;

import com.google.protobuf.Timestamp;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;

public interface IdempotencyStore {
  Optional<IdempotencyRecord> get(String key);

  boolean createPending(
      String key,
      String opName,
      String requestHash,
      Timestamp createdAt,
      Timestamp expiresAt);

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
