package ai.floedb.floecat.service.metagraph;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import com.google.protobuf.Timestamp;
import java.util.Optional;

/**
 * Minimal subset of {@link MetadataGraph} used by {@link
 * ai.floedb.floecat.service.query.resolver.QueryInputResolver}.
 */
public interface MetadataGraphClient {
  ResourceId resolveName(String correlationId, NameRef ref);

  SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault);
}
