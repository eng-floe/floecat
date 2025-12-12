package ai.floedb.floecat.service.metagraph.overlay.user;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import com.google.protobuf.Timestamp;
import java.util.Optional;

/**
 * Minimal contract required by {@link ai.floedb.floecat.service.query.resolver.QueryInputResolver}.
 */
public interface UserGraphClient {

  ResourceId resolveName(String correlationId, NameRef ref);

  SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault);
}
