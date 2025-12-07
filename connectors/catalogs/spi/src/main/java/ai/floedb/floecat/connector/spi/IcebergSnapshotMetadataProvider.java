package ai.floedb.floecat.connector.spi;

import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.Optional;

/**
 * Optional extension that connectors can implement to expose full Iceberg metadata per snapshot
 * without baking the payload into the core {@link FloecatConnector.SnapshotBundle} record.
 */
public interface IcebergSnapshotMetadataProvider {
  Optional<IcebergMetadata> icebergMetadata(long snapshotId);
}
