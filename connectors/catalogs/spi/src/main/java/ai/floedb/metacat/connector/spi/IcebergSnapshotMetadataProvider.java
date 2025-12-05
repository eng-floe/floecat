package ai.floedb.metacat.connector.spi;

import ai.floedb.metacat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.Optional;

/**
 * Optional extension that connectors can implement to expose full Iceberg metadata per snapshot
 * without baking the payload into the core {@link MetacatConnector.SnapshotBundle} record.
 */
public interface IcebergSnapshotMetadataProvider {
  Optional<IcebergMetadata> icebergMetadata(long snapshotId);
}
