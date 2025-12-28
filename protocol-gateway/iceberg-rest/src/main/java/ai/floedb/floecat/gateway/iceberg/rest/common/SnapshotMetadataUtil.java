package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public final class SnapshotMetadataUtil {
  private static final String ICEBERG_METADATA_KEY = "iceberg";

  private SnapshotMetadataUtil() {}

  public static IcebergMetadata parseSnapshotMetadata(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    ByteString raw = snapshot.getFormatMetadataOrDefault(ICEBERG_METADATA_KEY, ByteString.EMPTY);
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    try {
      return IcebergMetadata.parseFrom(raw);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "Failed to parse Iceberg metadata for snapshot " + snapshot.getSnapshotId(), e);
    }
  }
}
