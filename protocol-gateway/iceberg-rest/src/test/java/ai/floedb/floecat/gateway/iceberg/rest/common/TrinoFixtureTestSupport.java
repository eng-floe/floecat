package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import com.google.protobuf.util.Timestamps;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

public final class TrinoFixtureTestSupport {
  private static final String SIMPLE_METADATA =
      "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json";
  private static volatile Fixture SIMPLE_FIXTURE;

  private TrinoFixtureTestSupport() {}

  public static Fixture simpleFixture() {
    Fixture fixture = SIMPLE_FIXTURE;
    if (fixture != null) {
      return fixture;
    }
    synchronized (TrinoFixtureTestSupport.class) {
      fixture = SIMPLE_FIXTURE;
      if (fixture == null) {
        fixture = loadFixture(TestS3Fixtures.bucketUri(SIMPLE_METADATA));
        SIMPLE_FIXTURE = fixture;
      }
    }
    return fixture;
  }

  private static Fixture loadFixture(String metadataLocation) {
    Objects.requireNonNull(metadataLocation, "metadataLocation");
    TestS3Fixtures.seedFixturesOnce();
    Map<String, String> ioProps =
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString());
    TableMetadata metadata = readMetadata(metadataLocation, ioProps);
    Map<String, String> props = new LinkedHashMap<>(metadata.properties());
    props.put("table-uuid", metadata.uuid());
    props.put("format-version", Integer.toString(metadata.formatVersion()));
    MetadataLocationUtil.setMetadataLocation(props, metadataLocation);
    if (metadata.location() != null && !metadata.location().isBlank()) {
      props.put("location", metadata.location());
    }
    Table table =
        Table.newBuilder()
            .setDisplayName("trino_test")
            .setResourceId(ResourceId.newBuilder().setId("catalog:core:trino_test").build())
            .setSchemaJson(SchemaParser.toJson(metadata.schema()))
            .putAllProperties(props)
            .build();
    IcebergMetadata icebergMetadata = toIcebergMetadata(metadata, metadataLocation);
    List<Snapshot> snapshots = toSnapshots(metadata, table.getResourceId());
    return new Fixture(table, icebergMetadata, snapshots, metadataLocation);
  }

  private static TableMetadata readMetadata(String metadataLocation, Map<String, String> ioProps) {
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProps, null, false);
      return TableMetadataParser.read(fileIO, metadataLocation);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read fixture metadata " + metadataLocation, e);
    } finally {
      if (fileIO instanceof AutoCloseable closable) {
        try {
          closable.close();
        } catch (Exception ignored) {
          // best effort for tests
        }
      }
    }
  }

  private static IcebergMetadata toIcebergMetadata(
      TableMetadata metadata, String metadataLocation) {
    long currentSnapshotId =
        metadata.currentSnapshot() == null ? -1L : metadata.currentSnapshot().snapshotId();
    IcebergMetadata.Builder builder =
        IcebergMetadata.newBuilder()
            .setTableUuid(metadata.uuid())
            .setFormatVersion(metadata.formatVersion())
            .setMetadataLocation(metadataLocation)
            .setLastUpdatedMs(metadata.lastUpdatedMillis())
            .setLastColumnId(metadata.lastColumnId())
            .setCurrentSchemaId(metadata.currentSchemaId())
            .setDefaultSpecId(metadata.defaultSpecId())
            .setLastPartitionId(metadata.lastAssignedPartitionId())
            .setDefaultSortOrderId(metadata.defaultSortOrderId())
            .setLastSequenceNumber(metadata.lastSequenceNumber());
    if (currentSnapshotId > 0) {
      builder.setCurrentSnapshotId(currentSnapshotId);
    }

    String schemaJson = SchemaParser.toJson(metadata.schema());
    builder.addSchemas(
        IcebergSchema.newBuilder()
            .setSchemaId(metadata.currentSchemaId())
            .setSchemaJson(schemaJson)
            .build());

    for (Map.Entry<String, SnapshotRef> entry : metadata.refs().entrySet()) {
      SnapshotRef ref = entry.getValue();
      IcebergRef.Builder refBuilder =
          IcebergRef.newBuilder()
              .setSnapshotId(ref.snapshotId())
              .setType(ref.type().name().toLowerCase(Locale.ROOT));
      if (ref.maxRefAgeMs() != null) {
        refBuilder.setMaxReferenceAgeMs(ref.maxRefAgeMs());
      }
      if (ref.maxSnapshotAgeMs() != null) {
        refBuilder.setMaxSnapshotAgeMs(ref.maxSnapshotAgeMs());
      }
      if (ref.minSnapshotsToKeep() != null) {
        refBuilder.setMinSnapshotsToKeep(ref.minSnapshotsToKeep());
      }
      builder.putRefs(entry.getKey(), refBuilder.build());
    }
    return builder.build();
  }

  private static List<Snapshot> toSnapshots(TableMetadata metadata, ResourceId tableId) {
    List<Snapshot> snapshots = new ArrayList<>();
    for (org.apache.iceberg.Snapshot snapshot : metadata.snapshots()) {
      Snapshot.Builder builder =
          Snapshot.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshot.snapshotId())
              .setSequenceNumber(snapshot.sequenceNumber())
              .setManifestList(snapshot.manifestListLocation())
              .setSchemaId(snapshot.schemaId());
      if (snapshot.parentId() != null) {
        builder.setParentSnapshotId(snapshot.parentId());
      }
      long timestampMs = snapshot.timestampMillis();
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(timestampMs));
      builder.setIngestedAt(Timestamps.fromMillis(Instant.now().toEpochMilli()));
      if (snapshot.summary() != null) {
        builder.putAllSummary(snapshot.summary());
      }
      snapshots.add(builder.build());
    }
    return List.copyOf(snapshots);
  }

  public record Fixture(
      Table table, IcebergMetadata metadata, List<Snapshot> snapshots, String metadataLocation) {}
}
