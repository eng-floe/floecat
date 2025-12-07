package ai.floedb.floecat.client.trino;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;

import io.airlift.units.DataSize;
import io.trino.plugin.base.session.PropertyMetadataUtil;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Optional;

public class FloecatSessionProperties implements SessionPropertiesProvider {
  public static final String USE_FILE_SIZE_FROM_METADATA = "use_file_size_from_metadata";
  public static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
  public static final String PARQUET_BATCH_READING_ENABLED = "parquet_batch_reading_enabled";
  public static final String PARQUET_MAX_READ_BLOCK_ROW_COUNT = "parquet_max_read_block_row_count";
  public static final String PARQUET_SMALL_FILE_THRESHOLD = "parquet_small_file_threshold";
  public static final String PARQUET_IGNORE_STATISTICS = "parquet_ignore_statistics";
  public static final String PARQUET_USE_BLOOM_FILTER = "parquet_use_bloom_filter";
  public static final String PARQUET_VECTORIZED_DECODING_ENABLED =
      "parquet_vectorized_decoding_enabled";
  public static final String SNAPSHOT_ID = "snapshot_id";
  public static final String AS_OF_EPOCH_MILLIS = "as_of_epoch_millis";

  private final List<PropertyMetadata<?>> properties;

  public FloecatSessionProperties() {
    properties =
        List.of(
            booleanProperty(
                USE_FILE_SIZE_FROM_METADATA,
                "Use file size from metadata instead of probing the filesystem",
                true,
                false),
            PropertyMetadataUtil.dataSizeProperty(
                PARQUET_MAX_READ_BLOCK_SIZE,
                "Maximum Parquet read block size",
                DataSize.of(16, io.airlift.units.DataSize.Unit.MEGABYTE),
                false),
            booleanProperty(
                PARQUET_BATCH_READING_ENABLED, "Enable Parquet batch reading", true, false),
            PropertyMetadata.integerProperty(
                PARQUET_MAX_READ_BLOCK_ROW_COUNT,
                "Maximum number of rows to read in a Parquet block",
                8192,
                false),
            PropertyMetadataUtil.dataSizeProperty(
                PARQUET_SMALL_FILE_THRESHOLD,
                "Threshold for treating Parquet files as small",
                DataSize.of(64, io.airlift.units.DataSize.Unit.MEGABYTE),
                false),
            booleanProperty(PARQUET_IGNORE_STATISTICS, "Ignore Parquet statistics", false, false),
            booleanProperty(
                PARQUET_USE_BLOOM_FILTER, "Use Parquet bloom filter when available", true, false),
            booleanProperty(
                PARQUET_VECTORIZED_DECODING_ENABLED,
                "Enable Parquet vectorized decoding",
                true,
                false),
            longProperty(
                SNAPSHOT_ID,
                "Floecat snapshot id to read (overrides current). Set to -1 to disable.",
                -1L,
                false),
            longProperty(
                AS_OF_EPOCH_MILLIS,
                "Read table as of epoch millis (overrides current). Set to -1 to disable.",
                -1L,
                false));
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    return properties;
  }

  public static boolean isUseFileSizeFromMetadata(ConnectorSession session) {
    return session.getProperty(USE_FILE_SIZE_FROM_METADATA, Boolean.class);
  }

  public static Optional<Long> getSnapshotId(ConnectorSession session) {
    long v = session.getProperty(SNAPSHOT_ID, Long.class);
    return v >= 0 ? Optional.of(v) : Optional.empty();
  }

  public static Optional<Long> getAsOfEpochMillis(ConnectorSession session) {
    long v = session.getProperty(AS_OF_EPOCH_MILLIS, Long.class);
    return v >= 0 ? Optional.of(v) : Optional.empty();
  }
}
