package ai.floedb.metacat.trino;

import com.google.inject.Inject;
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;

public class MetacatPageSourceProvider implements ConnectorPageSourceProvider {

  private final IcebergPageSourceProvider iceberg;

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(MetacatPageSourceProvider.class);

  @Inject
  public MetacatPageSourceProvider(IcebergPageSourceProvider iceberg) {
    this.iceberg = iceberg;
  }

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter) {

    ConnectorTableHandle handleToUse = table;
    if (table instanceof MetacatTableHandle metacatTableHandle) {
      // Convert our handle to an Iceberg one expected by the underlying page source.
      handleToUse = metacatTableHandle.toIcebergTableHandle(java.util.Map.of());
    }

    ConnectorSplit splitToUse = split;
    if (split instanceof IcebergSplit icebergSplit) {
      // Ensure partition data/spec are non-null to avoid NPEs in Iceberg reader.
      String specJson =
          icebergSplit.getPartitionSpecJson() == null
              ? org.apache.iceberg.PartitionSpecParser.toJson(
                  org.apache.iceberg.PartitionSpec.unpartitioned())
              : icebergSplit.getPartitionSpecJson();

      String dataJson = icebergSplit.getPartitionDataJson();

      // --- CORRECT FIX: Use an empty JSON object "{}" if data is null/blank/empty-array ---
      if (dataJson == null || dataJson.isBlank() || dataJson.equals("{\"partition_values\":[]}")) {
        LOG.info(
            "Fixing partitionDataJson string by using correct casing: '{}'.",
            "{\"partitionValues\":[]}");
        dataJson = "{\"partitionValues\":[]}"; // <-- Change _values to Values
      }
      // ----------------------------------------------------------------------------------

      splitToUse =
          new IcebergSplit(
              icebergSplit.getPath(),
              icebergSplit.getStart(),
              icebergSplit.getLength(),
              icebergSplit.getFileSize(),
              icebergSplit.getFileRecordCount(),
              icebergSplit.getFileFormat(),
              java.util.Optional.of(java.util.List.of()),
              specJson,
              dataJson, // <-- Use the dataJson string set to "{}"
              icebergSplit.getDeletes(),
              icebergSplit.getSplitWeight(),
              icebergSplit.getFileStatisticsDomain(),
              icebergSplit.getFileIoProperties(),
              icebergSplit.getAddresses(),
              icebergSplit.getDataSequenceNumber());

      LOG.info("split partitionDataJson='{}', partitionSpecJson='{}'", dataJson, specJson);
    }

    // Delegate to Trino's Iceberg reader using the converted handle and the split we built.
    return iceberg.createPageSource(
        transaction, session, splitToUse, handleToUse, columns, dynamicFilter);
  }
}
