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
      handleToUse = metacatTableHandle.toIcebergTableHandle(java.util.Map.of());
    }

    ConnectorSplit splitToUse = split;
    if (split instanceof IcebergSplit icebergSplit) {
      String specJson =
          icebergSplit.getPartitionSpecJson() == null
              ? org.apache.iceberg.PartitionSpecParser.toJson(
                  org.apache.iceberg.PartitionSpec.unpartitioned())
              : icebergSplit.getPartitionSpecJson();

      String dataJson = icebergSplit.getPartitionDataJson();

      if (dataJson == null || dataJson.isBlank() || dataJson.equals("{\"partition_values\":[]}")) {
        LOG.info(
            "Fixing partitionDataJson string by using correct casing: '{}'.",
            "{\"partitionValues\":[]}");
        dataJson = "{\"partitionValues\":[]}";
      }

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
              dataJson,
              icebergSplit.getDeletes(),
              icebergSplit.getSplitWeight(),
              icebergSplit.getFileStatisticsDomain(),
              icebergSplit.getFileIoProperties(),
              icebergSplit.getAddresses(),
              icebergSplit.getDataSequenceNumber());

      LOG.info("split partitionDataJson='{}', partitionSpecJson='{}'", dataJson, specJson);
    }

    return iceberg.createPageSource(
        transaction, session, splitToUse, handleToUse, columns, dynamicFilter);
  }
}
