package ai.floedb.floecat.client.trino;

import com.google.inject.Inject;
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;
import java.util.Map;

public class FloecatPageSourceProvider implements ConnectorPageSourceProvider {

  private final IcebergPageSourceProvider iceberg;

  @Inject
  public FloecatPageSourceProvider(IcebergPageSourceProvider iceberg) {
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
    if (table instanceof FloecatTableHandle floecatTableHandle) {
      handleToUse = floecatTableHandle.toIcebergTableHandle(Map.of());
    }

    ConnectorSplit splitToUse = split;

    return iceberg.createPageSource(
        transaction, session, splitToUse, handleToUse, columns, dynamicFilter);
  }
}
