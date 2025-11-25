package ai.floedb.metacat.trino;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
// or just new TrinoException

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import java.util.List;

public class MetacatPageSourceProvider implements ConnectorPageSourceProvider {

  @Inject
  public MetacatPageSourceProvider() {
    // No deps for now
  }

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter) {

    // For now, fail cleanly so the connector can start while we wire up real reads later.
    throw new TrinoException(
        NOT_SUPPORTED,
        "Metacat connector: data reads not implemented yet (page source provider is a stub)");
  }
}
