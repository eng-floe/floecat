package ai.floedb.floecat.client.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum FloecatTransactionHandle implements ConnectorTransactionHandle {
  INSTANCE
}
