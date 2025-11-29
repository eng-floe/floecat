package ai.floedb.metacat.client.trino;

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;

public class MetacatConnector implements Connector {

  private final LifeCycleManager lifeCycleManager;
  private final MetacatMetadata metadata;
  private final MetacatSplitManager splitManager;
  private final MetacatPageSourceProvider pageSourceProvider;
  private final java.util.List<io.trino.spi.session.PropertyMetadata<?>> sessionProperties;

  @Inject
  public MetacatConnector(
      LifeCycleManager lifeCycleManager,
      MetacatMetadata metadata,
      MetacatSplitManager splitManager,
      MetacatPageSourceProvider pageSourceProvider,
      MetacatSessionProperties sessionProperties) {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    this.sessionProperties =
        requireNonNull(sessionProperties, "sessionProperties is null").getSessionProperties();
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return MetacatTransactionHandle.INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    return pageSourceProvider;
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    return sessionProperties;
  }

  @Override
  public void shutdown() {
    try {
      lifeCycleManager.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
