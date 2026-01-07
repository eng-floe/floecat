/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.client.trino;

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

public class FloecatConnector implements Connector {

  private final LifeCycleManager lifeCycleManager;
  private final FloecatMetadata metadata;
  private final FloecatSplitManager splitManager;
  private final FloecatPageSourceProvider pageSourceProvider;
  private final java.util.List<io.trino.spi.session.PropertyMetadata<?>> sessionProperties;

  @Inject
  public FloecatConnector(
      LifeCycleManager lifeCycleManager,
      FloecatMetadata metadata,
      FloecatSplitManager splitManager,
      FloecatPageSourceProvider pageSourceProvider,
      FloecatSessionProperties sessionProperties) {
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
    return FloecatTransactionHandle.INSTANCE;
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
