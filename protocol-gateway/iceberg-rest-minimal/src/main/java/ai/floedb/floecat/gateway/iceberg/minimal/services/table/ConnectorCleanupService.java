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

package ai.floedb.floecat.gateway.iceberg.minimal.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.ConnectorProvisioningService;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ConnectorCleanupService {
  private static final Logger LOG = Logger.getLogger(ConnectorCleanupService.class);

  private final GrpcClients grpcClients;
  private final ConnectorProvisioningService connectorProvisioningService;

  @Inject
  public ConnectorCleanupService(
      GrpcClients grpcClients, ConnectorProvisioningService connectorProvisioningService) {
    this.grpcClients = grpcClients;
    this.connectorProvisioningService = connectorProvisioningService;
  }

  public void deleteManagedConnector(Table tableRecord) {
    ResourceId connectorId = connectorProvisioningService.resolveConnectorId(tableRecord);
    if (connectorId == null) {
      return;
    }
    try {
      grpcClients
          .connector()
          .deleteConnector(DeleteConnectorRequest.newBuilder().setConnectorId(connectorId).build());
    } catch (StatusRuntimeException exception) {
      LOG.warnf(exception, "Failed to delete connector %s", connectorId.getId());
    }
  }
}
