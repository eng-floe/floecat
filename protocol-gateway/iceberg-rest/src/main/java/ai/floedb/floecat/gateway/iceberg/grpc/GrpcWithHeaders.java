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

package ai.floedb.floecat.gateway.iceberg.grpc;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import io.grpc.Metadata;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.MetadataUtils;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;

@RequestScoped
public class GrpcWithHeaders {
  private final GrpcClients clients;
  private final Metadata metadata;

  @Inject
  public GrpcWithHeaders(GrpcClients clients, IcebergGatewayConfig config, HttpHeaders headers) {
    this.clients = clients;
    this.metadata = AuthMetadata.fromHeaders(config, headers);
  }

  public <T extends AbstractBlockingStub<T>> T withHeaders(T stub) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  public GrpcClients raw() {
    return clients;
  }
}
