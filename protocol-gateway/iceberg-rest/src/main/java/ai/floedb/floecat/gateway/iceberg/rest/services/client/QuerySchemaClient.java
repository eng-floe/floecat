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

package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QuerySchemaClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public QuerySchemaClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public DescribeInputsResponse describeInputs(DescribeInputsRequest request) {
    return stub().describeInputs(request);
  }

  private QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().querySchema());
  }
}
