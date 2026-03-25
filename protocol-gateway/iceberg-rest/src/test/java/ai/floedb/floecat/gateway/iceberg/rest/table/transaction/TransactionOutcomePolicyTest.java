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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import io.grpc.Status;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

class TransactionOutcomePolicyTest {
  private final TransactionOutcomePolicy policy = new TransactionOutcomePolicy();

  @Test
  void beginReadbackNotFoundMapsToStateUnknownInsteadOfNoSuchTable() {
    Response response = policy.mapBeginReadbackFailure(Status.NOT_FOUND.asRuntimeException());
    IcebergErrorResponse body = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());

    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertEquals("CommitStateUnknownException", body.error().type());
  }

  @Test
  void beginReadbackUnavailableMapsToStateUnknown() {
    Response response = policy.mapBeginReadbackFailure(Status.UNAVAILABLE.asRuntimeException());

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
  }

  @Test
  void prepareNotFoundMappingRemainsNoSuchTable() {
    Response response = policy.mapPrepareFailure(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
