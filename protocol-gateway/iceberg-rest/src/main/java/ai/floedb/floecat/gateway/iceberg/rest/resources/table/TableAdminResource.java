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

package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitTrafficLogger;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableRenameService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction.TransactionCommitService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/v1/{prefix}")
@Produces(MediaType.APPLICATION_JSON)
public class TableAdminResource {
  @Inject TableRenameService tableRenameService;
  @Inject TransactionCommitService transactionCommitService;
  @Inject CommitTrafficLogger commitTrafficLogger;
  @Inject TableGatewaySupport tableSupport;

  @Path("/tables/rename")
  @POST
  public Response rename(
      @PathParam("prefix") String prefix, @NotNull @Valid RenameRequest request) {
    return tableRenameService.rename(prefix, request);
  }

  @Path("/transactions/commit")
  @POST
  @Blocking
  public Response commitTransaction(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @NotNull @Valid TransactionCommitRequest request) {
    String path = String.format("/v1/%s/transactions/commit", prefix);
    commitTrafficLogger.logRequest("POST", path, request);
    Response response =
        transactionCommitService.commit(prefix, idempotencyKey, request, tableSupport);
    commitTrafficLogger.logResponse("POST", path, response.getStatus(), response.getEntity());
    return response;
  }
}
