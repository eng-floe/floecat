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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.table;

import ai.floedb.floecat.gateway.iceberg.minimal.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/v1/{prefix}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableAdminResource {
  private final TableBackend backend;
  private final TransactionCommitService transactionCommitService;

  public TableAdminResource(
      TableBackend backend, TransactionCommitService transactionCommitService) {
    this.backend = backend;
    this.transactionCommitService = transactionCommitService;
  }

  @POST
  @Path("/tables/rename")
  public Response rename(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      RenameRequest request) {
    if (request == null
        || request.source() == null
        || request.destination() == null
        || request.source().namespace() == null
        || request.destination().namespace() == null
        || request.source().name() == null
        || request.source().name().isBlank()
        || request.destination().name() == null
        || request.destination().name().isBlank()) {
      return IcebergErrorResponses.validation("source and destination are required");
    }
    try {
      backend.rename(
          prefix,
          request.source().namespace(),
          request.source().name(),
          request.destination().namespace(),
          request.destination().name());
      return Response.noContent().build();
    } catch (StatusRuntimeException exception) {
      String message = exception.getStatus().getDescription();
      if (message != null && message.contains("Table ")) {
        return IcebergErrorResponses.noSuchTable(message);
      }
      return IcebergErrorResponses.grpc(exception);
    }
  }

  @POST
  @Path("/transactions/commit")
  public Response commitTransaction(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TransactionCommitRequest request) {
    return transactionCommitService.commit(prefix, idempotencyKey, request);
  }
}
