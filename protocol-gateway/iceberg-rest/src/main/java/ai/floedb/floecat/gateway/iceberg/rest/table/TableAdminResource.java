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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitTrafficLogger;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.table.transaction.TransactionCommitService;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
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
  @Inject GrpcWithHeaders grpc;
  @Inject ResourceResolver resourceResolver;
  @Inject GrpcServiceFacade grpcClient;
  @Inject CommitTrafficLogger commitTrafficLogger;
  @Inject TransactionCommitService transactionCommitService;
  @Inject TableGatewaySupport tableSupport;

  @Path("/tables/rename")
  @POST
  public Response rename(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @NotNull @Valid TableRequests.Rename request) {
    CatalogRef catalogContext = resourceResolver.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    var sourcePath = request.source().namespace();
    var destinationPath = request.destination().namespace();

    ResourceId tableId;
    try {
      tableId = NameResolution.resolveTable(grpc, catalogName, sourcePath, request.source().name());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespace = String.join(".", sourcePath);
        return IcebergErrorResponses.noSuchTable(
            "Table " + namespace + "." + request.source().name() + " not found");
      }
      throw e;
    }

    ResourceId namespaceId;
    try {
      namespaceId = NameResolution.resolveNamespace(grpc, catalogName, destinationPath);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespace = String.join(".", destinationPath);
        return IcebergErrorResponses.noSuchNamespace("Namespace " + namespace + " not found");
      }
      throw e;
    }

    TableSpec.Builder spec =
        TableSpec.newBuilder()
            .setNamespaceId(namespaceId)
            .setDisplayName(request.destination().name());
    FieldMask mask =
        FieldMask.newBuilder().addPaths("namespace_id").addPaths("display_name").build();
    grpcClient.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.noContent().build();
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
