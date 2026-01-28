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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class TableRenameService {
  @Inject GrpcWithHeaders grpc;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableClient tableClient;

  public Response rename(String prefix, RenameRequest request) {
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
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
    tableClient.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.noContent().build();
  }
}
