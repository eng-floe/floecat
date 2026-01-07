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

package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.ViewResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.ViewRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ViewClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class ViewCommitService {
  @Inject ViewClient viewClient;
  @Inject ViewMetadataService viewMetadataService;

  public Response commit(
      ViewRequestContext viewContext,
      List<String> namespacePath,
      String namespace,
      String viewName,
      ViewRequests.Commit req) {
    View current =
        viewClient
            .getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build())
            .getView();
    MetadataContext baseContext = viewMetadataService.fromView(namespacePath, viewName, current);
    MetadataContext updated;
    try {
      updated = viewMetadataService.applyCommit(namespacePath, baseContext, req);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec = ViewSpec.newBuilder().setSql(updated.sql());
    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("sql").addPaths("properties");
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(updated));
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    var resp =
        viewClient.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewContext.viewId())
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    MetadataContext responseContext =
        viewMetadataService.fromView(namespacePath, viewName, resp.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespace, viewName, resp.getView(), responseContext.metadata()))
        .build();
  }
}
