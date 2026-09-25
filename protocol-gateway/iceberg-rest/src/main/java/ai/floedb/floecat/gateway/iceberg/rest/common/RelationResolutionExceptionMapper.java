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
package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.engine.catalog.RelationResults.RelationResolutionException;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Renders a per-relation resolution failure.
 *
 * <p>ResolveRelations reports a failed reference in band, so the failure reaches the gateway as a
 * plain exception rather than a gRPC status. Without this it would fall through to a 500 and a
 * permission or validation failure would lose its status.
 */
@Provider
public class RelationResolutionExceptionMapper
    implements ExceptionMapper<RelationResolutionException> {

  @Override
  public Response toResponse(RelationResolutionException exception) {
    Error error = exception.error();
    Response.Status httpStatus = ErrorMapper.httpForCode(error.getCode());
    if (httpStatus == null) {
      httpStatus = Response.Status.INTERNAL_SERVER_ERROR;
    }
    String type = ErrorMapper.typeForCode(error);
    if (type == null) {
      type = error.getCode().name();
    }
    String message = error.getMessage().isBlank() ? error.getCode().name() : error.getMessage();

    return Response.status(httpStatus)
        .entity(
            new IcebergErrorResponse(new IcebergError(message, type, httpStatus.getStatusCode())))
        .build();
  }
}
