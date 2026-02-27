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

package ai.floedb.floecat.gateway.iceberg.rest.resources.common;

import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.core.Response;

public final class IcebergErrorResponses {
  private IcebergErrorResponses() {}

  public static Response validation(String message) {
    return error(message, "ValidationException", Response.Status.BAD_REQUEST.getStatusCode());
  }

  public static Response conflict(String message) {
    return error(message, "CommitFailedException", Response.Status.CONFLICT.getStatusCode());
  }

  public static Response notFound(String message) {
    return error(message, "NotFoundException", Response.Status.NOT_FOUND.getStatusCode());
  }

  public static Response noSuchNamespace(String message) {
    return error(message, "NoSuchNamespaceException", Response.Status.NOT_FOUND.getStatusCode());
  }

  public static Response noSuchTable(String message) {
    return error(message, "NoSuchTableException", Response.Status.NOT_FOUND.getStatusCode());
  }

  public static Response noSuchView(String message) {
    return error(message, "NoSuchViewException", Response.Status.NOT_FOUND.getStatusCode());
  }

  public static Response noSuchPlanId(String message) {
    return error(message, "NoSuchPlanIdException", Response.Status.NOT_FOUND.getStatusCode());
  }

  public static Response noSuchPlanTask(String message) {
    return error(message, "NoSuchPlanTaskException", Response.Status.NOT_FOUND.getStatusCode());
  }

  public static Response unprocessable(String message) {
    return error(message, "ValidationException", 422);
  }

  public static Response unsupported(String message) {
    return error(
        message, "UnsupportedOperationException", Response.Status.NOT_ACCEPTABLE.getStatusCode());
  }

  public static Response failure(String message, String type, Response.Status status) {
    return error(message, type, status.getStatusCode());
  }

  public static Response grpcError(StatusRuntimeException exception) {
    var status = exception.getStatus();
    Response.Status httpStatus;
    String type;
    switch (status.getCode()) {
      case NOT_FOUND -> {
        httpStatus = Response.Status.NOT_FOUND;
        type = "NoSuchObjectException";
      }
      case INVALID_ARGUMENT -> {
        httpStatus = Response.Status.BAD_REQUEST;
        type = "ValidationException";
      }
      case PERMISSION_DENIED -> {
        httpStatus = Response.Status.FORBIDDEN;
        type = "ForbiddenException";
      }
      case UNAUTHENTICATED -> {
        httpStatus = Response.Status.UNAUTHORIZED;
        type = "UnauthorizedException";
      }
      case UNIMPLEMENTED -> {
        httpStatus = Response.Status.NOT_ACCEPTABLE;
        type = "UnsupportedOperationException";
      }
      case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED -> {
        httpStatus = Response.Status.SERVICE_UNAVAILABLE;
        type = "ServiceUnavailableException";
      }
      default -> {
        httpStatus = Response.Status.INTERNAL_SERVER_ERROR;
        type = status.getCode().name();
      }
    }
    String message =
        status.getDescription() == null ? status.getCode().name() : status.getDescription();
    return error(message, type, httpStatus.getStatusCode());
  }

  private static Response error(String message, String type, int statusCode) {
    return Response.status(statusCode)
        .entity(new IcebergErrorResponse(new IcebergError(message, type, statusCode)))
        .build();
  }
}
