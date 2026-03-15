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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.common;

import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.core.Response;
import java.util.Map;

public final class IcebergErrorResponses {
  private IcebergErrorResponses() {}

  public static Response validation(String message) {
    return error(message, "ValidationException", Response.Status.BAD_REQUEST);
  }

  public static Response noSuchNamespace(String message) {
    return error(message, "NoSuchNamespaceException", Response.Status.NOT_FOUND);
  }

  public static Response noSuchTable(String message) {
    return error(message, "NoSuchTableException", Response.Status.NOT_FOUND);
  }

  public static Response conflict(String message) {
    return error(message, "CommitFailedException", Response.Status.CONFLICT);
  }

  public static Response commitStateUnknown(String message) {
    return error(message, "CommitStateUnknownException", Response.Status.SERVICE_UNAVAILABLE);
  }

  public static Response grpc(StatusRuntimeException exception) {
    return switch (exception.getStatus().getCode()) {
      case NOT_FOUND -> noSuchNamespace(descriptionOrCode(exception));
      case INVALID_ARGUMENT -> validation(descriptionOrCode(exception));
      case ALREADY_EXISTS, FAILED_PRECONDITION, ABORTED ->
          error(descriptionOrCode(exception), "CommitFailedException", Response.Status.CONFLICT);
      case PERMISSION_DENIED ->
          error(descriptionOrCode(exception), "ForbiddenException", Response.Status.FORBIDDEN);
      case UNAUTHENTICATED ->
          error(
              descriptionOrCode(exception), "UnauthorizedException", Response.Status.UNAUTHORIZED);
      default ->
          error(
              descriptionOrCode(exception),
              exception.getStatus().getCode().name(),
              Response.Status.INTERNAL_SERVER_ERROR);
    };
  }

  private static String descriptionOrCode(StatusRuntimeException exception) {
    return exception.getStatus().getDescription() == null
        ? exception.getStatus().getCode().name()
        : exception.getStatus().getDescription();
  }

  private static Response error(String message, String type, Response.Status status) {
    return Response.status(status)
        .entity(
            Map.of(
                "error", Map.of("message", message, "type", type, "code", status.getStatusCode())))
        .build();
  }
}
