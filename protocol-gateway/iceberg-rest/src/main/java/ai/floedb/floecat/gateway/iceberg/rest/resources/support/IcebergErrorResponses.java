package ai.floedb.floecat.gateway.iceberg.rest.resources.support;

import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.core.Response;

public final class IcebergErrorResponses {
  private IcebergErrorResponses() {}

  public static Response validation(String message) {
    return error(message, "ValidationException", Response.Status.BAD_REQUEST);
  }

  public static Response conflict(String message) {
    return error(message, "CommitFailedException", Response.Status.CONFLICT);
  }

  public static Response notFound(String message) {
    return error(message, "NotFoundException", Response.Status.NOT_FOUND);
  }

  public static Response unprocessable(String message) {
    return error(message, "ValidationException", 422);
  }

  public static Response unsupported(String message) {
    return error(message, "UnsupportedOperationException", Response.Status.NOT_IMPLEMENTED);
  }

  public static Response failure(String message, String type, Response.Status status) {
    return error(message, type, status);
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
      default -> {
        httpStatus = Response.Status.INTERNAL_SERVER_ERROR;
        type = status.getCode().name();
      }
    }
    String message =
        status.getDescription() == null ? status.getCode().name() : status.getDescription();
    return error(message, type, httpStatus);
  }

  private static Response error(String message, String type, Response.Status status) {
    return error(message, type, status.getStatusCode());
  }

  private static Response error(String message, String type, int statusCode) {
    return Response.status(statusCode)
        .entity(new IcebergErrorResponse(new IcebergError(message, type, statusCode)))
        .build();
  }
}
