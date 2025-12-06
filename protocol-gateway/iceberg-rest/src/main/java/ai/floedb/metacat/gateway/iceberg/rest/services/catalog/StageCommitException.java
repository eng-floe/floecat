package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import jakarta.ws.rs.core.Response;

public final class StageCommitException extends RuntimeException {
  private final Response.Status status;
  private final String errorType;

  private StageCommitException(Response.Status status, String errorType, String message) {
    super(message);
    this.status = status;
    this.errorType = errorType;
  }

  public static StageCommitException validation(String message) {
    return new StageCommitException(Response.Status.BAD_REQUEST, "ValidationException", message);
  }

  public static StageCommitException notFound(String message) {
    return new StageCommitException(Response.Status.NOT_FOUND, "NoSuchObjectException", message);
  }

  public static StageCommitException conflict(String message) {
    return new StageCommitException(Response.Status.CONFLICT, "CommitFailedException", message);
  }

  public Response toResponse() {
    return Response.status(status)
        .entity(
            new IcebergErrorResponse(
                new IcebergError(getMessage(), errorType, status.getStatusCode())))
        .build();
  }
}
