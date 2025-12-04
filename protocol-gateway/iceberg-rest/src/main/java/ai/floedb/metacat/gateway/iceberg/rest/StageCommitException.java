package ai.floedb.metacat.gateway.iceberg.rest;

import jakarta.ws.rs.core.Response;

final class StageCommitException extends RuntimeException {
  private final Response.Status status;
  private final String errorType;

  private StageCommitException(Response.Status status, String errorType, String message) {
    super(message);
    this.status = status;
    this.errorType = errorType;
  }

  static StageCommitException validation(String message) {
    return new StageCommitException(Response.Status.BAD_REQUEST, "ValidationException", message);
  }

  static StageCommitException notFound(String message) {
    return new StageCommitException(Response.Status.NOT_FOUND, "NoSuchObjectException", message);
  }

  static StageCommitException conflict(String message) {
    return new StageCommitException(Response.Status.CONFLICT, "CommitFailedException", message);
  }

  Response toResponse() {
    return Response.status(status)
        .entity(
            new IcebergErrorResponse(
                new IcebergError(getMessage(), errorType, status.getStatusCode())))
        .build();
  }
}
