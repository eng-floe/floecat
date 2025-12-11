package ai.floedb.floecat.gateway.iceberg.rest.support.error;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class ErrorMapper implements ExceptionMapper<StatusRuntimeException> {
  @Override
  public Response toResponse(StatusRuntimeException exception) {
    Status status = exception.getStatus();
    Error mcError = unpackMcError(exception);

    Response.Status httpStatus = toHttp(status, mcError);
    String message =
        status.getDescription() == null ? status.getCode().name() : status.getDescription();
    String type = mapType(mcError, status);
    IcebergErrorResponse payload =
        new IcebergErrorResponse(new IcebergError(message, type, httpStatus.getStatusCode()));
    return Response.status(httpStatus).entity(payload).build();
  }

  private Response.Status toHttp(Status status, Error mcError) {
    if (mcError != null) {
      ErrorCode code = mcError.getCode();
      return switch (code) {
        case MC_NOT_FOUND -> Response.Status.NOT_FOUND;
        case MC_PRECONDITION_FAILED, MC_CONFLICT -> Response.Status.CONFLICT;
        case MC_PERMISSION_DENIED -> Response.Status.FORBIDDEN;
        case MC_UNAUTHENTICATED -> Response.Status.UNAUTHORIZED;
        case MC_INVALID_ARGUMENT -> Response.Status.BAD_REQUEST;
        default -> fromGrpc(status);
      };
    }
    return fromGrpc(status);
  }

  private Response.Status fromGrpc(Status status) {
    return switch (status.getCode()) {
      case NOT_FOUND -> Response.Status.NOT_FOUND;
      case ALREADY_EXISTS, FAILED_PRECONDITION, ABORTED -> Response.Status.CONFLICT;
      case INVALID_ARGUMENT -> Response.Status.BAD_REQUEST;
      case PERMISSION_DENIED -> Response.Status.FORBIDDEN;
      case UNAUTHENTICATED -> Response.Status.UNAUTHORIZED;
      default -> Response.Status.INTERNAL_SERVER_ERROR;
    };
  }

  private Error unpackMcError(StatusRuntimeException ex) {
    var st = StatusProto.fromThrowable(ex);
    if (st == null) {
      return null;
    }
    for (Any any : st.getDetailsList()) {
      if (any.is(Error.class)) {
        try {
          return any.unpack(Error.class);
        } catch (Exception ignored) {
          // fall through
        }
      }
    }
    return null;
  }

  private String mapType(Error mcError, Status status) {
    if (mcError != null) {
      return switch (mcError.getCode()) {
        case MC_NOT_FOUND -> "NoSuchObjectException";
        case MC_PRECONDITION_FAILED, MC_CONFLICT -> "CommitFailedException";
        case MC_PERMISSION_DENIED -> "ForbiddenException";
        case MC_UNAUTHENTICATED -> "UnauthorizedException";
        case MC_INVALID_ARGUMENT -> "ValidationException";
        default -> status.getCode().name();
      };
    }
    return switch (status.getCode()) {
      case NOT_FOUND -> "NoSuchObjectException";
      case ALREADY_EXISTS -> "AlreadyExistsException";
      case INVALID_ARGUMENT -> "ValidationException";
      case PERMISSION_DENIED -> "ForbiddenException";
      case UNAUTHENTICATED -> "UnauthorizedException";
      default -> status.getCode().name();
    };
  }
}
