package ai.floedb.metacat.service.error.impl;

import java.util.Map;

import static java.util.Objects.requireNonNullElse;

import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import ai.floedb.metacat.common.rpc.Error;
import ai.floedb.metacat.common.rpc.ErrorCode;

public final class GrpcErrors {
  private GrpcErrors() {}

  public static StatusRuntimeException aborted(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.ABORTED, ErrorCode.MC_ABORT_RETRYABLE, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException invalidArgument(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException notFound(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.NOT_FOUND, ErrorCode.MC_NOT_FOUND, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException conflict(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.ABORTED, ErrorCode.MC_CONFLICT, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException preconditionFailed(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException permissionDenied(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.PERMISSION_DENIED, ErrorCode.MC_PERMISSION_DENIED, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException unauthenticated(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.UNAUTHENTICATED, ErrorCode.MC_UNAUTHENTICATED, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException rateLimited(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.RESOURCE_EXHAUSTED, ErrorCode.MC_RATE_LIMITED, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException timeout(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.DEADLINE_EXCEEDED, ErrorCode.MC_TIMEOUT, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException unavailable(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.UNAVAILABLE, ErrorCode.MC_UNAVAILABLE, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException cancelled(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.CANCELLED, ErrorCode.MC_CANCELLED, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException internal(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.INTERNAL, ErrorCode.MC_INTERNAL, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException snapshotExpired(String correlationId, String messageKey, Map<String,String> params) {
    return build(io.grpc.Status.FAILED_PRECONDITION, ErrorCode.MC_SNAPSHOT_EXPIRED, correlationId, params, Map.of(), messageKey);
  }

  public static StatusRuntimeException build(
    io.grpc.Status canonical,
    ErrorCode appCode,
    String correlationId,
    Map<String,String> params,
    Map<String,String> context,
    String messageKey
  ) {
    Error.Builder eb = Error.newBuilder()
      .setCode(appCode)
      .setCorrelationId(requireNonNullElse(correlationId, ""))
      .putAllParams(params == null ? Map.of() : params)
      .putAllContext(context == null ? Map.of() : context);
    if (messageKey != null && !messageKey.isBlank()) {
      eb.setMessageKey(messageKey);
    }
    Error err = eb.build();

    Status st = Status.newBuilder()
      .setCode(canonical.getCode().value())
      .addDetails(Any.pack(err))
      .build();

    return StatusProto.toStatusRuntimeException(st);
  }
}
