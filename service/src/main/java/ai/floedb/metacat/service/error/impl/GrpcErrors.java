package ai.floedb.metacat.service.error.impl;

import static java.util.Objects.requireNonNullElse;

import ai.floedb.metacat.common.rpc.Error;
import ai.floedb.metacat.common.rpc.ErrorCode;
import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.LinkedHashMap;
import java.util.Map;

public final class GrpcErrors {

  public static StatusRuntimeException aborted(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.ABORTED, ErrorCode.MC_ABORT_RETRYABLE, corrId, params, messageKey, t);
  }

  public static StatusRuntimeException invalidArgument(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        corrId,
        params,
        messageKey,
        t);
  }

  public static StatusRuntimeException notFound(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(io.grpc.Status.NOT_FOUND, ErrorCode.MC_NOT_FOUND, corrId, params, messageKey, t);
  }

  public static StatusRuntimeException conflict(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(io.grpc.Status.ABORTED, ErrorCode.MC_CONFLICT, corrId, params, messageKey, t);
  }

  public static StatusRuntimeException preconditionFailed(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED,
        corrId,
        params,
        messageKey,
        t);
  }

  public static StatusRuntimeException permissionDenied(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.PERMISSION_DENIED,
        ErrorCode.MC_PERMISSION_DENIED,
        corrId,
        params,
        messageKey,
        t);
  }

  public static StatusRuntimeException unauthenticated(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.UNAUTHENTICATED,
        ErrorCode.MC_UNAUTHENTICATED,
        corrId,
        params,
        messageKey,
        t);
  }

  public static StatusRuntimeException rateLimited(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.RESOURCE_EXHAUSTED,
        ErrorCode.MC_RATE_LIMITED,
        corrId,
        params,
        messageKey,
        t);
  }

  public static StatusRuntimeException timeout(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.DEADLINE_EXCEEDED, ErrorCode.MC_TIMEOUT, corrId, params, messageKey, t);
  }

  public static StatusRuntimeException unavailable(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.UNAVAILABLE, ErrorCode.MC_UNAVAILABLE, corrId, params, messageKey, t);
  }

  public static StatusRuntimeException cancelled(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(io.grpc.Status.CANCELLED, ErrorCode.MC_CANCELLED, corrId, params, messageKey, t);
  }

  public static StatusRuntimeException internal(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(io.grpc.Status.INTERNAL, ErrorCode.MC_INTERNAL, corrId, params, messageKey, t);
  }

  public static StatusRuntimeException snapshotExpired(
      String corrId, String messageKey, Map<String, String> params, Throwable t) {
    return build(
        io.grpc.Status.FAILED_PRECONDITION,
        ErrorCode.MC_SNAPSHOT_EXPIRED,
        corrId,
        params,
        messageKey,
        t);
  }

  public static StatusRuntimeException aborted(String c, String m, Map<String, String> p) {
    return aborted(c, m, p, null);
  }

  public static StatusRuntimeException invalidArgument(String c, String m, Map<String, String> p) {
    return invalidArgument(c, m, p, null);
  }

  public static StatusRuntimeException notFound(String c, String m, Map<String, String> p) {
    return notFound(c, m, p, null);
  }

  public static StatusRuntimeException conflict(String c, String m, Map<String, String> p) {
    return conflict(c, m, p, null);
  }

  public static StatusRuntimeException preconditionFailed(
      String c, String m, Map<String, String> p) {
    return preconditionFailed(c, m, p, null);
  }

  public static StatusRuntimeException permissionDenied(String c, String m, Map<String, String> p) {
    return permissionDenied(c, m, p, null);
  }

  public static StatusRuntimeException unauthenticated(String c, String m, Map<String, String> p) {
    return unauthenticated(c, m, p, null);
  }

  public static StatusRuntimeException rateLimited(String c, String m, Map<String, String> p) {
    return rateLimited(c, m, p, null);
  }

  public static StatusRuntimeException timeout(String c, String m, Map<String, String> p) {
    return timeout(c, m, p, null);
  }

  public static StatusRuntimeException unavailable(String c, String m, Map<String, String> p) {
    return unavailable(c, m, p, null);
  }

  public static StatusRuntimeException cancelled(String c, String m, Map<String, String> p) {
    return cancelled(c, m, p, null);
  }

  public static StatusRuntimeException internal(String c, String m, Map<String, String> p) {
    return internal(c, m, p, null);
  }

  public static StatusRuntimeException snapshotExpired(String c, String m, Map<String, String> p) {
    return snapshotExpired(c, m, p, null);
  }

  public static StatusRuntimeException build(
      io.grpc.Status canonical,
      ErrorCode appCode,
      String correlationId,
      Map<String, String> params,
      String messageKey,
      Throwable t) {

    Map<String, String> p = new LinkedHashMap<>();
    if (params != null) p.putAll(params);

    if (messageKey == null || messageKey.isBlank()) {
      if (t != null) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        p.put("error_class", t.getClass().getName());
        p.put("root_class", root.getClass().getName());
        p.put("cause", root.getClass().getSimpleName());
        String m = root.getMessage();
        if (m != null && !m.isBlank()) p.putIfAbsent("root_message", m);
      } else {
        String inferred = p.getOrDefault("error_class", p.getOrDefault("root_class", "unknown"));
        String simple =
            inferred.equals("unknown")
                ? "unknown"
                : inferred.substring(inferred.lastIndexOf('.') + 1);
        p.putIfAbsent("cause", simple);
      }
    }

    Error.Builder eb =
        Error.newBuilder()
            .setCode(appCode)
            .setCorrelationId(requireNonNullElse(correlationId, ""))
            .putAllParams(p);

    if (messageKey != null && !messageKey.isBlank()) {
      eb.setMessageKey(messageKey);
    }

    Error error = eb.build();

    Status st =
        Status.newBuilder()
            .setCode(canonical.getCode().value())
            .addDetails(Any.pack(error))
            .build();

    StatusRuntimeException ex = StatusProto.toStatusRuntimeException(st);
    if (t != null) ex.addSuppressed(t);
    return ex;
  }
}
