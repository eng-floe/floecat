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

package ai.floedb.floecat.service.error.impl;

import static java.util.Objects.requireNonNullElse;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import org.eclipse.microprofile.config.ConfigProvider;

public final class GrpcErrors {

  public static StatusRuntimeException aborted(
      String corrId, Map<String, String> params, Throwable t) {
    return build(io.grpc.Status.ABORTED, ErrorCode.MC_ABORT_RETRYABLE, corrId, params, null, t);
  }

  public static StatusRuntimeException invalidArgument(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("invalidArgument", key, ErrorCode.MC_INVALID_ARGUMENT);
    return build(
        io.grpc.Status.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        corrId,
        params,
        suffix(key),
        t);
  }

  public static StatusRuntimeException notFound(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("notFound", key, ErrorCode.MC_NOT_FOUND);
    return build(io.grpc.Status.NOT_FOUND, ErrorCode.MC_NOT_FOUND, corrId, params, suffix(key), t);
  }

  public static StatusRuntimeException conflict(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("conflict", key, ErrorCode.MC_CONFLICT);
    return build(io.grpc.Status.ABORTED, ErrorCode.MC_CONFLICT, corrId, params, suffix(key), t);
  }

  public static StatusRuntimeException alreadyExists(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("alreadyExists", key, ErrorCode.MC_CONFLICT);
    return build(
        io.grpc.Status.ALREADY_EXISTS, ErrorCode.MC_CONFLICT, corrId, params, suffix(key), t);
  }

  public static StatusRuntimeException preconditionFailed(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("preconditionFailed", key, ErrorCode.MC_PRECONDITION_FAILED);
    return build(
        io.grpc.Status.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED,
        corrId,
        params,
        suffix(key),
        t);
  }

  public static StatusRuntimeException permissionDenied(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("permissionDenied", key, ErrorCode.MC_PERMISSION_DENIED);
    return build(
        io.grpc.Status.PERMISSION_DENIED,
        ErrorCode.MC_PERMISSION_DENIED,
        corrId,
        params,
        suffix(key),
        t);
  }

  public static StatusRuntimeException unauthenticated(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("unauthenticated", key, ErrorCode.MC_UNAUTHENTICATED);
    return build(
        io.grpc.Status.UNAUTHENTICATED,
        ErrorCode.MC_UNAUTHENTICATED,
        corrId,
        params,
        suffix(key),
        t);
  }

  public static StatusRuntimeException rateLimited(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("rateLimited", key, ErrorCode.MC_RATE_LIMITED);
    return build(
        io.grpc.Status.RESOURCE_EXHAUSTED,
        ErrorCode.MC_RATE_LIMITED,
        corrId,
        params,
        suffix(key),
        t);
  }

  public static StatusRuntimeException timeout(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("timeout", key, ErrorCode.MC_TIMEOUT);
    return build(
        io.grpc.Status.DEADLINE_EXCEEDED, ErrorCode.MC_TIMEOUT, corrId, params, suffix(key), t);
  }

  public static StatusRuntimeException unavailable(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("unavailable", key, ErrorCode.MC_UNAVAILABLE);
    return build(
        io.grpc.Status.UNAVAILABLE, ErrorCode.MC_UNAVAILABLE, corrId, params, suffix(key), t);
  }

  public static StatusRuntimeException cancelled(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("cancelled", key, ErrorCode.MC_CANCELLED);
    return build(io.grpc.Status.CANCELLED, ErrorCode.MC_CANCELLED, corrId, params, suffix(key), t);
  }

  public static StatusRuntimeException internal(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("internal", key, ErrorCode.MC_INTERNAL);
    return build(io.grpc.Status.INTERNAL, ErrorCode.MC_INTERNAL, corrId, params, suffix(key), t);
  }

  public static StatusRuntimeException snapshotExpired(
      String corrId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> params,
      Throwable t) {
    validateErrorCode("snapshotExpired", key, ErrorCode.MC_SNAPSHOT_EXPIRED);
    return build(
        io.grpc.Status.FAILED_PRECONDITION,
        ErrorCode.MC_SNAPSHOT_EXPIRED,
        corrId,
        params,
        suffix(key),
        t);
  }

  public static StatusRuntimeException aborted(String corrId, Map<String, String> params) {
    return aborted(corrId, params, null);
  }

  public static StatusRuntimeException invalidArgument(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return invalidArgument(corrId, key, params, null);
  }

  public static StatusRuntimeException notFound(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return notFound(corrId, key, params, null);
  }

  public static StatusRuntimeException conflict(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return conflict(corrId, key, params, null);
  }

  public static StatusRuntimeException alreadyExists(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return alreadyExists(corrId, key, params, null);
  }

  public static StatusRuntimeException preconditionFailed(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return preconditionFailed(corrId, key, params, null);
  }

  public static StatusRuntimeException permissionDenied(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return permissionDenied(corrId, key, params, null);
  }

  public static StatusRuntimeException unauthenticated(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return unauthenticated(corrId, key, params, null);
  }

  public static StatusRuntimeException rateLimited(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return rateLimited(corrId, key, params, null);
  }

  public static StatusRuntimeException timeout(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return timeout(corrId, key, params, null);
  }

  public static StatusRuntimeException unavailable(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return unavailable(corrId, key, params, null);
  }

  public static StatusRuntimeException cancelled(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return cancelled(corrId, key, params, null);
  }

  public static StatusRuntimeException internal(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return internal(corrId, key, params, null);
  }

  public static StatusRuntimeException snapshotExpired(
      String corrId, GeneratedErrorMessages.MessageKey key, Map<String, String> params) {
    return snapshotExpired(corrId, key, params, null);
  }

  private static String suffix(GeneratedErrorMessages.MessageKey key) {
    return key == null ? null : key.suffix();
  }

  private static void validateErrorCode(
      String methodName, GeneratedErrorMessages.MessageKey key, ErrorCode expected) {
    if (key != null && key.errorCode() != expected) {
      throw new IllegalArgumentException(
          "MessageKey "
              + key.fullKey()
              + " is not valid for "
              + methodName
              + " (expected error code: "
              + expected
              + ")");
    }
  }

  public static StatusRuntimeException build(
      io.grpc.Status canonical,
      ErrorCode appCode,
      String correlationId,
      Map<String, String> params,
      String messageKey,
      Throwable t) {

    Map<String, String> p = new LinkedHashMap<>();
    if (params != null) {
      p.putAll(params);
    }

    if (messageKey == null || messageKey.isBlank()) {
      annotateCause(p, t);
    }

    Error.Builder eb =
        Error.newBuilder()
            .setCode(appCode)
            .setCorrelationId(requireNonNullElse(correlationId, ""))
            .putAllParams(p);

    if (messageKey != null && !messageKey.isBlank()) {
      eb.setMessageKey(messageKey);
    }

    String resolvedMessage = resolveStatusMessage(eb, canonical, t, correlationId);
    eb.setMessage(resolvedMessage);

    Status.Builder statusBuilder =
        Status.newBuilder()
            .setCode(canonical.getCode().value())
            .setMessage(resolvedMessage)
            .addDetails(Any.pack(eb.build()));

    addBadRequestDetail(statusBuilder, canonical, p, resolvedMessage);
    statusBuilder.addDetails(Any.pack(buildErrorInfo(canonical, appCode, correlationId)));

    if (debugDetailsEnabled() && t != null) {
      statusBuilder.addDetails(Any.pack(buildDebugInfo(t)));
    }

    Status st = statusBuilder.build();
    StatusRuntimeException ex = StatusProto.toStatusRuntimeException(st);
    return ex;
  }

  private static final Locale DEFAULT_LOCALE = Locale.ENGLISH;
  private static final MessageCatalog DEFAULT_MESSAGE_CATALOG = new MessageCatalog(DEFAULT_LOCALE);
  private static final String ERROR_DOMAIN = "ai.floedb.floecat";

  private static boolean fetchDebugDetailsFlag() {
    try {
      return ConfigProvider.getConfig()
          .getOptionalValue("floecat.errors.debug-details", Boolean.class)
          .orElse(false);
    } catch (Throwable e) {
      return false;
    }
  }

  private static boolean debugDetailsEnabled() {
    return fetchDebugDetailsFlag();
  }

  private static void annotateCause(Map<String, String> params, Throwable t) {
    if (t != null) {
      Throwable root = rootCause(t);
      params.put("error_class", t.getClass().getName());
      params.put("root_class", root.getClass().getName());
      params.put("cause", root.getClass().getSimpleName());
      String m = root.getMessage();
      if (m != null && !m.isBlank()) {
        params.putIfAbsent("root_message", m);
      }
      return;
    }

    String inferred =
        params.getOrDefault("error_class", params.getOrDefault("root_class", "unknown"));
    String simple =
        inferred.equals("unknown") ? "unknown" : inferred.substring(inferred.lastIndexOf('.') + 1);
    params.putIfAbsent("cause", simple);
  }

  private static Throwable rootCause(Throwable t) {
    Throwable root = t;
    while (root != null && root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return root == null ? t : root;
  }

  private static String resolveStatusMessage(
      Error.Builder errorBuilder, io.grpc.Status canonical, Throwable t, String correlationId) {
    if (shouldHideMessage(canonical) && !debugDetailsEnabled()) {
      return safeInternalMessage(correlationId);
    }
    String renderedMessage =
        renderCatalogMessage(
            errorBuilder.getCode(), errorBuilder.getMessageKey(), errorBuilder.getParamsMap());
    if (!renderedMessage.isBlank()) {
      return renderedMessage;
    }
    String messageKey = errorBuilder.getMessageKey();
    if (messageKey != null && !messageKey.isBlank()) {
      return messageKey;
    }
    if (rootCauseAllowed(canonical)) {
      String causeMessage = rootCauseMessage(t);
      if (!causeMessage.isBlank()) {
        return causeMessage;
      }
    }
    String codeName = errorBuilder.getCode().name();
    if (codeName != null && !codeName.isBlank()) {
      return codeName;
    }
    return canonical.getCode().name();
  }

  private static String renderCatalogMessage(
      ErrorCode code, String messageKey, Map<String, String> params) {
    try {
      Error.Builder preview =
          Error.newBuilder().setCode(code).setCorrelationId("").putAllParams(params);
      if (messageKey != null && !messageKey.isBlank()) {
        preview.setMessageKey(messageKey);
      }
      return DEFAULT_MESSAGE_CATALOG.render(preview.build());
    } catch (MissingResourceException mre) {
      return "";
    } catch (Throwable e) {
      return "";
    }
  }

  private static boolean shouldHideMessage(io.grpc.Status canonical) {
    return switch (canonical.getCode()) {
      case INVALID_ARGUMENT,
          NOT_FOUND,
          FAILED_PRECONDITION,
          ALREADY_EXISTS,
          PERMISSION_DENIED,
          UNAUTHENTICATED,
          RESOURCE_EXHAUSTED,
          UNAVAILABLE,
          DEADLINE_EXCEEDED,
          ABORTED,
          CANCELLED ->
          false;
      default -> true;
    };
  }

  private static boolean rootCauseAllowed(io.grpc.Status canonical) {
    return switch (canonical.getCode()) {
      case INVALID_ARGUMENT, FAILED_PRECONDITION, NOT_FOUND -> true;
      default -> false;
    };
  }

  private static String rootCauseMessage(Throwable t) {
    if (t == null) {
      return "";
    }
    Throwable root = rootCause(t);
    String msg = root.getMessage();
    return msg == null ? "" : msg;
  }

  private static String safeInternalMessage(String corrId) {
    if (corrId == null || corrId.isBlank()) {
      return "Internal error.";
    }
    return "Internal error. correlation_id=" + corrId;
  }

  private static void addBadRequestDetail(
      Status.Builder builder,
      io.grpc.Status canonical,
      Map<String, String> params,
      String message) {
    if (canonical.getCode() != io.grpc.Status.Code.INVALID_ARGUMENT) {
      return;
    }
    String field = params.getOrDefault("field", "").trim();
    if (field.isEmpty()) {
      field = params.getOrDefault("header", "").trim();
    }
    if (field.isEmpty()) {
      return;
    }
    BadRequest.FieldViolation fv =
        BadRequest.FieldViolation.newBuilder().setField(field).setDescription(message).build();
    BadRequest badRequest = BadRequest.newBuilder().addFieldViolations(fv).build();
    builder.addDetails(Any.pack(badRequest));
  }

  private static ErrorInfo buildErrorInfo(
      io.grpc.Status canonical, ErrorCode appCode, String correlationId) {
    return ErrorInfo.newBuilder()
        .setReason(appCode.name())
        .setDomain(ERROR_DOMAIN)
        .putMetadata("correlation_id", requireNonNullElse(correlationId, ""))
        .putMetadata("grpc_status", canonical.getCode().name())
        .build();
  }

  private static DebugInfo buildDebugInfo(Throwable t) {
    Throwable root = rootCause(t);
    DebugInfo.Builder builder =
        DebugInfo.newBuilder().setDetail(root.getClass().getName() + ": " + safeRootMessage(root));
    StackTraceElement[] stackTrace = root.getStackTrace();
    int limit = Math.min(stackTrace.length, 5);
    for (int i = 0; i < limit; i++) {
      builder.addStackEntries(stackTrace[i].toString());
    }
    return builder.build();
  }

  private static String safeRootMessage(Throwable root) {
    String msg = root.getMessage();
    return msg == null ? "" : msg;
  }
}
