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

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import com.google.protobuf.Any;
import com.google.rpc.DebugInfo;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class FloecatStatus {
  private final io.grpc.Status.Code canonicalCode;
  private final ErrorCode errorCode;
  private final String correlationId;
  private final String messageKey;
  private final String message;
  private final Map<String, String> params;
  private final DebugInfo debugInfo;

  private FloecatStatus(
      io.grpc.Status.Code canonicalCode,
      ErrorCode errorCode,
      String correlationId,
      String messageKey,
      String message,
      Map<String, String> params,
      DebugInfo debugInfo) {
    this.canonicalCode = canonicalCode;
    this.errorCode = errorCode;
    this.correlationId = correlationId;
    this.messageKey = messageKey;
    this.message = message;
    this.params = Map.copyOf(params);
    this.debugInfo = debugInfo;
  }

  public static FloecatStatus fromThrowable(Throwable throwable) {
    if (throwable == null) {
      return null;
    }

    Status statusProto = StatusProto.fromThrowable(throwable);
    if (statusProto == null) {
      return null;
    }

    Error errorDetail = extractError(statusProto);
    DebugInfo debugInfo = extractDebugInfo(statusProto);
    Map<String, String> params =
        errorDetail != null
            ? new LinkedHashMap<>(errorDetail.getParamsMap())
            : new LinkedHashMap<>();
    ErrorCode code = errorDetail != null ? errorDetail.getCode() : ErrorCode.MC_INTERNAL;
    String messageKey = errorDetail != null ? errorDetail.getMessageKey() : "";
    String correlationId = errorDetail != null ? errorDetail.getCorrelationId() : "";
    String message =
        errorDetail != null && !errorDetail.getMessage().isBlank()
            ? errorDetail.getMessage()
            : statusProto.getMessage();
    return new FloecatStatus(
        io.grpc.Status.fromCodeValue(statusProto.getCode()).getCode(),
        code,
        correlationId,
        messageKey,
        message,
        params,
        debugInfo);
  }

  private static Error extractError(Status statusProto) {
    for (Any detail : statusProto.getDetailsList()) {
      if (detail.is(Error.class)) {
        try {
          return detail.unpack(Error.class);
        } catch (Exception e) {
          return null;
        }
      }
    }
    return null;
  }

  private static DebugInfo extractDebugInfo(Status statusProto) {
    for (Any detail : statusProto.getDetailsList()) {
      if (detail.is(DebugInfo.class)) {
        try {
          return detail.unpack(DebugInfo.class);
        } catch (Exception e) {
          return null;
        }
      }
    }
    return null;
  }

  public io.grpc.Status.Code canonicalCode() {
    return canonicalCode;
  }

  public ErrorCode errorCode() {
    return errorCode;
  }

  public String correlationId() {
    return correlationId;
  }

  public String messageKey() {
    return messageKey;
  }

  public String message() {
    return message;
  }

  public Map<String, String> params() {
    return params;
  }

  public DebugInfo debugInfo() {
    return debugInfo;
  }

  public boolean hasDebugInfo() {
    return debugInfo != null;
  }

  @Override
  public int hashCode() {
    return Objects.hash(canonicalCode, errorCode, correlationId, messageKey, message);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof FloecatStatus other)) {
      return false;
    }

    return canonicalCode == other.canonicalCode
        && errorCode == other.errorCode
        && Objects.equals(correlationId, other.correlationId)
        && Objects.equals(messageKey, other.messageKey)
        && Objects.equals(message, other.message)
        && Objects.equals(params, other.params)
        && Objects.equals(debugInfo, other.debugInfo);
  }
}
