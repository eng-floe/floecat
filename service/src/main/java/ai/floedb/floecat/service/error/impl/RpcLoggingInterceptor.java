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
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.protobuf.StatusProto;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

@ApplicationScoped
@GlobalInterceptor
@Priority(5)
public class RpcLoggingInterceptor implements ServerInterceptor {
  private static final Logger LOG = Logger.getLogger(RpcLoggingInterceptor.class);
  private static final String MISSING = "-";

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    final long startNanos = System.nanoTime();
    final String method = call.getMethodDescriptor().getFullMethodName();

    String contextCorrelationId = InboundContextInterceptor.CORR_KEY.get();
    String headerCorrelationId = headers.get(CORRELATION_ID_KEY);
    final String correlationId =
        nonBlank(contextCorrelationId)
            ? contextCorrelationId
            : nonBlank(headerCorrelationId) ? headerCorrelationId : "";
    final String logCorrelationId = nonBlank(correlationId) ? correlationId : MISSING;

    ServerCall<ReqT, RespT> forwarding =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void close(io.grpc.Status status, Metadata trailers) {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            Metadata nextTrailers = new Metadata();
            nextTrailers.merge(trailers);
            if (nonBlank(correlationId)) {
              nextTrailers.put(CORRELATION_ID_KEY, correlationId);
            }
            logCall(status, nextTrailers, method, durationMs, logCorrelationId);
            super.close(status, nextTrailers);
          }
        };

    return next.startCall(forwarding, headers);
  }

  private static final Metadata.Key<String> CORRELATION_ID_KEY =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private void logCall(
      io.grpc.Status status,
      Metadata trailers,
      String method,
      long durationMs,
      String logCorrelationId) {

    Status statusProto = StatusProto.fromStatusAndTrailers(status, trailers);
    Error floecatError = extractFloecatError(statusProto);

    String appCode = floecatError != null ? floecatError.getCode().name() : MISSING;
    String messageKey =
        floecatError != null && !floecatError.getMessageKey().isBlank()
            ? floecatError.getMessageKey()
            : MISSING;

    String params =
        floecatError == null || floecatError.getParamsMap().isEmpty()
            ? ""
            : floecatError.getParamsMap().toString();
    String statusMessage = statusProto == null ? "" : statusProto.getMessage();

    String logMessage =
        "rpc method="
            + method
            + " status="
            + status.getCode().name()
            + " app_code="
            + appCode
            + " message_key="
            + messageKey
            + " correlation_id="
            + logCorrelationId
            + " duration_ms="
            + durationMs;

    if (!params.isBlank()) {
      logMessage += " params=" + params;
    }

    boolean floecatMessageLogged = false;
    if (!status.isOk() && floecatError != null && !floecatError.getMessage().isBlank()) {
      logMessage += " floecat_message=" + floecatError.getMessage();
      floecatMessageLogged = true;
    }

    if (!statusMessage.isBlank() && (!floecatMessageLogged || floecatError == null)) {
      logMessage += " rpc_message=" + statusMessage;
    }

    if (status.isOk()) {
      LOG.info(logMessage);
    } else {
      LOG.error(logMessage);
    }
  }

  private static Error extractFloecatError(Status statusProto) {
    if (statusProto == null) {
      return null;
    }

    for (Any detail : statusProto.getDetailsList()) {
      if (detail.is(Error.class)) {
        try {
          return detail.unpack(Error.class);
        } catch (Exception e) {
          LOG.debug("Failed to unpack Floecat error detail", e);
        }
        return null;
      }
    }
    return null;
  }

  private static boolean nonBlank(String value) {
    return value != null && !value.isBlank();
  }
}
