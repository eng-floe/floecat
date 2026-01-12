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

package ai.floedb.floecat.service.context.impl;

import ai.floedb.floecat.systemcatalog.util.EngineContext;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.opentelemetry.api.baggage.Baggage;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
@GlobalInterceptor
public class OutboundContextClientInterceptor implements io.grpc.ClientInterceptor {
  private static final Metadata.Key<byte[]> PRINC_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
  private static final Metadata.Key<String> QUERY_ID =
      Metadata.Key.of("x-query-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_KIND =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_VERSION =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        var principalContext = InboundContextInterceptor.PC_KEY.get();
        var queryId =
            Optional.ofNullable(InboundContextInterceptor.QUERY_KEY.get())
                .orElseGet(() -> Baggage.current().getEntryValue("query_id"));

        EngineContext engineContext = InboundContextInterceptor.ENGINE_CONTEXT_KEY.get();

        String engineKind =
            firstNonBlank(
                engineContext != null && engineContext.hasEngineKind()
                    ? engineContext.engineKind()
                    : null,
                InboundContextInterceptor.ENGINE_KIND_KEY.get(),
                Baggage.current().getEntryValue("engine_kind"));

        // Only propagate an engine version if we are propagating an engine kind.
        String engineVersion =
            (engineKind == null || engineKind.isBlank())
                ? null
                : firstNonBlank(
                    engineContext != null ? engineContext.engineVersion() : null,
                    InboundContextInterceptor.ENGINE_VERSION_KEY.get(),
                    Baggage.current().getEntryValue("engine_version"));

        var correlationId =
            Optional.ofNullable(InboundContextInterceptor.CORR_KEY.get())
                .orElseGet(() -> Baggage.current().getEntryValue("correlation_id"));

        if (principalContext != null) {
          headers.put(PRINC_BIN, principalContext.toByteArray());
        }

        putIfNonBlank(headers, QUERY_ID, queryId);
        putIfNonBlank(headers, ENGINE_KIND, engineKind);
        putIfNonBlank(headers, ENGINE_VERSION, engineVersion);
        putIfNonBlank(headers, CORR, correlationId);
        super.start(responseListener, headers);
      }
    };
  }

  private static void putIfNonBlank(Metadata headers, Metadata.Key<String> key, String value) {
    if (value != null && !value.isBlank()) {
      headers.put(key, value);
    }
  }

  private static String firstNonBlank(String... candidates) {
    for (String candidate : candidates) {
      if (candidate != null && !candidate.isBlank()) {
        return candidate;
      }
    }
    return null;
  }
}
