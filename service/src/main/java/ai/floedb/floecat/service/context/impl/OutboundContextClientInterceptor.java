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

import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
@GlobalInterceptor
public class OutboundContextClientInterceptor implements io.grpc.ClientInterceptor {
  private static final Metadata.Key<String> QUERY_ID =
      Metadata.Key.of("x-query-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_KIND =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_VERSION =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private final Optional<String> sessionHeader;
  private final Optional<String> authorizationHeader;

  public OutboundContextClientInterceptor(
      @ConfigProperty(name = "floecat.interceptor.session.header") Optional<String> sessionHeader,
      @ConfigProperty(name = "floecat.interceptor.authorization.header")
          Optional<String> authorizationHeader) {
    this.sessionHeader = sessionHeader.filter(header -> !header.isBlank());
    this.authorizationHeader = authorizationHeader.filter(header -> !header.isBlank());
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        // Prefer the resolved-call-context carrier: the io.grpc.Context keys alone are unreliable
        // on executor threads, which used to silently drop engine/correlation metadata from
        // outbound RPCs (eng-floe/floecat#361). Once the carrier is present it is trusted
        // entirely — falling back per-field would let a reused worker's stale io.grpc.Context
        // keys leak a previous call's values into a request that legitimately has none.
        ResolvedCallContext resolved = ResolvedCallContexts.currentOrNull();

        String sessionHeaderValue;
        String authorizationHeaderValue;
        String queryId;
        String engineKind;
        String engineVersion;
        String correlationId;
        if (resolved != null) {
          sessionHeaderValue = resolved.sessionHeaderValue();
          authorizationHeaderValue = resolved.authorizationHeaderValue();
          queryId = resolved.queryId();
          EngineContext engineContext = resolved.engineContext();
          engineKind = engineContext.hasEngineKind() ? engineContext.engineKind() : null;
          engineVersion = engineContext.engineVersion();
          correlationId = resolved.correlationId();
        } else {
          sessionHeaderValue = InboundContextInterceptor.SESSION_HEADER_VALUE_KEY.get();
          authorizationHeaderValue = InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY.get();
          queryId =
              Optional.ofNullable(InboundContextInterceptor.QUERY_KEY.get())
                  .orElseGet(() -> Baggage.current().getEntryValue("query_id"));
          EngineContext engineContext = InboundContextInterceptor.ENGINE_CONTEXT_KEY.get();
          engineKind =
              firstNonBlank(
                  engineContext != null && engineContext.hasEngineKind()
                      ? engineContext.engineKind()
                      : null,
                  InboundContextInterceptor.ENGINE_KIND_KEY.get(),
                  Baggage.current().getEntryValue("engine_kind"));
          engineVersion =
              firstNonBlank(
                  engineContext != null ? engineContext.engineVersion() : null,
                  InboundContextInterceptor.ENGINE_VERSION_KEY.get(),
                  Baggage.current().getEntryValue("engine_version"));
          correlationId =
              Optional.ofNullable(InboundContextInterceptor.CORR_KEY.get())
                  .orElseGet(() -> Baggage.current().getEntryValue("correlation_id"));
        }
        // Only propagate an engine version if we are propagating an engine kind.
        if (engineKind == null || engineKind.isBlank()) {
          engineVersion = null;
        }

        if (sessionHeaderValue != null && sessionHeader.isPresent()) {
          Metadata.Key<String> key =
              Metadata.Key.of(sessionHeader.orElseThrow(), Metadata.ASCII_STRING_MARSHALLER);
          headers.put(key, sessionHeaderValue);
        }
        if (authorizationHeaderValue != null
            && authorizationHeader.isPresent()
            && !hasAuthorizationHeader(headers)) {
          Metadata.Key<String> key =
              Metadata.Key.of(authorizationHeader.orElseThrow(), Metadata.ASCII_STRING_MARSHALLER);
          headers.put(key, authorizationHeaderValue);
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

  private boolean hasAuthorizationHeader(Metadata headers) {
    if (authorizationHeader.isEmpty()) {
      return false;
    }
    Metadata.Key<String> key =
        Metadata.Key.of(authorizationHeader.orElseThrow(), Metadata.ASCII_STRING_MARSHALLER);
    return headers.containsKey(key);
  }
}
