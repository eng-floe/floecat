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
        // The resolved-call-context carrier is the single source of outbound propagation. The
        // io.grpc.Context keys are unreliable on executor threads and can be stale on reused
        // workers, and nothing in the stack ever populates OTel baggage with these entries, so
        // the old key/baggage fallbacks could only propagate wrong values or none
        // (eng-floe/floecat#361). The carrier itself still reads the io.grpc keys as its last
        // channel, so callers that only drive io.grpc.Context (tests) keep working.
        ResolvedCallContext resolved = ResolvedCallContexts.currentOrNull();
        if (resolved == null) {
          // Machine-initiated call outside any request scope: nothing to propagate.
          super.start(responseListener, headers);
          return;
        }

        EngineContext engineContext = resolved.engineContext();
        String engineKind = engineContext.hasEngineKind() ? engineContext.engineKind() : null;
        // Only propagate an engine version if we are propagating an engine kind.
        String engineVersion =
            (engineKind == null || engineKind.isBlank()) ? null : engineContext.engineVersion();

        if (resolved.sessionHeaderValue() != null && sessionHeader.isPresent()) {
          Metadata.Key<String> key =
              Metadata.Key.of(sessionHeader.orElseThrow(), Metadata.ASCII_STRING_MARSHALLER);
          headers.put(key, resolved.sessionHeaderValue());
        }
        if (resolved.authorizationHeaderValue() != null
            && authorizationHeader.isPresent()
            && !hasAuthorizationHeader(headers)) {
          Metadata.Key<String> key =
              Metadata.Key.of(authorizationHeader.orElseThrow(), Metadata.ASCII_STRING_MARSHALLER);
          headers.put(key, resolved.authorizationHeaderValue());
        }

        putIfNonBlank(headers, QUERY_ID, resolved.queryId());
        putIfNonBlank(headers, ENGINE_KIND, engineKind);
        putIfNonBlank(headers, ENGINE_VERSION, engineVersion);
        putIfNonBlank(headers, CORR, resolved.correlationId());
        super.start(responseListener, headers);
      }
    };
  }

  private static void putIfNonBlank(Metadata headers, Metadata.Key<String> key, String value) {
    if (value != null && !value.isBlank()) {
      headers.put(key, value);
    }
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
