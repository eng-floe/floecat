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

package ai.floedb.floecat.reconciler.impl;

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
public class ReconcilerAuthClientInterceptor implements io.grpc.ClientInterceptor {
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final Optional<String> headerName;
  private final Optional<String> staticToken;

  public ReconcilerAuthClientInterceptor(
      @ConfigProperty(name = "floecat.reconciler.authorization.header") Optional<String> headerName,
      @ConfigProperty(name = "floecat.reconciler.authorization.token")
          Optional<String> staticToken) {
    this.headerName = headerName.map(String::trim).filter(v -> !v.isBlank());
    this.staticToken = staticToken.map(String::trim).filter(v -> !v.isBlank());
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        String contextToken = ReconcilerAuthContext.AUTHORIZATION_HEADER_VALUE_KEY.get();
        String token = firstNonBlank(contextToken, staticToken.orElse(null));
        if (token != null) {
          Metadata.Key<String> key = headerKey();
          headers.put(key, withBearerPrefix(token));
        }
        super.start(responseListener, headers);
      }
    };
  }

  private Metadata.Key<String> headerKey() {
    if (headerName.isPresent() && !"authorization".equalsIgnoreCase(headerName.get())) {
      return Metadata.Key.of(headerName.get(), Metadata.ASCII_STRING_MARSHALLER);
    }
    return AUTHORIZATION;
  }

  private static String withBearerPrefix(String token) {
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      return token;
    }
    return "Bearer " + token;
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
