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
package ai.floedb.floecat.client.cli.util;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.function.Supplier;

/** gRPC client interceptor that adds auth/session headers to outgoing calls. */
public final class AuthHeaderInterceptor implements ClientInterceptor {
  private final Supplier<String> tokenSupplier;
  private final Supplier<String> sessionSupplier;
  private final Supplier<String> accountSupplier;
  private final String authHeaderName;
  private final String sessionHeaderName;
  private final String accountHeaderName;

  public AuthHeaderInterceptor(
      Supplier<String> tokenSupplier,
      Supplier<String> sessionSupplier,
      Supplier<String> accountSupplier,
      String authHeaderName,
      String sessionHeaderName,
      String accountHeaderName) {
    this.tokenSupplier = tokenSupplier;
    this.sessionSupplier = sessionSupplier;
    this.accountSupplier = accountSupplier;
    this.authHeaderName = authHeaderName;
    this.sessionHeaderName = sessionHeaderName;
    this.accountHeaderName = accountHeaderName;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, io.grpc.Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        String token = tokenSupplier.get();
        if (!token.isBlank()) {
          String headerValue = token;
          if (!token.regionMatches(true, 0, "bearer ", 0, 7)) {
            headerValue = "Bearer " + token;
          }
          headers.put(
              Metadata.Key.of(authHeaderName, Metadata.ASCII_STRING_MARSHALLER), headerValue);
        }

        String session = sessionSupplier.get();
        if (!session.isBlank()) {
          headers.put(
              Metadata.Key.of(sessionHeaderName, Metadata.ASCII_STRING_MARSHALLER), session);
        }

        String account = accountSupplier.get();
        if (!account.isBlank()) {
          headers.put(
              Metadata.Key.of(accountHeaderName, Metadata.ASCII_STRING_MARSHALLER), account);
        }

        super.start(responseListener, headers);
      }
    };
  }
}
