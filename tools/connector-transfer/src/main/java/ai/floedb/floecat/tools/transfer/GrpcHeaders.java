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

package ai.floedb.floecat.tools.transfer;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

final class GrpcHeaders implements ClientInterceptor {
  private final String token;
  private final String sessionToken;
  private final String accountId;

  GrpcHeaders(String token, String sessionToken, String accountId) {
    this.token = value(token);
    this.sessionToken = value(sessionToken);
    this.accountId = value(accountId);
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions options, io.grpc.Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, options)) {
      @Override
      public void start(Listener<RespT> listener, Metadata headers) {
        if (!token.isBlank()) {
          String authorization =
              token.regionMatches(true, 0, "bearer ", 0, 7) ? token : "Bearer " + token;
          headers.put(
              Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), authorization);
        }
        if (!sessionToken.isBlank()) {
          headers.put(
              Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER), sessionToken);
        }
        if (!accountId.isBlank()) {
          headers.put(
              Metadata.Key.of("x-floe-account", Metadata.ASCII_STRING_MARSHALLER), accountId);
        }
        super.start(listener, headers);
      }
    };
  }

  private static String value(String input) {
    return input == null ? "" : input.trim();
  }
}
