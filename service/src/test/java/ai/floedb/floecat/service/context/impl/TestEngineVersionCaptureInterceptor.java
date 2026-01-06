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

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only server interceptor that captures the raw `x-engine-version` header seen by inbound gRPC
 * calls so integration tests can assert propagation.
 */
@ApplicationScoped
@GlobalInterceptor
@Priority(1000)
public class TestEngineVersionCaptureInterceptor implements ServerInterceptor {
  private static final AtomicReference<String> CAPTURED = new AtomicReference<>();

  public static void reset() {
    CAPTURED.set(null);
  }

  public static String captured() {
    return CAPTURED.get();
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Metadata.Key<String> ENGINE_VERSION =
        Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);
    CAPTURED.set(headers.get(ENGINE_VERSION));
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
        next.startCall(
            new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
              @Override
              public void sendHeaders(Metadata responseHeaders) {
                super.sendHeaders(responseHeaders);
              }
            },
            headers)) {};
  }
}
