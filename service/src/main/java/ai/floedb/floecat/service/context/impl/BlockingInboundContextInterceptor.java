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

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import io.vertx.core.Vertx;
import io.vertx.grpc.BlockingServerInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
@GlobalInterceptor
@Priority(1)
public class BlockingInboundContextInterceptor implements ServerInterceptor {
  private final ServerInterceptor delegate;

  @Inject
  public BlockingInboundContextInterceptor(Vertx vertx, InboundContextInterceptor actual) {
    // Wrap actual interceptor with BlockingServerInterceptor; delegate all activity to it
    this.delegate = BlockingServerInterceptor.wrap(vertx, actual);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    return this.delegate.interceptCall(call, headers, next);
  }
}
