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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.service.context.impl.InboundCallContextHelper;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RpcLoggingInterceptorTest {

  private static final MethodDescriptor<GetUserObjectsRequest, UserObjectsBundleChunk> METHOD =
      MethodDescriptor.<GetUserObjectsRequest, UserObjectsBundleChunk>newBuilder()
          .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName("ai.floedb.floecat.query.UserObjectsService/GetUserObjects")
          .setRequestMarshaller(ProtoUtils.marshaller(GetUserObjectsRequest.getDefaultInstance()))
          .setResponseMarshaller(ProtoUtils.marshaller(UserObjectsBundleChunk.getDefaultInstance()))
          .build();

  private static final Metadata.Key<String> QUERY_ID_HEADER =
      Metadata.Key.of(InboundCallContextHelper.HEADER_QUERY_ID, Metadata.ASCII_STRING_MARSHALLER);

  @Test
  void capturesQueryIdFromRequestMessage() {
    CapturingRpcLoggingInterceptor interceptor = new CapturingRpcLoggingInterceptor();
    AtomicReference<ServerCall<GetUserObjectsRequest, UserObjectsBundleChunk>> forwardedCall =
        new AtomicReference<>();
    RecordingServerCall<GetUserObjectsRequest, UserObjectsBundleChunk> underlying =
        new RecordingServerCall<>(METHOD);

    Context previous = Context.ROOT.attach();
    try {
      var listener =
          interceptor.interceptCall(
              underlying,
              new Metadata(),
              captureHandler(forwardedCall, new ServerCall.Listener<>() {}));
      listener.onMessage(GetUserObjectsRequest.newBuilder().setQueryId("req-query-id").build());
      forwardedCall.get().close(Status.OK, new Metadata());
    } finally {
      Context.ROOT.detach(previous);
    }

    assertEquals("req-query-id", interceptor.loggedQueryId());
  }

  @Test
  void capturesQueryIdFromResponseMessageWhenRequestDoesNotCarryIt() {
    CapturingRpcLoggingInterceptor interceptor = new CapturingRpcLoggingInterceptor();
    AtomicReference<ServerCall<GetUserObjectsRequest, UserObjectsBundleChunk>> forwardedCall =
        new AtomicReference<>();
    RecordingServerCall<GetUserObjectsRequest, UserObjectsBundleChunk> underlying =
        new RecordingServerCall<>(METHOD);

    Context previous = Context.ROOT.attach();
    try {
      var listener =
          interceptor.interceptCall(
              underlying,
              new Metadata(),
              captureHandler(forwardedCall, new ServerCall.Listener<>() {}));
      listener.onMessage(GetUserObjectsRequest.getDefaultInstance());
      forwardedCall
          .get()
          .sendMessage(UserObjectsBundleChunk.newBuilder().setQueryId("resp-query-id").build());
      forwardedCall.get().close(Status.OK, new Metadata());
    } finally {
      Context.ROOT.detach(previous);
    }

    assertEquals("resp-query-id", interceptor.loggedQueryId());
  }

  @Test
  void fallsBackToHeaderQueryIdWhenContextAndMessagesDoNotProvideOne() {
    CapturingRpcLoggingInterceptor interceptor = new CapturingRpcLoggingInterceptor();
    AtomicReference<ServerCall<GetUserObjectsRequest, UserObjectsBundleChunk>> forwardedCall =
        new AtomicReference<>();
    RecordingServerCall<GetUserObjectsRequest, UserObjectsBundleChunk> underlying =
        new RecordingServerCall<>(METHOD);
    Metadata headers = new Metadata();
    headers.put(QUERY_ID_HEADER, "header-query-id");

    Context previous = Context.ROOT.attach();
    try {
      var listener =
          interceptor.interceptCall(
              underlying, headers, captureHandler(forwardedCall, new ServerCall.Listener<>() {}));
      listener.onMessage(GetUserObjectsRequest.getDefaultInstance());
      forwardedCall.get().close(Status.OK, new Metadata());
    } finally {
      Context.ROOT.detach(previous);
    }

    assertEquals("header-query-id", interceptor.loggedQueryId());
  }

  @Test
  void contextQueryIdTakesPrecedenceOverHeaderQueryId() {
    CapturingRpcLoggingInterceptor interceptor = new CapturingRpcLoggingInterceptor();
    AtomicReference<ServerCall<GetUserObjectsRequest, UserObjectsBundleChunk>> forwardedCall =
        new AtomicReference<>();
    RecordingServerCall<GetUserObjectsRequest, UserObjectsBundleChunk> underlying =
        new RecordingServerCall<>(METHOD);
    Metadata headers = new Metadata();
    headers.put(QUERY_ID_HEADER, "header-query-id");

    Context context = Context.current().withValue(InboundContextInterceptor.QUERY_KEY, "ctx-query");
    Context previous = context.attach();
    try {
      var listener =
          interceptor.interceptCall(
              underlying, headers, captureHandler(forwardedCall, new ServerCall.Listener<>() {}));
      listener.onMessage(GetUserObjectsRequest.getDefaultInstance());
      forwardedCall.get().close(Status.OK, new Metadata());
    } finally {
      context.detach(previous);
    }

    assertEquals("ctx-query", interceptor.loggedQueryId());
  }

  @Test
  void extractQueryIdSupportsNonProtobufGetter() {
    assertEquals("getter-query-id", RpcLoggingInterceptor.extractQueryId(new GetterOnly("getter-query-id")));
    assertEquals("", RpcLoggingInterceptor.extractQueryId(new Object()));
  }

  private static <ReqT, RespT> ServerCallHandler<ReqT, RespT> captureHandler(
      AtomicReference<ServerCall<ReqT, RespT>> forwardedCall, ServerCall.Listener<ReqT> delegate) {
    return new ServerCallHandler<>() {
      @Override
      public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
        forwardedCall.set(call);
        return delegate;
      }
    };
  }

  private static final class RecordingServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
    private final MethodDescriptor<ReqT, RespT> method;

    private RecordingServerCall(MethodDescriptor<ReqT, RespT> method) {
      this.method = method;
    }

    @Override
    public void request(int numMessages) {}

    @Override
    public void sendHeaders(Metadata headers) {}

    @Override
    public void sendMessage(RespT message) {}

    @Override
    public void close(Status status, Metadata trailers) {}

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
      return method;
    }
  }

  private static final class CapturingRpcLoggingInterceptor extends RpcLoggingInterceptor {
    private String loggedQueryId = "";

    @Override
    void logCall(
        Status status,
        Metadata trailers,
        String method,
        long durationMs,
        String logCorrelationId,
        String logQueryId) {
      this.loggedQueryId = logQueryId;
    }

    private String loggedQueryId() {
      return loggedQueryId;
    }
  }

  private record GetterOnly(String queryId) {
    public String getQueryId() {
      return queryId;
    }
  }
}
