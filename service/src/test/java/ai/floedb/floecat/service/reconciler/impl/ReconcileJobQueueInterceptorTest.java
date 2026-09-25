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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ReconcileJobQueueInterceptorTest {
  @Test
  void rejectsCaptureAdmissionWhenDisabled() {
    assertBlocked(ReconcileControlGrpc.getCaptureNowMethod());
    assertBlocked(ReconcileControlGrpc.getStartCaptureMethod());
  }

  @Test
  void rejectsExecutorRpcWhenDisabled() {
    assertBlocked(ReconcileExecutorControlGrpc.getLeaseReconcileJobMethod());
  }

  private static void assertBlocked(MethodDescriptor<?, ?> methodDescriptor) {
    var call = callFor(methodDescriptor);
    @SuppressWarnings("unchecked")
    ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);

    new ReconcileJobQueueInterceptor(false).interceptCall(call, new Metadata(), next);

    ArgumentCaptor<Status> status = ArgumentCaptor.forClass(Status.class);
    verify(call).close(status.capture(), any(Metadata.class));
    assertEquals(Status.Code.FAILED_PRECONDITION, status.getValue().getCode());
    verify(next, never()).startCall(any(), any());
  }

  @Test
  void forwardsReadAndSettingsRpcsWhenDisabled() {
    assertForwarded(false, ReconcileControlGrpc.getListReconcileJobsMethod());
    assertForwarded(false, ReconcileControlGrpc.getGetReconcilerSettingsMethod());
    assertForwarded(false, ReconcileControlGrpc.getUpdateReconcilerSettingsMethod());
  }

  @Test
  void forwardsCaptureAdmissionWhenEnabled() {
    assertForwarded(true, ReconcileControlGrpc.getStartCaptureMethod());
  }

  private static void assertForwarded(boolean enabled, MethodDescriptor<?, ?> methodDescriptor) {
    var call = callFor(methodDescriptor);
    @SuppressWarnings("unchecked")
    ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
    ServerCall.Listener<Object> expected = new ServerCall.Listener<>() {};
    when(next.startCall(any(), any())).thenReturn(expected);

    ServerCall.Listener<Object> actual =
        new ReconcileJobQueueInterceptor(enabled).interceptCall(call, new Metadata(), next);

    assertSame(expected, actual);
    verify(call, never()).close(any(), any());
  }

  @SuppressWarnings("unchecked")
  private static ServerCall<Object, Object> callFor(MethodDescriptor<?, ?> methodDescriptor) {
    ServerCall<Object, Object> call = mock(ServerCall.class);
    when(call.getMethodDescriptor())
        .thenReturn((MethodDescriptor<Object, Object>) methodDescriptor);
    return call;
  }
}
