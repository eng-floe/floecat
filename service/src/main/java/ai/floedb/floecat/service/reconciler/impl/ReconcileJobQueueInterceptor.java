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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobQueue;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Rejects queue admission and executor operations when the reconcile job queue is disabled. */
@ApplicationScoped
@GlobalInterceptor
public class ReconcileJobQueueInterceptor implements ServerInterceptor {
  private static final String CAPTURE_NOW_METHOD =
      ReconcileControlGrpc.getCaptureNowMethod().getFullMethodName();
  private static final String START_CAPTURE_METHOD =
      ReconcileControlGrpc.getStartCaptureMethod().getFullMethodName();

  @ConfigProperty(name = ReconcileJobQueue.ENABLED_PROPERTY, defaultValue = "true")
  boolean jobQueueEnabled = true;

  public ReconcileJobQueueInterceptor() {}

  ReconcileJobQueueInterceptor(boolean jobQueueEnabled) {
    this.jobQueueEnabled = jobQueueEnabled;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    if (!jobQueueEnabled && isBlockedQueueOperation(call)) {
      call.close(
          Status.FAILED_PRECONDITION.withDescription("Floecat reconcile job queue is disabled"),
          new Metadata());
      return new ServerCall.Listener<>() {};
    }
    return next.startCall(call, headers);
  }

  private static boolean isBlockedQueueOperation(ServerCall<?, ?> call) {
    String serviceName = call.getMethodDescriptor().getServiceName();
    if (ReconcileExecutorControlGrpc.SERVICE_NAME.equals(serviceName)) {
      return true;
    }
    if (!ReconcileControlGrpc.SERVICE_NAME.equals(serviceName)) {
      return false;
    }
    String methodName = call.getMethodDescriptor().getFullMethodName();
    return CAPTURE_NOW_METHOD.equals(methodName) || START_CAPTURE_METHOD.equals(methodName);
  }
}
