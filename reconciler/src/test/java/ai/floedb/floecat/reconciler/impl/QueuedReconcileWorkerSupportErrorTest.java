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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

class QueuedReconcileWorkerSupportErrorTest {
  private static final String MISSING_AUTHORITY =
      "Credential vending was requested but no storage credential authority is configured for this table";

  @Test
  void safeFloecatErrorUsesStructuredRootMessageAndIsTerminal() {
    Error detail =
        Error.newBuilder()
            .setCode(ErrorCode.MC_PRECONDITION_FAILED)
            .setMessageKey("storage.authority.missing")
            .putParams("root_message", MISSING_AUTHORITY)
            .build();
    StatusRuntimeException failure =
        StatusProto.toStatusRuntimeException(
            com.google.rpc.Status.newBuilder()
                .setCode(Status.Code.FAILED_PRECONDITION.value())
                .setMessage("Precondition failed.")
                .addDetails(Any.pack(detail))
                .build());

    assertEquals(
        "grpc=FAILED_PRECONDITION desc=" + MISSING_AUTHORITY,
        QueuedReconcileWorkerSupport.rootCauseMessage(failure));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        QueuedReconcileWorkerSupport.retryDispositionOf(failure));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryClass.NONE,
        QueuedReconcileWorkerSupport.retryClassOf(failure));
  }

  @Test
  void foreignGrpcErrorKeepsDescriptionFallback() {
    StatusRuntimeException failure =
        Status.FAILED_PRECONDITION
            .withDescription("foreign precondition detail")
            .asRuntimeException();

    assertEquals(
        "grpc=FAILED_PRECONDITION desc=foreign precondition detail",
        QueuedReconcileWorkerSupport.rootCauseMessage(failure));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
        QueuedReconcileWorkerSupport.retryDispositionOf(failure));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
        QueuedReconcileWorkerSupport.retryClassOf(failure));
  }

  @Test
  void snapshotNotFinalizedIsDependencyNotReady() {
    Error detail =
        Error.newBuilder()
            .setCode(ErrorCode.MC_PRECONDITION_FAILED)
            .setMessageKey("query.snapshot.not.finalized")
            .build();
    StatusRuntimeException failure =
        StatusProto.toStatusRuntimeException(
            com.google.rpc.Status.newBuilder()
                .setCode(Status.Code.FAILED_PRECONDITION.value())
                .setMessage("Snapshot is not finalized.")
                .addDetails(Any.pack(detail))
                .build());

    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
        QueuedReconcileWorkerSupport.retryDispositionOf(failure));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryClass.DEPENDENCY_NOT_READY,
        QueuedReconcileWorkerSupport.retryClassOf(failure));
  }

  @Test
  void invalidArgumentPlannerFailureIsTerminal() {
    StatusRuntimeException failure =
        Status.INVALID_ARGUMENT.withDescription("invalid planner request").asRuntimeException();

    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        QueuedReconcileWorkerSupport.retryDispositionOf(failure));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryClass.NONE,
        QueuedReconcileWorkerSupport.retryClassOf(failure));
  }
}
