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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import ai.floedb.floecat.connector.common.auth.TerminalCredentialRefreshException;
import ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus;
import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

class ReconcileFailureClassifierTest {
  @Test
  void terminalCredentialRefreshFailureStopsExecution() {
    TerminalCredentialRefreshException error =
        new TerminalCredentialRefreshException("lease lost", new IllegalStateException("stale"));

    ReconcileFailureException classified =
        assertInstanceOf(
            ReconcileFailureException.class, ReconcileFailureClassifier.normalize(error));

    assertEquals(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL, classified.failureKind());
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, classified.retryDisposition());
  }

  /**
   * The two vending refusals are matched by structured reason rather than status code, because
   * FAILED_PRECONDITION is shared with lease-precondition failures that are retryable by design.
   * That makes the arms depend on three exact strings and on hasReason walking the cause chain -- a
   * typo in any of them silently turns a terminal refusal back into one retried forever.
   */
  @Test
  void bothVendingRefusalsAreTerminal() {
    for (StatusRuntimeException refusal :
        new StatusRuntimeException[] {
          SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused("external access disabled"),
          SourceCatalogVendingGrpcStatus.vendedCredentialsNotRefreshable("no expiry")
        }) {
      ReconcileFailureException classified =
          assertInstanceOf(
              ReconcileFailureException.class,
              ReconcileFailureClassifier.terminalAuthFailure(refusal),
              refusal.getMessage());

      assertEquals(
          ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
          classified.retryDisposition(),
          refusal.getMessage());
    }
  }

  @Test
  void aVendingRefusalIsFoundThroughTheCauseChain() {
    var wrapped =
        new IllegalStateException(
            "file group failed",
            new RuntimeException(
                "vend", SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused("unknown table")));

    assertInstanceOf(
        ReconcileFailureException.class, ReconcileFailureClassifier.terminalAuthFailure(wrapped));
  }

  @Test
  void aForeignDomainOrReasonOnTheSameStatusIsNotTerminal() {
    // What a lease-precondition failure looks like: same code, no vending reason. Retryable.
    assertNull(
        ReconcileFailureClassifier.terminalAuthFailure(
            Status.FAILED_PRECONDITION.withDescription("lease expired").asRuntimeException()));

    // The right reason under someone else's domain, and the right domain with a reason that is not
    // one of ours. Both must miss.
    assertNull(
        ReconcileFailureClassifier.terminalAuthFailure(
            failedPrecondition(
                "com.example.other",
                SourceCatalogVendingGrpcStatus.SOURCE_CATALOG_VEND_REFUSED_REASON)));
    assertNull(
        ReconcileFailureClassifier.terminalAuthFailure(
            failedPrecondition(
                SourceCatalogVendingGrpcStatus.ERROR_DOMAIN, "SOMETHING_ELSE_ENTIRELY")));
  }

  private static StatusRuntimeException failedPrecondition(String domain, String reason) {
    return StatusProto.toStatusRuntimeException(
        com.google.rpc.Status.newBuilder()
            .setCode(Status.Code.FAILED_PRECONDITION.value())
            .setMessage("refused")
            .addDetails(
                Any.pack(ErrorInfo.newBuilder().setDomain(domain).setReason(reason).build()))
            .build());
  }
}
