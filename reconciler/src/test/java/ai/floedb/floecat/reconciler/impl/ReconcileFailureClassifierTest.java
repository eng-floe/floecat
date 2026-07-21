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

import ai.floedb.floecat.connector.common.auth.TerminalCredentialRefreshException;
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
}
