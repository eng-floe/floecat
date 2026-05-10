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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.junit.jupiter.api.Test;

class ReconcileFailureClassifierTest {

  @Test
  void normalizeMarksIcebergForbiddenTerminal() {
    Exception normalized =
        ReconcileFailureClassifier.normalize(new ForbiddenException("Forbidden: invalid token"));

    assertThat(normalized).isInstanceOf(ReconcileFailureException.class);
    assertThat(((ReconcileFailureException) normalized).retryDisposition())
        .isEqualTo(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL);
  }

  @Test
  void normalizeMarksIcebergUnauthorizedTerminal() {
    Exception normalized =
        ReconcileFailureClassifier.normalize(
            new NotAuthorizedException("Not authorized: expired token"));

    assertThat(normalized).isInstanceOf(ReconcileFailureException.class);
    assertThat(((ReconcileFailureException) normalized).retryDisposition())
        .isEqualTo(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL);
  }

  @Test
  void normalizeMarksNestedIcebergForbiddenTerminal() {
    Exception normalized =
        ReconcileFailureClassifier.normalize(
            new RuntimeException(new ForbiddenException("Forbidden: invalid token")));

    assertThat(normalized).isInstanceOf(ReconcileFailureException.class);
    assertThat(((ReconcileFailureException) normalized).retryDisposition())
        .isEqualTo(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL);
  }
}
