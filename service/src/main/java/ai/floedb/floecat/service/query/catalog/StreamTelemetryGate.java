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

package ai.floedb.floecat.service.query.catalog;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** Arbitrates the single terminal telemetry outcome against producer activity. */
final class StreamTelemetryGate {
  /** The terminal outcome whose sole telemetry publication a gate transition may grant. */
  enum Publication {
    NONE,
    COMPLETION,
    FAILURE,
    CANCELLATION
  }

  /** Whether this cancellation is duplicate, owns teardown, or also publishes immediately. */
  enum CancellationDecision {
    IGNORED,
    ACCEPTED,
    PUBLISH
  }

  private boolean producerActive;
  private boolean cancellationPending;
  private boolean publicationClaimed;

  /** Begin one producer step unless cancellation already won. */
  synchronized void begin(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean()) {
      throw new CancellationException("GetUserObjects stream cancelled");
    }
    producerActive = true;
  }

  /**
   * Atomically expose cancellation to request work and terminal-outcome arbitration. The caller
   * owns teardown unless another cancellation already won.
   */
  synchronized CancellationDecision cancel(AtomicBoolean cancelled) {
    if (!cancelled.compareAndSet(false, true)) {
      return CancellationDecision.IGNORED;
    }
    cancellationPending = true;
    if (producerActive) {
      return CancellationDecision.ACCEPTED;
    }
    cancellationPending = false;
    return claimInternal() ? CancellationDecision.PUBLISH : CancellationDecision.ACCEPTED;
  }

  /**
   * Finish a producer step with {@link Publication#NONE}, {@link Publication#COMPLETION}, or {@link
   * Publication#FAILURE}. Failure takes precedence over racing cancellation, and cancellation takes
   * precedence over completion. A non-{@code NONE} return grants ownership of the stream's sole
   * terminal telemetry publication.
   */
  synchronized Publication finish(Publication terminalOutcome) {
    boolean publishFailure = terminalOutcome == Publication.FAILURE && claimInternal();
    producerActive = false;
    boolean publishCancellation = !publishFailure && cancellationPending && claimInternal();
    cancellationPending = false;
    if (publishFailure) {
      return Publication.FAILURE;
    }
    if (publishCancellation) {
      return Publication.CANCELLATION;
    }
    return terminalOutcome == Publication.COMPLETION && claimInternal()
        ? Publication.COMPLETION
        : Publication.NONE;
  }

  /** Claim publication from a non-racing terminal path. */
  synchronized boolean claim() {
    return claimInternal();
  }

  private boolean claimInternal() {
    if (publicationClaimed) {
      return false;
    }
    publicationClaimed = true;
    return true;
  }
}
