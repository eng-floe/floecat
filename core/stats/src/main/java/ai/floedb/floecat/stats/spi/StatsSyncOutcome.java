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

package ai.floedb.floecat.stats.spi;

/**
 * Outcome of the sync-first resolution path in the stats orchestrator.
 *
 * <p>Distinguishes authoritative store hits from sync-captured, partial, timed-out, failed, and
 * skipped outcomes so that callers can make quality-aware decisions.
 */
public enum StatsSyncOutcome {
  /** Stats were found in the persisted store; no capture was needed. */
  HIT,
  /** Sync capture completed successfully and data is now in the store. */
  CAPTURED,
  /**
   * Sync capture ran but the store did not contain data on re-read.
   *
   * <p>Indicates the capture job succeeded according to the job store but the stats record was not
   * visible yet (e.g. write propagation lag). An async follow-up is enqueued.
   */
  PARTIAL,
  /**
   * Sync capture did not complete within the latency budget.
   *
   * <p>An async follow-up is enqueued so the data will eventually be available.
   */
  TIMEOUT,
  /**
   * Sync capture encountered an error (connector unavailable, job failed, etc.).
   *
   * <p>An async follow-up is enqueued so the data may be retried.
   */
  FAILED,
  /**
   * Sync capture was not attempted.
   *
   * <p>Reasons include: the request specifies {@link StatsExecutionMode#ASYNC}, no latency budget
   * is present, or sync is disabled via configuration. An async follow-up is enqueued.
   */
  SKIPPED
}
