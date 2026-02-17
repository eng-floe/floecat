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
package ai.floedb.floecat.telemetry;

/**
 * Reasons why the metric validator dropped a metric emission in lenient mode.
 *
 * <p>The reason is only populated when {@link MetricValidator.ValidationResult#emit()} is {@code
 * false}. Tag-level drops are instead accounted for via {@link
 * MetricValidator.ValidationResult#droppedTags()}.
 */
public enum DropMetricReason {
  MISSING_REQUIRED_TAG,
  UNKNOWN_TAG,
  INVALID_KEY,
  TYPE_MISMATCH,
  UNKNOWN_METRIC
}
