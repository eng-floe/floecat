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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricValidatorTest {

  private TelemetryRegistry registry;

  @BeforeEach
  void setUp() {
    registry = Telemetry.newRegistryWithCore();
  }

  @Test
  void strictRejectsDisallowedTags() {
    MetricValidator validator = new MetricValidator(registry, TelemetryPolicy.STRICT);
    Tag disallowed = Tag.of("foo", "bar");
    assertThatThrownBy(
            () ->
                validator.validate(Telemetry.Metrics.RPC_REQUESTS, MetricType.COUNTER, disallowed))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void lenientDropsDisallowedTag() {
    MetricValidator validator = new MetricValidator(registry, TelemetryPolicy.LENIENT);
    MetricValidator.ValidationResult result =
        validator.validate(
            Telemetry.Metrics.RPC_REQUESTS,
            MetricType.COUNTER,
            Tag.of("component", "svc"),
            Tag.of("operation", "op"),
            Tag.of("account", "acc"),
            Tag.of("status", "ok"),
            Tag.of("foo", "bar"));
    assertThat(result.emit()).isTrue();
    assertThat(result.tags())
        .extracting(Tag::key)
        .containsExactlyInAnyOrder("component", "operation", "account", "status");
    assertThat(result.droppedTags()).isEqualTo(1);
  }

  @Test
  void lenientDropsMetricWhenRequiredTagsMissing() {
    MetricValidator validator = new MetricValidator(registry, TelemetryPolicy.LENIENT);
    MetricValidator.ValidationResult result =
        validator.validate(
            Telemetry.Metrics.RPC_ERRORS,
            MetricType.COUNTER,
            Tag.of("component", "svc"),
            Tag.of("operation", "op"),
            Tag.of("status", "error"));
    assertThat(result.emit()).isFalse();
    assertThat(result.reason()).isEqualTo(DropMetricReason.MISSING_REQUIRED_TAG);
    assertThat(result.detail()).contains("missing required tag");
  }

  @Test
  void missingRequiredTagFails() {
    MetricValidator validator = new MetricValidator(registry, TelemetryPolicy.STRICT);
    assertThatThrownBy(
            () ->
                validator.validate(
                    Telemetry.Metrics.RPC_ERRORS, MetricType.COUNTER, Tag.of("status", "ok")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void typeMismatchFails() {
    MetricValidator validator = new MetricValidator(registry, TelemetryPolicy.STRICT);
    assertThatThrownBy(
            () ->
                validator.validate(
                    Telemetry.Metrics.RPC_LATENCY, MetricType.COUNTER, Tag.of("component", "a")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
