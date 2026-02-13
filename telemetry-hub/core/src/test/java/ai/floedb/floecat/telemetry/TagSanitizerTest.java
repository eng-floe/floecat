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

import java.util.Optional;
import org.junit.jupiter.api.Test;

class TagSanitizerTest {

  @Test
  void trimsAndLowercasesKey() {
    Optional<Tag> sanitized =
        TagSanitizer.sanitize(Tag.of("  Foo-Bar  ", "value "), TelemetryPolicy.STRICT);
    assertThat(sanitized).contains(Tag.of("foo-bar", "value"));
  }

  @Test
  void collapsesWhitespaceAndLimitsValue() {
    String value = "a    lot    of\n whitespace \t characters ";
    Optional<Tag> sanitized = TagSanitizer.sanitize(Tag.of("key", value), TelemetryPolicy.STRICT);
    assertThat(sanitized).isPresent();
    assertThat(sanitized.get().value()).isEqualTo("a lot of whitespace characters");
    String longValue = "x".repeat(200);
    Optional<Tag> truncated =
        TagSanitizer.sanitize(Tag.of("key", longValue), TelemetryPolicy.STRICT);
    assertThat(truncated).isPresent();
    assertThat(truncated.get().value().length()).isEqualTo(128);
  }

  @Test
  void dropsInvalidKeysInLenientMode() {
    Optional<Tag> sanitized =
        TagSanitizer.sanitize(Tag.of("Bad Key!", "val"), TelemetryPolicy.LENIENT);
    assertThat(sanitized).isEmpty();
  }

  @Test
  void rejectsInvalidKeysInStrictMode() {
    assertThatThrownBy(
            () -> TagSanitizer.sanitize(Tag.of("Bad Key!", "val"), TelemetryPolicy.STRICT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void lenientDropsNullTag() {
    assertThat(TagSanitizer.sanitize(null, TelemetryPolicy.LENIENT)).isEmpty();
  }

  @Test
  void strictRejectsNullTag() {
    assertThatThrownBy(() -> TagSanitizer.sanitize(null, TelemetryPolicy.STRICT))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
