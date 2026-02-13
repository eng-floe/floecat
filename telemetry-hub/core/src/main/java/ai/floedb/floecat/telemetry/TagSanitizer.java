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

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/** Sanitizes tag keys/values before metric emission. */
public final class TagSanitizer {
  private static final Pattern KEY_PATTERN = Pattern.compile("^[a-z0-9_.-]+$");
  private static final int MAX_VALUE_LENGTH = 128;
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  private TagSanitizer() {}

  public static Optional<Tag> sanitize(Tag tag, TelemetryPolicy policy) {
    if (tag == null) {
      if (policy.isStrict()) {
        throw new IllegalArgumentException("tag must not be null");
      }
      return Optional.empty();
    }
    String key = sanitizeKey(tag.key());
    String value = sanitizeValue(tag.value());
    if (key.isEmpty()) {
      if (policy.isStrict()) {
        throw new IllegalArgumentException("tag key must not be blank");
      }
      return Optional.empty();
    }
    if (!KEY_PATTERN.matcher(key).matches()) {
      if (policy.isStrict()) {
        throw new IllegalArgumentException("invalid tag key: " + key);
      }
      return Optional.empty();
    }
    if (value.isEmpty()) {
      if (policy.isStrict()) {
        throw new IllegalArgumentException("tag value must not be blank");
      }
      return Optional.empty();
    }
    return Optional.of(Tag.of(key, value));
  }

  private static String sanitizeKey(String key) {
    if (key == null) {
      return "";
    }
    return key.trim().toLowerCase(Locale.ROOT);
  }

  private static String sanitizeValue(String value) {
    if (value == null) {
      return "";
    }
    String trimmed = value.trim();
    trimmed = WHITESPACE.matcher(trimmed).replaceAll(" ");
    if (trimmed.length() > MAX_VALUE_LENGTH) {
      trimmed = trimmed.substring(0, MAX_VALUE_LENGTH);
    }
    return trimmed;
  }
}
