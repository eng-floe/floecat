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
