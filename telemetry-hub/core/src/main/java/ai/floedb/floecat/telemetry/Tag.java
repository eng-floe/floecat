package ai.floedb.floecat.telemetry;

import java.util.Objects;

/** Simple key/value metadata for metrics. */
public final class Tag {
  private final String key;
  private final String value;

  private Tag(String key, String value) {
    this.key = requireNonBlank(key, "key");
    this.value = Objects.requireNonNull(value, "value");
  }

  public static Tag of(String key, String value) {
    return new Tag(key, value);
  }

  public String key() {
    return key;
  }

  public String value() {
    return value;
  }

  private static String requireNonBlank(String value, String label) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(label + " must not be blank");
    }
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Tag other)) {
      return false;
    }
    return key.equals(other.key) && value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public String toString() {
    return key + "=" + value;
  }
}
