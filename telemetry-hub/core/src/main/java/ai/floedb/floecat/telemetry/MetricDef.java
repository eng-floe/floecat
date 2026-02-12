package ai.floedb.floecat.telemetry;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Definition of a metric contract entry. */
public final class MetricDef {
  private final MetricId id;
  private final Set<String> requiredTags;
  private final Set<String> allowedTags;
  private final String description;

  public MetricDef(MetricId id, Set<String> requiredTags, Set<String> allowedTags) {
    this(id, requiredTags, allowedTags, "");
  }

  public MetricDef(
      MetricId id, Set<String> requiredTags, Set<String> allowedTags, String description) {
    this.id = Objects.requireNonNull(id, "id");
    this.requiredTags = normalizeTags(requiredTags);
    this.allowedTags = normalizeTags(allowedTags);
    if (!this.allowedTags.containsAll(this.requiredTags)) {
      throw new IllegalArgumentException("allowedTags must include requiredTags");
    }
    this.description = description == null ? "" : description;
  }

  private static Set<String> normalizeTags(Set<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> normalized = new HashSet<>();
    for (String tag : tags) {
      normalized.add(requireNonBlank(tag, "tag"));
    }
    return Collections.unmodifiableSet(normalized);
  }

  private static String requireNonBlank(String value, String label) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(label + " entries must not be blank");
    }
    return value;
  }

  public MetricId id() {
    return id;
  }

  public Set<String> requiredTags() {
    return requiredTags;
  }

  public Set<String> allowedTags() {
    return allowedTags;
  }

  public String description() {
    return description;
  }

  @Override
  public String toString() {
    return id().name();
  }
}
