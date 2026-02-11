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

  public MetricDef(MetricId id, Set<String> requiredTags, Set<String> allowedTags) {
    this.id = Objects.requireNonNull(id, "id");
    this.requiredTags = normalizeTags(requiredTags);
    this.allowedTags = normalizeTags(allowedTags);
    if (!this.allowedTags.containsAll(this.requiredTags)) {
      throw new IllegalArgumentException("allowedTags must include requiredTags");
    }
  }

  private static Set<String> normalizeTags(Set<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(new HashSet<>(tags));
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

  @Override
  public String toString() {
    return id().name();
  }
}
