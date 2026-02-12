package ai.floedb.floecat.telemetry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Enforces the telemetry contract against emitted metrics. */
public final class MetricValidator {
  private final TelemetryRegistry registry;
  private final TelemetryPolicy policy;

  public MetricValidator(TelemetryRegistry registry, TelemetryPolicy policy) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.policy = Objects.requireNonNull(policy, "policy");
  }

  public ValidationResult validate(MetricId metric, MetricType expected, Tag... tags) {
    Objects.requireNonNull(metric, "metric");
    Objects.requireNonNull(expected, "expected");
    MetricDef def = Telemetry.metricDef(registry, metric).orElse(null);
    if (def == null) {
      if (policy.isStrict()) {
        throw new IllegalArgumentException(
            "Metric not registered: " + metric.name() + " (ensure the registry has core metrics)");
      }
      return new ValidationResult(
          Collections.emptyList(),
          false,
          0,
          DropMetricReason.UNKNOWN_METRIC,
          "metric not registered: " + metric.name());
    }
    if (def.id().type() != expected) {
      throw new IllegalArgumentException(
          "Metric type mismatch: expected " + expected + " but got " + def.id().type());
    }

    List<Tag> sanitized = new ArrayList<>();
    int droppedTags = 0;
    Tag[] safeTags = tags == null ? new Tag[0] : tags;
    for (Tag tag : safeTags) {
      Optional<Tag> clean = TagSanitizer.sanitize(tag, policy);
      if (clean.isEmpty()) {
        droppedTags++;
        continue;
      }
      Tag sanitizedTag = clean.get();
      if (!def.allowedTags().isEmpty() && !def.allowedTags().contains(sanitizedTag.key())) {
        if (policy.isStrict()) {
          throw new IllegalArgumentException(
              "Tag not allowed: " + sanitizedTag.key() + " for metric " + metric.name());
        }
        droppedTags++;
        continue;
      }
      sanitized.add(sanitizedTag);
    }

    Set<String> providedKeys = new HashSet<>();
    for (Tag tag : sanitized) {
      providedKeys.add(tag.key());
    }

    for (String required : def.requiredTags()) {
      if (!providedKeys.contains(required)) {
        if (policy.isStrict()) {
          throw new IllegalArgumentException("Missing required tag: " + required);
        }
        return new ValidationResult(
            Collections.emptyList(),
            false,
            droppedTags,
            DropMetricReason.MISSING_REQUIRED_TAG,
            "missing required tag: " + required);
      }
    }

    return new ValidationResult(
        Collections.unmodifiableList(sanitized), true, droppedTags, null, null);
  }

  public record ValidationResult(
      List<Tag> tags, boolean emit, int droppedTags, DropMetricReason reason, String detail) {}
}
