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
  TYPE_MISMATCH
}
