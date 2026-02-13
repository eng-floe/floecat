package ai.floedb.floecat.telemetry.helpers;

import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Common helpers for metrics wrappers that share component/operation tags. */
abstract class BaseMetrics {
  private final String component;
  private final String operation;
  private final List<Tag> extraTags;

  BaseMetrics(String component, String operation, Tag... extraTags) {
    this.component = Objects.requireNonNull(component, "component");
    this.operation = Objects.requireNonNull(operation, "operation");
    List<Tag> tags = new ArrayList<>();
    if (extraTags != null) {
      for (Tag tag : extraTags) {
        if (tag != null) {
          tags.add(tag);
        }
      }
    }
    this.extraTags = Collections.unmodifiableList(tags);
  }

  protected String component() {
    return component;
  }

  protected String operation() {
    return operation;
  }

  protected Tag[] metricTags(Tag... extra) {
    return metricTagList(extra).toArray(Tag[]::new);
  }

  protected Tag[] metricTags() {
    return metricTagList().toArray(Tag[]::new);
  }

  protected Tag[] metricTags(List<Tag> extra) {
    return metricTagList(extra).toArray(Tag[]::new);
  }

  protected List<Tag> metricTagList(Tag... extra) {
    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of(TagKey.COMPONENT, component));
    tags.add(Tag.of(TagKey.OPERATION, operation));
    tags.addAll(extraTags);
    addExtra(tags, extra);
    return List.copyOf(tags);
  }

  protected List<Tag> metricTagList(List<Tag> extra) {
    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of(TagKey.COMPONENT, component));
    tags.add(Tag.of(TagKey.OPERATION, operation));
    tags.addAll(extraTags);
    addExtra(tags, extra);
    return List.copyOf(tags);
  }

  protected Tag[] scopeTags(Tag... extra) {
    return metricTags(extra);
  }

  protected Tag[] scopeTags(List<Tag> extra) {
    return metricTags(extra);
  }

  protected Tag[] scopeTagsWithResult(String result, Tag... extra) {
    List<Tag> tags = new ArrayList<>();
    if (result != null && !result.isBlank()) {
      tags.add(Tag.of(TagKey.RESULT, result));
    }
    addExtra(tags, extra);
    return scopeTags(tags);
  }

  protected Tag[] scopeExtraTags(Tag... extra) {
    return scopeExtraTagList(extra).toArray(Tag[]::new);
  }

  protected Tag[] scopeExtraTags(List<Tag> extra) {
    return scopeExtraTagList(extra).toArray(Tag[]::new);
  }

  protected List<Tag> scopeExtraTagList(Tag... extra) {
    List<Tag> tags = new ArrayList<>(extraTags);
    addExtra(tags, extra);
    return List.copyOf(tags);
  }

  protected List<Tag> scopeExtraTagList(List<Tag> extra) {
    List<Tag> tags = new ArrayList<>(extraTags);
    addExtra(tags, extra);
    return List.copyOf(tags);
  }

  /** Wraps a supplier to return {@link Double#NaN} when the delegate returns {@code null}. */
  protected static Supplier<Number> safeSupplier(Supplier<? extends Number> supplier) {
    Objects.requireNonNull(supplier, "supplier");
    return () -> {
      Number value = supplier.get();
      return value == null ? Double.NaN : value;
    };
  }

  protected static void addExtra(List<Tag> target, Tag... extra) {
    if (extra == null) {
      return;
    }
    for (Tag tag : extra) {
      if (tag != null) {
        target.add(tag);
      }
    }
  }

  protected static void addExtra(List<Tag> target, List<Tag> extra) {
    if (extra == null) {
      return;
    }
    for (Tag tag : extra) {
      if (tag != null) {
        target.add(tag);
      }
    }
  }
}
