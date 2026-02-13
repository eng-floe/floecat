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
