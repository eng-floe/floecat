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

import java.util.Objects;

/**
 * Unique identifier for a metric in the contract.
 *
 * <p>TODO: add equals/hashCode when tooling needs to compare metrics by identity.
 */
public final class MetricId {
  private final String name;
  private final MetricType type;
  private final String unit;
  private final String since;
  private final String origin;

  public MetricId(String name, MetricType type, String unit, String since, String origin) {
    this.name = requireNonBlank(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.unit = requireUnit(unit);
    this.since = requireNonBlank(since, "since");
    this.origin = requireNonBlank(origin, "origin");
  }

  private static String requireUnit(String unit) {
    Objects.requireNonNull(unit, "unit");
    if (!unit.isEmpty() && unit.isBlank()) {
      throw new IllegalArgumentException("unit must not be blank");
    }
    return unit;
  }

  private static String requireNonBlank(String value, String label) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(label + " must not be blank");
    }
    return value;
  }

  public String name() {
    return name;
  }

  public MetricType type() {
    return type;
  }

  public String unit() {
    return unit;
  }

  public String since() {
    return since;
  }

  public String origin() {
    return origin;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MetricId other)) {
      return false;
    }
    return name.equals(other.name)
        && type == other.type
        && unit.equals(other.unit)
        && since.equals(other.since)
        && origin.equals(other.origin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, unit, since, origin);
  }

  @Override
  public String toString() {
    return name + "(" + type + ")";
  }
}
