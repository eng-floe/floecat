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

package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

public final class ConstraintNormalizer {
  private ConstraintNormalizer() {}

  public static SnapshotConstraints normalize(SnapshotConstraints in) {
    var b = in.toBuilder();

    if (b.getConstraintsCount() > 0) {
      List<ConstraintDefinition> normalized = new ArrayList<>(b.getConstraintsCount());
      for (ConstraintDefinition definition : b.getConstraintsList()) {
        var cb = definition.toBuilder();
        if (!cb.getPropertiesMap().isEmpty()) {
          var sorted = new TreeMap<>(cb.getPropertiesMap());
          cb.clearProperties();
          cb.putAllProperties(sorted);
        }
        normalized.add(cb.build());
      }
      normalized.sort(ConstraintNormalizer::compareConstraint);
      b.clearConstraints();
      b.addAllConstraints(normalized);
    }

    if (!b.getPropertiesMap().isEmpty()) {
      var sorted = new TreeMap<>(b.getPropertiesMap());
      b.clearProperties();
      b.putAllProperties(sorted);
    }

    return b.build();
  }

  // Keep this comparator aligned with all persisted ConstraintDefinition fields so normalization
  // is stable across replay/input ordering differences. Properties are compared via toString()
  // directly: normalize() already sorts each constraint's properties map before this sort runs.
  private static final Comparator<ConstraintDefinition> CONSTRAINT_ORDER =
      Comparator.comparingInt(ConstraintDefinition::getTypeValue)
          .thenComparing(ConstraintDefinition::getName)
          .thenComparingInt(ConstraintDefinition::getEnforcementValue)
          .thenComparing(ConstraintNormalizer::columnSignature)
          .thenComparing(ConstraintDefinition::getCheckExpression)
          .thenComparing(ConstraintNormalizer::referencedTableSignature)
          .thenComparing(ConstraintDefinition::getReferencedTableName)
          .thenComparing(ConstraintNormalizer::referencedColumnsSignature)
          .thenComparing(c -> c.getPropertiesMap().toString());

  private static int compareConstraint(ConstraintDefinition left, ConstraintDefinition right) {
    return CONSTRAINT_ORDER.compare(left, right);
  }

  private static String referencedTableSignature(ConstraintDefinition definition) {
    if (!definition.hasReferencedTableId()) {
      return "";
    }
    ResourceId id = definition.getReferencedTableId();
    return id.getAccountId() + "|" + id.getId() + "|" + id.getKindValue();
  }

  private static String columnSignature(ConstraintDefinition definition) {
    return columnSignature(definition.getColumnsList());
  }

  private static String referencedColumnsSignature(ConstraintDefinition definition) {
    return columnSignature(definition.getReferencedColumnsList());
  }

  private static String columnSignature(List<ConstraintColumnRef> columns) {
    if (columns.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder(columns.size() * 16);
    for (ConstraintColumnRef column : columns) {
      sb.append(column.getOrdinal())
          .append('#')
          .append(column.getColumnId())
          .append('#')
          .append(column.getColumnName())
          .append(';');
    }
    return sb.toString();
  }
}
