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
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
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
      // Normalize each definition and build Schwartzian sort keys in one pass.
      // Precomputing the three string signatures here (O(n)) avoids recomputing them
      // on every comparator call (O(n log n)).
      record SortKey(ConstraintDefinition c, String colSig, String refColSig, String refTblSig) {}
      List<SortKey> keyed = new ArrayList<>(b.getConstraintsCount());
      for (ConstraintDefinition definition : b.getConstraintsList()) {
        var cb = definition.toBuilder();
        normalizeForeignKeyDefaults(cb);
        if (!cb.getPropertiesMap().isEmpty()) {
          var sorted = new TreeMap<>(cb.getPropertiesMap());
          cb.clearProperties();
          cb.putAllProperties(sorted);
        }
        ConstraintDefinition c = cb.build();
        keyed.add(
            new SortKey(
                c, columnSignature(c), referencedColumnsSignature(c), referencedTableSignature(c)));
      }
      keyed.sort(
          Comparator.comparingInt((SortKey k) -> k.c().getTypeValue())
              .thenComparing(k -> k.c().getName())
              .thenComparingInt(k -> k.c().getEnforcementValue())
              .thenComparing(SortKey::colSig)
              .thenComparing(k -> k.c().getCheckExpression())
              .thenComparing(SortKey::refTblSig)
              .thenComparing(SortKey::refColSig)
              .thenComparing(k -> k.c().getReferencedConstraintName())
              .thenComparingInt(k -> k.c().getMatchOptionValue())
              .thenComparingInt(k -> k.c().getUpdateRuleValue())
              .thenComparingInt(k -> k.c().getDeleteRuleValue())
              .thenComparing(k -> k.c().getPropertiesMap().toString()));
      b.clearConstraints();
      for (SortKey k : keyed) {
        b.addConstraints(k.c());
      }
    }

    if (!b.getPropertiesMap().isEmpty()) {
      var sorted = new TreeMap<>(b.getPropertiesMap());
      b.clearProperties();
      b.putAllProperties(sorted);
    }

    return b.build();
  }

  private static String referencedTableSignature(ConstraintDefinition definition) {
    StringBuilder sig = new StringBuilder();
    if (definition.hasReferencedTableId()) {
      ResourceId id = definition.getReferencedTableId();
      sig.append(id.getAccountId())
          .append('|')
          .append(id.getId())
          .append('|')
          .append(id.getKindValue());
    }
    if (definition.hasReferencedTable()) {
      var ref = definition.getReferencedTable();
      sig.append('|')
          .append(ref.getCatalog())
          .append('|')
          .append(String.join(".", ref.getPathList()))
          .append('|')
          .append(ref.getName());
      if (ref.hasResourceId()) {
        ResourceId rid = ref.getResourceId();
        sig.append('|')
            .append(rid.getAccountId())
            .append('|')
            .append(rid.getId())
            .append('|')
            .append(rid.getKindValue());
      }
    }
    return sig.toString();
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

  private static void normalizeForeignKeyDefaults(ConstraintDefinition.Builder builder) {
    if (builder.getType() != ConstraintType.CT_FOREIGN_KEY) {
      return;
    }
    if (builder.getMatchOption() == ForeignKeyMatchOption.FK_MATCH_OPTION_UNSPECIFIED) {
      builder.setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_NONE);
    }
    if (builder.getUpdateRule() == ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED) {
      builder.setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION);
    }
    if (builder.getDeleteRule() == ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED) {
      builder.setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION);
    }
  }
}
