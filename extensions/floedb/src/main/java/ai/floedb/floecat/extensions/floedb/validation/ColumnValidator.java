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

package ai.floedb.floecat.extensions.floedb.validation;

import ai.floedb.floecat.extensions.floedb.proto.FloeColumnSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class ColumnValidator implements SectionValidator<SimpleValidationResult> {
  private static final String CODE_ATTNUM_REQUIRED = "floe.column.attnum_required";
  private static final String CODE_ATTNUM_DUPLICATE = "floe.column.attnum_duplicate";
  private static final String CODE_ORDINAL_REQUIRED = "floe.column.ordinal_required";
  private static final String CODE_NAME_REQUIRED = "floe.column.name_required";
  private static final String CODE_NAME_MISMATCH = "floe.column.name_mismatch";
  private static final String CODE_NULLABILITY_MISMATCH = "floe.column.nullability_mismatch";
  private static final String CODE_TYPE_UNKNOWN = "floe.column.type_unknown";
  private static final String CODE_ORDINAL_DUPLICATE = "floe.column.ordinal_duplicate";
  private static final String CODE_COLUMN_SCHEMA_REQUIRED = "floe.column.schema.required";

  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;
  private final Lookup typeLookup;

  ColumnValidator(
      ValidationScope scope, ValidationSupport.ValidationRunContext runContext, Lookup typeLookup) {
    this.scope = scope;
    this.runContext = runContext;
    this.typeLookup = typeLookup;
  }

  @Override
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    if (catalog == null) {
      return new SimpleValidationResult(errors);
    }
    validateColumns(catalog.tables(), "table", errors);
    validateColumns(catalog.views(), "view", errors);
    return new SimpleValidationResult(errors);
  }

  private void validateColumns(
      List<? extends SystemObjectDef> relations, String domain, List<ValidationIssue> errors) {
    for (SystemObjectDef relation : relations) {
      String ctx =
          ValidationSupport.context(
              domain,
              relation instanceof SystemTableDef t ? t.name() : ((SystemViewDef) relation).name());
      List<SystemColumnDef> columns =
          relation instanceof SystemTableDef t ? t.columns() : ((SystemViewDef) relation).columns();
      if (columns == null || columns.isEmpty()) {
        ValidationSupport.err(errors, CODE_COLUMN_SCHEMA_REQUIRED, ctx);
        continue;
      }
      Map<Integer, SystemColumnDef> columnsByOrdinal = buildColumnMap(columns, errors, ctx);
      Set<Integer> seenAttnums = new HashSet<>();
      List<ValidationSupport.DecodedRule<FloeColumnSpecific>> decoded = new ArrayList<>();
      boolean columnPayloadsPresent =
          columnsByOrdinal.values().stream()
              .anyMatch(col -> col.engineSpecific() != null && !col.engineSpecific().isEmpty());
      if (columnPayloadsPresent) {
        for (SystemColumnDef column : columnsByOrdinal.values()) {
          if (column.engineSpecific() == null || column.engineSpecific().isEmpty()) {
            continue;
          }
          String columnCtx =
              ctx + ":column=" + (column.name() == null ? column.ordinal() : column.name());
          decoded.addAll(
              ValidationSupport.decodeAllPayloads(
                  runContext,
                  scope,
                  column.engineSpecific(),
                  FloePayloads.COLUMN,
                  columnCtx,
                  errors));
        }
      }
      for (ValidationSupport.DecodedRule<FloeColumnSpecific> dr : decoded) {
        FloeColumnSpecific payload = dr.payload();
        int attnum = payload.getAttnum();
        if (attnum <= 0) {
          ValidationSupport.err(errors, CODE_ATTNUM_REQUIRED, ctx, attnum);
          continue;
        }
        if (!seenAttnums.add(attnum)) {
          ValidationSupport.err(errors, CODE_ATTNUM_DUPLICATE, ctx, attnum);
        }
        SystemColumnDef column = columnsByOrdinal.get(attnum);
        String attname = payload.getAttname();
        if (ValidationSupport.isBlank(attname)) {
          ValidationSupport.err(errors, CODE_NAME_REQUIRED, ctx, attnum);
        } else if (column != null && !Objects.equals(column.name(), attname)) {
          ValidationSupport.err(errors, CODE_NAME_MISMATCH, ctx, column.name(), attname);
        }
        if (column != null && payload.hasAttnotnull()) {
          boolean columnNotNull = !column.nullable();
          if (payload.getAttnotnull() != columnNotNull) {
            ValidationSupport.err(
                errors, CODE_NULLABILITY_MISMATCH, ctx, column.name(), payload.getAttnotnull());
          }
        }
        int atttypid = payload.getAtttypid();
        if (atttypid > 0 && typeLookup.byOid(atttypid).isEmpty()) {
          ValidationSupport.err(errors, CODE_TYPE_UNKNOWN, ctx, atttypid);
        }
      }
    }
  }

  private Map<Integer, SystemColumnDef> buildColumnMap(
      List<SystemColumnDef> columns, List<ValidationIssue> errors, String ctx) {
    Map<Integer, SystemColumnDef> byOrdinal = new LinkedHashMap<>();
    Set<Integer> seenOrdinals = new HashSet<>();
    for (SystemColumnDef column : columns) {
      if (column == null) {
        continue;
      }
      int ordinal = column.ordinal();
      if (ordinal <= 0) {
        ValidationSupport.err(errors, CODE_ORDINAL_REQUIRED, ctx, ordinal);
        continue;
      }
      if (!seenOrdinals.add(ordinal)) {
        ValidationSupport.err(errors, CODE_ORDINAL_DUPLICATE, ctx, ordinal);
        continue;
      }
      byOrdinal.put(ordinal, column);
    }
    return byOrdinal;
  }
}
