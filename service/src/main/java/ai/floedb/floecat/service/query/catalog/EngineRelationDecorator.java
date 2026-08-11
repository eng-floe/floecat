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
package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.ColumnFailure;
import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.ColumnResult;
import ai.floedb.floecat.query.rpc.ColumnStatus;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.ViewDefinition;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.DecorationException;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.ViewDecoration;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

/** Owns engine-decoration selection, callback serialization, failure mapping, and persistence. */
final class EngineRelationDecorator {
  private static final Logger LOG = Logger.getLogger(EngineRelationDecorator.class);
  private static final String RELATION_HINT_PERSIST_NANOS_KEY =
      "decorator.relation_hint_persist_nanos";
  private static final String COLUMN_HINT_PERSIST_NANOS_KEY = "decorator.column_hint_persist_nanos";
  private static final String COLUMN_WARM_HIT_COUNT_KEY = "decorator.column_warm_hits";

  private final EngineMetadataDecoratorProvider provider;
  private final boolean enabled;

  EngineRelationDecorator(EngineMetadataDecoratorProvider provider, boolean enabled) {
    this.provider = provider;
    this.enabled = enabled;
  }

  /** Immutable request-thread selection used by every relation in one build stream. */
  record Selection(
      EngineContext context, boolean required, Optional<EngineMetadataDecorator> decorator) {
    boolean supportsWorkerThreadCallbacks() {
      return !required || decorator.isEmpty() || decorator.get().supportsWorkerThreadCallbacks();
    }
  }

  /** Select the exact decorator on the request's producer thread before any fan-out begins. */
  Selection select(EngineContext context) {
    boolean required = isRequired(context);
    return new Selection(
        context, required, required ? provider.decorator(context) : Optional.empty());
  }

  /** Result of one complete decorator lifecycle and its payload-cacheability gates. */
  record Outcome(
      List<ColumnResult> columnResults,
      long relationWarmHitCount,
      boolean relationDecorationSucceeded,
      boolean viewDecorationSucceeded,
      boolean completeRelationSucceeded) {}

  Outcome decorate(
      ResolvedRelation relation,
      RelationInfo.Builder builder,
      ViewDefinition.Builder viewBuilder,
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      List<SchemaColumn> schemaColumns,
      MetadataResolutionContext resolutionContext,
      Selection selection,
      TimingAccumulator timings) {
    EngineContext context = selection.context();
    boolean required = selection.required();
    Optional<EngineMetadataDecorator> decorator = selection.decorator();
    RelationDecoration relationDecoration = null;
    ViewDecoration viewDecoration = null;
    if (required && decorator.isPresent()) {
      relationDecoration =
          new RelationDecoration(
              builder,
              relation.relationId(),
              relation.node(),
              immutable(schemaColumns),
              immutable(pruned),
              resolutionContext);
      if (viewBuilder != null) {
        viewDecoration =
            new ViewDecoration(
                builder, viewBuilder, relation.relationId(), relation.node(), resolutionContext);
      }
    }

    if (relationDecoration == null) {
      return runLifecycle(
          relation,
          builder,
          viewBuilder,
          columns,
          pruned,
          null,
          null,
          decorator,
          context,
          required,
          timings);
    }
    synchronized (decorator.orElseThrow()) {
      return runLifecycle(
          relation,
          builder,
          viewBuilder,
          columns,
          pruned,
          relationDecoration,
          viewDecoration,
          decorator,
          context,
          required,
          timings);
    }
  }

  private Outcome runLifecycle(
      ResolvedRelation relation,
      RelationInfo.Builder builder,
      ViewDefinition.Builder viewBuilder,
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      ViewDecoration viewDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext context,
      boolean required,
      TimingAccumulator timings) {
    boolean relationSucceeded = true;
    boolean viewSucceeded = true;
    boolean completeSucceeded = true;

    if (relationDecoration != null) {
      try {
        long startNs = System.nanoTime();
        try {
          decorator.orElseThrow().decorateRelation(context, relationDecoration);
        } finally {
          timings.addDecorateRelationNanos(System.nanoTime() - startNs);
        }
      } catch (java.util.concurrent.CancellationException e) {
        throw e;
      } catch (RuntimeException e) {
        relationSucceeded = false;
        LOG.debugf(
            e,
            "Decorator threw while decorating relation %s (engine=%s)",
            relation.relationId(),
            context.normalizedKind());
      }

      if (viewDecoration != null) {
        try {
          long startNs = System.nanoTime();
          try {
            decorator.orElseThrow().decorateView(context, viewDecoration);
          } finally {
            timings.addDecorateViewNanos(System.nanoTime() - startNs);
          }
        } catch (java.util.concurrent.CancellationException e) {
          throw e;
        } catch (RuntimeException e) {
          viewSucceeded = false;
          LOG.debugf(
              e,
              "Decorator threw while decorating view %s (engine=%s)",
              relation.relationId(),
              context.normalizedKind());
        }
      }
    }

    if (viewBuilder != null) {
      builder.setViewDefinition(viewBuilder);
    }
    List<ColumnResult> columnResults =
        decorateColumns(
            columns,
            pruned,
            relationDecoration,
            decorator,
            context,
            required,
            relation.relationId(),
            timings);
    long warmHitCount = counter(relationDecoration, COLUMN_WARM_HIT_COUNT_KEY);
    timings.addDecorateColumnWarmHits(warmHitCount);

    if (relationDecoration != null) {
      boolean commitRelationHints = relationSucceeded;
      boolean commitColumnHints = relationSucceeded && shouldCommitColumns(columnResults);
      Set<Long> readyColumnIds = commitColumnHints ? readyColumnIds(columnResults) : Set.of();
      try {
        long startNs = System.nanoTime();
        try {
          decorator
              .orElseThrow()
              .completeRelation(
                  context,
                  relationDecoration,
                  commitRelationHints,
                  commitColumnHints,
                  readyColumnIds);
        } finally {
          timings.addDecorateCompleteNanos(System.nanoTime() - startNs);
          timings.addDecoratePersistRelationNanos(
              counter(relationDecoration, RELATION_HINT_PERSIST_NANOS_KEY));
          timings.addDecoratePersistColumnsNanos(
              counter(relationDecoration, COLUMN_HINT_PERSIST_NANOS_KEY));
        }
      } catch (java.util.concurrent.CancellationException e) {
        throw e;
      } catch (RuntimeException e) {
        completeSucceeded = false;
        LOG.debugf(
            e,
            "Decorator threw while completing relation %s (engine=%s)",
            relation.relationId(),
            context == null ? "" : context.normalizedKind());
      }
    }
    return new Outcome(
        columnResults, warmHitCount, relationSucceeded, viewSucceeded, completeSucceeded);
  }

  List<ColumnResult> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext context,
      boolean required,
      ResourceId relationId) {
    return decorateColumns(
        columns,
        pruned,
        relationDecoration,
        decorator,
        context,
        required,
        relationId,
        new TimingAccumulator());
  }

  private List<ColumnResult> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext context,
      boolean required,
      ResourceId relationId,
      TimingAccumulator timings) {
    if (pruned == null || pruned.size() != columns.size()) {
      String message =
          String.format(
              "Column/schema mismatch columns=%d pruned=%s",
              columns.size(), pruned == null ? "null" : Integer.toString(pruned.size()));
      if (!required) {
        return columns.stream().map(EngineRelationDecorator::readyColumn).toList();
      }
      List<ColumnResult> failed = new ArrayList<>(columns.size());
      for (ColumnInfo column : columns) {
        failed.add(
            failedColumn(
                column,
                ColumnFailureCode.COLUMN_FAILURE_CODE_SCHEMA_MISMATCH,
                message,
                Map.of("relation_id", relationId.getId())));
      }
      return failed;
    }
    if (!required) {
      return columns.stream().map(EngineRelationDecorator::readyColumn).toList();
    }
    if (decorator.isEmpty() || relationDecoration == null) {
      List<ColumnResult> failed = new ArrayList<>(columns.size());
      for (ColumnInfo column : columns) {
        failed.add(
            failedColumn(
                column,
                ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATOR_UNAVAILABLE,
                "Engine-specific column decorator is unavailable",
                Map.of(
                    "engine_kind", safe(context == null ? null : context.normalizedKind()),
                    "engine_version", safe(context == null ? null : context.normalizedVersion()))));
      }
      return failed;
    }

    List<ColumnResult> decorated = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      long totalStartNs = System.nanoTime();
      ColumnInfo column = columns.get(i);
      SchemaColumn schema = pruned.get(i);
      ColumnDecoration columnDecoration =
          new ColumnDecoration(
              column.toBuilder(),
              schema,
              parseLogicalType(schema),
              column.getOrdinal(),
              relationDecoration);
      try {
        long invokeStartNs = System.nanoTime();
        try {
          decorator.get().decorateColumn(context, columnDecoration);
        } finally {
          timings.addDecorateColumnInvokeNanos(System.nanoTime() - invokeStartNs);
        }
        ColumnInfo decoratedColumn = columnDecoration.builder().build();
        if (hasRequiredPayload(decoratedColumn, context)) {
          decorated.add(readyColumn(decoratedColumn));
        } else {
          decorated.add(
              failedColumn(
                  decoratedColumn,
                  ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING,
                  "Engine-specific payload is required but missing",
                  Map.of(
                      "engine_kind", safe(context == null ? null : context.normalizedKind()),
                      "engine_version",
                          safe(context == null ? null : context.normalizedVersion()))));
        }
      } catch (java.util.concurrent.CancellationException e) {
        throw e;
      } catch (RuntimeException e) {
        decorated.add(failedColumn(column, mapFailure(e, context)));
      } finally {
        timings.addDecorateColumnsNanos(System.nanoTime() - totalStartNs);
      }
    }
    return decorated;
  }

  boolean isRequired(EngineContext context) {
    return enabled && context != null && context.enginePluginOverlaysEnabled();
  }

  private static List<SchemaColumn> immutable(List<SchemaColumn> schema) {
    return schema == null ? List.of() : List.copyOf(schema);
  }

  private static long counter(RelationDecoration decoration, String key) {
    if (decoration == null || key == null || key.isBlank()) {
      return 0L;
    }
    Object value = decoration.attribute(key);
    return value instanceof Number number ? Math.max(0L, number.longValue()) : 0L;
  }

  private static ColumnResult readyColumn(ColumnInfo column) {
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_OK)
        .setColumn(column)
        .build();
  }

  private static ColumnResult failedColumn(
      ColumnInfo column, ColumnFailureCode code, String message, Map<String, String> details) {
    ColumnFailure.Builder failure = ColumnFailure.newBuilder().setCode(code).setMessage(message);
    failure.putAllDetails(details);
    return failedColumn(column, failure.build());
  }

  private static ColumnResult failedColumn(ColumnInfo column, ColumnFailure failure) {
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_FAILED)
        .setFailure(failure)
        .build();
  }

  private static ColumnFailure mapFailure(RuntimeException exception, EngineContext context) {
    if (exception instanceof DecorationException decorationException) {
      ColumnFailureCode code =
          decorationException.hasExtensionCodeValue()
              ? ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION
              : decorationException.code();
      String message = userFacingFailureMessage(code);
      if (decorationException.hasExtensionCodeValue()
          && !safe(decorationException.getMessage()).trim().isBlank()) {
        message = decorationException.getMessage().trim();
      }
      ColumnFailure.Builder failure = ColumnFailure.newBuilder().setCode(code).setMessage(message);
      failure.putAllDetails(decorationException.details());
      if (decorationException.hasExtensionCodeValue()) {
        failure.setExtensionCodeValue(decorationException.extensionCodeValue());
      }
      addEngineDetails(failure, context);
      return failure.build();
    }
    ColumnFailureCode code = ColumnFailureCode.COLUMN_FAILURE_CODE_INTERNAL_ERROR;
    if (exception instanceof SecurityException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_PERMISSION_DENIED;
    } else if (exception instanceof UnsupportedOperationException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED;
    } else if (exception instanceof java.util.NoSuchElementException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_NOT_FOUND;
    }
    ColumnFailure.Builder failure =
        ColumnFailure.newBuilder().setCode(code).setMessage(userFacingFailureMessage(code));
    addEngineDetails(failure, context);
    return failure.build();
  }

  private static boolean hasRequiredPayload(ColumnInfo column, EngineContext context) {
    String normalizedKind = context == null ? "" : safe(context.normalizedKind());
    for (EngineSpecific specific : column.getEngineSpecificList()) {
      String specificKind = safe(specific.getEngineKind());
      boolean kindMatches =
          specificKind.isBlank() || normalizedKind.isBlank() || specificKind.equals(normalizedKind);
      if (kindMatches
          && !safe(specific.getPayloadType()).isBlank()
          && !specific.getPayload().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  private static String userFacingFailureMessage(ColumnFailureCode code) {
    if (code == null) {
      return "Column resolution failed.";
    }
    return switch (code) {
      case COLUMN_FAILURE_CODE_SCHEMA_MISMATCH ->
          "Column metadata does not match the relation schema.";
      case COLUMN_FAILURE_CODE_DECORATOR_UNAVAILABLE ->
          "Engine-specific column metadata is unavailable.";
      case COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING ->
          "Required engine-specific metadata is missing for this column.";
      case COLUMN_FAILURE_CODE_PERMISSION_DENIED ->
          "Permission denied while decorating this column.";
      case COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED ->
          "This column type is not supported by the engine metadata decorator.";
      case COLUMN_FAILURE_CODE_LOGICAL_TYPE_INVALID ->
          "The column logical type is invalid for engine metadata decoration.";
      case COLUMN_FAILURE_CODE_NOT_FOUND -> "Column metadata was not found during decoration.";
      case COLUMN_FAILURE_CODE_ENGINE_EXTENSION ->
          "Engine extension failed to provide column metadata.";
      default -> "Column resolution failed.";
    };
  }

  private static void addEngineDetails(ColumnFailure.Builder failure, EngineContext context) {
    if (context != null) {
      failure.putDetails("engine_kind", safe(context.normalizedKind()));
      failure.putDetails("engine_version", safe(context.normalizedVersion()));
    }
  }

  private static boolean shouldCommitColumns(List<ColumnResult> results) {
    return results == null
        || results.isEmpty()
        || results.stream().anyMatch(result -> result.getStatus() == ColumnStatus.COLUMN_STATUS_OK);
  }

  private static Set<Long> readyColumnIds(List<ColumnResult> results) {
    if (results == null || results.isEmpty()) {
      return Set.of();
    }
    Set<Long> ids = new java.util.HashSet<>();
    for (ColumnResult result : results) {
      if (result.getStatus() == ColumnStatus.COLUMN_STATUS_OK && result.getColumnId() > 0) {
        ids.add(result.getColumnId());
      }
    }
    return ids;
  }

  private static LogicalType parseLogicalType(SchemaColumn column) {
    if (column == null || !column.hasType()) {
      return null;
    }
    try {
      return LogicalTypeProtoAdapter.columnType(column);
    } catch (IllegalArgumentException e) {
      LOG.debugf(e, "Failed to decode logical type for column '%s'", column.getName());
      return null;
    }
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }
}
