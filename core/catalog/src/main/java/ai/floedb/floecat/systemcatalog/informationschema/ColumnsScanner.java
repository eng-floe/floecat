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

package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.columnar.AbstractArrowBatchBuilder;
import ai.floedb.floecat.systemcatalog.columnar.ArrowSchemaUtil;
import ai.floedb.floecat.systemcatalog.columnar.ArrowValueWriters;
import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import ai.floedb.floecat.systemcatalog.spi.scanner.ScanOutputFormat;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;

/** information_schema.columns */
public final class ColumnsScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          SchemaColumn.newBuilder()
              .setName("table_catalog")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(true)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_schema")
              .setLogicalType("VARCHAR")
              .setFieldId(1)
              .setNullable(true)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_name")
              .setLogicalType("VARCHAR")
              .setFieldId(2)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("column_name")
              .setLogicalType("VARCHAR")
              .setFieldId(3)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("data_type")
              .setLogicalType("VARCHAR")
              .setFieldId(4)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("ordinal_position")
              .setLogicalType("INT")
              .setFieldId(5)
              .setNullable(false)
              .build());

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  private static final Schema ARROW_SCHEMA = ArrowSchemaUtil.toArrowSchema(SCHEMA);
  private static final int ARROW_BATCH_SIZE = 512;

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    // Small per-scan caches (cheap, bounded)
    Map<ResourceId, String> catalogNames = new HashMap<>();

    return ctx.listNamespaces().stream()
        .flatMap(ns -> ctx.listRelations(ns.id()).stream())
        .flatMap(node -> scanRelation(ctx, node, catalogNames));
  }

  @Override
  public EnumSet<ScanOutputFormat> supportedFormats() {
    return EnumSet.of(ScanOutputFormat.ROWS, ScanOutputFormat.ARROW_IPC);
  }

  @Override
  public Stream<ColumnarBatch> scanArrow(
      SystemObjectScanContext ctx,
      Expr predicate,
      List<String> requiredColumns,
      BufferAllocator allocator) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(allocator, "allocator");
    Objects.requireNonNull(requiredColumns, "requiredColumns");
    Set<String> requiredSet = ArrowSchemaUtil.normalizeRequiredColumns(requiredColumns);
    List<NamespaceNode> namespaces = ctx.listNamespaces();
    Iterator<NamespaceNode> namespaceIterator = namespaces.iterator();
    Spliterator<ColumnarBatch> spliterator =
        new Spliterators.AbstractSpliterator<ColumnarBatch>(
            Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL) {
          private final Iterator<NamespaceNode> nsIter = namespaceIterator;
          private Iterator<GraphNode> relationIter = Collections.emptyIterator();
          private Iterator<ColumnEntry> columnIter = Collections.emptyIterator();
          private NamespaceNode currentNamespace;
          private final Map<ResourceId, String> catalogNames = new HashMap<>();

          @Override
          public boolean tryAdvance(Consumer<? super ColumnarBatch> action) {
            ColumnsBatchBuilder builder = null;
            try {
              while (true) {
                if (columnIter.hasNext()) {
                  if (builder == null) {
                    builder = new ColumnsBatchBuilder(allocator, requiredSet);
                  }
                  builder.append(columnIter.next());
                  if (builder.isFull()) {
                    action.accept(builder.build());
                    return true;
                  }
                  continue;
                }
                GraphNode relation = nextRelation(ctx);
                if (relation == null) {
                  if (builder == null || builder.isEmpty()) {
                    return false;
                  }
                  action.accept(builder.build());
                  return true;
                }
                columnIter =
                    columnsForRelation(ctx, currentNamespace, relation, catalogNames).iterator();
              }
            } finally {
              if (builder != null) {
                builder.release();
              }
            }
          }

          private GraphNode nextRelation(SystemObjectScanContext ctx) {
            while (!relationIter.hasNext()) {
              if (!nsIter.hasNext()) {
                return null;
              }
              currentNamespace = nsIter.next();
              relationIter = ctx.listRelations(currentNamespace.id()).iterator();
            }
            return relationIter.next();
          }
        };
    return StreamSupport.stream(spliterator, false);
  }

  private Stream<SystemObjectRow> scanRelation(
      SystemObjectScanContext ctx, GraphNode node, Map<ResourceId, String> catalogNames) {

    if (node instanceof TableNode table) {
      return scanTable(ctx, table, catalogNames);
    }

    if (node instanceof ViewNode view) {
      return scanView(ctx, view, catalogNames);
    }

    return Stream.empty();
  }

  private Stream<SystemObjectRow> scanTable(
      SystemObjectScanContext ctx, TableNode table, Map<ResourceId, String> catalogNames) {

    NamespaceNode namespace = (NamespaceNode) ctx.resolve(table.namespaceId());
    ResourceId catalogId = namespace.catalogId();

    String catalogName =
        catalogNames.computeIfAbsent(
            catalogId, id -> ((CatalogNode) ctx.resolve(id)).displayName());

    String schemaName = schemaName(namespace);

    List<SchemaColumn> columns = ctx.graph().tableSchema(table.id());

    if (table instanceof UserTableNode ut) {
      return columns.stream()
          .sorted(Comparator.comparingInt(SchemaColumn::getFieldId))
          .map(
              col ->
                  new SystemObjectRow(
                      new Object[] {
                        catalogName,
                        schemaName,
                        table.displayName(),
                        col.getName(),
                        blankToNull(col.getLogicalType()),
                        col.getFieldId()
                      }));
    }

    // System tables (no field ids) – preserve declared order
    List<SystemObjectRow> rows = new ArrayList<>(columns.size());
    int ordinal = 1;
    for (SchemaColumn col : columns) {
      rows.add(
          new SystemObjectRow(
              new Object[] {
                catalogName,
                schemaName,
                table.displayName(),
                col.getName(),
                blankToNull(col.getLogicalType()),
                ordinal++
              }));
    }
    return rows.stream();
  }

  private Stream<SystemObjectRow> scanView(
      SystemObjectScanContext ctx, ViewNode view, Map<ResourceId, String> catalogNames) {

    NamespaceNode namespace = (NamespaceNode) ctx.resolve(view.namespaceId());
    ResourceId catalogId = namespace.catalogId();

    String catalogName =
        catalogNames.computeIfAbsent(
            catalogId, id -> ((CatalogNode) ctx.resolve(id)).displayName());

    String schemaName = schemaName(namespace);
    List<SchemaColumn> cols = view.outputColumns();

    List<SystemObjectRow> rows = new ArrayList<>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      SchemaColumn col = cols.get(i);
      rows.add(
          new SystemObjectRow(
              new Object[] {
                catalogName,
                schemaName,
                view.displayName(),
                col.getName(),
                col.getLogicalType(),
                i + 1
              }));
    }
    return rows.stream();
  }

  private static String schemaName(NamespaceNode namespace) {
    List<String> segments = new ArrayList<>(namespace.pathSegments());
    if (!namespace.displayName().isBlank()) {
      segments.add(namespace.displayName());
    }
    return String.join(".", segments);
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private List<ColumnEntry> columnsForRelation(
      SystemObjectScanContext ctx,
      NamespaceNode namespace,
      GraphNode node,
      Map<ResourceId, String> catalogNames) {
    String catalogName =
        catalogNames.computeIfAbsent(
            namespace.catalogId(), id -> ((CatalogNode) ctx.resolve(id)).displayName());
    String schemaName = schemaName(namespace);

    if (node instanceof TableNode table) {
      List<SchemaColumn> columns = ctx.graph().tableSchema(table.id());
      if (table instanceof UserTableNode) {
        return columns.stream()
            .sorted(Comparator.comparingInt(SchemaColumn::getFieldId))
            .map(
                col ->
                    ColumnEntry.of(
                        catalogName,
                        schemaName,
                        table.displayName(),
                        col.getName(),
                        blankToNull(col.getLogicalType()),
                        col.getFieldId()))
            .toList();
      }
      List<ColumnEntry> entries = new ArrayList<>(columns.size());
      int ordinal = 1;
      for (SchemaColumn col : columns) {
        entries.add(
            ColumnEntry.of(
                catalogName,
                schemaName,
                table.displayName(),
                col.getName(),
                blankToNull(col.getLogicalType()),
                ordinal++));
      }
      return entries;
    }

    if (node instanceof ViewNode view) {
      List<SchemaColumn> cols = view.outputColumns();
      List<ColumnEntry> entries = new ArrayList<>(cols.size());
      for (int i = 0; i < cols.size(); i++) {
        SchemaColumn col = cols.get(i);
        entries.add(
            ColumnEntry.of(
                catalogName,
                schemaName,
                view.displayName(),
                col.getName(),
                col.getLogicalType(),
                i + 1));
      }
      return entries;
    }

    return List.of();
  }

  private static final class ColumnEntry {

    final String tableCatalog;
    final String tableSchema;
    final String tableName;
    final String columnName;
    final String dataType;
    final int ordinalPosition;

    private ColumnEntry(
        String tableCatalog,
        String tableSchema,
        String tableName,
        String columnName,
        String dataType,
        int ordinalPosition) {
      this.tableCatalog = tableCatalog;
      this.tableSchema = tableSchema;
      this.tableName = tableName;
      this.columnName = columnName;
      this.dataType = dataType;
      this.ordinalPosition = ordinalPosition;
    }

    static ColumnEntry of(
        String catalog, String schema, String table, String column, String dataType, int ordinal) {
      return new ColumnEntry(catalog, schema, table, column, dataType, ordinal);
    }
  }

  private static final class ColumnsBatchBuilder extends AbstractArrowBatchBuilder {

    private final VarCharVector tableCatalog;
    private final VarCharVector tableSchema;
    private final VarCharVector tableName;
    private final VarCharVector columnName;
    private final VarCharVector dataType;
    private final IntVector ordinalPosition;
    private final boolean includeCatalog;
    private final boolean includeSchema;
    private final boolean includeTable;
    private final boolean includeColumn;
    private final boolean includeType;
    private final boolean includeOrdinal;

    private ColumnsBatchBuilder(BufferAllocator allocator, Set<String> requiredColumns) {
      super(ARROW_SCHEMA, allocator);
      List<FieldVector> vectors = root().getFieldVectors();
      this.tableCatalog = (VarCharVector) vectors.get(0);
      this.tableSchema = (VarCharVector) vectors.get(1);
      this.tableName = (VarCharVector) vectors.get(2);
      this.columnName = (VarCharVector) vectors.get(3);
      this.dataType = (VarCharVector) vectors.get(4);
      this.ordinalPosition = (IntVector) vectors.get(5);
      this.includeCatalog = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_catalog");
      this.includeSchema = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_schema");
      this.includeTable = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_name");
      this.includeColumn = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "column_name");
      this.includeType = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "data_type");
      this.includeOrdinal =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "ordinal_position");
    }

    private boolean isFull() {
      return rowCount() >= ARROW_BATCH_SIZE;
    }

    private void append(ColumnEntry entry) {
      if (includeCatalog) {
        ArrowValueWriters.writeVarChar(tableCatalog, rowCount(), entry.tableCatalog);
      } else {
        tableCatalog.setNull(rowCount());
      }
      if (includeSchema) {
        ArrowValueWriters.writeVarChar(tableSchema, rowCount(), entry.tableSchema);
      } else {
        tableSchema.setNull(rowCount());
      }
      if (includeTable) {
        ArrowValueWriters.writeVarChar(tableName, rowCount(), entry.tableName);
      } else {
        tableName.setNull(rowCount());
      }
      if (includeColumn) {
        ArrowValueWriters.writeVarChar(columnName, rowCount(), entry.columnName);
      } else {
        columnName.setNull(rowCount());
      }
      if (includeType) {
        ArrowValueWriters.writeVarChar(dataType, rowCount(), entry.dataType);
      } else {
        dataType.setNull(rowCount());
      }
      if (includeOrdinal) {
        ordinalPosition.setSafe(rowCount(), entry.ordinalPosition);
      } else {
        ordinalPosition.setNull(rowCount());
      }
      incrementRow();
    }

    private ColumnarBatch build() {
      return super.buildBatch();
    }
  }
}
