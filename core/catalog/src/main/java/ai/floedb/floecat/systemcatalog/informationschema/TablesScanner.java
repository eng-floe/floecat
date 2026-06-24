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

import ai.floedb.floecat.arrow.ArrowSchemaUtil;
import ai.floedb.floecat.arrow.ArrowValueWriters;
import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.columnar.AbstractArrowBatchBuilder;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.ScanOutputFormat;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.spi.SystemScanRequest;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.systemcatalog.informationschema.NamespaceScanSupport.NamespaceEntry;
import java.util.Collections;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;

/** information_schema.tables */
public final class TablesScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          SchemaColumn.newBuilder()
              .setName("table_catalog")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_schema")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_name")
              .setLogicalType("VARCHAR")
              .setFieldId(1)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_type")
              .setLogicalType("VARCHAR")
              .setFieldId(2)
              .setNullable(false)
              .build());

  private static final Schema ARROW_SCHEMA = ArrowSchemaUtil.toArrowSchema(SCHEMA);
  private static final int ARROW_BATCH_SIZE = 512;

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return scan(ctx, SystemScanRequest.empty());
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx, SystemScanRequest request) {
    boolean supportsLightweightRefs = ctx.supportsLightweightRefs();
    List<NamespaceEntry> namespaces = NamespaceScanSupport.entries(ctx, request, "table_schema");
    Map<ResourceId, String> catalogNames = new HashMap<>();

    return namespaces.stream()
        .flatMap(
            ns -> {
              String catalogName =
                  catalogNames.computeIfAbsent(
                      ns.catalogId(), id -> ((CatalogNode) ctx.resolve(id)).displayName());
              if (supportsLightweightRefs) {
                List<TopologyGraph.RelationRef> refs =
                    NamespaceScanSupport.relationRefs(ctx, ns.id(), request, "table_name");
                return refs.stream()
                    .filter(ref -> matchesTableType(request, ref))
                    .map(ref -> rowForRef(catalogName, ns.schemaName(), ref));
              }
              // Fall back to full relation load when the overlay has no lightweight ref source.
              return NamespaceScanSupport.relations(ctx, ns.id(), request, "table_name").stream()
                  .filter(rel -> matchesTableType(request, rel))
                  .map(rel -> rowForRelation(catalogName, ns.schemaName(), rel));
            });
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
    return scanArrow(ctx, SystemScanRequest.of(predicate, requiredColumns), allocator);
  }

  @Override
  public Stream<ColumnarBatch> scanArrow(
      SystemObjectScanContext ctx, SystemScanRequest request, BufferAllocator allocator) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(allocator, "allocator");
    Objects.requireNonNull(request, "request");
    Set<String> requiredSet = ArrowSchemaUtil.normalizeRequiredColumns(request.requiredColumns());
    boolean supportsLightweightRefs = ctx.supportsLightweightRefs();
    Iterator<NamespaceEntry> namespaceIterator =
        NamespaceScanSupport.entries(ctx, request, "table_schema").iterator();
    // Each entry is {name, kind_string} -- populated from lightweight refs (fast, no S3)
    // or full RelationNode objects when the overlay has no lightweight ref source.
    Spliterator<ColumnarBatch> spliterator =
        new Spliterators.AbstractSpliterator<ColumnarBatch>(
            Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL) {
          private final Iterator<NamespaceEntry> namespaceIter = namespaceIterator;
          private Iterator<String[]> entryIterator = Collections.emptyIterator();
          private NamespaceEntry currentNamespace;
          private final Map<ResourceId, String> catalogNames = new HashMap<>();

          @Override
          public boolean tryAdvance(Consumer<? super ColumnarBatch> action) {
            TablesBatchBuilder builder = null;
            try {
              while (true) {
                String[] entry = nextEntry(ctx);
                if (entry != null) {
                  if (builder == null) {
                    builder = new TablesBatchBuilder(allocator, requiredSet);
                  }
                  ResourceId catalogId = currentNamespace.catalogId();
                  String catalogName =
                      catalogNames.computeIfAbsent(
                          catalogId, id -> ((CatalogNode) ctx.resolve(id)).displayName());
                  builder.append(catalogName, currentNamespace.schemaName(), entry[0], entry[1]);
                  if (builder.isFull()) {
                    action.accept(builder.build());
                    return true;
                  }
                  continue;
                }
                if (builder == null || builder.isEmpty()) {
                  return false;
                }
                action.accept(builder.build());
                return true;
              }
            } finally {
              if (builder != null) {
                builder.release();
              }
            }
          }

          private String[] nextEntry(SystemObjectScanContext ctx) {
            while (!entryIterator.hasNext()) {
              if (!namespaceIter.hasNext()) {
                return null;
              }
              currentNamespace = namespaceIter.next();
              if (supportsLightweightRefs) {
                List<TopologyGraph.RelationRef> refs =
                    NamespaceScanSupport.relationRefs(
                        ctx, currentNamespace.id(), request, "table_name");
                entryIterator =
                    refs.stream()
                        .filter(ref -> matchesTableType(request, ref))
                        .map(ref -> new String[] {ref.name(), refKindString(ref.kind())})
                        .iterator();
              } else {
                entryIterator =
                    NamespaceScanSupport.relations(
                            ctx, currentNamespace.id(), request, "table_name")
                        .stream()
                        .filter(rel -> matchesTableType(request, rel))
                        .map(rel -> new String[] {rel.displayName(), relationKind(rel)})
                        .iterator();
              }
            }
            return entryIterator.next();
          }
        };
    return StreamSupport.stream(spliterator, false);
  }

  private static boolean matchesTableType(
      SystemScanRequest request, TopologyGraph.RelationRef ref) {
    return request
        .constraints()
        .values("table_type")
        .map(types -> types.contains(refKindString(ref.kind())))
        .orElse(true);
  }

  private static boolean matchesTableType(SystemScanRequest request, RelationNode node) {
    return request
        .constraints()
        .values("table_type")
        .map(types -> types.contains(relationKind(node)))
        .orElse(true);
  }

  private static SystemObjectRow rowForRelation(
      String catalogName, String schemaName, RelationNode node) {
    return new SystemObjectRow(
        new Object[] {catalogName, schemaName, node.displayName(), relationKind(node)});
  }

  private static SystemObjectRow rowForRef(
      String catalogName, String schemaName, TopologyGraph.RelationRef ref) {
    return new SystemObjectRow(
        new Object[] {catalogName, schemaName, ref.name(), refKindString(ref.kind())});
  }

  private static String refKindString(ResourceKind kind) {
    return switch (kind) {
      case RK_TABLE -> "BASE TABLE";
      case RK_VIEW -> "VIEW";
      default -> "UNKNOWN";
    };
  }

  private static String relationKind(RelationNode node) {
    return switch (node.kind()) {
      case TABLE -> "BASE TABLE";
      case VIEW -> "VIEW";
      default -> "UNKNOWN";
    };
  }

  private static final class TablesBatchBuilder extends AbstractArrowBatchBuilder {

    private final VarCharVector tableCatalog;
    private final VarCharVector tableSchema;
    private final VarCharVector tableName;
    private final VarCharVector tableType;
    private final boolean includeCatalog;
    private final boolean includeSchema;
    private final boolean includeName;
    private final boolean includeType;

    private TablesBatchBuilder(BufferAllocator allocator, Set<String> requiredColumns) {
      super(ARROW_SCHEMA, allocator);
      List<FieldVector> vectors = root().getFieldVectors();
      this.tableCatalog = (VarCharVector) vectors.get(0);
      this.tableSchema = (VarCharVector) vectors.get(1);
      this.tableName = (VarCharVector) vectors.get(2);
      this.tableType = (VarCharVector) vectors.get(3);
      this.includeCatalog = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_catalog");
      this.includeSchema = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_schema");
      this.includeName = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_name");
      this.includeType = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_type");
    }

    private boolean isFull() {
      return rowCount() >= ARROW_BATCH_SIZE;
    }

    private void append(String catalog, String schema, String name, String type) {
      if (includeCatalog) {
        ArrowValueWriters.writeVarChar(tableCatalog, rowCount(), catalog);
      } else {
        tableCatalog.setNull(rowCount());
      }
      if (includeSchema) {
        ArrowValueWriters.writeVarChar(tableSchema, rowCount(), schema);
      } else {
        tableSchema.setNull(rowCount());
      }
      if (includeName) {
        ArrowValueWriters.writeVarChar(tableName, rowCount(), name);
      } else {
        tableName.setNull(rowCount());
      }
      if (includeType) {
        ArrowValueWriters.writeVarChar(tableType, rowCount(), type);
      } else {
        tableType.setNull(rowCount());
      }
      incrementRow();
    }

    private ColumnarBatch build() {
      return super.buildBatch();
    }
  }
}
