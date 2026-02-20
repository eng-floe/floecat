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
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.columnar.AbstractArrowBatchBuilder;
import ai.floedb.floecat.arrow.ArrowSchemaUtil;
import ai.floedb.floecat.arrow.ArrowValueWriters;
import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.ScanOutputFormat;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import java.util.ArrayList;
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

/** information_schema.schemata */
public final class SchemataScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          SchemaColumn.newBuilder()
              .setName("catalog_name")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("schema_name")
              .setLogicalType("VARCHAR")
              .setFieldId(1)
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
    Map<ResourceId, String> catalogById = new HashMap<>();

    return ctx.listNamespaces().stream()
        .map(
            ns ->
                new SystemObjectRow(
                    new Object[] {
                      catalogById.computeIfAbsent(
                          ns.catalogId(), id -> ((CatalogNode) ctx.resolve(id)).displayName()),
                      schemaName(ns)
                    }));
  }

  private static String schemaName(NamespaceNode namespace) {
    List<String> segments = new ArrayList<>(namespace.pathSegments());
    if (!namespace.displayName().isBlank()) {
      segments.add(namespace.displayName());
    }
    return String.join(".", segments);
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
          private final Map<ResourceId, String> catalogNames = new HashMap<>();

          @Override
          public boolean tryAdvance(Consumer<? super ColumnarBatch> action) {
            SchemataBatchBuilder builder = null;
            try {
              while (true) {
                if (!nsIter.hasNext()) {
                  if (builder == null || builder.isEmpty()) {
                    return false;
                  }
                  action.accept(builder.build());
                  return true;
                }
                if (builder == null) {
                  builder = new SchemataBatchBuilder(allocator, requiredSet);
                }
                NamespaceNode namespace = nsIter.next();
                ResourceId catalogId = namespace.catalogId();
                String catalogName =
                    catalogNames.computeIfAbsent(
                        catalogId, id -> ((CatalogNode) ctx.resolve(id)).displayName());
                builder.append(catalogName, schemaName(namespace));
                if (builder.isFull()) {
                  action.accept(builder.build());
                  return true;
                }
              }
            } finally {
              if (builder != null) {
                builder.release();
              }
            }
          }
        };
    return StreamSupport.stream(spliterator, false);
  }

  private static final class SchemataBatchBuilder extends AbstractArrowBatchBuilder {

    private final VarCharVector catalogName;
    private final VarCharVector schemaName;
    private final boolean includeCatalog;
    private final boolean includeSchema;

    private SchemataBatchBuilder(BufferAllocator allocator, Set<String> requiredColumns) {
      super(ARROW_SCHEMA, allocator);
      List<FieldVector> vectors = root().getFieldVectors();
      this.catalogName = (VarCharVector) vectors.get(0);
      this.schemaName = (VarCharVector) vectors.get(1);
      this.includeCatalog = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "catalog_name");
      this.includeSchema = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "schema_name");
    }

    private boolean isFull() {
      return rowCount() >= ARROW_BATCH_SIZE;
    }

    private void append(String catalog, String schema) {
      if (includeCatalog) {
        ArrowValueWriters.writeVarChar(catalogName, rowCount(), catalog);
      } else {
        catalogName.setNull(rowCount());
      }
      if (includeSchema) {
        ArrowValueWriters.writeVarChar(schemaName, rowCount(), schema);
      } else {
        schemaName.setNull(rowCount());
      }
      incrementRow();
    }

    private ColumnarBatch build() {
      return super.buildBatch();
    }
  }
}
