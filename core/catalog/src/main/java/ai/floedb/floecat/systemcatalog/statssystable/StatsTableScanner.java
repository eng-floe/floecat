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

package ai.floedb.floecat.systemcatalog.statssystable;

import ai.floedb.floecat.arrow.ArrowSchemaUtil;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.columnar.AbstractArrowBatchBuilder;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;

/** Scanner for {@code sys.stats_table}. */
public final class StatsTableScanner extends AbstractStatsScanner {

  static final List<SchemaColumn> SCHEMA =
      List.of(
          column("account_id", "VARCHAR", false),
          column("catalog", "VARCHAR", false),
          column("schema", "VARCHAR", false),
          column("table", "VARCHAR", false),
          column("table_id", "VARCHAR", false),
          column("snapshot_id", "INT", false),
          column("row_count", "INT", true),
          column("file_count", "INT", true),
          column("total_bytes", "INT", true),
          column("completeness", "VARCHAR", true),
          column("provenance", "VARCHAR", true),
          column("confidence", "DOUBLE", true),
          column("capture_time", "TIMESTAMPTZ", true),
          column("refresh_time", "TIMESTAMPTZ", true));

  private static final Schema ARROW_SCHEMA = ArrowSchemaUtil.toArrowSchema(SCHEMA);

  @Override
  protected TargetType targetType() {
    return TargetType.TABLE;
  }

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  protected AbstractArrowBatchBuilder newBatchBuilder(
      BufferAllocator allocator, Set<String> requiredColumns) {
    return new TableBatchBuilder(allocator, requiredColumns);
  }

  @Override
  protected void appendArrowRow(AbstractArrowBatchBuilder builder, StatsScanRecord row) {
    ((TableBatchBuilder) builder).append(row);
  }

  @Override
  protected SystemObjectRow toRow(StatsScanRecord row) {
    return new SystemObjectRow(
        new Object[] {
          row.accountId(),
          row.catalog(),
          row.schema(),
          row.table(),
          row.tableId(),
          row.snapshotId(),
          tableRowCount(row).isPresent() ? tableRowCount(row).getAsLong() : null,
          tableFileCount(row).isPresent() ? tableFileCount(row).getAsLong() : null,
          tableTotalBytes(row).isPresent() ? tableTotalBytes(row).getAsLong() : null,
          metadataCompleteness(row.record()),
          metadataProvenance(row.record()),
          metadataConfidence(row.record()).isPresent()
              ? metadataConfidence(row.record()).getAsDouble()
              : null,
          metadataCaptureTime(row.record()),
          metadataRefreshTime(row.record())
        });
  }

  private static OptionalLong tableRowCount(StatsScanRecord row) {
    if (!row.record().hasTable()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(row.record().getTable().getRowCount());
  }

  private static OptionalLong tableFileCount(StatsScanRecord row) {
    if (!row.record().hasTable()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(row.record().getTable().getDataFileCount());
  }

  private static OptionalLong tableTotalBytes(StatsScanRecord row) {
    if (!row.record().hasTable()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(row.record().getTable().getTotalSizeBytes());
  }

  private static SchemaColumn column(String name, String logicalType, boolean nullable) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setLogicalType(logicalType)
        .setNullable(nullable)
        .build();
  }

  private static final class TableBatchBuilder extends AbstractArrowBatchBuilder {
    private final VarCharVector accountId;
    private final VarCharVector catalog;
    private final VarCharVector schema;
    private final VarCharVector table;
    private final VarCharVector tableId;
    private final BigIntVector snapshotId;
    private final BigIntVector rowCount;
    private final BigIntVector fileCount;
    private final BigIntVector totalBytes;
    private final VarCharVector completeness;
    private final VarCharVector provenance;
    private final Float8Vector confidence;
    private final TimeStampMicroTZVector captureTime;
    private final TimeStampMicroTZVector refreshTime;

    private final boolean includeAccountId;
    private final boolean includeCatalog;
    private final boolean includeSchema;
    private final boolean includeTable;
    private final boolean includeTableId;
    private final boolean includeSnapshotId;
    private final boolean includeRowCount;
    private final boolean includeFileCount;
    private final boolean includeTotalBytes;
    private final boolean includeCompleteness;
    private final boolean includeProvenance;
    private final boolean includeConfidence;
    private final boolean includeCaptureTime;
    private final boolean includeRefreshTime;

    private TableBatchBuilder(BufferAllocator allocator, Set<String> requiredColumns) {
      super(ARROW_SCHEMA, allocator);
      List<FieldVector> vectors = root().getFieldVectors();
      this.accountId = (VarCharVector) vectors.get(0);
      this.catalog = (VarCharVector) vectors.get(1);
      this.schema = (VarCharVector) vectors.get(2);
      this.table = (VarCharVector) vectors.get(3);
      this.tableId = (VarCharVector) vectors.get(4);
      this.snapshotId = (BigIntVector) vectors.get(5);
      this.rowCount = (BigIntVector) vectors.get(6);
      this.fileCount = (BigIntVector) vectors.get(7);
      this.totalBytes = (BigIntVector) vectors.get(8);
      this.completeness = (VarCharVector) vectors.get(9);
      this.provenance = (VarCharVector) vectors.get(10);
      this.confidence = (Float8Vector) vectors.get(11);
      this.captureTime = (TimeStampMicroTZVector) vectors.get(12);
      this.refreshTime = (TimeStampMicroTZVector) vectors.get(13);

      this.includeAccountId = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "account_id");
      this.includeCatalog = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "catalog");
      this.includeSchema = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "schema");
      this.includeTable = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table");
      this.includeTableId = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_id");
      this.includeSnapshotId = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "snapshot_id");
      this.includeRowCount = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "row_count");
      this.includeFileCount = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "file_count");
      this.includeTotalBytes = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "total_bytes");
      this.includeCompleteness =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "completeness");
      this.includeProvenance = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "provenance");
      this.includeConfidence = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "confidence");
      this.includeCaptureTime =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "capture_time");
      this.includeRefreshTime =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "refresh_time");
    }

    private void append(StatsScanRecord row) {
      int rowIndex = rowCount();
      writeText(accountId, includeAccountId, rowIndex, row.accountId());
      writeText(catalog, includeCatalog, rowIndex, row.catalog());
      writeText(schema, includeSchema, rowIndex, row.schema());
      writeText(table, includeTable, rowIndex, row.table());
      writeText(tableId, includeTableId, rowIndex, row.tableId());
      writeLong(snapshotId, includeSnapshotId, rowIndex, row.snapshotId());
      writeOptionalLong(rowCount, includeRowCount, rowIndex, tableRowCount(row));
      writeOptionalLong(fileCount, includeFileCount, rowIndex, tableFileCount(row));
      writeOptionalLong(totalBytes, includeTotalBytes, rowIndex, tableTotalBytes(row));
      writeText(completeness, includeCompleteness, rowIndex, metadataCompleteness(row.record()));
      writeText(provenance, includeProvenance, rowIndex, metadataProvenance(row.record()));
      writeOptionalDouble(
          confidence, includeConfidence, rowIndex, metadataConfidence(row.record()));
      writeTimestamp(captureTime, includeCaptureTime, rowIndex, metadataCaptureTime(row.record()));
      writeTimestamp(refreshTime, includeRefreshTime, rowIndex, metadataRefreshTime(row.record()));
      incrementRow();
    }
  }
}
