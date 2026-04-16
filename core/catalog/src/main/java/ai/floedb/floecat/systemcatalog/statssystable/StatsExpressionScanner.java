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
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;

/** Scanner for {@code sys.stats_expression}. */
public final class StatsExpressionScanner extends AbstractStatsScanner {

  static final List<SchemaColumn> SCHEMA =
      List.of(
          schemaColumn("account_id", "VARCHAR", false),
          schemaColumn("catalog", "VARCHAR", false),
          schemaColumn("schema", "VARCHAR", false),
          schemaColumn("table", "VARCHAR", false),
          schemaColumn("table_id", "VARCHAR", false),
          schemaColumn("snapshot_id", "INT", false),
          schemaColumn("engine_kind", "VARCHAR", false),
          schemaColumn("expression_key", "VARCHAR", false),
          schemaColumn("data_type", "VARCHAR", true),
          schemaColumn("value_count", "INT", true),
          schemaColumn("null_count", "INT", true),
          schemaColumn("nan_count", "INT", true),
          schemaColumn("distinct_count", "DOUBLE", true),
          schemaColumn("min_value", "VARCHAR", true),
          schemaColumn("max_value", "VARCHAR", true),
          schemaColumn("histogram_json", "VARCHAR", true),
          schemaColumn("completeness", "VARCHAR", true),
          schemaColumn("provenance", "VARCHAR", true),
          schemaColumn("confidence", "DOUBLE", true),
          schemaColumn("capture_time", "TIMESTAMPTZ", true),
          schemaColumn("refresh_time", "TIMESTAMPTZ", true),
          schemaColumn("rows_seen_count", "INT", true),
          schemaColumn("files_seen_count", "INT", true),
          schemaColumn("row_groups_seen_count", "INT", true));

  private static final Schema ARROW_SCHEMA = ArrowSchemaUtil.toArrowSchema(SCHEMA);

  @Override
  /** Declares that this scanner reads expression-target stats records. */
  protected TargetType targetType() {
    return TargetType.EXPRESSION;
  }

  @Override
  /** Returns the public schema for {@code sys.stats_expression}. */
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  /** Allocates a batch builder for Arrow output rows. */
  protected AbstractArrowBatchBuilder newBatchBuilder(
      BufferAllocator allocator, Set<String> requiredColumns) {
    return new ExpressionBatchBuilder(allocator, requiredColumns);
  }

  @Override
  /** Appends one projected row into the current Arrow batch. */
  protected void appendArrowRow(AbstractArrowBatchBuilder builder, StatsScanRecord row) {
    ((ExpressionBatchBuilder) builder).append(row);
  }

  @Override
  /** Builds the row-oriented representation for {@code sys.stats_expression}. */
  protected SystemObjectRow toRow(StatsScanRecord row) {
    ScalarProjection scalar = scalarProjection(row.record());
    MetadataCoverageProjection metadata = metadataCoverageProjection(row.record());
    return new SystemObjectRow(
        new Object[] {
          row.accountId(),
          row.catalog(),
          row.schema(),
          row.table(),
          row.tableId(),
          row.snapshotId(),
          row.record().getTarget().getExpression().getEngineKind(),
          base64Url(row.record().getTarget().getExpression().getEngineExpressionKey()),
          scalar.dataType(),
          scalar.valueCount(),
          scalar.nullCount(),
          scalar.nanCount(),
          scalar.distinctCount(),
          scalar.minValue(),
          scalar.maxValue(),
          scalar.histogramJson(),
          metadata.completeness(),
          metadata.provenance(),
          metadata.confidence(),
          metadata.captureTime(),
          metadata.refreshTime(),
          metadata.rowsSeenCount(),
          metadata.filesSeenCount(),
          metadata.rowGroupsSeenCount()
        });
  }

  private static final class ExpressionBatchBuilder extends AbstractArrowBatchBuilder {
    private final VarCharVector accountId;
    private final VarCharVector catalog;
    private final VarCharVector schema;
    private final VarCharVector table;
    private final VarCharVector tableId;
    private final BigIntVector snapshotId;
    private final VarCharVector engineKind;
    private final VarCharVector expressionKey;
    private final VarCharVector dataType;
    private final BigIntVector valueCount;
    private final BigIntVector nullCount;
    private final BigIntVector nanCount;
    private final Float8Vector distinctCount;
    private final VarCharVector minValue;
    private final VarCharVector maxValue;
    private final VarCharVector histogramJson;
    private final VarCharVector completeness;
    private final VarCharVector provenance;
    private final Float8Vector confidence;
    private final TimeStampMicroTZVector captureTime;
    private final TimeStampMicroTZVector refreshTime;
    private final BigIntVector rowsSeen;
    private final BigIntVector filesSeen;
    private final BigIntVector rowGroupsSeen;

    private final boolean includeAccountId;
    private final boolean includeCatalog;
    private final boolean includeSchema;
    private final boolean includeTable;
    private final boolean includeTableId;
    private final boolean includeSnapshotId;
    private final boolean includeEngineKind;
    private final boolean includeExpressionKey;
    private final boolean includeDataType;
    private final boolean includeValueCount;
    private final boolean includeNullCount;
    private final boolean includeNanCount;
    private final boolean includeDistinctCount;
    private final boolean includeMinValue;
    private final boolean includeMaxValue;
    private final boolean includeHistogramJson;
    private final boolean includeCompleteness;
    private final boolean includeProvenance;
    private final boolean includeConfidence;
    private final boolean includeCaptureTime;
    private final boolean includeRefreshTime;
    private final boolean includeRowsSeen;
    private final boolean includeFilesSeen;
    private final boolean includeRowGroupsSeen;

    /** Initializes vectors and projection flags once per emitted Arrow batch. */
    private ExpressionBatchBuilder(BufferAllocator allocator, Set<String> requiredColumns) {
      super(ARROW_SCHEMA, allocator);
      List<FieldVector> vectors = root().getFieldVectors();
      this.accountId = (VarCharVector) vectors.get(0);
      this.catalog = (VarCharVector) vectors.get(1);
      this.schema = (VarCharVector) vectors.get(2);
      this.table = (VarCharVector) vectors.get(3);
      this.tableId = (VarCharVector) vectors.get(4);
      this.snapshotId = (BigIntVector) vectors.get(5);
      this.engineKind = (VarCharVector) vectors.get(6);
      this.expressionKey = (VarCharVector) vectors.get(7);
      this.dataType = (VarCharVector) vectors.get(8);
      this.valueCount = (BigIntVector) vectors.get(9);
      this.nullCount = (BigIntVector) vectors.get(10);
      this.nanCount = (BigIntVector) vectors.get(11);
      this.distinctCount = (Float8Vector) vectors.get(12);
      this.minValue = (VarCharVector) vectors.get(13);
      this.maxValue = (VarCharVector) vectors.get(14);
      this.histogramJson = (VarCharVector) vectors.get(15);
      this.completeness = (VarCharVector) vectors.get(16);
      this.provenance = (VarCharVector) vectors.get(17);
      this.confidence = (Float8Vector) vectors.get(18);
      this.captureTime = (TimeStampMicroTZVector) vectors.get(19);
      this.refreshTime = (TimeStampMicroTZVector) vectors.get(20);
      this.rowsSeen = (BigIntVector) vectors.get(21);
      this.filesSeen = (BigIntVector) vectors.get(22);
      this.rowGroupsSeen = (BigIntVector) vectors.get(23);

      this.includeAccountId = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "account_id");
      this.includeCatalog = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "catalog");
      this.includeSchema = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "schema");
      this.includeTable = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table");
      this.includeTableId = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "table_id");
      this.includeSnapshotId = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "snapshot_id");
      this.includeEngineKind = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "engine_kind");
      this.includeExpressionKey =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "expression_key");
      this.includeDataType = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "data_type");
      this.includeValueCount = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "value_count");
      this.includeNullCount = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "null_count");
      this.includeNanCount = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "nan_count");
      this.includeDistinctCount =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "distinct_count");
      this.includeMinValue = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "min_value");
      this.includeMaxValue = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "max_value");
      this.includeHistogramJson =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "histogram_json");
      this.includeCompleteness =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "completeness");
      this.includeProvenance = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "provenance");
      this.includeConfidence = ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "confidence");
      this.includeCaptureTime =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "capture_time");
      this.includeRefreshTime =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "refresh_time");
      this.includeRowsSeen =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "rows_seen_count");
      this.includeFilesSeen =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "files_seen_count");
      this.includeRowGroupsSeen =
          ArrowSchemaUtil.shouldIncludeColumn(requiredColumns, "row_groups_seen_count");
    }

    /** Writes one expression stats row into the batch. */
    private void append(StatsScanRecord row) {
      int rowIndex = rowCount();
      writeText(accountId, includeAccountId, rowIndex, row.accountId());
      writeText(catalog, includeCatalog, rowIndex, row.catalog());
      writeText(schema, includeSchema, rowIndex, row.schema());
      writeText(table, includeTable, rowIndex, row.table());
      writeText(tableId, includeTableId, rowIndex, row.tableId());
      writeLong(snapshotId, includeSnapshotId, rowIndex, row.snapshotId());
      writeText(
          engineKind,
          includeEngineKind,
          rowIndex,
          row.record().getTarget().getExpression().getEngineKind());
      writeText(
          expressionKey,
          includeExpressionKey,
          rowIndex,
          base64Url(row.record().getTarget().getExpression().getEngineExpressionKey()));
      writeText(dataType, includeDataType, rowIndex, scalarDataType(row.record()));
      writeOptionalLong(valueCount, includeValueCount, rowIndex, scalarValueCount(row.record()));
      writeOptionalLong(nullCount, includeNullCount, rowIndex, scalarNullCount(row.record()));
      writeOptionalLong(nanCount, includeNanCount, rowIndex, scalarNanCount(row.record()));
      writeOptionalDouble(
          distinctCount, includeDistinctCount, rowIndex, scalarDistinctCount(row.record()));
      writeText(minValue, includeMinValue, rowIndex, scalarMinValue(row.record()));
      writeText(maxValue, includeMaxValue, rowIndex, scalarMaxValue(row.record()));
      writeText(histogramJson, includeHistogramJson, rowIndex, scalarHistogram(row.record()));
      writeText(completeness, includeCompleteness, rowIndex, metadataCompleteness(row.record()));
      writeText(provenance, includeProvenance, rowIndex, metadataProvenance(row.record()));
      writeOptionalDouble(
          confidence, includeConfidence, rowIndex, metadataConfidence(row.record()));
      writeTimestamp(captureTime, includeCaptureTime, rowIndex, metadataCaptureTime(row.record()));
      writeTimestamp(refreshTime, includeRefreshTime, rowIndex, metadataRefreshTime(row.record()));
      writeOptionalLong(rowsSeen, includeRowsSeen, rowIndex, coverageRowsSeen(row.record()));
      writeOptionalLong(filesSeen, includeFilesSeen, rowIndex, coverageFilesSeen(row.record()));
      writeOptionalLong(
          rowGroupsSeen, includeRowGroupsSeen, rowIndex, coverageRowGroupsSeen(row.record()));
      incrementRow();
    }
  }
}
