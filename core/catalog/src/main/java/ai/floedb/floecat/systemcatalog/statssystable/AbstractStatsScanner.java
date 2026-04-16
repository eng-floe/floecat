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
import ai.floedb.floecat.arrow.ArrowValueWriters;
import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.columnar.AbstractArrowBatchBuilder;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.ScanOutputFormat;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;

/** Shared base scanner for sys.stats_* system tables. */
abstract class AbstractStatsScanner implements SystemObjectScanner {
  static final int STATS_BATCH_SIZE = 4096;
  static final int STORE_PAGE_SIZE = 256;

  enum TargetType {
    TABLE("TABLE"),
    COLUMN("COLUMN"),
    EXPRESSION("EXPRESSION");

    private final String wireValue;

    TargetType(String wireValue) {
      this.wireValue = wireValue;
    }

    String wireValue() {
      return wireValue;
    }
  }

  protected record StatsScanRecord(
      String accountId,
      String catalog,
      String schema,
      String table,
      String tableId,
      long snapshotId,
      TargetStatsRecord record) {}

  protected record ScalarProjection(
      String dataType,
      Long valueCount,
      Long nullCount,
      Long nanCount,
      Double distinctCount,
      String minValue,
      String maxValue,
      String histogramJson) {}

  protected record MetadataCoverageProjection(
      String completeness,
      String provenance,
      Double confidence,
      Timestamp captureTime,
      Timestamp refreshTime,
      Long rowsSeenCount,
      Long filesSeenCount,
      Long rowGroupsSeenCount) {}

  protected static SchemaColumn schemaColumn(String name, String logicalType, boolean nullable) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setLogicalType(logicalType)
        .setNullable(nullable)
        .build();
  }

  record StatsPushdown(
      boolean impossible,
      boolean tableFiltered,
      Set<String> tableIds,
      OptionalLong snapshotId,
      OptionalLong columnId,
      Optional<String> engineKind) {

    static final StatsPushdown NONE =
        new StatsPushdown(
            false, false, Set.of(), OptionalLong.empty(), OptionalLong.empty(), Optional.empty());

    static StatsPushdown parse(Expr expr) {
      if (expr == null) {
        return NONE;
      }
      return switch (expr) {
        case Expr.And and -> mergeAnd(parse(and.left()), parse(and.right()));
        case Expr.Eq eq -> parseEq(eq.left(), eq.right());
        default -> NONE;
      };
    }

    private static StatsPushdown parseEq(Expr left, Expr right) {
      ColumnLiteral pair = asColumnLiteral(left, right);
      if (pair == null) {
        return NONE;
      }
      String column = pair.column().toLowerCase(Locale.ROOT);
      return switch (column) {
        case "table_id" ->
            new StatsPushdown(
                false,
                true,
                Set.of(pair.literal()),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty());
        case "snapshot_id" -> buildLongConstraint(pair.literal(), true);
        case "column_id" -> buildLongConstraint(pair.literal(), false);
        case "engine_kind" ->
            new StatsPushdown(
                false,
                false,
                Set.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.of(pair.literal()));
        default -> NONE;
      };
    }

    private static StatsPushdown mergeAnd(StatsPushdown left, StatsPushdown right) {
      if (left.impossible || right.impossible) {
        return impossiblePushdown();
      }
      boolean tableFiltered = left.tableFiltered || right.tableFiltered;
      Set<String> tableIds = mergeTableIds(left, right);
      if (tableFiltered && tableIds.isEmpty()) {
        return impossiblePushdown();
      }
      OptionalLong snapshotId = mergeLongConstraint(left.snapshotId, right.snapshotId);
      if (snapshotId == null) {
        return impossiblePushdown();
      }
      OptionalLong columnId = mergeLongConstraint(left.columnId, right.columnId);
      if (columnId == null) {
        return impossiblePushdown();
      }
      Optional<String> engineKind = mergeStringConstraint(left.engineKind, right.engineKind);
      if (engineKind == null) {
        return impossiblePushdown();
      }
      return new StatsPushdown(false, tableFiltered, tableIds, snapshotId, columnId, engineKind);
    }

    private static StatsPushdown impossiblePushdown() {
      return new StatsPushdown(
          true, false, Set.of(), OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
    }

    private static StatsPushdown buildLongConstraint(String literal, boolean snapshot) {
      OptionalLong parsed = parseLong(literal);
      if (parsed.isEmpty()) {
        return NONE;
      }
      return snapshot
          ? new StatsPushdown(
              false,
              false,
              Set.of(),
              OptionalLong.of(parsed.getAsLong()),
              OptionalLong.empty(),
              Optional.empty())
          : new StatsPushdown(
              false,
              false,
              Set.of(),
              OptionalLong.empty(),
              OptionalLong.of(parsed.getAsLong()),
              Optional.empty());
    }

    private static Set<String> mergeTableIds(StatsPushdown left, StatsPushdown right) {
      if (left.tableFiltered && right.tableFiltered) {
        Set<String> intersection = new HashSet<>(left.tableIds);
        intersection.retainAll(right.tableIds);
        return Set.copyOf(intersection);
      }
      if (left.tableFiltered) {
        return left.tableIds;
      }
      if (right.tableFiltered) {
        return right.tableIds;
      }
      return Set.of();
    }

    private static OptionalLong mergeLongConstraint(OptionalLong left, OptionalLong right) {
      if (left.isPresent() && right.isPresent() && left.getAsLong() != right.getAsLong()) {
        return null;
      }
      return left.isPresent() ? left : right;
    }

    private static Optional<String> mergeStringConstraint(
        Optional<String> left, Optional<String> right) {
      if (left.isPresent() && right.isPresent() && !left.get().equalsIgnoreCase(right.get())) {
        return null;
      }
      return left.isPresent() ? left : right;
    }

    private static OptionalLong parseLong(String literal) {
      try {
        return OptionalLong.of(Long.parseLong(literal));
      } catch (NumberFormatException e) {
        return OptionalLong.empty();
      }
    }

    private static ColumnLiteral asColumnLiteral(Expr left, Expr right) {
      if (left instanceof Expr.ColumnRef columnRef && right instanceof Expr.Literal literal) {
        return new ColumnLiteral(columnRef.name(), literal.value());
      }
      if (right instanceof Expr.ColumnRef columnRef && left instanceof Expr.Literal literal) {
        return new ColumnLiteral(columnRef.name(), literal.value());
      }
      return null;
    }
  }

  private record ColumnLiteral(String column, String literal) {}

  private record ResolvedTable(
      String accountId,
      String catalog,
      String schema,
      String table,
      String tableId,
      TableNode node) {}

  protected abstract TargetType targetType();

  protected int batchSize() {
    return STATS_BATCH_SIZE;
  }

  protected abstract AbstractArrowBatchBuilder newBatchBuilder(
      BufferAllocator allocator, Set<String> requiredColumns);

  protected abstract void appendArrowRow(AbstractArrowBatchBuilder builder, StatsScanRecord row);

  protected abstract SystemObjectRow toRow(StatsScanRecord row);

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    return streamRecords(ctx, null).map(this::toRow);
  }

  @Override
  public Stream<ColumnarBatch> scanArrow(
      SystemObjectScanContext ctx,
      Expr predicate,
      List<String> requiredColumns,
      BufferAllocator allocator) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(requiredColumns, "requiredColumns");
    Objects.requireNonNull(allocator, "allocator");
    Set<String> normalizedRequired = ArrowSchemaUtil.normalizeRequiredColumns(requiredColumns);
    Iterator<StatsScanRecord> rows = iterateRecords(ctx, predicate);

    Spliterator<ColumnarBatch> spliterator =
        new Spliterators.AbstractSpliterator<ColumnarBatch>(
            Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL) {
          @Override
          public boolean tryAdvance(Consumer<? super ColumnarBatch> action) {
            AbstractArrowBatchBuilder builder = null;
            int batchRows = 0;
            try {
              while (rows.hasNext()) {
                if (builder == null) {
                  builder = newBatchBuilder(allocator, normalizedRequired);
                }
                appendArrowRow(builder, rows.next());
                batchRows++;
                if (batchRows >= batchSize()) {
                  action.accept(builder.buildBatch());
                  return true;
                }
              }
              if (builder == null || builder.isEmpty()) {
                return false;
              }
              action.accept(builder.buildBatch());
              return true;
            } finally {
              if (builder != null) {
                builder.release();
              }
            }
          }
        };
    return StreamSupport.stream(spliterator, false);
  }

  @Override
  public final EnumSet<ScanOutputFormat> supportedFormats() {
    return EnumSet.of(ScanOutputFormat.ROWS, ScanOutputFormat.ARROW_IPC);
  }

  final Stream<StatsScanRecord> streamRecords(SystemObjectScanContext ctx, Expr predicate) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterateRecords(ctx, predicate), Spliterator.ORDERED),
        false);
  }

  final Iterator<StatsScanRecord> iterateRecords(SystemObjectScanContext ctx, Expr predicate) {
    StatsPushdown pushdown = StatsPushdown.parse(predicate);
    if (pushdown.impossible) {
      return Collections.emptyIterator();
    }
    return new StatsRecordIterator(ctx, pushdown, resolveTables(ctx, pushdown), targetType());
  }

  /** Returns normalized completeness label from stats metadata, or {@code null} when absent. */
  static String metadataCompleteness(TargetStatsRecord record) {
    if (!record.hasMetadata()) {
      return null;
    }
    return switch (record.getMetadata().getCompleteness()) {
      case SC_COMPLETE -> "complete";
      case SC_PARTIAL -> "partial";
      case SC_UNAVAILABLE -> "unavailable";
      case SC_UNSPECIFIED, UNRECOGNIZED -> null;
    };
  }

  /** Returns normalized provenance label from stats metadata, or {@code null} when absent. */
  static String metadataProvenance(TargetStatsRecord record) {
    if (!record.hasMetadata()) {
      return null;
    }
    return switch (record.getMetadata().getProducer()) {
      case SPROD_SOURCE_NATIVE -> "source_native";
      case SPROD_UPSTREAM_METADATA_DERIVED -> "upstream_metadata_derived";
      case SPROD_ENGINE_COMPUTED -> "engine_computed";
      case SPROD_UNSPECIFIED, UNRECOGNIZED -> null;
    };
  }

  /** Returns optional metadata confidence value. */
  static OptionalDouble metadataConfidence(TargetStatsRecord record) {
    if (!record.hasMetadata() || !record.getMetadata().hasConfidenceLevel()) {
      return OptionalDouble.empty();
    }
    return OptionalDouble.of(record.getMetadata().getConfidenceLevel());
  }

  /** Returns capture timestamp from metadata when present. */
  static Timestamp metadataCaptureTime(TargetStatsRecord record) {
    if (!record.hasMetadata() || !record.getMetadata().hasCapturedAt()) {
      return null;
    }
    return record.getMetadata().getCapturedAt();
  }

  /** Returns refresh timestamp from metadata when present. */
  static Timestamp metadataRefreshTime(TargetStatsRecord record) {
    if (!record.hasMetadata() || !record.getMetadata().hasRefreshedAt()) {
      return null;
    }
    return record.getMetadata().getRefreshedAt();
  }

  /** Returns optional row coverage count from metadata coverage payload. */
  static OptionalLong coverageRowsSeen(TargetStatsRecord record) {
    if (!record.hasMetadata() || !record.getMetadata().getCoverage().hasRowsScanned()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(record.getMetadata().getCoverage().getRowsScanned());
  }

  /** Returns optional file coverage count from metadata coverage payload. */
  static OptionalLong coverageFilesSeen(TargetStatsRecord record) {
    if (!record.hasMetadata() || !record.getMetadata().getCoverage().hasFilesScanned()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(record.getMetadata().getCoverage().getFilesScanned());
  }

  /** Returns optional row-group coverage count from metadata coverage payload. */
  static OptionalLong coverageRowGroupsSeen(TargetStatsRecord record) {
    if (!record.hasMetadata() || !record.getMetadata().getCoverage().hasRowGroupsSampled()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(record.getMetadata().getCoverage().getRowGroupsSampled());
  }

  /** Converts an optional long to nullable boxed form for row-mode output. */
  protected static Long nullable(OptionalLong value) {
    return value.isPresent() ? value.getAsLong() : null;
  }

  /** Converts an optional double to nullable boxed form for row-mode output. */
  protected static Double nullable(OptionalDouble value) {
    return value.isPresent() ? value.getAsDouble() : null;
  }

  /** Materializes scalar stat fields into one projection used by row scanners. */
  protected static ScalarProjection scalarProjection(TargetStatsRecord record) {
    return new ScalarProjection(
        scalarDataType(record),
        nullable(scalarValueCount(record)),
        nullable(scalarNullCount(record)),
        nullable(scalarNanCount(record)),
        nullable(scalarDistinctCount(record)),
        scalarMinValue(record),
        scalarMaxValue(record),
        scalarHistogram(record));
  }

  /** Materializes metadata and coverage fields into one projection used by row scanners. */
  protected static MetadataCoverageProjection metadataCoverageProjection(TargetStatsRecord record) {
    return new MetadataCoverageProjection(
        metadataCompleteness(record),
        metadataProvenance(record),
        nullable(metadataConfidence(record)),
        metadataCaptureTime(record),
        metadataRefreshTime(record),
        nullable(coverageRowsSeen(record)),
        nullable(coverageFilesSeen(record)),
        nullable(coverageRowGroupsSeen(record)));
  }

  /** Writes a nullable text column when projected, otherwise forces null at the current row. */
  protected static void writeText(
      VarCharVector vector, boolean include, int rowIndex, String value) {
    if (include) {
      ArrowValueWriters.writeVarChar(vector, rowIndex, value);
    } else {
      vector.setNull(rowIndex);
    }
  }

  /** Writes a required long column when projected, otherwise forces null at the current row. */
  protected static void writeLong(BigIntVector vector, boolean include, int rowIndex, long value) {
    if (include) {
      ArrowValueWriters.writeBigInt(vector, rowIndex, value);
    } else {
      vector.setNull(rowIndex);
    }
  }

  /** Writes an optional long column when projected, otherwise forces null at the current row. */
  protected static void writeOptionalLong(
      BigIntVector vector, boolean include, int rowIndex, OptionalLong value) {
    if (include) {
      ArrowValueWriters.writeBigIntNullable(vector, rowIndex, value);
    } else {
      vector.setNull(rowIndex);
    }
  }

  /** Writes an optional double column when projected, otherwise forces null at the current row. */
  protected static void writeOptionalDouble(
      Float8Vector vector, boolean include, int rowIndex, OptionalDouble value) {
    if (include) {
      ArrowValueWriters.writeDoubleNullable(vector, rowIndex, value);
    } else {
      vector.setNull(rowIndex);
    }
  }

  /** Writes a nullable timestamp when projected, otherwise forces null at the current row. */
  protected static void writeTimestamp(
      TimeStampMicroTZVector vector,
      boolean include,
      int rowIndex,
      com.google.protobuf.Timestamp value) {
    if (include) {
      ArrowValueWriters.writeTimestamp(vector, rowIndex, value);
    } else {
      vector.setNull(rowIndex);
    }
  }

  /** Returns scalar display name when available. */
  static String scalarColumnName(TargetStatsRecord record) {
    if (!record.hasScalar() || record.getScalar().getDisplayName().isBlank()) {
      return null;
    }
    return record.getScalar().getDisplayName();
  }

  /** Returns scalar logical type when available. */
  static String scalarDataType(TargetStatsRecord record) {
    if (!record.hasScalar() || record.getScalar().getLogicalType().isBlank()) {
      return null;
    }
    return record.getScalar().getLogicalType();
  }

  /** Returns scalar value count. */
  static OptionalLong scalarValueCount(TargetStatsRecord record) {
    if (!record.hasScalar()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(record.getScalar().getValueCount());
  }

  /** Returns scalar null count. */
  static OptionalLong scalarNullCount(TargetStatsRecord record) {
    if (!record.hasScalar() || !record.getScalar().hasNullCount()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(record.getScalar().getNullCount());
  }

  /** Returns scalar NaN count when emitted by the producer. */
  static OptionalLong scalarNanCount(TargetStatsRecord record) {
    if (!record.hasScalar() || !record.getScalar().hasNanCount()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(record.getScalar().getNanCount());
  }

  /** Returns scalar NDV estimate, preferring exact values when provided. */
  static OptionalDouble scalarDistinctCount(TargetStatsRecord record) {
    if (!record.hasScalar() || !record.getScalar().hasNdv()) {
      return OptionalDouble.empty();
    }
    if (record.getScalar().getNdv().hasExact()) {
      return OptionalDouble.of(record.getScalar().getNdv().getExact());
    }
    if (record.getScalar().getNdv().hasApprox()) {
      return OptionalDouble.of(record.getScalar().getNdv().getApprox().getEstimate());
    }
    return OptionalDouble.empty();
  }

  /** Returns scalar minimum value in canonical textual form when available. */
  static String scalarMinValue(TargetStatsRecord record) {
    if (!record.hasScalar() || !record.getScalar().hasMin()) {
      return null;
    }
    return record.getScalar().getMin();
  }

  /** Returns scalar maximum value in canonical textual form when available. */
  static String scalarMaxValue(TargetStatsRecord record) {
    if (!record.hasScalar() || !record.getScalar().hasMax()) {
      return null;
    }
    return record.getScalar().getMax();
  }

  /** Returns scalar histogram payload encoded as base64url when available. */
  static String scalarHistogram(TargetStatsRecord record) {
    if (!record.hasScalar() || record.getScalar().getHistogram().isEmpty()) {
      return null;
    }
    return base64Url(record.getScalar().getHistogram());
  }

  /** Encodes binary payloads using unpadded base64url for system table rendering. */
  static String base64Url(ByteString value) {
    if (value == null || value.isEmpty()) {
      return "";
    }
    return Base64.getUrlEncoder().withoutPadding().encodeToString(value.toByteArray());
  }

  private static List<ResolvedTable> resolveTables(SystemObjectScanContext ctx, StatsPushdown pd) {
    return ctx.listNamespaces().stream()
        .flatMap(
            namespace -> {
              String catalog = ((CatalogNode) ctx.resolve(namespace.catalogId())).displayName();
              String schema =
                  NameRefUtil.namespaceName(namespace.pathSegments(), namespace.displayName());
              return ctx.listTables(namespace.id()).stream()
                  .filter(table -> !pd.tableFiltered || pd.tableIds.contains(table.id().getId()))
                  .map(
                      table ->
                          new ResolvedTable(
                              table.id().getAccountId(),
                              catalog,
                              schema,
                              table.displayName(),
                              table.id().getId(),
                              table));
            })
        .toList();
  }

  private static OptionalLong tableCurrentSnapshotId(TableNode table) {
    if (!(table instanceof UserTableNode userTable) || userTable.currentSnapshot().isEmpty()) {
      return OptionalLong.empty();
    }
    return switch (userTable.currentSnapshot().get().getWhichCase()) {
      case SNAPSHOT_ID -> OptionalLong.of(userTable.currentSnapshot().get().getSnapshotId());
      default -> OptionalLong.empty();
    };
  }

  private static final class StatsRecordIterator implements Iterator<StatsScanRecord> {

    private final SystemObjectScanContext ctx;
    private final StatsPushdown pushdown;
    private final List<ResolvedTable> tables;
    private final TargetType targetType;
    private final ArrayDeque<StatsScanRecord> buffered = new ArrayDeque<>();

    private int tableIndex = -1;
    private ResolvedTable currentTable;
    private long currentSnapshotId;
    private String pageToken = "";
    private boolean pageLoaded;
    private Iterator<TargetStatsRecord> currentPage = Collections.emptyIterator();

    private StatsRecordIterator(
        SystemObjectScanContext ctx,
        StatsPushdown pushdown,
        List<ResolvedTable> tables,
        TargetType targetType) {
      this.ctx = ctx;
      this.pushdown = pushdown;
      this.tables = tables;
      this.targetType = targetType;
    }

    @Override
    public boolean hasNext() {
      fillBuffer();
      return !buffered.isEmpty();
    }

    @Override
    public StatsScanRecord next() {
      fillBuffer();
      if (buffered.isEmpty()) {
        throw new NoSuchElementException("no more stats records");
      }
      return buffered.removeFirst();
    }

    private void fillBuffer() {
      while (buffered.isEmpty()) {
        TargetStatsRecord nextRecord = pollRecord();
        if (nextRecord == null) {
          return;
        }
        if (!matches(nextRecord)) {
          continue;
        }
        buffered.add(
            new StatsScanRecord(
                currentTable.accountId,
                currentTable.catalog,
                currentTable.schema,
                currentTable.table,
                currentTable.tableId,
                currentSnapshotId,
                nextRecord));
      }
    }

    private TargetStatsRecord pollRecord() {
      while (true) {
        if (currentPage.hasNext()) {
          return currentPage.next();
        }
        if (currentTable == null && !advanceTable()) {
          return null;
        }
        if (loadNextPage()) {
          continue;
        }
        currentTable = null;
      }
    }

    private boolean advanceTable() {
      while (++tableIndex < tables.size()) {
        ResolvedTable next = tables.get(tableIndex);
        OptionalLong snapshot;
        if (pushdown.snapshotId.isPresent()) {
          snapshot = OptionalLong.of(pushdown.snapshotId.getAsLong());
        } else {
          OptionalLong persistedSnapshot =
              ctx.statsProvider()
                  .latestPersistedStatsSnapshotId(
                      next.node.id(), Optional.of(targetType.wireValue()));
          snapshot = persistedSnapshot;
          if (snapshot.isEmpty()) {
            snapshot = ctx.statsProvider().latestSnapshotId(next.node.id());
          }
          if (snapshot.isEmpty()) {
            snapshot = tableCurrentSnapshotId(next.node);
          }
        }
        if (snapshot.isEmpty()) {
          continue;
        }
        currentTable = next;
        currentSnapshotId = snapshot.getAsLong();
        pageToken = "";
        pageLoaded = false;
        currentPage = Collections.emptyIterator();
        return true;
      }
      return false;
    }

    private boolean loadNextPage() {
      if (currentTable == null) {
        return false;
      }
      if (pageLoaded && pageToken.isEmpty()) {
        return false;
      }
      var page =
          ctx.statsProvider()
              .listPersistedStats(
                  currentTable.node.id(),
                  currentSnapshotId,
                  Optional.of(targetType.wireValue()),
                  STORE_PAGE_SIZE,
                  pageToken);
      pageLoaded = true;
      pageToken = page.nextToken();
      currentPage = page.items().iterator();
      if (currentPage.hasNext()) {
        return true;
      }
      return !pageToken.isEmpty();
    }

    private boolean matches(TargetStatsRecord record) {
      if (pushdown.columnId.isPresent()) {
        if (!record.hasTarget() || !record.getTarget().hasColumn()) {
          return false;
        }
        if (record.getTarget().getColumn().getColumnId() != pushdown.columnId.getAsLong()) {
          return false;
        }
      }
      if (pushdown.engineKind.isPresent()) {
        if (!record.hasTarget() || !record.getTarget().hasExpression()) {
          return false;
        }
        if (!record
            .getTarget()
            .getExpression()
            .getEngineKind()
            .equalsIgnoreCase(pushdown.engineKind.get())) {
          return false;
        }
      }
      return true;
    }
  }
}
