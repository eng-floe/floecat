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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QueryClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QuerySchemaClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;

@ApplicationScoped
public class DeltaManifestMaterializer {
  private static final Logger LOG = Logger.getLogger(DeltaManifestMaterializer.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();
  private static final String METADATA_DIR = "metadata";

  @Inject QueryClient queryClient;
  @Inject QuerySchemaClient querySchemaClient;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject IcebergGatewayConfig config;

  public List<Snapshot> materialize(Table table, List<Snapshot> snapshots) {
    if (table == null || !table.hasResourceId() || snapshots == null || snapshots.isEmpty()) {
      return snapshots == null ? List.of() : snapshots;
    }
    String metadataRoot = metadataRoot(table);
    if (metadataRoot == null || metadataRoot.isBlank()) {
      return snapshots;
    }

    FileIO fileIo = null;
    try {
      fileIo = newFileIo(table);
      List<Snapshot> rewritten = new ArrayList<>(snapshots.size());
      for (Snapshot snapshot : snapshots) {
        if (snapshot == null || snapshot.getSnapshotId() < 0) {
          rewritten.add(snapshot);
          continue;
        }
        if (!snapshot.getManifestList().isBlank()) {
          rewritten.add(snapshot);
          continue;
        }
        try {
          String manifestList = ensureCompatArtifacts(fileIo, table, snapshot, metadataRoot);
          if (manifestList != null && !manifestList.isBlank()) {
            rewritten.add(snapshot.toBuilder().setManifestList(manifestList).build());
          } else {
            rewritten.add(snapshot);
          }
        } catch (Exception e) {
          LOG.warnf(
              e,
              "Delta compat manifest generation failed table=%s snapshot=%d; returning snapshot without manifest-list",
              table.getResourceId().getId(),
              snapshot.getSnapshotId());
          rewritten.add(snapshot);
        }
      }
      return List.copyOf(rewritten);
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Delta compat manifest setup failed for table=%s; returning translated metadata without manifest artifacts",
          table.getResourceId().getId());
      return snapshots;
    } finally {
      closeQuietly(fileIo);
    }
  }

  protected FileIO newFileIo(Table table) {
    Map<String, String> props = new LinkedHashMap<>(tableGatewaySupport.defaultFileIoProperties());
    if (table.getPropertiesCount() > 0) {
      table
          .getPropertiesMap()
          .forEach(
              (k, v) -> {
                if (k != null && v != null && FileIoFactory.isFileIoProperty(k)) {
                  props.put(k, v);
                }
              });
    }
    return FileIoFactory.createFileIo(props, config, true);
  }

  private String writeManifestArtifacts(
      FileIO fileIo, Table table, Snapshot snapshot, String metadataRoot) throws Exception {
    ScanBundle bundle = fetchScanBundle(table, snapshot.getSnapshotId());
    long snapshotId = snapshot.getSnapshotId();
    String compatMetadataPath = compatMetadataPath(metadataRoot, snapshotId);
    deleteIfExists(fileIo, compatMetadataPath);
    PartitionSpec requestedSpec = resolveDataSpec(table, snapshot);
    String tableLocation = tableRootFromMetadataRoot(metadataRoot);
    if (tableLocation == null || tableLocation.isBlank()) {
      tableLocation = metadataRoot;
    }
    Schema schema = parseSnapshotSchema(snapshot, table);
    if (schema == null) {
      schema = new Schema();
    }

    CompatTableOperations tableOps =
        new CompatTableOperations(fileIo, tableLocation, metadataRoot, compatMetadataPath);
    tableOps.initialize(
        TableMetadata.newTableMetadata(
            schema,
            requestedSpec == null ? UNPARTITIONED : requestedSpec,
            SortOrder.unsorted(),
            tableLocation,
            Map.of()));
    PartitionSpec effectiveSpec =
        tableOps.current() == null || tableOps.current().spec() == null
            ? UNPARTITIONED
            : tableOps.current().spec();
    List<DataFile> dataFiles = new ArrayList<>();
    if (bundle != null) {
      for (ScanFile dataFile : bundle.getDataFilesList()) {
        dataFiles.add(toDataFile(dataFile, effectiveSpec));
      }
    }
    List<DeleteFile> deleteFiles =
        buildDeleteFiles(fileIo, table, snapshot, metadataRoot, bundle, effectiveSpec);

    BaseTable compatTable = new BaseTable(tableOps, "delta-compat-" + snapshotId);
    Transaction transaction = compatTable.newTransaction();
    RowDelta rowDelta = transaction.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      rowDelta.addRows(dataFile);
    }
    for (DeleteFile deleteFile : deleteFiles) {
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();
    transaction.commitTransaction();
    compatTable.refresh();
    if (compatTable.currentSnapshot() == null
        || compatTable.currentSnapshot().manifestListLocation() == null
        || compatTable.currentSnapshot().manifestListLocation().isBlank()) {
      throw new IllegalStateException("Iceberg transaction did not produce a manifest list");
    }
    return compatTable.currentSnapshot().manifestListLocation();
  }

  private List<DeleteFile> buildDeleteFiles(
      FileIO fileIo,
      Table table,
      Snapshot snapshot,
      String metadataRoot,
      ScanBundle bundle,
      PartitionSpec dataSpec) {
    try {
      List<DeleteFileEntry> entries =
          new ArrayList<>(buildDeleteFilesFromScanBundle(bundle, dataSpec));
      if (entries.isEmpty()) {
        for (DeleteFile deleteFile :
            loadDeltaPositionDeleteFiles(fileIo, table, snapshot, metadataRoot)) {
          entries.add(new DeleteFileEntry(deleteFile, null));
        }
      }
      return entries.stream().map(DeleteFileEntry::file).toList();
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Delta compat delete file generation failed table=%s snapshot=%d; continuing without delete files",
          table.getResourceId().getId(),
          snapshot.getSnapshotId());
      return List.of();
    }
  }

  private List<DeleteFileEntry> buildDeleteFilesFromScanBundle(
      ScanBundle bundle, PartitionSpec dataSpec) {
    if (bundle == null || bundle.getDeleteFilesCount() == 0) {
      return List.of();
    }
    PartitionSpec spec = dataSpec == null ? UNPARTITIONED : dataSpec;
    Map<Integer, List<String>> referencedDataPaths = referencedDataPathsByDeleteIndex(bundle);
    List<DeleteFileEntry> out = new ArrayList<>(bundle.getDeleteFilesCount());
    for (int i = 0; i < bundle.getDeleteFilesCount(); i++) {
      ScanFile deleteScanFile = bundle.getDeleteFiles(i);
      DeleteFile deleteFile = toDeleteFile(deleteScanFile, spec, referencedDataPaths.get(i));
      if (deleteFile != null) {
        Long seq =
            deleteScanFile.hasSequenceNumber() && deleteScanFile.getSequenceNumber() > 0
                ? deleteScanFile.getSequenceNumber()
                : null;
        out.add(new DeleteFileEntry(deleteFile, seq));
      }
    }
    return out;
  }

  private Map<Integer, List<String>> referencedDataPathsByDeleteIndex(ScanBundle bundle) {
    if (bundle == null || bundle.getDataFilesCount() == 0) {
      return Map.of();
    }
    Map<Integer, List<String>> refs = new LinkedHashMap<>();
    for (ScanFile dataFile : bundle.getDataFilesList()) {
      if (dataFile == null || dataFile.getDeleteFileIndicesCount() == 0) {
        continue;
      }
      for (int index : dataFile.getDeleteFileIndicesList()) {
        if (index < 0 || index >= bundle.getDeleteFilesCount()) {
          continue;
        }
        refs.computeIfAbsent(index, ignored -> new ArrayList<>()).add(dataFile.getFilePath());
      }
    }
    return refs;
  }

  private DeleteFile toDeleteFile(
      ScanFile file, PartitionSpec spec, List<String> referencedDataFiles) {
    if (file == null || file.getFilePath() == null || file.getFilePath().isBlank()) {
      return null;
    }
    FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec);
    if (file.getFileContent() == ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES) {
      if (file.getEqualityFieldIdsCount() == 0) {
        return null;
      }
      int[] equalityIds = new int[file.getEqualityFieldIdsCount()];
      for (int i = 0; i < file.getEqualityFieldIdsCount(); i++) {
        equalityIds[i] = file.getEqualityFieldIds(i);
      }
      builder.ofEqualityDeletes(equalityIds);
    } else {
      builder.ofPositionDeletes();
    }
    builder
        .withPath(file.getFilePath())
        .withFormat(resolveFileFormat(file.getFileFormat()))
        .withFileSizeInBytes(file.getFileSizeInBytes())
        .withRecordCount(file.getRecordCount());
    if (spec.isPartitioned()) {
      StructLike partition = parsePartitionStruct(file, spec);
      if (partition != null) {
        builder.withPartition(partition);
      }
    }
    if (file.getFileContent() == ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES
        && referencedDataFiles != null
        && referencedDataFiles.size() == 1) {
      builder.withReferencedDataFile(referencedDataFiles.get(0));
    }
    return builder.build();
  }

  protected List<DeleteFile> loadDeltaPositionDeleteFiles(
      FileIO icebergFileIo, Table table, Snapshot snapshot, String metadataRoot) throws Exception {
    long snapshotId = snapshot.getSnapshotId();
    if (snapshotId < 0) {
      return List.of();
    }

    String tableRoot = tableRootFromMetadataRoot(metadataRoot);
    if (tableRoot == null || tableRoot.isBlank()) {
      return List.of();
    }

    if (!deltaLogExists(icebergFileIo, tableRoot)) {
      return List.of();
    }

    Engine engine = newDeltaEngine(icebergFileIo);
    io.delta.kernel.Table deltaTable = io.delta.kernel.Table.forPath(engine, tableRoot);
    io.delta.kernel.Snapshot deltaSnapshot = deltaTable.getSnapshotAsOfVersion(engine, snapshotId);
    ScanBuilder scanBuilder = deltaSnapshot.getScanBuilder();
    Scan scan = scanBuilder.build();

    List<DeleteFile> deletes = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> batches = scan.getScanFiles(engine)) {
      while (batches.hasNext()) {
        FilteredColumnarBatch batch = batches.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row scanFileRow = rows.next();
            String dataPath = InternalScanFileUtils.getAddFileStatus(scanFileRow).getPath();
            AddFile add =
                new AddFile(scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
            Optional<DeletionVectorDescriptor> maybeDv = add.getDeletionVector();
            if (maybeDv.isEmpty()) {
              continue;
            }
            DeletionVectorDescriptor dv = maybeDv.get();
            if (!dv.isOnDisk() || dv.isInline()) {
              continue;
            }
            long[] positions =
                DeletionVectorUtils.loadNewDvAndBitmap(engine, tableRoot, dv)._2.toArray();
            if (positions == null || positions.length == 0) {
              continue;
            }
            deletes.add(
                writePositionDeleteFile(
                    icebergFileIo,
                    metadataRoot,
                    snapshotId,
                    dataPath,
                    dv.getUniqueId(),
                    positions));
          }
        }
      }
    }
    return List.copyOf(deletes);
  }

  protected Engine newDeltaEngine(FileIO icebergFileIo) {
    return DefaultEngine.create(new IcebergFileIoAdapter(icebergFileIo));
  }

  private DeleteFile writePositionDeleteFile(
      FileIO fileIo,
      String metadataRoot,
      long snapshotId,
      String dataFilePath,
      String dvId,
      long[] positions)
      throws Exception {
    Arrays.sort(positions);
    String suffix = Integer.toUnsignedString((dataFilePath + "|" + dvId).hashCode(), 16);
    String deleteFilePath = metadataRoot + "/" + snapshotId + "-compat-pd-" + suffix + ".avro";
    deleteIfExists(fileIo, deleteFilePath);

    PositionDeleteWriter<Void> writer =
        Avro.writeDeletes(fileIo.newOutputFile(deleteFilePath))
            .withSpec(UNPARTITIONED)
            .buildPositionWriter();
    try {
      PositionDelete<Void> row = PositionDelete.create();
      for (long position : positions) {
        writer.write(row.set(dataFilePath, position));
      }
      writer.close();
      return writer.toDeleteFile();
    } catch (Exception e) {
      try {
        writer.close();
      } catch (Exception ignored) {
      }
      throw e;
    }
  }

  private String ensureCompatArtifacts(
      FileIO fileIo, Table table, Snapshot snapshot, String metadataRoot) throws Exception {
    String manifestListPath =
        readManifestListFromCompatMetadata(fileIo, metadataRoot, snapshot.getSnapshotId());
    if (manifestListPath != null) {
      return manifestListPath;
    }
    return writeManifestArtifacts(fileIo, table, snapshot, metadataRoot);
  }

  private String readManifestListFromCompatMetadata(
      FileIO fileIo, String metadataRoot, long snapshotId) {
    String compatMetadata = compatMetadataPath(metadataRoot, snapshotId);
    if (!inputExists(fileIo, compatMetadata)) {
      return null;
    }
    try {
      TableMetadata metadata = TableMetadataParser.read(fileIo, compatMetadata);
      if (metadata == null || metadata.currentSnapshot() == null) {
        return null;
      }
      String manifestList = metadata.currentSnapshot().manifestListLocation();
      if (manifestList == null || manifestList.isBlank()) {
        return null;
      }
      if (!inputExists(fileIo, manifestList)) {
        return null;
      }
      return manifestList;
    } catch (RuntimeException e) {
      LOG.debugf(e, "Unable to read compat metadata %s", compatMetadata);
      return null;
    }
  }

  private ScanBundle fetchScanBundle(Table table, long snapshotId) {
    String queryId = null;
    try {
      BeginQueryRequest.Builder begin = BeginQueryRequest.newBuilder();
      if (table.hasCatalogId()) {
        begin.setDefaultCatalogId(table.getCatalogId());
      }
      QueryDescriptor query = queryClient.beginQuery(begin.build()).getQuery();
      queryId = query == null ? null : query.getQueryId();
      if (queryId == null || queryId.isBlank()) {
        throw new IllegalStateException("BeginQuery returned empty query id");
      }

      QueryInput input =
          snapshotId > 0
              ? QueryInput.newBuilder()
                  .setTableId(table.getResourceId())
                  .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                  .build()
              : QueryInput.newBuilder()
                  .setTableId(table.getResourceId())
                  .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                  .build();
      querySchemaClient.describeInputs(
          DescribeInputsRequest.newBuilder().setQueryId(queryId).addInputs(input).build());

      return queryClient
          .fetchScanBundle(
              FetchScanBundleRequest.newBuilder()
                  .setQueryId(queryId)
                  .setTableId(table.getResourceId())
                  .build())
          .getBundle();
    } finally {
      if (queryId != null && !queryId.isBlank()) {
        try {
          queryClient.endQuery(
              EndQueryRequest.newBuilder().setQueryId(queryId).setCommit(false).build());
        } catch (Exception e) {
          LOG.debugf(e, "Failed to end compat query %s", queryId);
        }
      }
    }
  }

  private DataFile toDataFile(ScanFile file, PartitionSpec dataSpec) {
    PartitionSpec spec = dataSpec == null ? UNPARTITIONED : dataSpec;
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath(file.getFilePath())
            .withFormat(resolveFileFormat(file.getFileFormat()))
            .withFileSizeInBytes(file.getFileSizeInBytes())
            .withRecordCount(file.getRecordCount());
    if (spec.isPartitioned()) {
      maybeApplyPartition(builder, file, spec);
    }
    Metrics metrics = toMetrics(file);
    if (metrics != null) {
      builder.withMetrics(metrics);
    }
    return builder.build();
  }

  private PartitionSpec resolveDataSpec(Table table, Snapshot snapshot) {
    if (snapshot == null || !snapshot.hasPartitionSpec()) {
      return UNPARTITIONED;
    }
    PartitionSpecInfo info = snapshot.getPartitionSpec();
    if (info.getFieldsCount() == 0) {
      return UNPARTITIONED;
    }
    Schema schema = parseSnapshotSchema(snapshot, table);
    if (schema == null) {
      return UNPARTITIONED;
    }
    try {
      PartitionSpec.Builder builder = PartitionSpec.builderFor(schema).withSpecId(info.getSpecId());
      int added = 0;
      for (ai.floedb.floecat.catalog.rpc.PartitionField field : info.getFieldsList()) {
        String sourceName = resolveSourceName(schema, field);
        if (sourceName == null || sourceName.isBlank()) {
          continue;
        }
        if (applyTransform(builder, sourceName, field.getName(), field.getTransform())) {
          added++;
        }
      }
      return added == 0 ? UNPARTITIONED : builder.build();
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Falling back to unpartitioned compat manifest for table=%s snapshot=%d",
          table != null && table.hasResourceId() ? table.getResourceId().getId() : "<unknown>",
          snapshot.getSnapshotId());
      return UNPARTITIONED;
    }
  }

  private Schema parseSnapshotSchema(Snapshot snapshot, Table table) {
    String schemaJson = null;
    if (snapshot != null
        && snapshot.getSchemaJson() != null
        && !snapshot.getSchemaJson().isBlank()) {
      schemaJson = snapshot.getSchemaJson();
    } else if (table != null && table.getSchemaJson() != null && !table.getSchemaJson().isBlank()) {
      schemaJson = table.getSchemaJson();
    }
    if (schemaJson == null || schemaJson.isBlank()) {
      return null;
    }
    try {
      return SchemaParser.fromJson(schemaJson);
    } catch (RuntimeException e) {
      LOG.debugf(e, "Unable to parse schema JSON for compat partition spec construction");
      return null;
    }
  }

  private String resolveSourceName(
      Schema schema, ai.floedb.floecat.catalog.rpc.PartitionField field) {
    if (schema == null || field == null) {
      return null;
    }
    if (field.getFieldId() > 0) {
      String byId = schema.findColumnName(field.getFieldId());
      if (byId != null && !byId.isBlank()) {
        return byId;
      }
    }
    String candidate = field.getName();
    if (candidate != null && !candidate.isBlank() && schema.findField(candidate) != null) {
      return candidate;
    }
    return null;
  }

  private boolean applyTransform(
      PartitionSpec.Builder builder, String sourceName, String partitionName, String rawTransform) {
    String transform =
        rawTransform == null || rawTransform.isBlank()
            ? "identity"
            : rawTransform.trim().toLowerCase(Locale.ROOT);
    String name = partitionName == null || partitionName.isBlank() ? sourceName : partitionName;
    if ("identity".equals(transform)) {
      builder.identity(sourceName, name);
      return true;
    }
    if ("year".equals(transform)) {
      builder.year(sourceName, name);
      return true;
    }
    if ("month".equals(transform)) {
      builder.month(sourceName, name);
      return true;
    }
    if ("day".equals(transform)) {
      builder.day(sourceName, name);
      return true;
    }
    if ("hour".equals(transform)) {
      builder.hour(sourceName, name);
      return true;
    }
    if ("void".equals(transform)
        || "alwaysnull".equals(transform)
        || "always_null".equals(transform)) {
      builder.alwaysNull(sourceName, name);
      return true;
    }
    Integer width = parseTransformWidth(transform, "bucket");
    if (width != null && width > 0) {
      builder.bucket(sourceName, width, name);
      return true;
    }
    width = parseTransformWidth(transform, "truncate");
    if (width != null && width > 0) {
      builder.truncate(sourceName, width, name);
      return true;
    }
    return false;
  }

  private Integer parseTransformWidth(String transform, String prefix) {
    if (!transform.startsWith(prefix + "[")) {
      return null;
    }
    int open = transform.indexOf('[');
    int close = transform.indexOf(']', open + 1);
    if (open < 0 || close < 0) {
      return null;
    }
    try {
      return Integer.parseInt(transform.substring(open + 1, close).trim());
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  private void maybeApplyPartition(DataFiles.Builder builder, ScanFile file, PartitionSpec spec) {
    if (builder == null
        || file == null
        || spec == null
        || !spec.isPartitioned()
        || file.getPartitionDataJson().isBlank()) {
      return;
    }
    List<String> values = extractPartitionValues(file.getPartitionDataJson(), spec);
    if (values.size() != spec.fields().size()) {
      return;
    }
    builder.withPartitionValues(values);
  }

  private StructLike parsePartitionStruct(ScanFile file, PartitionSpec spec) {
    if (file == null || spec == null || !spec.isPartitioned()) {
      return null;
    }
    List<String> values = extractPartitionValues(file.getPartitionDataJson(), spec);
    if (values.size() != spec.fields().size()) {
      return null;
    }
    String path = file.getFilePath();
    if (path == null || path.isBlank()) {
      path = "s3://compat/delete-placeholder.parquet";
    }
    FileFormat format = resolveFileFormat(file.getFileFormat());
    return DataFiles.builder(spec)
        .withPath(path)
        .withFormat(format)
        .withFileSizeInBytes(Math.max(0L, file.getFileSizeInBytes()))
        .withRecordCount(Math.max(0L, file.getRecordCount()))
        .withPartitionValues(values)
        .build()
        .partition();
  }

  private List<String> extractPartitionValues(String partitionDataJson, PartitionSpec spec) {
    if (partitionDataJson == null || partitionDataJson.isBlank() || spec == null) {
      return List.of();
    }
    int fieldCount = spec.fields().size();
    if (fieldCount == 0) {
      return List.of();
    }
    List<String> values = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      values.add(null);
    }
    int assigned = 0;
    try {
      JsonNode root = JSON.readTree(partitionDataJson);
      JsonNode partitionValues = root == null ? null : root.get("partitionValues");
      if (partitionValues != null && partitionValues.isArray()) {
        boolean hasObjectEntries =
            partitionValues.size() > 0
                && partitionValues.get(0) != null
                && partitionValues.get(0).isObject();
        if (hasObjectEntries) {
          for (JsonNode entry : partitionValues) {
            if (entry == null || !entry.isObject()) {
              continue;
            }
            int index = findPartitionFieldIndex(spec, entry.get("id"));
            if (index < 0 || index >= values.size()) {
              continue;
            }
            values.set(index, jsonScalar(entry.get("value")));
            assigned++;
          }
        } else {
          int limit = Math.min(values.size(), partitionValues.size());
          for (int i = 0; i < limit; i++) {
            values.set(i, jsonScalar(partitionValues.get(i)));
            assigned++;
          }
        }
      }
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse partition_data_json for compat manifest");
      return List.of();
    }
    return assigned == 0 ? List.of() : values;
  }

  private int findPartitionFieldIndex(PartitionSpec spec, JsonNode idNode) {
    if (spec == null || idNode == null || idNode.isNull()) {
      return -1;
    }
    Integer numericId = null;
    if (idNode.isNumber()) {
      numericId = idNode.asInt();
    }
    String id = idNode.asText();
    if (numericId == null && id != null && !id.isBlank()) {
      try {
        numericId = Integer.parseInt(id.trim());
      } catch (RuntimeException ignored) {
        // non-numeric id strings are handled below.
      }
    }
    if (numericId != null) {
      for (int i = 0; i < spec.fields().size(); i++) {
        org.apache.iceberg.PartitionField field = spec.fields().get(i);
        if (field.sourceId() == numericId || field.fieldId() == numericId) {
          return i;
        }
      }
    }
    if (id == null || id.isBlank()) {
      return -1;
    }
    for (int i = 0; i < spec.fields().size(); i++) {
      org.apache.iceberg.PartitionField field = spec.fields().get(i);
      if (id.equals(field.name())) {
        return i;
      }
      String sourceName = spec.schema().findColumnName(field.sourceId());
      if (sourceName != null && id.equals(sourceName)) {
        return i;
      }
    }
    return -1;
  }

  private String jsonScalar(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isTextual() || node.isNumber() || node.isBoolean()) {
      return node.asText();
    }
    return node.toString();
  }

  private record DeleteFileEntry(DeleteFile file, Long sequenceNumber) {}

  private Metrics toMetrics(ScanFile file) {
    if (file == null || file.getColumnsCount() == 0) {
      return null;
    }
    Map<Integer, Long> valueCounts = new LinkedHashMap<>();
    Map<Integer, Long> nullValueCounts = new LinkedHashMap<>();
    Map<Integer, ByteBuffer> lowerBounds = new LinkedHashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new LinkedHashMap<>();
    for (ColumnStats column : file.getColumnsList()) {
      if (column == null || column.getColumnId() <= 0 || column.getColumnId() > Integer.MAX_VALUE) {
        continue;
      }
      int fieldId = (int) column.getColumnId();
      valueCounts.put(fieldId, Math.max(0L, column.getValueCount()));
      if (column.hasNullCount()) {
        nullValueCounts.put(fieldId, Math.max(0L, column.getNullCount()));
      }
      if (column.hasMin()) {
        encodeBound(lowerBounds, fieldId, column.getLogicalType(), column.getMin());
      }
      if (column.hasMax()) {
        encodeBound(upperBounds, fieldId, column.getLogicalType(), column.getMax());
      }
    }
    if (valueCounts.isEmpty()
        && nullValueCounts.isEmpty()
        && lowerBounds.isEmpty()
        && upperBounds.isEmpty()) {
      return null;
    }
    return new Metrics(
        file.getRecordCount(),
        null,
        valueCounts.isEmpty() ? null : valueCounts,
        nullValueCounts.isEmpty() ? null : nullValueCounts,
        null,
        lowerBounds.isEmpty() ? null : lowerBounds,
        upperBounds.isEmpty() ? null : upperBounds);
  }

  private void encodeBound(
      Map<Integer, ByteBuffer> out, int fieldId, String rawLogicalType, String encodedValue) {
    if (encodedValue == null || encodedValue.isBlank()) {
      return;
    }
    Type.PrimitiveType icebergType = toIcebergType(rawLogicalType);
    if (icebergType == null) {
      return;
    }
    Object decoded = decodeValue(icebergType, encodedValue);
    if (decoded == null) {
      return;
    }
    try {
      out.put(fieldId, Conversions.toByteBuffer(icebergType, decoded));
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Skipping compat metrics bound for field=%d type=%s value=%s",
          fieldId,
          rawLogicalType,
          encodedValue);
    }
  }

  private Type.PrimitiveType toIcebergType(String rawLogicalType) {
    if (rawLogicalType == null || rawLogicalType.isBlank()) {
      return null;
    }
    String normalized = rawLogicalType.trim().toUpperCase(Locale.ROOT);
    if (normalized.startsWith("DECIMAL(")) {
      int open = normalized.indexOf('(');
      int comma = normalized.indexOf(',', open + 1);
      int close = normalized.indexOf(')', comma + 1);
      if (open < 0 || comma < 0 || close < 0) {
        return null;
      }
      try {
        int precision = Integer.parseInt(normalized.substring(open + 1, comma).trim());
        int scale = Integer.parseInt(normalized.substring(comma + 1, close).trim());
        return Types.DecimalType.of(precision, scale);
      } catch (RuntimeException ignored) {
        return null;
      }
    }
    return switch (normalized) {
      case "BOOLEAN" -> Types.BooleanType.get();
      case "INT16", "SMALLINT", "INT32", "INT", "INTEGER" -> Types.IntegerType.get();
      case "INT64", "BIGINT", "LONG" -> Types.LongType.get();
      case "FLOAT32", "FLOAT", "REAL" -> Types.FloatType.get();
      case "FLOAT64", "DOUBLE" -> Types.DoubleType.get();
      case "DATE" -> Types.DateType.get();
      case "TIME" -> Types.TimeType.get();
      case "TIMESTAMP" -> Types.TimestampType.withZone();
      case "STRING", "TEXT", "VARCHAR", "CHAR" -> Types.StringType.get();
      case "BINARY", "VARBINARY" -> Types.BinaryType.get();
      case "UUID" -> Types.UUIDType.get();
      default -> null;
    };
  }

  private Object decodeValue(Type.PrimitiveType type, String encodedValue) {
    try {
      return switch (type.typeId()) {
        case BOOLEAN -> Boolean.parseBoolean(encodedValue);
        case INTEGER -> Integer.parseInt(encodedValue);
        case LONG -> Long.parseLong(encodedValue);
        case FLOAT -> Float.parseFloat(encodedValue);
        case DOUBLE -> Double.parseDouble(encodedValue);
        case DATE -> (int) LocalDate.parse(encodedValue).toEpochDay();
        case TIME -> LocalTime.parse(encodedValue).toNanoOfDay() / 1_000L;
        case TIMESTAMP -> {
          Instant instant = Instant.parse(encodedValue);
          long micros = Math.multiplyExact(instant.getEpochSecond(), 1_000_000L);
          yield Math.addExact(micros, instant.getNano() / 1_000L);
        }
        case STRING -> encodedValue;
        case BINARY -> Base64.getDecoder().decode(encodedValue);
        case DECIMAL -> new BigDecimal(encodedValue);
        case UUID -> java.util.UUID.fromString(encodedValue);
        default -> null;
      };
    } catch (RuntimeException e) {
      LOG.debugf(
          e, "Skipping compat metrics bound decode type=%s value=%s", type.typeId(), encodedValue);
      return null;
    }
  }

  private FileFormat resolveFileFormat(String raw) {
    if (raw == null || raw.isBlank()) {
      return FileFormat.PARQUET;
    }
    try {
      return FileFormat.fromString(raw);
    } catch (IllegalArgumentException ignored) {
      return FileFormat.fromString(raw.toLowerCase(Locale.ROOT));
    } catch (RuntimeException ignored) {
      return FileFormat.PARQUET;
    }
  }

  private String compatMetadataPath(String metadataRoot, long snapshotId) {
    return metadataRoot + "/compat-" + snapshotId + ".metadata.json";
  }

  private void deleteIfExists(FileIO fileIo, String location) {
    try {
      if (inputExists(fileIo, location)) {
        fileIo.deleteFile(location);
      }
    } catch (RuntimeException ignored) {
      // File may not exist or backends may not support exists checks consistently.
    }
  }

  private boolean inputExists(FileIO fileIo, String location) {
    try {
      return fileIo.newInputFile(location).exists();
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  private String metadataRoot(Table table) {
    String location = firstNonBlank(table.getPropertiesMap().get("location"));
    if (location == null) {
      location = firstNonBlank(table.getPropertiesMap().get("storage_location"));
    }
    if (location == null) {
      location = firstNonBlank(table.getPropertiesMap().get("delta.table-root"));
    }
    if (location == null) {
      location = firstNonBlank(table.getPropertiesMap().get("external.location"));
    }
    if (location == null && table.hasUpstream()) {
      location = firstNonBlank(table.getUpstream().getUri());
    }
    if (location == null) {
      return null;
    }
    String trimmed =
        location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
    if (trimmed.endsWith("/" + METADATA_DIR)) {
      return trimmed;
    }
    return trimmed + "/" + METADATA_DIR;
  }

  private String firstNonBlank(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private String tableRootFromMetadataRoot(String metadataRoot) {
    if (metadataRoot == null || metadataRoot.isBlank()) {
      return null;
    }
    String normalized =
        metadataRoot.endsWith("/")
            ? metadataRoot.substring(0, metadataRoot.length() - 1)
            : metadataRoot;
    if (!normalized.endsWith("/" + METADATA_DIR)) {
      return normalized;
    }
    return normalized.substring(0, normalized.length() - (METADATA_DIR.length() + 1));
  }

  private boolean deltaLogExists(FileIO fileIo, String tableRoot) {
    String prefix = tableRoot + "/_delta_log/";
    if (fileIo instanceof SupportsPrefixOperations prefixOps) {
      try {
        Iterable<org.apache.iceberg.io.FileInfo> files = prefixOps.listPrefix(prefix);
        return files.iterator().hasNext();
      } catch (Exception ignored) {
        // Fall through to direct object checks below.
      }
    }
    return inputExists(fileIo, prefix + "00000000000000000000.json")
        || inputExists(fileIo, prefix + "_last_checkpoint");
  }

  private void closeQuietly(FileIO fileIo) {
    if (fileIo instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.debugf(e, "Failed to close compat FileIO %s", fileIo.getClass().getName());
      }
    }
  }

  private static final class IcebergFileIoAdapter
      implements io.delta.kernel.defaults.engine.fileio.FileIO {
    private final FileIO icebergFileIo;

    private IcebergFileIoAdapter(FileIO icebergFileIo) {
      this.icebergFileIo = icebergFileIo;
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String path) {
      if (!(icebergFileIo instanceof SupportsPrefixOperations prefixOps)) {
        return closeableIterator(List.<FileStatus>of().iterator());
      }
      String prefix = parentPrefix(path);
      List<FileStatus> statuses =
          StreamSupport.stream(prefixOps.listPrefix(prefix).spliterator(), false)
              .map(
                  info ->
                      FileStatus.of(
                          info.location(), info.size(), Math.max(0L, info.createdAtMillis())))
              .filter(status -> status.getPath().compareTo(path) >= 0)
              .sorted(Comparator.comparing(FileStatus::getPath))
              .toList();
      return closeableIterator(statuses.iterator());
    }

    @Override
    public FileStatus getFileStatus(String path) throws java.io.IOException {
      try {
        org.apache.iceberg.io.InputFile input = icebergFileIo.newInputFile(path);
        return FileStatus.of(path, input.getLength(), 0L);
      } catch (RuntimeException e) {
        throw new java.io.IOException("Failed to stat " + path, e);
      }
    }

    @Override
    public String resolvePath(String path) {
      return path;
    }

    @Override
    public boolean mkdirs(String path) {
      return true;
    }

    @Override
    public io.delta.kernel.defaults.engine.fileio.InputFile newInputFile(String path, long length) {
      org.apache.iceberg.io.InputFile input = icebergFileIo.newInputFile(path);
      return new io.delta.kernel.defaults.engine.fileio.InputFile() {
        @Override
        public long length() throws java.io.IOException {
          return input.getLength();
        }

        @Override
        public String path() {
          return path;
        }

        @Override
        public io.delta.kernel.defaults.engine.fileio.SeekableInputStream newStream()
            throws java.io.IOException {
          org.apache.iceberg.io.SeekableInputStream in = input.newStream();
          return new io.delta.kernel.defaults.engine.fileio.SeekableInputStream() {
            @Override
            public long getPos() throws java.io.IOException {
              return in.getPos();
            }

            @Override
            public void seek(long newPos) throws java.io.IOException {
              in.seek(newPos);
            }

            @Override
            public void readFully(byte[] b, int off, int len) throws java.io.IOException {
              int remaining = len;
              while (remaining > 0) {
                int read = in.read(b, off + (len - remaining), remaining);
                if (read < 0) {
                  throw new java.io.IOException("Unexpected EOF");
                }
                remaining -= read;
              }
            }

            @Override
            public int read() throws java.io.IOException {
              return in.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws java.io.IOException {
              return in.read(b, off, len);
            }

            @Override
            public void close() throws java.io.IOException {
              in.close();
            }
          };
        }
      };
    }

    @Override
    public io.delta.kernel.defaults.engine.fileio.OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException("Delta engine output is not supported in gateway");
    }

    @Override
    public boolean delete(String path) {
      throw new UnsupportedOperationException("Delta engine delete is not supported in gateway");
    }

    @Override
    public Optional<String> getConf(String s) {
      return Optional.empty();
    }

    private static String parentPrefix(String location) {
      if (location == null || location.isBlank()) {
        return "";
      }
      int slash = location.lastIndexOf('/');
      return slash >= 0 ? location.substring(0, slash + 1) : "";
    }

    private static <T> CloseableIterator<T> closeableIterator(java.util.Iterator<T> delegate) {
      return new CloseableIterator<>() {
        @Override
        public boolean hasNext() {
          return delegate.hasNext();
        }

        @Override
        public T next() {
          return delegate.next();
        }

        @Override
        public void close() {}
      };
    }
  }

  private static final class CompatTableOperations implements TableOperations {
    private final FileIO fileIo;
    private final String tableLocation;
    private final String metadataRoot;
    private final String compatMetadataPath;
    private final LocationProvider locationProvider;
    private TableMetadata current;

    private CompatTableOperations(
        FileIO fileIo, String tableLocation, String metadataRoot, String compatMetadataPath) {
      this.fileIo = fileIo;
      this.tableLocation = tableLocation;
      this.metadataRoot = metadataRoot;
      this.compatMetadataPath = compatMetadataPath;
      this.locationProvider = LocationProviders.locationsFor(tableLocation, Map.of());
    }

    private void initialize(TableMetadata metadata) {
      this.current = metadata;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      if (current != null) {
        return current;
      }
      if (!fileIo.newInputFile(compatMetadataPath).exists()) {
        return null;
      }
      current = TableMetadataParser.read(fileIo, compatMetadataPath);
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      TableMetadataParser.write(metadata, fileIo.newOutputFile(compatMetadataPath));
      current = metadata;
    }

    @Override
    public FileIO io() {
      return fileIo;
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return metadataRoot + "/" + fileName;
    }

    @Override
    public LocationProvider locationProvider() {
      return locationProvider;
    }
  }
}
