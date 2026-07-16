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

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.connector.common.resolver.DeltaSchemaNormalizer;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageRequest;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

@ApplicationScoped
public class DeltaManifestMaterializer {
  private static final Logger LOG = Logger.getLogger(DeltaManifestMaterializer.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();
  private static final String METADATA_DIR = "metadata";
  private static final int DEFAULT_COMPAT_FORMAT_VERSION = 2;

  @Inject GrpcServiceFacade grpcClient;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject ConnectorIntegrationConfig config;
  @Inject IcebergGatewayConfig gatewayConfig;

  protected record SourceTableAccess(Map<String, String> fileIoProperties, FileIO sourceFileIo) {}

  public List<Snapshot> materialize(Table table, List<Snapshot> snapshots) {
    if (table == null || !table.hasResourceId() || snapshots == null || snapshots.isEmpty()) {
      return snapshots == null ? List.of() : snapshots;
    }
    String sourceMetadataRoot = metadataRoot(table);
    LOG.infof(
        "Delta compat materialize setup table=%s sourceMetadataRoot=%s snapshotCount=%d",
        table.getResourceId().getId(), sourceMetadataRoot, snapshots.size());

    SourceTableAccess sourceAccess = null;
    try {
      sourceAccess = newSourceTableAccess(table, sourceMetadataRoot);
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
        CompatStorageContext compatStorage = null;
        FileIO compatFileIo = null;
        try {
          compatStorage = resolveCompatStorage(table, snapshot);
          compatFileIo = newCompatFileIo(table, snapshot, compatStorage);
          String manifestList =
              ensureCompatArtifacts(
                  compatFileIo,
                  sourceAccess,
                  table,
                  snapshot,
                  compatStorage.metadataRoot(),
                  sourceMetadataRoot);
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
        } finally {
          closeQuietly(compatFileIo);
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
      closeQuietly(sourceAccess == null ? null : sourceAccess.sourceFileIo());
    }
  }

  protected FileIO newSourceFileIo(Table table, String metadataRoot) {
    return FileIoFactory.createFileIo(newSourceFileIoProperties(table, metadataRoot), config, true);
  }

  protected Map<String, String> newSourceFileIoProperties(Table table, String metadataRoot) {
    if (tableGatewaySupport != null) {
      return tableGatewaySupport.serverSideFileIoPropertiesForLocation(table, metadataRoot);
    }
    LinkedHashMap<String, String> props = new LinkedHashMap<>();
    if (table != null && table.getPropertiesCount() > 0) {
      table
          .getPropertiesMap()
          .forEach(
              (k, v) -> {
                if (k != null && v != null && FileIoFactory.isFileIoProperty(k)) {
                  props.put(k, v);
                }
              });
    }
    return props.isEmpty() ? Map.of() : Map.copyOf(props);
  }

  protected SourceTableAccess newSourceTableAccess(Table table, String metadataRoot) {
    return new SourceTableAccess(
        newSourceFileIoProperties(table, metadataRoot), newSourceFileIo(table, metadataRoot));
  }

  protected record CompatSnapshotFiles(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {}

  protected FileIO newFileIo(Table table) {
    Map<String, String> props = new LinkedHashMap<>();
    if (tableGatewaySupport != null) {
      props.putAll(
          tableGatewaySupport.serverSideFileIoPropertiesForLocation(table, metadataRoot(table)));
    } else if (table != null && table.getPropertiesCount() > 0) {
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
      FileIO compatFileIo,
      SourceTableAccess sourceAccess,
      Table table,
      Snapshot snapshot,
      String compatMetadataRoot,
      String sourceMetadataRoot)
      throws Exception {
    long snapshotId = snapshot.getSnapshotId();
    String compatMetadataPath = compatMetadataPath(compatMetadataRoot, snapshotId);
    PartitionSpec requestedSpec = resolveDataSpec(table, snapshot);
    String tableLocation = tableRootFromMetadataRoot(compatMetadataRoot);
    Schema schema = parseSnapshotSchema(snapshot, table);
    if (schema == null) {
      schema = new Schema();
    }

    CompatTableOperations tableOps =
        new CompatTableOperations(
            compatFileIo, tableLocation, compatMetadataRoot, compatMetadataPath);
    int formatVersion = requiredFormatVersion(schema);
    Map<String, String> initialProps =
        formatVersion > DEFAULT_COMPAT_FORMAT_VERSION
            ? Map.of("format-version", Integer.toString(formatVersion))
            : Map.of();
    tableOps.initialize(
        TableMetadata.newTableMetadata(
            schema,
            requestedSpec == null ? UNPARTITIONED : requestedSpec,
            SortOrder.unsorted(),
            tableLocation,
            initialProps));
    PartitionSpec effectiveSpec =
        tableOps.current() == null || tableOps.current().spec() == null
            ? UNPARTITIONED
            : tableOps.current().spec();
    CompatSnapshotFiles snapshotFiles =
        loadCompatSnapshotFiles(
            compatFileIo,
            sourceAccess,
            table,
            snapshot,
            compatMetadataRoot,
            sourceMetadataRoot,
            effectiveSpec,
            schema);

    BaseTable compatTable = new BaseTable(tableOps, "delta-compat-" + snapshotId);
    Transaction transaction = compatTable.newTransaction();
    RowDelta rowDelta = transaction.newRowDelta();
    for (DataFile dataFile : snapshotFiles.dataFiles()) {
      rowDelta.addRows(dataFile);
    }
    for (DeleteFile deleteFile : snapshotFiles.deleteFiles()) {
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

  /**
   * Returns the minimum Iceberg format version the schema can be expressed in. Types such as {@code
   * variant} (carried over from Delta tables) are only valid from format version 3 onwards;
   * building {@link TableMetadata} at v2 would otherwise fail {@code Schema.checkCompatibility} and
   * leave the snapshot without a manifest list.
   */
  private static int requiredFormatVersion(Schema schema) {
    if (schema == null) {
      return DEFAULT_COMPAT_FORMAT_VERSION;
    }
    return requiresV3(schema.asStruct()) ? 3 : DEFAULT_COMPAT_FORMAT_VERSION;
  }

  private static boolean requiresV3(Type type) {
    if (type == null) {
      return false;
    }
    switch (type.typeId()) {
      case VARIANT:
      case GEOMETRY:
      case GEOGRAPHY:
      case TIMESTAMP_NANO:
        return true;
      default:
        break;
    }
    if (type.isStructType()) {
      for (Types.NestedField field : type.asStructType().fields()) {
        if (requiresV3(field.type())) {
          return true;
        }
      }
      return false;
    }
    if (type.isListType()) {
      return requiresV3(type.asListType().elementType());
    }
    if (type.isMapType()) {
      return requiresV3(type.asMapType().keyType()) || requiresV3(type.asMapType().valueType());
    }
    return false;
  }

  protected CompatSnapshotFiles loadCompatSnapshotFiles(
      FileIO compatFileIo,
      SourceTableAccess sourceAccess,
      Table table,
      Snapshot snapshot,
      String compatMetadataRoot,
      String sourceMetadataRoot,
      PartitionSpec dataSpec,
      Schema schema)
      throws Exception {
    long snapshotId = snapshot.getSnapshotId();
    if (snapshotId < 0) {
      LOG.warnf(
          "Delta compat snapshot skipped table=%s snapshot=%d because snapshot id is negative",
          table != null && table.hasResourceId() ? table.getResourceId().getId() : "<unknown>",
          snapshotId);
      return new CompatSnapshotFiles(List.of(), List.of());
    }

    String tableRoot = tableRootFromMetadataRoot(sourceMetadataRoot);
    LOG.infof(
        "Delta compat loading snapshot files table=%s snapshot=%d sourceMetadataRoot=%s tableRoot=%s",
        table != null && table.hasResourceId() ? table.getResourceId().getId() : "<unknown>",
        snapshotId,
        sourceMetadataRoot,
        tableRoot);
    if (tableRoot == null || tableRoot.isBlank()) {
      LOG.warnf(
          "Delta compat snapshot skipped table=%s snapshot=%d because table root could not be derived from sourceMetadataRoot=%s",
          table != null && table.hasResourceId() ? table.getResourceId().getId() : "<unknown>",
          snapshotId,
          sourceMetadataRoot);
      return new CompatSnapshotFiles(List.of(), List.of());
    }

    DeltaEngineContext deltaEngine = newDeltaEngineContext(sourceAccess, tableRoot);
    try {
      Engine engine = deltaEngine.engine();
      io.delta.kernel.Table deltaTable = io.delta.kernel.Table.forPath(engine, tableRoot);
      io.delta.kernel.Snapshot deltaSnapshot =
          deltaTable.getSnapshotAsOfVersion(engine, snapshotId);
      LOG.infof(
          "Delta compat resolved upstream snapshot table=%s snapshot=%d tableRoot=%s",
          table != null && table.hasResourceId() ? table.getResourceId().getId() : "<unknown>",
          snapshotId,
          tableRoot);
      ScanBuilder scanBuilder = deltaSnapshot.getScanBuilder();
      Scan scan = scanBuilder.build();

      List<DataFile> dataFiles = new ArrayList<>();
      List<DeleteFile> deletes = new ArrayList<>();
      CloseableIterator<FilteredColumnarBatch> scanFiles =
          scan instanceof ScanImpl scanImpl
              ? scanImpl.getScanFiles(engine, true)
              : scan.getScanFiles(engine);
      try (CloseableIterator<FilteredColumnarBatch> batches = scanFiles) {
        while (batches.hasNext()) {
          FilteredColumnarBatch batch = batches.next();
          try (CloseableIterator<Row> rows = batch.getRows()) {
            while (rows.hasNext()) {
              Row scanFileRow = rows.next();
              AddFile add =
                  new AddFile(scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
              dataFiles.add(toDataFile(scanFileRow, add, dataSpec, schema));

              String dataPath = InternalScanFileUtils.getAddFileStatus(scanFileRow).getPath();
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
                      compatFileIo,
                      compatMetadataRoot,
                      snapshotId,
                      dataPath,
                      dv.getUniqueId(),
                      positions));
            }
          }
        }
      }
      LOG.infof(
          "Delta compat snapshot files loaded table=%s snapshot=%d dataFiles=%d deleteFiles=%d tableRoot=%s compatMetadataRoot=%s",
          table != null && table.hasResourceId() ? table.getResourceId().getId() : "<unknown>",
          snapshotId,
          dataFiles.size(),
          deletes.size(),
          tableRoot,
          compatMetadataRoot);
      return new CompatSnapshotFiles(List.copyOf(dataFiles), List.copyOf(deletes));
    } finally {
      closeQuietly(deltaEngine);
    }
  }

  protected DeltaEngineContext newDeltaEngineContext(
      SourceTableAccess sourceAccess, String tableRoot) {
    if (tableRoot != null && tableRoot.toLowerCase(Locale.ROOT).startsWith("s3://")) {
      RefreshingAwsClient<S3Client> s3 =
          buildS3Client(sourceAccess == null ? Map.of() : sourceAccess.fileIoProperties());
      return new DeltaEngineContext(DefaultEngine.create(new S3FileSystemClient(s3)), s3);
    }
    FileIO sourceFileIo = sourceAccess == null ? null : sourceAccess.sourceFileIo();
    if (sourceFileIo == null) {
      throw new IllegalArgumentException("Source FileIO is required for non-S3 Delta table roots");
    }
    return new DeltaEngineContext(
        DefaultEngine.create(new IcebergFileIoAdapter(sourceFileIo)), null);
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
      FileIO compatFileIo,
      SourceTableAccess sourceAccess,
      Table table,
      Snapshot snapshot,
      String compatMetadataRoot,
      String sourceMetadataRoot)
      throws Exception {
    String manifestListPath =
        readManifestListFromCompatMetadata(
            compatFileIo, compatMetadataRoot, snapshot.getSnapshotId());
    if (manifestListPath != null) {
      return manifestListPath;
    }
    return writeManifestArtifacts(
        compatFileIo, sourceAccess, table, snapshot, compatMetadataRoot, sourceMetadataRoot);
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

  private DataFile toDataFile(Row scanFileRow, AddFile add, PartitionSpec dataSpec, Schema schema) {
    PartitionSpec spec = dataSpec == null ? UNPARTITIONED : dataSpec;
    FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath(fileStatus.getPath())
            .withFormat(resolveFileFormat(fileStatus.getPath()))
            .withFileSizeInBytes(fileStatus.getSize())
            .withRecordCount(resolveRecordCount(add));
    if (spec.isPartitioned()) {
      maybeApplyPartition(builder, InternalScanFileUtils.getPartitionValues(scanFileRow), spec);
    }
    Metrics metrics = toMetrics(add, schema);
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
    String normalizedSchemaJson =
        DeltaSchemaNormalizer.normalizeSchemaJson(
            schemaJson, snapshot == null ? 0 : snapshot.getSchemaId(), rewriteVariantAsStruct());
    try {
      return SchemaParser.fromJson(
          normalizedSchemaJson == null || normalizedSchemaJson.isBlank()
              ? schemaJson
              : normalizedSchemaJson);
    } catch (RuntimeException e) {
      LOG.debugf(e, "Unable to parse schema JSON for compat partition spec construction");
      return null;
    }
  }

  private boolean rewriteVariantAsStruct() {
    if (gatewayConfig == null || gatewayConfig.deltaCompat().isEmpty()) {
      return false;
    }
    return gatewayConfig.deltaCompat().get().rewriteVariantAsStruct();
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

  private void maybeApplyPartition(
      DataFiles.Builder builder, Map<String, String> partitionValues, PartitionSpec spec) {
    if (builder == null
        || partitionValues == null
        || spec == null
        || !spec.isPartitioned()
        || partitionValues.isEmpty()) {
      return;
    }
    List<String> values = extractPartitionValues(partitionValues, spec);
    if (values.size() != spec.fields().size()) {
      return;
    }
    builder.withPartitionValues(values);
  }

  private List<String> extractPartitionValues(
      Map<String, String> partitionValues, PartitionSpec spec) {
    if (partitionValues == null || partitionValues.isEmpty() || spec == null) {
      return List.of();
    }
    int fieldCount = spec.fields().size();
    List<String> values = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      values.add(null);
    }
    int assigned = 0;
    for (int i = 0; i < spec.fields().size(); i++) {
      org.apache.iceberg.PartitionField field = spec.fields().get(i);
      String sourceName = spec.schema().findColumnName(field.sourceId());
      String value = partitionValues.get(field.name());
      if (value == null && sourceName != null) {
        value = partitionValues.get(sourceName);
      }
      if (value == null) {
        continue;
      }
      values.set(i, value);
      assigned++;
    }
    return assigned == 0 ? List.of() : values;
  }

  private Metrics toMetrics(AddFile add, Schema schema) {
    if (add == null || schema == null) {
      return null;
    }
    Optional<String> maybeStatsJson = add.getStatsJson();
    if (maybeStatsJson.isEmpty() || maybeStatsJson.get().isBlank()) {
      return null;
    }
    Map<Integer, Long> valueCounts = new LinkedHashMap<>();
    Map<Integer, Long> nullValueCounts = new LinkedHashMap<>();
    Map<Integer, ByteBuffer> lowerBounds = new LinkedHashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new LinkedHashMap<>();
    long recordCount;
    JsonNode root;
    try {
      root = JSON.readTree(maybeStatsJson.get());
      recordCount = root == null ? 0L : Math.max(0L, root.path("numRecords").asLong(0L));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse Delta stats JSON for compat metrics");
      return null;
    }
    JsonNode nullCounts = root.path("nullCount");
    JsonNode minValues = root.path("minValues");
    JsonNode maxValues = root.path("maxValues");
    for (Types.NestedField field : schema.columns()) {
      if (!field.type().isPrimitiveType()) {
        continue;
      }
      String fieldName = field.name();
      if (nullCounts.has(fieldName)) {
        valueCounts.put(field.fieldId(), recordCount);
        nullValueCounts.put(field.fieldId(), Math.max(0L, nullCounts.path(fieldName).asLong(0L)));
      }
      if (minValues.has(fieldName)) {
        valueCounts.putIfAbsent(field.fieldId(), recordCount);
        encodeBound(lowerBounds, field, minValues.get(fieldName));
      }
      if (maxValues.has(fieldName)) {
        valueCounts.putIfAbsent(field.fieldId(), recordCount);
        encodeBound(upperBounds, field, maxValues.get(fieldName));
      }
    }
    if (valueCounts.isEmpty()
        && nullValueCounts.isEmpty()
        && lowerBounds.isEmpty()
        && upperBounds.isEmpty()) {
      return null;
    }
    return new Metrics(
        recordCount,
        null,
        valueCounts.isEmpty() ? null : valueCounts,
        nullValueCounts.isEmpty() ? null : nullValueCounts,
        null,
        lowerBounds.isEmpty() ? null : lowerBounds,
        upperBounds.isEmpty() ? null : upperBounds);
  }

  private long resolveRecordCount(AddFile add) {
    if (add == null) {
      return 0L;
    }
    Optional<Long> count = add.getNumRecords();
    if (count.isPresent()) {
      return Math.max(0L, count.get());
    }
    Optional<String> statsJson = add.getStatsJson();
    if (statsJson.isEmpty() || statsJson.get().isBlank()) {
      return 0L;
    }
    try {
      return Math.max(0L, JSON.readTree(statsJson.get()).path("numRecords").asLong(0L));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse Delta stats JSON for compat record count");
      return 0L;
    }
  }

  private void encodeBound(
      Map<Integer, ByteBuffer> out, Types.NestedField field, JsonNode valueNode) {
    if (field == null
        || valueNode == null
        || valueNode.isNull()
        || !field.type().isPrimitiveType()) {
      return;
    }
    Object decoded = decodeValue(field.type().asPrimitiveType(), valueNode);
    if (decoded == null) {
      return;
    }
    try {
      out.put(field.fieldId(), Conversions.toByteBuffer(field.type().asPrimitiveType(), decoded));
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Skipping compat metrics bound for field=%d type=%s value=%s",
          field.fieldId(),
          field.type(),
          valueNode);
    }
  }

  private Object decodeValue(Type.PrimitiveType type, JsonNode value) {
    try {
      return switch (type.typeId()) {
        case BOOLEAN -> value.asBoolean();
        case INTEGER -> value.asInt();
        case LONG -> value.asLong();
        case FLOAT -> (float) value.asDouble();
        case DOUBLE -> value.asDouble();
        case DATE -> (int) LocalDate.parse(value.asText()).toEpochDay();
        case TIME -> value.asLong();
        case TIMESTAMP -> java.time.Instant.parse(value.asText()).toEpochMilli() * 1000L;
        case STRING -> value.asText();
        case BINARY -> value.binaryValue();
        case DECIMAL -> new java.math.BigDecimal(value.asText());
        case UUID -> java.util.UUID.fromString(value.asText());
        default -> null;
      };
    } catch (Exception e) {
      LOG.debugf(e, "Skipping compat metrics bound decode type=%s value=%s", type.typeId(), value);
      return null;
    }
  }

  private FileFormat resolveFileFormat(String raw) {
    if (raw == null || raw.isBlank()) {
      return FileFormat.PARQUET;
    }
    String normalized = raw.toLowerCase(Locale.ROOT);
    if (normalized.endsWith(".parquet")) {
      return FileFormat.PARQUET;
    }
    if (normalized.endsWith(".avro")) {
      return FileFormat.AVRO;
    }
    if (normalized.endsWith(".orc")) {
      return FileFormat.ORC;
    }
    try {
      return FileFormat.fromString(raw);
    } catch (RuntimeException ignored) {
      try {
        return FileFormat.fromString(normalized);
      } catch (RuntimeException ignoredAgain) {
        return FileFormat.PARQUET;
      }
    }
  }

  protected FileFormat fileFormatForPath(String raw) {
    return resolveFileFormat(raw);
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
    String location = firstNonBlank(table.getPropertiesMap().get("storage_location"));
    if (location == null) {
      location = firstNonBlank(table.getPropertiesMap().get("delta.table-root"));
    }
    if (location == null) {
      location = firstNonBlank(table.getPropertiesMap().get("external.location"));
    }
    if (location == null) {
      location = firstNonBlank(table.getPropertiesMap().get("location"));
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

  protected CompatStorageContext resolveCompatStorage(Table table, Snapshot snapshot) {
    ResolveSnapshotCompatStorageResponse response =
        grpcClient.resolveSnapshotCompatStorage(
            ResolveSnapshotCompatStorageRequest.newBuilder()
                .setTableId(table.getResourceId())
                .setSnapshotId(snapshot.getSnapshotId())
                .build());
    if (response == null || response.getLocationPrefix().isBlank()) {
      throw new IllegalStateException("Compat storage resolution returned no writable location");
    }
    Map<String, String> fileIoProps = new LinkedHashMap<>();
    if (response.hasStorage() && response.getStorage().getClientSafeConfigCount() > 0) {
      fileIoProps.putAll(response.getStorage().getClientSafeConfigMap());
    }
    if (response.hasStorage() && response.getStorage().getStorageCredentialsCount() > 0) {
      fileIoProps.putAll(response.getStorage().getStorageCredentials(0).getConfigMap());
    }
    String location = trimTrailingSlash(response.getLocationPrefix());
    return new CompatStorageContext(location + "/" + METADATA_DIR, Map.copyOf(fileIoProps));
  }

  protected FileIO newCompatFileIo(
      Table table, Snapshot snapshot, CompatStorageContext compatStorage) {
    return FileIoFactory.createFileIo(compatStorage.fileIoProperties(), config, true);
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

  private String trimTrailingSlash(String value) {
    if (value == null) {
      return null;
    }
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
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

  private void closeQuietly(DeltaEngineContext deltaEngine) {
    if (deltaEngine == null || deltaEngine.closeable() == null) {
      return;
    }
    try {
      deltaEngine.closeable().close();
    } catch (Exception e) {
      LOG.debugf(e, "Failed to close Delta engine resource %s", deltaEngine.closeable());
    }
  }

  private RefreshingAwsClient<S3Client> buildS3Client(Map<String, String> props) {
    Map<String, String> sourceProps = props == null ? Map.of() : props;
    Region region = Region.of(resolveOption(sourceProps, "s3.region", "aws.region", "us-east-1"));
    boolean pathStyle =
        Boolean.parseBoolean(resolveOption(sourceProps, "s3.path-style-access", "false"));
    Supplier<AwsCredentialsProvider> credentials = () -> resolveCredentials(sourceProps);
    String endpoint = resolveOption(sourceProps, "s3.endpoint", null);
    return RefreshingAwsClient.withResourceFactory(
        () -> {
          AwsCredentialsProvider provider = credentials.get();
          software.amazon.awssdk.services.s3.S3ClientBuilder builder =
              S3Client.builder()
                  .region(region)
                  .serviceConfiguration(
                      S3Configuration.builder().pathStyleAccessEnabled(pathStyle).build())
                  .credentialsProvider(provider);
          try {
            if (endpoint != null && !endpoint.isBlank()) {
              builder.endpointOverride(URI.create(endpoint));
            }
            return RefreshingAwsClient.clientResource(
                builder.build(), RefreshingAwsClient.closeableResource(provider));
          } catch (RuntimeException | Error e) {
            RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(provider));
            throw e;
          }
        });
  }

  private AwsCredentialsProvider resolveCredentials(Map<String, String> props) {
    String access = resolveOption(props, "s3.access-key-id", null);
    String secret = resolveOption(props, "s3.secret-access-key", null);
    String token = resolveOption(props, "s3.session-token", null);
    if (access != null && !access.isBlank() && secret != null && !secret.isBlank()) {
      AwsCredentials creds =
          token != null && !token.isBlank()
              ? AwsSessionCredentials.create(access, secret, token)
              : AwsBasicCredentials.create(access, secret);
      return StaticCredentialsProvider.create(creds);
    }
    return DefaultCredentialsProvider.builder().build();
  }

  private String resolveOption(Map<String, String> props, String key, String defaultValue) {
    if (props != null) {
      String value = props.get(key);
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return defaultValue;
  }

  private String resolveOption(
      Map<String, String> props, String key, String fallbackKey, String defaultValue) {
    if (props != null) {
      String value = props.get(key);
      if (value != null && !value.isBlank()) {
        return value;
      }
      String fallback = props.get(fallbackKey);
      if (fallback != null && !fallback.isBlank()) {
        return fallback;
      }
    }
    return defaultValue;
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
    public void copyFileAtomically(String sourcePath, String destinationPath, boolean overwrite) {
      throw new UnsupportedOperationException("Delta engine copy is not supported in gateway");
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
      try {
        TableMetadataParser.write(metadata, fileIo.newOutputFile(compatMetadataPath));
        current = metadata;
      } catch (AlreadyExistsException e) {
        TableMetadata existing = readCommittedMetadata();
        if (existing == null) {
          throw e;
        }
        current = existing;
      }
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

    private TableMetadata readCommittedMetadata() {
      if (!fileIo.newInputFile(compatMetadataPath).exists()) {
        return null;
      }
      TableMetadata metadata = TableMetadataParser.read(fileIo, compatMetadataPath);
      if (metadata == null
          || metadata.currentSnapshot() == null
          || metadata.currentSnapshot().manifestListLocation() == null
          || metadata.currentSnapshot().manifestListLocation().isBlank()) {
        return null;
      }
      return metadata;
    }
  }

  protected static record CompatStorageContext(
      String metadataRoot, Map<String, String> fileIoProperties) {}

  protected record DeltaEngineContext(Engine engine, AutoCloseable closeable) {}

  private static final class S3FileSystemClient
      implements io.delta.kernel.defaults.engine.fileio.FileIO {
    private final RefreshingAwsClient<S3Client> s3;

    private S3FileSystemClient(RefreshingAwsClient<S3Client> s3) {
      this.s3 = s3;
    }

    @Override
    public io.delta.kernel.defaults.engine.fileio.InputFile newInputFile(
        String path, long fileSize) {
      String resolved = resolvePath(path);
      if (isMissingCheckpoint(resolved)) {
        return new MissingInputFile(resolved);
      }
      return new S3InputFile(s3, resolved);
    }

    @Override
    public io.delta.kernel.defaults.engine.fileio.OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException(
          "Writing files not implemented for read-only S3 FileIO");
    }

    @Override
    public boolean delete(String path) {
      throw new UnsupportedOperationException(
          "Deleting files not implemented for read-only S3 FileIO");
    }

    @Override
    public void copyFileAtomically(String sourcePath, String destinationPath, boolean overwrite) {
      throw new UnsupportedOperationException(
          "Copying files not implemented for read-only S3 FileIO");
    }

    @Override
    public String resolvePath(String path) {
      if (path.startsWith("s3a://")) {
        return "s3://" + path.substring(6);
      }
      if (path.startsWith("s3://")) {
        return path;
      }
      throw new IllegalArgumentException("Unsupported file system path: " + path);
    }

    @Override
    public FileStatus getFileStatus(String path) throws IOException {
      URI uri = URI.create(resolvePath(path));
      String bucket = uri.getHost();
      String key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
      try {
        HeadObjectResponse head =
            s3.call(
                client ->
                    client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()));
        return FileStatus.of(path, head.contentLength(), Instant.now().toEpochMilli());
      } catch (S3Exception e) {
        if (e.statusCode() == 404) {
          throw new IOException("File not found: " + path, e);
        }
        throw new IOException("Failed to get file status for: " + path, e);
      }
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
      String resolved = resolvePath(filePath);
      URI uri = URI.create(resolved);
      String bucket = uri.getHost();
      if (bucket == null || bucket.isEmpty()) {
        throw new IOException("Invalid S3 path for listFrom: " + filePath);
      }
      String fullKey = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
      int lastSlash = fullKey.lastIndexOf('/');
      String dirPrefix = lastSlash < 0 ? "" : fullKey.substring(0, lastSlash + 1);
      String startKey = fullKey;

      FileStatus probedFirstStatus;
      try {
        HeadObjectResponse head =
            s3.call(
                client ->
                    client.headObject(
                        HeadObjectRequest.builder().bucket(bucket).key(startKey).build()));
        probedFirstStatus =
            FileStatus.of(
                filePath,
                head.contentLength(),
                head.lastModified() != null
                    ? head.lastModified().toEpochMilli()
                    : Instant.now().toEpochMilli());
      } catch (S3Exception e) {
        probedFirstStatus = null;
      }
      final FileStatus firstStatus = probedFirstStatus;

      return new CloseableIterator<>() {
        private String continuationToken = null;
        private Iterator<S3Object> pageIter = null;
        private boolean yieldedFirst = firstStatus == null;
        private FileStatus bufferedNext = null;
        private boolean closed = false;

        @Override
        public boolean hasNext() {
          if (closed) {
            return false;
          }
          return ensureBufferedNext();
        }

        @Override
        public FileStatus next() {
          if (closed) {
            throw new NoSuchElementException("Iterator closed");
          }
          if (!ensureBufferedNext()) {
            throw new NoSuchElementException();
          }
          FileStatus out = bufferedNext;
          bufferedNext = null;
          return out;
        }

        @Override
        public void close() {
          closed = true;
          pageIter = null;
          bufferedNext = null;
        }

        private boolean ensureBufferedNext() {
          if (bufferedNext != null) {
            return true;
          }
          if (!yieldedFirst && firstStatus != null) {
            yieldedFirst = true;
            bufferedNext = firstStatus;
            return true;
          }
          while (true) {
            if (pageIter != null && pageIter.hasNext()) {
              S3Object object = pageIter.next();
              String key = object.key();
              if (key == null || key.endsWith("/") || !key.startsWith(dirPrefix)) {
                continue;
              }
              bufferedNext =
                  FileStatus.of(
                      "s3://" + bucket + "/" + key,
                      object.size() == null ? 0L : object.size(),
                      object.lastModified() == null
                          ? Instant.now().toEpochMilli()
                          : object.lastModified().toEpochMilli());
              return true;
            }
            if (continuationToken == null && pageIter != null) {
              return false;
            }
            fetchNextPage();
          }
        }

        private void fetchNextPage() {
          ListObjectsV2Request.Builder request =
              ListObjectsV2Request.builder().bucket(bucket).prefix(dirPrefix).maxKeys(1000);
          request = request.startAfter(startKey);
          if (continuationToken != null) {
            request = request.continuationToken(continuationToken);
          }
          ListObjectsV2Request built = request.build();
          ListObjectsV2Response response = s3.callUnchecked(client -> client.listObjectsV2(built));
          continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
          List<S3Object> objects = response.contents();
          pageIter = (objects == null ? Collections.<S3Object>emptyList() : objects).iterator();
        }
      };
    }

    @Override
    public boolean mkdirs(String path) {
      return true;
    }

    @Override
    public Optional<String> getConf(String confKey) {
      return Optional.empty();
    }

    private boolean isMissingCheckpoint(String resolvedPath) {
      if (!resolvedPath.endsWith("/_last_checkpoint")) {
        return false;
      }
      URI uri = URI.create(resolvedPath);
      String bucket = uri.getHost();
      String key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
      try {
        s3.callUnchecked(
            client ->
                client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()));
        return false;
      } catch (S3Exception e) {
        if (e.statusCode() == 404) {
          return true;
        }
        throw e;
      }
    }
  }

  private static final class S3InputFile
      implements io.delta.kernel.defaults.engine.fileio.InputFile {
    private final RefreshingAwsClient<S3Client> s3;
    private final String resolvedPath;
    private final String bucket;
    private final String key;
    private final long length;

    private S3InputFile(RefreshingAwsClient<S3Client> s3, String resolvedPath) {
      this.s3 = s3;
      this.resolvedPath = resolvedPath;
      URI uri = URI.create(resolvedPath);
      this.bucket = uri.getHost();
      this.key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
      try {
        this.length = new S3RangeReader(s3, bucket, key).length();
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize S3RangeReader for " + resolvedPath, e);
      }
    }

    @Override
    public io.delta.kernel.defaults.engine.fileio.SeekableInputStream newStream() {
      try {
        return new S3SeekableInputStream(new S3RangeReader(s3, bucket, key, length));
      } catch (IOException e) {
        throw new RuntimeException("Failed to open stream for " + resolvedPath, e);
      }
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public String path() {
      return resolvedPath;
    }
  }

  private static final class S3SeekableInputStream
      extends io.delta.kernel.defaults.engine.fileio.SeekableInputStream {
    private final S3RangeReader rangeReader;
    private long pos = 0L;

    private S3SeekableInputStream(S3RangeReader rangeReader) {
      this.rangeReader = rangeReader;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0) {
        throw new IOException("negative seek");
      }
      pos = newPos;
    }

    @Override
    public void readFully(byte[] bytes, int off, int len) throws IOException {
      int remaining = len;
      while (remaining > 0) {
        int read = read(bytes, off + (len - remaining), remaining);
        if (read < 0) {
          throw new IOException("Unexpected EOF");
        }
        remaining -= read;
      }
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int read = read(one, 0, 1);
      return read < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      int read = rangeReader.readAt(pos, bytes, off, len);
      if (read > 0) {
        pos += read;
      }
      return read;
    }

    @Override
    public void close() {
      rangeReader.close();
    }
  }

  private static final class S3RangeReader {
    private static final int DEFAULT_CHUNK_SIZE = 16 * 1024 * 1024;

    private final RefreshingAwsClient<S3Client> s3;
    private final String bucket;
    private final String key;
    private final long fileLength;
    private final byte[] buffer;
    private long bufferStart = 0L;
    private int bufferLength = 0;
    private boolean closed = false;

    private S3RangeReader(RefreshingAwsClient<S3Client> s3, String bucket, String key)
        throws IOException {
      this(s3, bucket, key, DEFAULT_CHUNK_SIZE);
    }

    private S3RangeReader(
        RefreshingAwsClient<S3Client> s3, String bucket, String key, long fileLength)
        throws IOException {
      this(s3, bucket, key, DEFAULT_CHUNK_SIZE, fileLength);
    }

    private S3RangeReader(
        RefreshingAwsClient<S3Client> s3, String bucket, String key, int chunkSize)
        throws IOException {
      this(s3, bucket, key, chunkSize, null);
    }

    private S3RangeReader(
        RefreshingAwsClient<S3Client> s3,
        String bucket,
        String key,
        int chunkSize,
        Long knownLength)
        throws IOException {
      this.s3 = s3;
      this.bucket = bucket;
      this.key = key;
      this.buffer = new byte[Math.max(1024, chunkSize)];
      if (knownLength != null) {
        this.fileLength = knownLength;
      } else {
        try {
          HeadObjectResponse head =
              s3.call(
                  client ->
                      client.headObject(
                          HeadObjectRequest.builder().bucket(bucket).key(key).build()));
          this.fileLength = head.contentLength();
        } catch (S3Exception e) {
          throw new IOException("Failed to get S3 object length for " + bucket + "/" + key, e);
        }
      }
    }

    private synchronized void close() {
      closed = true;
    }

    private long length() {
      return fileLength;
    }

    private int readAt(long position, byte[] dest, int off, int len) throws IOException {
      if (closed) {
        throw new IOException("reader closed");
      }
      if (len == 0) {
        return 0;
      }
      if (position >= fileLength) {
        return -1;
      }
      ensureBufferFor(position);
      if (bufferLength <= 0) {
        return -1;
      }
      long bufferEnd = bufferStart + bufferLength;
      if (position < bufferStart || position >= bufferEnd) {
        ensureBufferFor(position);
        if (bufferLength <= 0) {
          return -1;
        }
        bufferEnd = bufferStart + bufferLength;
      }
      int available = (int) Math.min(bufferEnd - position, (long) len);
      int bufferOffset = (int) (position - bufferStart);
      System.arraycopy(buffer, bufferOffset, dest, off, available);
      return available;
    }

    private void ensureBufferFor(long position) throws IOException {
      if (position >= bufferStart && position < bufferStart + bufferLength) {
        return;
      }
      long start = position;
      long end = Math.min(start + buffer.length - 1L, fileLength - 1L);
      if (start > end) {
        bufferLength = 0;
        return;
      }
      String range = "bytes=" + start + "-" + end;
      try {
        int offset =
            s3.call(
                client -> {
                  try (var object =
                      client.getObject(
                          software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                              .bucket(bucket)
                              .key(key)
                              .range(range)
                              .build())) {
                    int bytesRead = 0;
                    int read;
                    while (bytesRead < buffer.length
                        && (read = object.read(buffer, bytesRead, buffer.length - bytesRead)) > 0) {
                      bytesRead += read;
                    }
                    return bytesRead;
                  }
                });
        bufferStart = start;
        bufferLength = offset;
      } catch (S3Exception e) {
        if (e.statusCode() == 416) {
          bufferLength = 0;
          return;
        }
        throw new IOException("S3 read error for " + bucket + "/" + key + " at range " + range, e);
      }
    }
  }

  private static final class MissingInputFile
      implements io.delta.kernel.defaults.engine.fileio.InputFile {
    private final String path;

    private MissingInputFile(String path) {
      this.path = path;
    }

    @Override
    public io.delta.kernel.defaults.engine.fileio.SeekableInputStream newStream() {
      return new MissingSeekableInputStream(path);
    }

    @Override
    public long length() {
      return 0L;
    }

    @Override
    public String path() {
      return path;
    }
  }

  private static final class MissingSeekableInputStream
      extends io.delta.kernel.defaults.engine.fileio.SeekableInputStream {
    private final String path;

    private MissingSeekableInputStream(String path) {
      this.path = path;
    }

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos != 0) {
        throw new FileNotFoundException(path);
      }
    }

    @Override
    public void readFully(byte[] bytes, int off, int len) throws IOException {
      throw new FileNotFoundException(path);
    }

    @Override
    public int read() throws IOException {
      throw new FileNotFoundException(path);
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      throw new FileNotFoundException(path);
    }
  }
}
