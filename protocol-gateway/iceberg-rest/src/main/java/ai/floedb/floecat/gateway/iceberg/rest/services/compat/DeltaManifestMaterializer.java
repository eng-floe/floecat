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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.jboss.logging.Logger;

@ApplicationScoped
public class DeltaManifestMaterializer {
  private static final Logger LOG = Logger.getLogger(DeltaManifestMaterializer.class);
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
    long sequenceNumber =
        snapshot.getSequenceNumber() > 0 ? snapshot.getSequenceNumber() : snapshotId;
    Long parentSnapshotId =
        snapshot.getParentSnapshotId() > 0 ? snapshot.getParentSnapshotId() : null;

    String manifestPath = metadataRoot + "/" + snapshotId + "-compat-m0.avro";
    String manifestListPath = metadataRoot + "/snap-" + snapshotId + "-compat.avro";
    deleteIfExists(fileIo, manifestPath);
    deleteIfExists(fileIo, manifestListPath);

    ManifestFile manifestFile;
    OutputFile manifestOutput = fileIo.newOutputFile(manifestPath);
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(2, UNPARTITIONED, manifestOutput, snapshotId);
    try {
      if (bundle != null) {
        for (ScanFile dataFile : bundle.getDataFilesList()) {
          writer.add(toDataFile(dataFile));
        }
      }
      writer.close();
      manifestFile = writer.toManifestFile();
    } catch (Exception e) {
      try {
        writer.close();
      } catch (Exception ignored) {
      }
      throw e;
    }

    writeManifestList(
        fileIo.newOutputFile(manifestListPath),
        snapshotId,
        parentSnapshotId,
        sequenceNumber,
        manifestFile);
    return manifestListPath;
  }

  private String ensureCompatArtifacts(
      FileIO fileIo, Table table, Snapshot snapshot, String metadataRoot) throws Exception {
    long snapshotId = snapshot.getSnapshotId();
    String manifestListPath = metadataRoot + "/snap-" + snapshotId + "-compat.avro";
    if (inputExists(fileIo, manifestListPath)) {
      return manifestListPath;
    }
    return writeManifestArtifacts(fileIo, table, snapshot, metadataRoot);
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

  private DataFile toDataFile(ScanFile file) {
    return DataFiles.builder(UNPARTITIONED)
        .withPath(file.getFilePath())
        .withFormat(resolveFileFormat(file.getFileFormat()))
        .withFileSizeInBytes(file.getFileSizeInBytes())
        .withRecordCount(file.getRecordCount())
        .build();
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

  private void writeManifestList(
      OutputFile outputFile,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      ManifestFile manifestFile)
      throws Exception {
    Class<?> manifestLists = Class.forName("org.apache.iceberg.ManifestLists");
    Method write =
        manifestLists.getDeclaredMethod(
            "write", int.class, OutputFile.class, long.class, Long.class, long.class, Long.class);
    write.setAccessible(true);
    Object writer =
        write.invoke(null, 2, outputFile, snapshotId, parentSnapshotId, sequenceNumber, null);
    Method add = findMethod(writer.getClass(), "add", Object.class);
    Method close = findMethod(writer.getClass(), "close");
    try {
      add.invoke(writer, manifestFile);
    } finally {
      close.invoke(writer);
    }
  }

  private Method findMethod(Class<?> type, String name, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Class<?> current = type;
    while (current != null) {
      try {
        Method method = current.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
      } catch (NoSuchMethodException ignored) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchMethodException(type.getName() + "." + name);
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

  private void closeQuietly(FileIO fileIo) {
    if (fileIo instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.debugf(e, "Failed to close compat FileIO %s", fileIo.getClass().getName());
      }
    }
  }

}
