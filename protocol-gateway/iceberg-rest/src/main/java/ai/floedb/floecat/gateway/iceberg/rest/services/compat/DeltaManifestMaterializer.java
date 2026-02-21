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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
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

    ManifestFile dataManifest;
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
      dataManifest = writer.toManifestFile();
    } catch (Exception e) {
      try {
        writer.close();
      } catch (Exception ignored) {
      }
      throw e;
    }

    List<ManifestFile> manifests = new ArrayList<>();
    manifests.add(dataManifest);
    manifests.addAll(buildDeleteManifests(fileIo, table, snapshot, metadataRoot));

    writeManifestList(
        fileIo.newOutputFile(manifestListPath),
        snapshotId,
        parentSnapshotId,
        sequenceNumber,
        manifests);
    return manifestListPath;
  }

  private List<ManifestFile> buildDeleteManifests(
      FileIO fileIo, Table table, Snapshot snapshot, String metadataRoot) {
    try {
      List<DeleteFile> deleteFiles =
          loadDeltaPositionDeleteFiles(fileIo, table, snapshot, metadataRoot);
      if (deleteFiles.isEmpty()) {
        return List.of();
      }
      long snapshotId = snapshot.getSnapshotId();
      String deleteManifestPath = metadataRoot + "/" + snapshotId + "-compat-d0.avro";
      deleteIfExists(fileIo, deleteManifestPath);
      ManifestWriter<DeleteFile> deleteWriter =
          ManifestFiles.writeDeleteManifest(
              2, UNPARTITIONED, fileIo.newOutputFile(deleteManifestPath), snapshotId);
      try {
        for (DeleteFile deleteFile : deleteFiles) {
          deleteWriter.add(deleteFile);
        }
        deleteWriter.close();
        return List.of(deleteWriter.toManifestFile());
      } catch (Exception e) {
        try {
          deleteWriter.close();
        } catch (Exception ignored) {
        }
        throw e;
      }
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Delta compat delete manifest generation failed table=%s snapshot=%d; continuing without delete manifests",
          table.getResourceId().getId(),
          snapshot.getSnapshotId());
      return List.of();
    }
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
            .rowSchema(org.apache.iceberg.io.DeleteSchemaUtil.pathPosSchema())
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
      List<ManifestFile> manifests)
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
      if (manifests != null) {
        for (ManifestFile manifest : manifests) {
          if (manifest != null) {
            add.invoke(writer, manifest);
          }
        }
      }
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
}
