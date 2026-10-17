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

package ai.floedb.floecat.delta.identity;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CommitRangeNotFoundException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.StartVersionNotFoundException;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableChangesUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Isolates uses of Delta Kernel implementation classes from the identity model and service. */
public final class KernelAdapter {
  private KernelAdapter() {}

  public static StructType deserializeSchema(String schemaJson) {
    try {
      return DataTypeJsonSerDe.deserializeStructType(schemaJson);
    } catch (KernelException failure) {
      throw classify(failure);
    }
  }

  public static Snapshot latestSnapshot(Table table, Engine engine) {
    try {
      return table.getLatestSnapshot(engine);
    } catch (KernelException failure) {
      throw classify(failure);
    }
  }

  /** Returns the oldest version Kernel can reconstruct from the retained log and checkpoints. */
  public static long earliestRecreatableCommit(Engine engine, String tablePath) {
    try {
      return DeltaHistoryManager.getEarliestRecreatableCommit(
          engine, new Path(new Path(tablePath), "_delta_log"), Optional.empty());
    } catch (KernelException failure) {
      throw classify(failure);
    }
  }

  static List<Long> metadataVersions(
      Engine engine, String tablePath, long firstVersion, long lastVersion) throws Exception {
    List<FileStatus> commitFiles =
        DeltaLogActionUtils.getCommitFilesForVersionRange(
            engine, new Path(tablePath), firstVersion, Optional.of(lastVersion));
    LinkedHashSet<Long> versions = new LinkedHashSet<>();
    try (var commits =
            DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
                engine,
                tablePath,
                commitFiles,
                Set.of(DeltaAction.METADATA, DeltaAction.PROTOCOL));
        var batches = TableChangesUtils.flattenCommitsAndAddMetadata(engine, commits)) {
      while (batches.hasNext()) {
        var batch = batches.next();
        int versionOrdinal = batch.getSchema().indexOf("version");
        int metadataOrdinal = batch.getSchema().indexOf(DeltaAction.METADATA.colName);
        int protocolOrdinal = batch.getSchema().indexOf(DeltaAction.PROTOCOL.colName);
        ColumnVector versionVector = batch.getColumnVector(versionOrdinal);
        for (int row = 0; row < batch.getSize(); row++) {
          boolean metadata =
              metadataOrdinal >= 0 && !batch.getColumnVector(metadataOrdinal).isNullAt(row);
          boolean protocol =
              protocolOrdinal >= 0 && !batch.getColumnVector(protocolOrdinal).isNullAt(row);
          if (metadata || protocol) {
            versions.add(versionVector.getLong(row));
          }
        }
      }
    }
    return List.copyOf(versions);
  }

  public static Snapshot loadExactSnapshot(
      Table table, Engine engine, String tablePath, long version) {
    try {
      Table versioned = table instanceof TableImpl ? Table.forPath(engine, tablePath) : table;
      Snapshot snapshot = versioned.getSnapshotAsOfVersion(engine, version);
      if (!(table instanceof TableImpl) || snapshot == null) {
        return snapshot;
      }
      return withExactCommitMetadata(snapshot, engine, tablePath, version);
    } catch (KernelException failure) {
      throw classify(failure);
    }
  }

  static boolean isTruncatedHistory(Throwable failure) {
    return causedBy(failure, TruncatedHistoryException.class);
  }

  static boolean isUnsupportedVoid(Throwable failure) {
    return causedBy(failure, UnsupportedVoidTypeException.class);
  }

  private static RuntimeException classify(KernelException failure) {
    if (causedBy(failure, StartVersionNotFoundException.class)
        || causedBy(failure, CommitRangeNotFoundException.class)) {
      return new TruncatedHistoryException(failure.getMessage(), failure);
    }
    String message = failure.getMessage();
    if (message != null
        && message
            .toUpperCase(java.util.Locale.ROOT)
            .contains("UNSUPPORTED DELTA DATA TYPE: VOID")) {
      return new UnsupportedVoidTypeException(message, failure);
    }
    return failure;
  }

  private static boolean causedBy(Throwable failure, Class<? extends Throwable> type) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (type.isInstance(current)) {
        return true;
      }
    }
    return false;
  }

  private static final class TruncatedHistoryException extends RuntimeException {
    private TruncatedHistoryException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final class UnsupportedVoidTypeException extends RuntimeException {
    private UnsupportedVoidTypeException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static String schemaJson(Snapshot snapshot) {
    if (snapshot instanceof CommitMetadataSnapshot exact) {
      return exact.metadata().getSchemaString();
    }
    if (snapshot instanceof SnapshotImpl implementation && implementation.getMetadata() != null) {
      return implementation.getMetadata().getSchemaString();
    }
    throw new IllegalStateException("Delta snapshot metadata schema JSON is required");
  }

  private static Snapshot withExactCommitMetadata(
      Snapshot snapshot, Engine engine, String tablePath, long version) {
    List<FileStatus> commitFiles = new ArrayList<>(1);
    try (var files =
        DeltaLogActionUtils.listDeltaLogFilesAsIter(
            engine,
            Set.of(FileNames.DeltaLogFileType.COMMIT),
            new Path(tablePath),
            version,
            Optional.of(version),
            true)) {
      while (files.hasNext()) {
        commitFiles.add(files.next());
      }
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to list Delta commit metadata for version " + version, e);
    }
    if (commitFiles.isEmpty()) {
      return snapshot;
    }
    Metadata metadata = null;
    Protocol protocol = protocol(snapshot);
    try (var commits =
        DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
            engine, tablePath, commitFiles, Set.of(DeltaAction.METADATA, DeltaAction.PROTOCOL))) {
      while (commits.hasNext()) {
        try (var actions = commits.next().getActions()) {
          while (actions.hasNext()) {
            var batch = actions.next();
            int metadataOrdinal = batch.getSchema().indexOf(DeltaAction.METADATA.colName);
            int protocolOrdinal = batch.getSchema().indexOf(DeltaAction.PROTOCOL.colName);
            for (int row = 0; row < batch.getSize(); row++) {
              if (metadataOrdinal >= 0 && !batch.getColumnVector(metadataOrdinal).isNullAt(row)) {
                metadata = Metadata.fromColumnVector(batch.getColumnVector(metadataOrdinal), row);
              }
              if (protocolOrdinal >= 0 && !batch.getColumnVector(protocolOrdinal).isNullAt(row)) {
                protocol = Protocol.fromColumnVector(batch.getColumnVector(protocolOrdinal), row);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read Delta commit metadata for version " + version, e);
    }
    return metadata == null
        ? snapshot
        : new CommitMetadataSnapshot(snapshot, metadata, protocol, version);
  }

  static boolean supportsColumnMapping(Snapshot snapshot) {
    return protocol(snapshot).supportsFeature(TableFeatures.COLUMN_MAPPING_RW_FEATURE);
  }

  static boolean supportsIcebergCompatV2(Snapshot snapshot) {
    return protocol(snapshot).supportsFeature(TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE);
  }

  static StructType physicalSchema(Snapshot snapshot) {
    if (snapshot instanceof SnapshotImpl snapshotImpl && snapshotImpl.getMetadata() != null) {
      return snapshotImpl.getMetadata().getPhysicalSchema();
    }
    return null;
  }

  private static Protocol protocol(Snapshot snapshot) {
    if (snapshot instanceof SnapshotImpl snapshotImpl) {
      return snapshotImpl.getProtocol();
    }
    if (snapshot instanceof ProtocolSnapshot protocolSnapshot) {
      return protocolSnapshot.protocol();
    }
    throw new IllegalArgumentException(
        "A Delta snapshot with column mapping configured must expose its protocol");
  }

  /** Test seam for protocol-bearing snapshots that are not Kernel implementations. */
  public interface ProtocolSnapshot {
    Protocol protocol();
  }

  private record CommitMetadataSnapshot(
      Snapshot delegate, Metadata metadata, Protocol protocol, long version)
      implements Snapshot, ProtocolSnapshot {
    @Override
    public String getPath() {
      return delegate.getPath();
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public List<String> getPartitionColumnNames() {
      return List.copyOf(metadata.getPartitionColNames());
    }

    @Override
    public long getTimestamp(Engine engine) {
      return delegate.getTimestamp(engine);
    }

    @Override
    public StructType getSchema() {
      return metadata.getSchema();
    }

    @Override
    public Optional<String> getDomainMetadata(String domain) {
      return delegate.getDomainMetadata(domain);
    }

    @Override
    public Map<String, String> getTableProperties() {
      return metadata.getConfiguration();
    }

    @Override
    public io.delta.kernel.statistics.SnapshotStatistics getStatistics() {
      return delegate.getStatistics();
    }

    @Override
    public io.delta.kernel.ScanBuilder getScanBuilder() {
      return delegate.getScanBuilder();
    }

    @Override
    public io.delta.kernel.transaction.UpdateTableTransactionBuilder buildUpdateTableTransaction(
        String engineInfo, io.delta.kernel.Operation operation) {
      return delegate.buildUpdateTableTransaction(engineInfo, operation);
    }

    @Override
    public Snapshot publish(Engine engine) throws io.delta.kernel.commit.PublishFailedException {
      return delegate.publish(engine);
    }

    @Override
    public void writeChecksum(Engine engine, ChecksumWriteMode checksumWriteMode)
        throws java.io.IOException {
      delegate.writeChecksum(engine, checksumWriteMode);
    }

    @Override
    public void writeCheckpoint(Engine engine)
        throws java.io.IOException, io.delta.kernel.exceptions.CheckpointAlreadyExistsException {
      delegate.writeCheckpoint(engine);
    }
  }
}
