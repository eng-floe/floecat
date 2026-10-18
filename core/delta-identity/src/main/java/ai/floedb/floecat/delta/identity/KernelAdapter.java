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
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableChangesUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
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

  /** Resolves identity facts when a catalog exposes only its current schema and properties. */
  public static DeltaIdentitySnapshot currentSchemaIdentity(
      long version, String schemaJson, Map<String, String> properties) {
    ColumnMappingMode mappingMode = ColumnMappingMode.fromTableProperties(properties);
    Set<String> writerFeatures = new LinkedHashSet<>();
    Set<String> readerFeatures = new LinkedHashSet<>();
    if (mappingMode.isEnabled()) {
      readerFeatures.add("columnMapping");
      writerFeatures.add("columnMapping");
    }
    if (Boolean.parseBoolean(
        properties.getOrDefault(DeltaColumnMapping.ICEBERG_COMPAT_V2_ENABLED, "false"))) {
      writerFeatures.add("icebergCompatV2");
    }
    int minReaderVersion = writerFeatures.isEmpty() ? 1 : 3;
    int minWriterVersion = writerFeatures.isEmpty() ? 2 : 7;
    return currentSchemaIdentity(
        version,
        schemaJson,
        properties,
        minReaderVersion,
        minWriterVersion,
        readerFeatures,
        writerFeatures);
  }

  /** Resolves identity facts from a catalog schema and an explicitly supplied Delta protocol. */
  public static DeltaIdentitySnapshot currentSchemaIdentity(
      long version,
      String schemaJson,
      Map<String, String> properties,
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    Protocol protocol =
        new Protocol(
            minReaderVersion,
            minWriterVersion,
            Set.copyOf(readerFeatures),
            Set.copyOf(writerFeatures));
    ColumnMappingMode mode =
        ColumnMappingMode.effectiveFromTableProperties(
            properties, () -> protocol.supportsFeature(TableFeatures.COLUMN_MAPPING_RW_FEATURE));
    long highWaterMark = mode.isEnabled() ? DeltaColumnMapping.maxColumnId(properties) : 0L;
    boolean requireNestedIds =
        Boolean.parseBoolean(
                properties.getOrDefault(DeltaColumnMapping.ICEBERG_COMPAT_V2_ENABLED, "false"))
            && protocol.supportsFeature(TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE);
    return new DeltaIdentitySnapshot(
        version,
        DeltaColumnMapping.resolveSchema(
            deserializeSchema(schemaJson), mode, highWaterMark, requireNestedIds),
        highWaterMark);
  }

  static List<Long> metadataVersions(
      Engine engine, String tablePath, long firstVersion, long lastVersion)
      throws java.io.IOException {
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
      return versioned.getSnapshotAsOfVersion(engine, version);
    } catch (KernelException failure) {
      throw classify(failure);
    }
  }

  static boolean isUnsupportedVoid(Throwable failure) {
    return causedBy(failure, UnsupportedVoidTypeException.class);
  }

  private static RuntimeException classify(KernelException failure) {
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

  private static final class UnsupportedVoidTypeException extends RuntimeException {
    private UnsupportedVoidTypeException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static String schemaJson(Snapshot snapshot) {
    if (snapshot instanceof SnapshotImpl implementation && implementation.getMetadata() != null) {
      return implementation.getMetadata().getSchemaString();
    }
    throw new IllegalStateException("Delta snapshot metadata schema JSON is required");
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
}
