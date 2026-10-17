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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import io.delta.kernel.Operation;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.statistics.SnapshotStatistics;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DeltaCanonicalIdentityTest {
  // Cross-language golden vector for canonical field 100 followed by ARRAY_ELEMENT (kind 2).
  private static final long DERIVED_TAG_ELEMENT = 6298796673527613865L;

  @Test
  void mappedCollectionsUseNativeAndDerivedIdentityWithoutIcebergCompatV2() {
    Snapshot snapshot = mappedSnapshot(7L, false, false);

    ColumnIdentityMap identity =
        reset(snapshot, 7L, ColumnIdentityMap.getDefaultInstance(), 0L, HistoryTier.VERSIONED_LOG)
            .identityMap();

    assertThat(identity.getMode())
        .isEqualTo(ColumnIdentityMode.COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID);
    assertThat(identity.getHighWaterMark()).isEqualTo(100L);
    assertThat(columnId(identity, ColumnPath.ROOT.field("tags"))).isEqualTo(100L);
    assertThat(columnId(identity, ColumnPath.ROOT.field("tags").arrayElement()))
        .isEqualTo(DERIVED_TAG_ELEMENT);
  }

  @Test
  void enablingNativeMappingReplacesStructuredIdsWithDeltaIds() {
    StructType schema = new StructType().add("tags", new ArrayType(StringType.STRING, true), true);
    Snapshot unmapped =
        new TestSnapshot(6L, schema, Map.of(), new Protocol(1, 2, Set.of(), Set.of()));
    ColumnIdentityMap structured =
        reset(unmapped, 6L, ColumnIdentityMap.getDefaultInstance(), 0L, HistoryTier.VERSIONED_LOG)
            .identityMap();

    ColumnIdentityMap mapped =
        reset(mappedSnapshot(7L, false, false), 7L, structured, 0L, HistoryTier.VERSIONED_LOG)
            .identityMap();

    assertThat(mapped.getMode()).isEqualTo(ColumnIdentityMode.COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID);
    assertThat(columnId(mapped, ColumnPath.ROOT.field("tags"))).isEqualTo(100L);
    assertThat(mapped.getFingerprint()).isNotEqualTo(structured.getFingerprint());
  }

  @Test
  void activeIcebergCompatV2RequiresNestedIds() {
    assertThatThrownBy(() -> DeltaColumnMapping.resolveSchema(mappedSnapshot(7L, true, false)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("IcebergCompatV2 requires a nested field ID")
        .hasMessageContaining("tags[]");
  }

  @Test
  void mappedTableReportsMissingMaxColumnIdDistinctly() {
    TestSnapshot source = (TestSnapshot) mappedSnapshot(7L, false, false);
    Snapshot missing =
        new TestSnapshot(
            source.version(),
            source.schema(),
            Map.of(
                ColumnMappingMode.PROPERTY,
                "name",
                DeltaColumnMapping.ICEBERG_COMPAT_V2_ENABLED,
                "false"),
            source.protocol());

    assertThatThrownBy(() -> DeltaColumnMapping.resolveSchema(missing))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing required property")
        .hasMessageContaining(DeltaColumnMapping.MAX_COLUMN_ID);
  }

  @Test
  void mappedTableReportsDeclaredMaxColumnIdBelowLiveIds() {
    TestSnapshot source = (TestSnapshot) mappedSnapshot(7L, false, false);
    Snapshot tooLow =
        new TestSnapshot(
            source.version(),
            source.schema(),
            Map.of(
                ColumnMappingMode.PROPERTY,
                "name",
                DeltaColumnMapping.MAX_COLUMN_ID,
                "1",
                DeltaColumnMapping.ICEBERG_COMPAT_V2_ENABLED,
                "false"),
            source.protocol());

    assertThatThrownBy(() -> DeltaColumnMapping.resolveSchema(tooLow))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares " + DeltaColumnMapping.MAX_COLUMN_ID + "=1")
        .hasMessageContaining("below field ID 2");
  }

  @Test
  void inactiveIcebergCompatV2ToleratesNestedIdsWithoutUsingThemAsIdentity() {
    Snapshot snapshot = mappedSnapshot(7L, false, true);

    ColumnIdentityMap identity =
        reset(snapshot, 7L, ColumnIdentityMap.getDefaultInstance(), 0L, HistoryTier.VERSIONED_LOG)
            .identityMap();

    var element =
        ColumnIdentityMaps.restore(identity)
            .byPath(ColumnPath.ROOT.field("tags").arrayElement())
            .orElseThrow();
    assertThat(element.canonicalId()).isEqualTo(DERIVED_TAG_ELEMENT);
    assertThat(element.nativeFieldId()).hasValue(77);
  }

  @Test
  void enablingIcebergCompatV2AddsProvenanceWithoutChangingIdentity() {
    ColumnIdentityMap inactive =
        reset(
                mappedSnapshot(7L, false, false),
                7L,
                ColumnIdentityMap.getDefaultInstance(),
                0L,
                HistoryTier.VERSIONED_LOG)
            .identityMap();

    ColumnIdentityMap active =
        reconcile(
                mappedSnapshot(8L, true, true),
                8L,
                inactive,
                0L,
                PreferredCanonicalIds.NONE,
                HistoryTier.VERSIONED_LOG)
            .identityMap();

    assertThat(active.getFingerprint()).isEqualTo(inactive.getFingerprint());
    assertThat(
            ColumnIdentityMaps.restore(active)
                .byPath(ColumnPath.ROOT.field("tags").arrayElement())
                .orElseThrow()
                .nativeFieldId())
        .hasValue(77);
  }

  private static long columnId(ColumnIdentityMap identity, ColumnPath path) {
    return ColumnIdentityMaps.restore(identity).byPath(path).orElseThrow().canonicalId();
  }

  private static TestReconciled reset(
      Snapshot snapshot,
      long sourceVersion,
      ColumnIdentityMap previous,
      long highWaterMarkFloor,
      HistoryTier historyTier) {
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = DeltaCanonicalIdentity.identityMode(resolved);
    long floor =
        Math.max(
            highWaterMarkFloor,
            previous.equals(ColumnIdentityMap.getDefaultInstance())
                ? 0L
                : ColumnIdentityMaps.restore(previous).highWaterMark());
    if (mode == IdentityMode.NATIVE_FIELD_ID) {
      floor = Math.max(floor, DeltaColumnMapping.maxColumnId(snapshot));
    }
    SchemaIdentityReconciler.Result result =
        SchemaIdentityReconciler.reset(resolved.schema(), sourceVersion, mode, floor, historyTier);
    return new TestReconciled(identityMap(result.state(), snapshot));
  }

  private static TestReconciled reconcile(
      Snapshot snapshot,
      long sourceVersion,
      ColumnIdentityMap previous,
      long highWaterMarkFloor,
      PreferredCanonicalIds preferred,
      HistoryTier historyTier) {
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = DeltaCanonicalIdentity.identityMode(resolved);
    long floor =
        mode == IdentityMode.NATIVE_FIELD_ID
            ? Math.max(highWaterMarkFloor, DeltaColumnMapping.maxColumnId(snapshot))
            : highWaterMarkFloor;
    SchemaIdentityReconciler.Result result =
        SchemaIdentityReconciler.reconcile(
            resolved.schema(),
            sourceVersion,
            mode,
            Optional.of(ColumnIdentityMaps.restore(previous)),
            floor,
            preferred,
            historyTier);
    return new TestReconciled(identityMap(result.state(), snapshot));
  }

  private static ColumnIdentityMap identityMap(SchemaIdentityState state, Snapshot snapshot) {
    ColumnIdentityMap map = ColumnIdentityMaps.toProto(state);
    return state.mode() == IdentityMode.NATIVE_FIELD_ID
        ? ColumnIdentityMaps.withNativeHighWaterMark(map, DeltaColumnMapping.maxColumnId(snapshot))
        : map;
  }

  private record TestReconciled(ColumnIdentityMap identityMap) {}

  private static Snapshot mappedSnapshot(
      long version, boolean icebergCompatV2, boolean includeNestedId) {
    FieldMetadata.Builder metadata =
        FieldMetadata.builder()
            .putLong(DeltaSchemaResolver.COLUMN_ID, 2L)
            .putString(DeltaSchemaResolver.PHYSICAL_NAME, "col-tags");
    if (includeNestedId) {
      metadata.putFieldMetadata(
          DeltaSchemaResolver.NESTED_IDS,
          FieldMetadata.builder().putLong("col-tags.element", 77L).build());
    }
    StructType schema =
        new StructType()
            .add(
                new StructField(
                    "tags", new ArrayType(StringType.STRING, true), true, metadata.build()));
    Map<String, String> properties =
        Map.of(
            ColumnMappingMode.PROPERTY,
            "name",
            DeltaColumnMapping.MAX_COLUMN_ID,
            "99",
            DeltaColumnMapping.ICEBERG_COMPAT_V2_ENABLED,
            Boolean.toString(icebergCompatV2));
    Set<String> writerFeatures =
        icebergCompatV2 ? Set.of("columnMapping", "icebergCompatV2") : Set.of("columnMapping");
    Protocol protocol = new Protocol(3, 7, Set.of("columnMapping"), writerFeatures);
    return new TestSnapshot(version, schema, properties, protocol);
  }

  private record TestSnapshot(
      long version, StructType schema, Map<String, String> properties, Protocol protocol)
      implements Snapshot, KernelAdapter.ProtocolSnapshot {
    @Override
    public String getPath() {
      return "s3://bucket/table";
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public List<String> getPartitionColumnNames() {
      return List.of();
    }

    @Override
    public long getTimestamp(Engine engine) {
      return version;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public Optional<String> getDomainMetadata(String domain) {
      return Optional.empty();
    }

    @Override
    public Map<String, String> getTableProperties() {
      return properties;
    }

    @Override
    public SnapshotStatistics getStatistics() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScanBuilder getScanBuilder() {
      throw new UnsupportedOperationException();
    }

    @Override
    public UpdateTableTransactionBuilder buildUpdateTableTransaction(
        String engineInfo, Operation operation) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Snapshot publish(Engine engine) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeChecksum(Engine engine, ChecksumWriteMode checksumWriteMode) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeCheckpoint(Engine engine) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
