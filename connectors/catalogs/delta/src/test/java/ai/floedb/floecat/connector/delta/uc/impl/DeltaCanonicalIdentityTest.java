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

package ai.floedb.floecat.connector.delta.uc.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import ai.floedb.floecat.connector.delta.identity.ColumnMappingMode;
import ai.floedb.floecat.connector.delta.identity.DeltaSchemaResolver;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryCoverage;
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
  private static final long DERIVED_TAG_ELEMENT = (1L << 62) | (2L << 24) | 1L;

  @Test
  void mappedCollectionsUseNativeAndDerivedIdentityWithoutIcebergCompatV2() {
    Snapshot snapshot = mappedSnapshot(7L, false, false);

    ColumnIdentityMap identity =
        DeltaCanonicalIdentity.reset(snapshot, 7L, ColumnIdentityMap.getDefaultInstance())
            .identityMap();

    assertThat(identity.getMode())
        .isEqualTo(ColumnIdentityMode.COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID);
    assertThat(identity.getHighWaterMark()).isEqualTo(99L);
    assertThat(columnId(identity, ColumnPath.ROOT.field("tags"))).isEqualTo(2L);
    assertThat(columnId(identity, ColumnPath.ROOT.field("tags").arrayElement()))
        .isEqualTo(DERIVED_TAG_ELEMENT);
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
        DeltaCanonicalIdentity.reset(snapshot, 7L, ColumnIdentityMap.getDefaultInstance())
            .identityMap();

    var element =
        DeltaCanonicalIdentity.fromProto(identity)
            .byPath(ColumnPath.ROOT.field("tags").arrayElement())
            .orElseThrow();
    assertThat(element.canonicalId()).isEqualTo(DERIVED_TAG_ELEMENT);
    assertThat(element.nativeFieldId()).hasValue(77);
  }

  @Test
  void enablingIcebergCompatV2AddsProvenanceWithoutChangingIdentity() {
    ColumnIdentityMap inactive =
        DeltaCanonicalIdentity.reset(
                mappedSnapshot(7L, false, false), 7L, ColumnIdentityMap.getDefaultInstance())
            .identityMap();

    ColumnIdentityMap active =
        DeltaCanonicalIdentity.reconcile(
                mappedSnapshot(8L, true, true),
                8L,
                inactive,
                HistoryCoverage.COMPLETE_METADATA_HISTORY)
            .identityMap();

    assertThat(active.getFingerprint()).isEqualTo(inactive.getFingerprint());
    assertThat(
            DeltaCanonicalIdentity.fromProto(active)
                .byPath(ColumnPath.ROOT.field("tags").arrayElement())
                .orElseThrow()
                .nativeFieldId())
        .hasValue(77);
  }

  @Test
  void protoRoundTripCarriesStateChecksumAndStampKeepsFingerprint() {
    Snapshot atSeven = mappedSnapshot(7L, false, false);
    ColumnIdentityMap initial =
        DeltaCanonicalIdentity.reset(atSeven, 7L, ColumnIdentityMap.getDefaultInstance())
            .identityMap();

    assertThat(DeltaCanonicalIdentity.fromProto(initial).stateChecksum())
        .isEqualTo(initial.getStateChecksum());

    ColumnIdentityMap stamped =
        DeltaCanonicalIdentity.stampSourceVersion(
            mappedSnapshot(1_000_000L, false, false), 1_000_000L, initial);
    assertThat(stamped.getFingerprint()).isEqualTo(initial.getFingerprint());
    assertThat(stamped.getStateChecksum()).isNotEqualTo(initial.getStateChecksum());
    assertThat(stamped.getSourceVersion()).isEqualTo(1_000_000L);
  }

  private static long columnId(ColumnIdentityMap identity, ColumnPath path) {
    return DeltaCanonicalIdentity.fromProto(identity).byPath(path).orElseThrow().canonicalId();
  }

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
      implements Snapshot, DeltaColumnMapping.ProtocolSnapshot {
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
