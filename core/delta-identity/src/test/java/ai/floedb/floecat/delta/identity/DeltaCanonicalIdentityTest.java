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
import static org.assertj.core.api.Assertions.tuple;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
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

    ColumnIdentityMap identity = freshIdentity(snapshot, 7L);

    assertThat(identity.getMode())
        .isEqualTo(ColumnIdentityMode.COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID);
    assertThat(identity.getHighWaterMark()).isEqualTo(100L);
    assertThat(columnId(identity, ColumnPath.ROOT.field("tags"))).isEqualTo(100L);
    assertThat(columnId(identity, ColumnPath.ROOT.field("tags").arrayElement()))
        .isEqualTo(DERIVED_TAG_ELEMENT);
    var resolved = DeltaCanonicalIdentity.resolve(snapshot, identity);
    assertThat(resolved.canonicalIdsByStatsKey()).containsEntry("tags", 100L);
    assertThat(resolved.ordinalsByStatsKey()).containsEntry("tags", 1);
  }

  @Test
  void mappedSchemaColumnsCarryNativeFieldIds() {
    Snapshot snapshot = mappedSnapshot(7L, true, true);

    var columns =
        DeltaSchemaMapper.map(snapshot.getSchema().toJson(), Set.of(), freshIdentity(snapshot, 7L))
            .getColumnsList();

    assertThat(columns)
        .extracting(column -> column.getPhysicalPath(), column -> column.getFieldId())
        .containsExactly(tuple("tags", 2), tuple("tags[]", 77));
  }

  @Test
  void enablingNativeMappingReplacesStructuredIdsWithDeltaIds() {
    StructType schema = new StructType().add("tags", new ArrayType(StringType.STRING, true), true);
    Snapshot unmapped =
        new TestSnapshot(6L, schema, Map.of(), new Protocol(1, 2, Set.of(), Set.of()));
    ColumnIdentityMap structured = freshIdentity(unmapped, 6L);

    ColumnIdentityMap mapped = nextGeneration(mappedSnapshot(7L, false, false), 7L, structured);

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

    ColumnIdentityMap identity = freshIdentity(snapshot, 7L);

    var element =
        ColumnIdentityMaps.restore(identity)
            .byPath(ColumnPath.ROOT.field("tags").arrayElement())
            .orElseThrow();
    assertThat(element.canonicalId()).isEqualTo(DERIVED_TAG_ELEMENT);
    assertThat(element.nativeFieldId()).hasValue(77);
  }

  @Test
  void enablingIcebergCompatV2AddsProvenanceWithoutChangingIdentity() {
    ColumnIdentityMap inactive = freshIdentity(mappedSnapshot(7L, false, false), 7L);

    ColumnIdentityMap active = reconcileIdentity(mappedSnapshot(8L, true, true), 8L, inactive);

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

  /** A first-generation map for {@code snapshot}, as identity reconciliation persists it. */
  private static ColumnIdentityMap freshIdentity(Snapshot snapshot, long version) {
    return canonicalIdentity(snapshot, version, Optional.empty(), 0L);
  }

  /** A new generation for {@code snapshot}, allocated above {@code previous}. */
  private static ColumnIdentityMap nextGeneration(
      Snapshot snapshot, long version, ColumnIdentityMap previous) {
    return canonicalIdentity(snapshot, version, Optional.empty(), previous.getHighWaterMark());
  }

  /** {@code snapshot} reconciled against {@code previous} across complete history. */
  private static ColumnIdentityMap reconcileIdentity(
      Snapshot snapshot, long version, ColumnIdentityMap previous) {
    return canonicalIdentity(
        snapshot, version, Optional.of(ColumnIdentityMaps.restore(previous)), 0L);
  }

  private static ColumnIdentityMap canonicalIdentity(
      Snapshot snapshot, long version, Optional<SchemaIdentityState> previous, long floor) {
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = DeltaCanonicalIdentity.identityMode(resolved);
    boolean nativeIds = mode == IdentityMode.NATIVE_FIELD_ID;
    long highWaterMarkFloor =
        nativeIds ? Math.max(floor, DeltaColumnMapping.maxColumnId(snapshot)) : floor;
    ColumnIdentityMap map =
        ColumnIdentityMaps.toProto(
            SchemaIdentityReconciler.reconcile(
                resolved.schema(),
                version,
                mode,
                previous,
                highWaterMarkFloor,
                PreferredCanonicalIds.NONE,
                HistoryTier.VERSIONED_LOG));
    return nativeIds
        ? ColumnIdentityMaps.withNativeHighWaterMark(map, DeltaColumnMapping.maxColumnId(snapshot))
        : map;
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
}
