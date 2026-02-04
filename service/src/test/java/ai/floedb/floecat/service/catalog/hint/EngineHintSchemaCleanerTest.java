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

package ai.floedb.floecat.service.catalog.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.systemcatalog.hint.HintClearDecision;
import ai.floedb.floecat.systemcatalog.provider.ServiceLoaderSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import com.google.protobuf.FieldMask;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class EngineHintSchemaCleanerTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
          .setId("tbl-1")
          .build();

  private final ServiceLoaderSystemCatalogProvider provider =
      Mockito.mock(ServiceLoaderSystemCatalogProvider.class);
  private final EngineSystemCatalogExtension extension =
      Mockito.mock(EngineSystemCatalogExtension.class);

  private EngineHintSchemaCleaner cleaner;

  @BeforeEach
  void setUp() {
    Mockito.when(provider.extensionFor("floedb")).thenReturn(Optional.of(extension));
    cleaner = new EngineHintSchemaCleaner(provider);
  }

  // ----------------------
  // Helpers
  // ----------------------

  private static FieldMask schemaMask() {
    return FieldMask.newBuilder().addPaths("schema_json").build();
  }

  private static FieldMask upstreamMask() {
    return FieldMask.newBuilder().addPaths("upstream.pointer").build();
  }

  private static FieldMask nonSchemaMask() {
    return FieldMask.newBuilder().addPaths("display_name").build();
  }

  private static HintClearDecision none() {
    return new HintClearDecision(false, false, Set.of(), Set.of(), Set.of());
  }

  private static HintClearDecision clearAllColumns() {
    return new HintClearDecision(false, true, Set.of(), Set.of(), Set.of());
  }

  private static HintClearDecision clearAllRelations() {
    return new HintClearDecision(true, false, Set.of(), Set.of(), Set.of());
  }

  private static String encoded(String engineKind, String engineVersion, int payloadByte) {
    return EngineHintMetadata.encodeValue(
        engineKind, engineVersion, new byte[] {(byte) payloadByte});
  }

  private static Table.Builder baseBuilder() {
    return Table.newBuilder().setResourceId(TABLE_ID);
  }

  private static Table.Builder putRelationHint(
      Table.Builder builder, String payloadType, String engineKind, String engineVersion, int b) {
    builder.putProperties(
        EngineHintMetadata.tableHintKey(payloadType), encoded(engineKind, engineVersion, b));
    return builder;
  }

  private static Table.Builder putColumnHint(
      Table.Builder builder,
      String payloadType,
      long columnId,
      String engineKind,
      String engineVersion,
      int b) {
    builder.putProperties(
        EngineHintMetadata.columnHintKey(payloadType, columnId),
        encoded(engineKind, engineVersion, b));
    return builder;
  }

  private void runClean(Table.Builder builder, FieldMask mask) {
    cleaner.cleanTableHints(builder, mask, builder.build(), builder.build());
  }

  // ----------------------
  // Tests
  // ----------------------

  @Test
  void shouldClearHints_schemaAndUpstreamPaths() {
    assertThat(cleaner.shouldClearHints(schemaMask())).isTrue();
    assertThat(cleaner.shouldClearHints(upstreamMask())).isTrue();
    assertThat(cleaner.shouldClearHints(nonSchemaMask())).isFalse();
  }

  @Test
  void perEngineDecision_onlyRemovesRelationHints() {
    Table.Builder builder = baseBuilder();
    putRelationHint(builder, "floe.relation+proto", "floedb", "1", 5);
    putColumnHint(builder, "floe.column+proto", 3L, "floedb", "1", 7);
    builder.putProperties("custom", "keep");

    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any()))
        .thenReturn(clearAllRelations());

    runClean(builder, schemaMask());

    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.tableHintKey("floe.relation+proto"));
    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.columnHintKey("floe.column+proto", 3L));
    assertThat(builder.getPropertiesMap()).containsEntry("custom", "keep");
  }

  @Test
  void missingHeader_hintEntryIsRemovedButOtherHintsRemain() {
    Table.Builder builder = baseBuilder();
    builder.putProperties(
        EngineHintMetadata.tableHintKey("floe.rel"), "engineKind=floedb;payload=x");
    builder.putProperties("custom", "keep");

    runClean(builder, schemaMask());

    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.tableHintKey("floe.rel"));
    assertThat(builder.getPropertiesMap()).containsEntry("custom", "keep");
  }

  @Test
  void malformedHintKey_isRemovedEvenIfDecisionWouldNotRemove() {
    // parseHintKey() fails -> key is removed defensively once we know the update needs hint
    // clearing while valid keys remain.
    Table.Builder builder = baseBuilder();
    builder.putProperties(
        "engine.hint.column.floe.column+proto.NOT_AN_INT",
        "engineKind=floedb;engineVersion=1;payload=abcd");
    putColumnHint(builder, "floe.column+proto.v2", 3L, "floedb", "1", 5);

    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any())).thenReturn(none());

    runClean(builder, schemaMask());

    assertThat(builder.getPropertiesMap())
        .doesNotContainKey("engine.hint.column.floe.column+proto.NOT_AN_INT");
    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.columnHintKey("floe.column+proto.v2", 3L));
  }

  @Test
  void relationPayloadType_selection() {
    Table.Builder builder = baseBuilder();
    putRelationHint(builder, "floe.relation+proto.v1", "floedb", "1", 1);
    putRelationHint(builder, "floe.relation+proto.v2", "floedb", "1", 2);

    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any()))
        .thenReturn(
            new HintClearDecision(
                false, false, Set.of("floe.relation+proto.v1"), Set.of(), Set.of()));

    runClean(builder, schemaMask());

    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.tableHintKey("floe.relation+proto.v1"));
    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.tableHintKey("floe.relation+proto.v2"));
  }

  @Test
  void columnPayloadType_selection() {
    Table.Builder builder = baseBuilder();
    putColumnHint(builder, "floe.column+proto", 3L, "floedb", "1", 1);
    putColumnHint(builder, "floe.bar+proto", 3L, "floedb", "1", 2);

    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any()))
        .thenReturn(
            new HintClearDecision(false, false, Set.of(), Set.of("floe.bar+proto"), Set.of()));

    runClean(builder, schemaMask());

    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.columnHintKey("floe.column+proto", 3L));
    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.columnHintKey("floe.bar+proto", 3L));
  }

  @Test
  void columnId_selection() {
    Table.Builder builder = baseBuilder();
    putColumnHint(builder, "floe.column+proto", 3L, "floedb", "1", 1);
    putColumnHint(builder, "floe.column+proto", 4L, "floedb", "1", 2);

    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any()))
        .thenReturn(new HintClearDecision(false, false, Set.of(), Set.of(), Set.of(4L)));

    runClean(builder, schemaMask());

    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.columnHintKey("floe.column+proto", 3L));
    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.columnHintKey("floe.column+proto", 4L));
  }

  @Test
  void extensionThrows_fallsBackToDropAllForThatEngineIdentity() {
    Table.Builder builder = baseBuilder();
    putRelationHint(builder, "floe.rel", "floedb", "1", 1);
    putColumnHint(builder, "floe.col", 3L, "floedb", "1", 2);

    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any()))
        .thenThrow(new RuntimeException("boom"));

    runClean(builder, schemaMask());

    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.tableHintKey("floe.rel"));
    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.columnHintKey("floe.col", 3L));
  }

  @Test
  void multiEngineHints_unknownEngineIsDropped_butKnownEngineCanBeSelective() {
    // floedb has an extension and chooses to only clear columns.
    // otherdb has no extension -> dropAll for that identity.
    Table.Builder builder = baseBuilder();

    putRelationHint(builder, "floe.rel", "floedb", "1", 1);
    putColumnHint(builder, "floe.col", 3L, "floedb", "1", 2);

    putRelationHint(builder, "other.rel", "otherdb", "1", 9);
    putColumnHint(builder, "other.col", 7L, "otherdb", "1", 8);

    // By default provider only knows floedb; otherdb will resolve empty.
    Mockito.when(provider.extensionFor("otherdb")).thenReturn(Optional.empty());
    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any()))
        .thenReturn(clearAllColumns());

    runClean(builder, schemaMask());

    // floedb: only columns cleared.
    assertThat(builder.getPropertiesMap()).containsKey(EngineHintMetadata.tableHintKey("floe.rel"));
    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.columnHintKey("floe.col", 3L));

    // otherdb: dropAll (both relation + columns cleared for that identity).
    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.tableHintKey("other.rel"));
    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.columnHintKey("other.col", 7L));
  }

  @Test
  void perEngineIdentity_policyIsInvokedOncePerDistinctEngineVersion() {
    Table.Builder builder = baseBuilder();
    putRelationHint(builder, "floe.rel.v1", "floedb", "1.0", 1);
    putRelationHint(builder, "floe.rel.v2", "floedb", "2.0", 2);
    putColumnHint(builder, "floe.col", 3L, "floedb", "1.0", 3);

    // Return no-op for both versions to keep relation hints, but we'll verify the ctx values.
    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any())).thenReturn(none());

    runClean(builder, schemaMask());

    ArgumentCaptor<EngineContext> ctxCaptor = ArgumentCaptor.forClass(EngineContext.class);
    Mockito.verify(extension, Mockito.times(2)).decideHintClear(ctxCaptor.capture(), Mockito.any());

    // The cleaner should invoke the extension per engine identity (kind+version) present in the
    // properties.
    List<String> versions = new ArrayList<>();
    List<String> kinds = new ArrayList<>();
    for (EngineContext ctx : ctxCaptor.getAllValues()) {
      kinds.add(ctx.engineKind());
      versions.add(ctx.engineVersion());
    }
    assertThat(kinds).containsExactlyInAnyOrder("floedb", "floedb");
    assertThat(versions).containsExactlyInAnyOrder("1.0", "2.0");
    Set<String> identities = new LinkedHashSet<>();
    for (EngineContext ctx : ctxCaptor.getAllValues()) {
      identities.add(ctx.engineKind() + ":" + ctx.engineVersion());
    }
    assertThat(identities).containsExactlyInAnyOrder("floedb:1.0", "floedb:2.0");

    // No-op decision should keep relation hints, but malformed keys are still removed defensively
    // elsewhere.
    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.tableHintKey("floe.rel.v1"));
    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.tableHintKey("floe.rel.v2"));
    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.columnHintKey("floe.col", 3L));
  }

  @Test
  void onlyTargetedVersion_clearsOnlyThatIdentity() {
    Table.Builder builder = baseBuilder();
    putRelationHint(builder, "floe.rel.v1", "floedb", "1.0", 1);
    putRelationHint(builder, "floe.rel.v2", "floedb", "2.0", 2);
    putColumnHint(builder, "floe.col", 3L, "floedb", "1.0", 3);

    Mockito.when(extension.decideHintClear(Mockito.any(), Mockito.any()))
        .thenAnswer(
            invocation -> {
              EngineContext ctx = invocation.getArgument(0);
              if ("1.0".equals(ctx.engineVersion())) {
                return clearAllColumns();
              }
              return none();
            });

    runClean(builder, schemaMask());

    // v1.0: columns cleared; relation kept.
    assertThat(builder.getPropertiesMap())
        .containsKey(EngineHintMetadata.tableHintKey("floe.rel.v1"));
    assertThat(builder.getPropertiesMap())
        .doesNotContainKey(EngineHintMetadata.columnHintKey("floe.col", 3L));

    // v2.0: no-op => stays.
    assertThat(builder.getPropertiesMap())
        .containsEntry(EngineHintMetadata.tableHintKey("floe.rel.v2"), encoded("floedb", "2.0", 2));
  }
}
