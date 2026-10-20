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
package ai.floedb.floecat.catalog.sharing;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.HistoricalProtocol;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Protocol;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableHistory;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableMetadata;
import ai.floedb.floecat.delta.identity.CurrentSchemaDeltaHistorySource;
import ai.floedb.floecat.delta.identity.DeltaIdentityService;
import ai.floedb.floecat.delta.identity.PersistedIdentityLookup;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DeltaSharingQueryHistorySourceTest {
  private static final String MAPPED_SCHEMA =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"long\","
          + "\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":1,"
          + "\"delta.columnMapping.physicalName\":\"col-1\"}}]}";
  private static final Map<String, String> MAPPED_PROPERTIES =
      Map.of("delta.columnMapping.mode", "name", "delta.columnMapping.maxColumnId", "1");

  @Test
  void unorderedDropAndReaddResetsIdentityWithMappingOnOrOff() {
    for (boolean mapped : List.of(false, true)) {
      Map<String, String> properties = mapped ? MAPPED_PROPERTIES : Map.of();
      DeltaIdentityService service = new DeltaIdentityService();
      var predecessorSource =
          new CurrentSchemaDeltaHistorySource(
              "table",
              ai.floedb.floecat.delta.identity.KernelAdapter.currentSchemaIdentity(
                  10L, MAPPED_SCHEMA, properties),
              true);
      ColumnIdentityMap predecessor =
          service
              .reconcileCurrent(
                  predecessorSource, PersistedIdentityLookup.forTableMap(Optional.empty()))
              .maps()
              .get(10L);

      TableDescription current =
          new TableDescription(
              mapped
                  ? new Protocol(3, 7, List.of("columnMapping"), List.of("columnMapping"))
                  : new Protocol(1, 2, List.of(), List.of()),
              metadata(MAPPED_SCHEMA, properties),
              Optional.of(30L));
      TableHistory unorderedHistory =
          new TableHistory(
              List.of(
                  new HistoricalProtocol(
                      new Protocol(1, 2, List.of(), List.of()), Optional.empty())),
              List.of(metadata("{\"type\":\"struct\",\"fields\":[]}", Map.of())),
              Optional.of(30L));
      var source = new DeltaSharingQueryHistorySource("table", 30L, current, unorderedHistory);

      ColumnIdentityMap reconciled =
          service
              .reconcileCurrent(
                  source, PersistedIdentityLookup.forTableMap(Optional.of(predecessor)))
              .maps()
              .get(30L);

      assertThat(reconciled.getEntries(0).getColumnId())
          .isGreaterThan(predecessor.getHighWaterMark());
    }
  }

  @Test
  void unorderedMetadataChangesWithoutADropKeepExistingIdentity() {
    String unmappedA =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"long\","
            + "\"nullable\":true,\"metadata\":{}}]}";
    String unmappedAb =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"long\","
            + "\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"long\","
            + "\"nullable\":true,\"metadata\":{}}]}";
    DeltaIdentityService service = new DeltaIdentityService();
    ColumnIdentityMap predecessor =
        service
            .reconcileCurrent(
                new CurrentSchemaDeltaHistorySource(
                    "table",
                    ai.floedb.floecat.delta.identity.KernelAdapter.currentSchemaIdentity(
                        10L, unmappedA, Map.of()),
                    true),
                PersistedIdentityLookup.forTableMap(Optional.empty()))
            .maps()
            .get(10L);
    TableHistory unorderedHistory =
        new TableHistory(
            List.of(),
            List.of(
                metadata(unmappedA, Map.of("comment", "changed")), metadata(unmappedAb, Map.of())),
            Optional.of(30L));
    var source =
        new DeltaSharingQueryHistorySource(
            "table",
            30L,
            new TableDescription(
                new Protocol(1, 2, List.of(), List.of()),
                metadata(unmappedAb, Map.of()),
                Optional.of(30L)),
            unorderedHistory);

    ColumnIdentityMap reconciled =
        service
            .reconcileCurrent(source, PersistedIdentityLookup.forTableMap(Optional.of(predecessor)))
            .maps()
            .get(30L);

    assertThat(reconciled.getEntries(0).getColumnId())
        .isEqualTo(predecessor.getEntries(0).getColumnId());
    assertThat(reconciled.getEntries(1).getColumnId())
        .isGreaterThan(predecessor.getHighWaterMark());
  }

  private static TableMetadata metadata(String schema, Map<String, String> properties) {
    return new TableMetadata(
        Optional.empty(),
        Optional.empty(),
        "parquet",
        schema,
        List.of(),
        properties,
        Optional.empty(),
        Optional.empty(),
        List.of(),
        List.of());
  }
}
