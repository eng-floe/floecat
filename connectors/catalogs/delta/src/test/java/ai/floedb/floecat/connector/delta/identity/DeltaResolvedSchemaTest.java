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

package ai.floedb.floecat.connector.delta.identity;

import static org.assertj.core.api.Assertions.assertThat;

import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DeltaResolvedSchemaTest {

  private static final String MAPPED_NESTED =
      """
      {"type":"struct","fields":[{
        "name":"address",
        "type":{"type":"struct","fields":[{
          "name":"zip",
          "type":"integer",
          "nullable":true,
          "metadata":{
            "delta.columnMapping.id":2,
            "delta.columnMapping.physicalName":"col-zip"
          }
        }]},
        "nullable":true,
        "metadata":{
          "delta.columnMapping.id":1,
          "delta.columnMapping.physicalName":"col-address"
        }
      }]}
      """;

  private static final String COLLIDING_LOGICAL =
      """
      {"type":"struct","fields":[
        {
          "name":"a.b",
          "type":"integer",
          "nullable":true,
          "metadata":{
            "delta.columnMapping.id":1,
            "delta.columnMapping.physicalName":"p-one"
          }
        },
        {
          "name":"a",
          "type":{"type":"struct","fields":[{
            "name":"b",
            "type":"integer",
            "nullable":true,
            "metadata":{
              "delta.columnMapping.id":3,
              "delta.columnMapping.physicalName":"p-three"
            }
          }]},
          "nullable":true,
          "metadata":{
            "delta.columnMapping.id":2,
            "delta.columnMapping.physicalName":"p-two"
          }
        }
      ]}
      """;

  @Test
  void anAmbiguousLogicalKeyNeverFansOutIntoSeveralColumns() {
    DeltaResolvedSchema resolved = resolveJson(COLLIDING_LOGICAL, ColumnMappingMode.NAME);

    assertThat(resolved.statsKeysFor(Set.of("a.b"))).isEmpty();
  }

  @Test
  void anAmbiguousLogicalKeyIsRejectedWithoutColumnMapping() {
    DeltaResolvedSchema resolved = resolveJson(COLLIDING_LOGICAL, ColumnMappingMode.NONE);

    assertThat(resolved.statsKeysFor(Set.of("a.b"))).isEmpty();
  }

  @Test
  void unambiguousSiblingsOfAnAmbiguousKeySurvive() {
    DeltaResolvedSchema resolved = resolveJson(COLLIDING_LOGICAL, ColumnMappingMode.NAME);

    assertThat(resolved.statsKeysFor(Set.of("a", "a.b"))).containsExactly("p-two");
  }

  @Test
  void ambiguityIsJudgedAgainstTheWholeSchemaNotTheRequest() {
    DeltaResolvedSchema mapped = resolveJson(COLLIDING_LOGICAL, ColumnMappingMode.NAME);
    DeltaResolvedSchema unmapped = resolveJson(COLLIDING_LOGICAL, ColumnMappingMode.NONE);

    assertThat(mapped.statsKeysFor(Set.of("a.b"))).isEmpty();
    assertThat(unmapped.statsKeysFor(Set.of("a.b"))).isEmpty();
  }

  @Test
  void unknownKeysAreDropped() {
    DeltaResolvedSchema resolved = resolveJson(MAPPED_NESTED, ColumnMappingMode.NAME);

    assertThat(resolved.statsKeysFor(Set.of("no.such.column"))).isEmpty();
  }

  @Test
  void theResultDoesNotAliasTheRequestedSet() {
    DeltaResolvedSchema resolved =
        resolveJson(
            """
            {"type":"struct","fields":[
              {"name":"zip","type":"integer","nullable":true,"metadata":{}}
            ]}
            """,
            ColumnMappingMode.NONE);
    Set<String> requested = new java.util.LinkedHashSet<>(Set.of("zip"));

    Set<String> statsKeys = resolved.statsKeysFor(requested);
    requested.add("added-later");

    assertThat(statsKeys).containsExactly("zip");
  }

  @Test
  void statsKeysAreLogicalWhenMappingIsOff() {
    DeltaResolvedSchema resolved =
        resolveJson(
            """
            {"type":"struct","fields":[
              {"name":"zip","type":"integer","nullable":true,"metadata":{}}
            ]}
            """,
            ColumnMappingMode.NONE);

    assertThat(resolved.statsKeysFor(Set.of("zip"))).containsExactly("zip");
  }

  @Test
  void statsKeysBecomePhysicalWhenMappingIsOn() {
    DeltaResolvedSchema resolved = resolveJson(MAPPED_NESTED, ColumnMappingMode.NAME);

    assertThat(resolved.statsKeysFor(Set.of("address.zip"))).containsExactly("col-address.col-zip");
  }

  @Test
  void unselectedColumnsAreNotTranslated() {
    DeltaResolvedSchema resolved = resolveJson(MAPPED_NESTED, ColumnMappingMode.NAME);

    assertThat(resolved.statsKeysFor(Set.of())).isEmpty();
  }

  @Test
  void distinctPhysicalPathsSharingALegacyKeyAreDropped() {
    DeltaResolvedSchema resolved =
        resolveJson(
            """
            {"type":"struct","fields":[
              {
                "name":"literal",
                "type":"integer",
                "nullable":true,
                "metadata":{
                  "delta.columnMapping.id":1,
                  "delta.columnMapping.physicalName":"a.b"
                }
              },
              {
                "name":"nested",
                "type":{"type":"struct","fields":[{
                  "name":"child",
                  "type":"integer",
                  "nullable":true,
                  "metadata":{
                    "delta.columnMapping.id":3,
                    "delta.columnMapping.physicalName":"b"
                  }
                }]},
                "nullable":true,
                "metadata":{
                  "delta.columnMapping.id":2,
                  "delta.columnMapping.physicalName":"a"
                }
              }
            ]}
            """,
            ColumnMappingMode.NAME);

    assertThat(resolved.statsKeysFor(Set.of("literal", "nested.child"))).isEmpty();
  }

  @Test
  void footerColumnsResolveByFieldIdUnderIdMapping() {
    DeltaResolvedSchema resolved = resolveJson(MAPPED_NESTED, ColumnMappingMode.ID);

    assertThat(resolved.nodeForFooterColumn(List.of("col-address", "col-zip"), 2))
        .hasValueSatisfying(
            node -> assertThat(node.path().legacyDottedKey()).isEqualTo("address.zip"));
    assertThat(resolved.nodeForFooterColumn(List.of("col-address", "col-zip"), null)).isEmpty();
  }

  @Test
  void footerColumnsResolveByPhysicalNameUnderNameMapping() {
    DeltaResolvedSchema resolved = resolveJson(MAPPED_NESTED, ColumnMappingMode.NAME);

    assertThat(resolved.nodeForFooterColumn(List.of("col-address", "col-zip"), null))
        .hasValueSatisfying(
            node -> assertThat(node.path().legacyDottedKey()).isEqualTo("address.zip"));
    assertThat(resolved.nodeForFooterColumn(List.of("address", "zip"), null)).isEmpty();
  }

  private static DeltaResolvedSchema resolveJson(String schemaJson, ColumnMappingMode mode) {
    return DeltaSchemaResolver.resolve(DataTypeJsonSerDe.deserializeStructType(schemaJson), mode);
  }
}
