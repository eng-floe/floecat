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

package ai.floedb.floecat.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RequiredColumnsTest {

  @Test
  void normalize_trimsLowerCasesAndDeduplicatesInRequestedOrder() {
    assertThat(RequiredColumns.normalize(List.of(" B", "a", "b ", "A"))).containsExactly("b", "a");
  }

  @Test
  void normalize_dropsBlankAndNullNames() {
    assertThat(RequiredColumns.normalize(Arrays.asList(null, " ", "", "x"))).containsExactly("x");
    assertThat(RequiredColumns.normalize(Arrays.asList(null, " "))).isEmpty();
    assertThat(RequiredColumns.normalize(null)).isEmpty();
  }

  @Test
  void includes_matchesNormalizedNamesAndTreatsEmptyAsEveryColumn() {
    List<String> required = RequiredColumns.normalize(List.of(" Name "));

    assertThat(RequiredColumns.includes(required, "NAME")).isTrue();
    assertThat(RequiredColumns.includes(required, "id")).isFalse();
    assertThat(RequiredColumns.includes(List.of(), "id")).isTrue();
  }

  @Test
  void indexOf_matchesUnderTheNameRules() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("Id").build(),
            SchemaColumn.newBuilder().setName("name").build());

    assertThat(RequiredColumns.indexOf(schema, " NAME ")).isEqualTo(1);
    assertThat(RequiredColumns.indexOf(schema, "id")).isZero();
    assertThat(RequiredColumns.indexOf(schema, "missing")).isEqualTo(-1);
  }
}
