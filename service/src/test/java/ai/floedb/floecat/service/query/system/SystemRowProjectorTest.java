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

package ai.floedb.floecat.service.query.system;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemRowProjectorTest {

  private static SchemaColumn col(String name) {
    return SchemaColumn.newBuilder().setName(name).build();
  }

  @Test
  void project_singleColumn() {
    var schema = List.of(col("a"), col("b"), col("c"));
    var row = new SystemObjectRow(new Object[] {"1", "2", "3"});

    var out = SystemRowProjector.project(List.of(row), schema, List.of("b"));

    assertThat(out).hasSize(1);
    assertThat(out.get(0).values()).containsExactly("2");
  }

  @Test
  void project_multipleColumns_preservesOrderOfRequest() {
    var schema = List.of(col("a"), col("b"), col("c"));
    var row = new SystemObjectRow(new Object[] {"1", "2", "3"});

    var out = SystemRowProjector.project(List.of(row), schema, List.of("c", "a"));

    assertThat(out.get(0).values()).containsExactly("3", "1");
  }

  @Test
  void project_isCaseInsensitive() {
    var schema = List.of(col("Table_Name"), col("TABLE_SCHEMA"));
    var row = new SystemObjectRow(new Object[] {"t", "s"});

    var out = SystemRowProjector.project(List.of(row), schema, List.of("table_schema"));

    assertThat(out.get(0).values()).containsExactly("s");
  }

  @Test
  void project_missingColumnIsIgnored() {
    var schema = List.of(col("a"), col("b"));
    var row = new SystemObjectRow(new Object[] {"1", "2"});

    var out = SystemRowProjector.project(List.of(row), schema, List.of("does_not_exist"));

    assertThat(out.get(0).values()).isEmpty();
  }

  @Test
  void project_emptyProjectionReturnsOriginalRows() {
    var schema = List.of(col("a"), col("b"));
    var row = new SystemObjectRow(new Object[] {"1", "2"});

    var out = SystemRowProjector.project(List.of(row), schema, List.of());

    assertThat(out.get(0).values()).containsExactly("1", "2");
  }

  @Test
  void project_duplicateRequestedColumns_areRepeatedInOutput() {
    var schema = List.of(col("a"), col("b"));
    var row = new SystemObjectRow(new Object[] {"1", "2"});

    var out = SystemRowProjector.project(List.of(row), schema, List.of("a", "a", "b"));

    assertThat(out).hasSize(1);
    assertThat(out.get(0).values()).containsExactly("1", "1", "2");
  }

  @Test
  void project_multipleRows_preservesRowCountAndProjectsEachRow() {
    var schema = List.of(col("a"), col("b"));
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {"1", "2"}),
            new SystemObjectRow(new Object[] {"3", "4"}));

    var out = SystemRowProjector.project(rows, schema, List.of("b"));

    assertThat(out).hasSize(2);
    assertThat(out.get(0).values()).containsExactly("2");
    assertThat(out.get(1).values()).containsExactly("4");
  }
}
