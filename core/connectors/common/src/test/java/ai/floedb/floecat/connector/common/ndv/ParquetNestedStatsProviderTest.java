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

package ai.floedb.floecat.connector.common.ndv;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.common.ParquetPageIndexReader;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetNestedStatsProviderTest {

  @TempDir Path tempDir;

  @Test
  void nestedLeafNdvUsesQualifiedPathsForSameNamedLeaves() throws Exception {
    Path file = writeNestedFile();
    Map<String, ColumnNdv> sinks = new LinkedHashMap<>();
    sinks.put("left.code", new ColumnNdv());
    sinks.put("right.code", new ColumnNdv());

    new ParquetNdvProvider(path -> new LocalInputFile(Path.of(path)))
        .contributeNdv(file.toString(), sinks);

    assertThat(sinks.get("left.code").approx.estimate).isEqualTo(2.0d);
    assertThat(sinks.get("right.code").approx.estimate).isEqualTo(2.0d);
    assertThat(sinks.get("left.code").approx.rowsSeen).isEqualTo(3L);
    assertThat(sinks.get("right.code").approx.rowsSeen).isEqualTo(3L);
  }

  @Test
  void nestedLeafWidthsUseQualifiedParquetColumnPaths() throws Exception {
    Path file = writeNestedFile();
    Map<String, ParquetAvgWidthProvider.AvgWidthAcc> sinks = new LinkedHashMap<>();
    sinks.put("left.code", new ParquetAvgWidthProvider.AvgWidthAcc());
    sinks.put("right.code", new ParquetAvgWidthProvider.AvgWidthAcc());

    new ParquetAvgWidthProvider(path -> new LocalInputFile(Path.of(path)))
        .contributeAvgWidth(file.toString(), sinks);

    assertThat(sinks.get("left.code").avgWidthBytes()).isNotNull().isPositive();
    assertThat(sinks.get("right.code").avgWidthBytes())
        .isGreaterThan(sinks.get("left.code").avgWidthBytes());
  }

  @Test
  void nestedPageIndexesKeepSameNamedNumericLeavesQualified() throws Exception {
    Path file = writeNestedIntFile();

    var entries =
        new ParquetPageIndexReader(path -> new LocalInputFile(Path.of(path)))
            .readEntries(file.toString());

    assertThat(entries)
        .extracting(entry -> entry.columnName())
        .containsOnly("left.code", "right.code");
    assertThat(entries)
        .filteredOn(entry -> entry.columnName().equals("left.code"))
        .allSatisfy(
            entry -> {
              assertThat(entry.minI32()).isEqualTo(1);
              assertThat(entry.maxI32()).isEqualTo(3);
            });
    assertThat(entries)
        .filteredOn(entry -> entry.columnName().equals("right.code"))
        .allSatisfy(
            entry -> {
              assertThat(entry.minI32()).isEqualTo(1_000);
              assertThat(entry.maxI32()).isEqualTo(3_000);
            });
  }

  private Path writeNestedFile() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message nested_stats {"
                + " optional group left { required binary code (UTF8); }"
                + " optional group right { required binary code (UTF8); }"
                + " }");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    Path file = tempDir.resolve("nested.parquet");
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(schema).build()) {
      writer.write(row(groups, "a", "x".repeat(128)));
      writer.write(row(groups, "b", "x".repeat(128)));
      writer.write(row(groups, "b", "y".repeat(128)));
    }
    return file;
  }

  private Path writeNestedIntFile() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message nested_page_index {"
                + " optional group left { required int32 code; }"
                + " optional group right { required int32 code; }"
                + " }");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    Path file = tempDir.resolve("nested-int.parquet");
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(schema).build()) {
      for (int value = 1; value <= 3; value++) {
        Group row = groups.newGroup();
        row.addGroup("left").append("code", value);
        row.addGroup("right").append("code", value * 1_000);
        writer.write(row);
      }
    }
    return file;
  }

  private static Group row(SimpleGroupFactory groups, String left, String right) {
    Group row = groups.newGroup();
    row.addGroup("left").append("code", left);
    row.addGroup("right").append("code", right);
    return row;
  }
}
