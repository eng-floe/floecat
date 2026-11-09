/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.connector.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetPageIndexReaderTest {

  @Test
  void attemptsToReadExtensionlessParquetDataFiles() {
    var reader =
        new ParquetPageIndexReader(
            path -> {
              throw new ExtensionlessLookupReached(path);
            });

    assertThatThrownBy(() -> reader.readEntries("s3://bucket/extensionless-data-file"))
        .isInstanceOf(ExtensionlessLookupReached.class)
        .hasMessage("s3://bucket/extensionless-data-file");
  }

  @Test
  void aPageStringBoundIsInCodePointOrder(@TempDir Path dir) throws IOException {
    // U+FFFF is above U+1F600's surrogate pair in UTF-16 order, below U+1F600 in code point order.
    FloecatConnector.ParquetPageIndexEntry page =
        pageOf(dir, Binary.fromString("\uFFFF"), Binary.fromString("\uD83D\uDE00"));
    assertThat(page.minUtf8()).isEqualTo("\uFFFF");
    assertThat(page.maxUtf8()).isEqualTo("\uD83D\uDE00");
  }

  @Test
  void aPageStringBoundThatIsNotUtf8IsDropped(@TempDir Path dir) throws IOException {
    FloecatConnector.ParquetPageIndexEntry page =
        pageOf(dir, Binary.fromString("a"), Binary.fromConstantByteArray(new byte[] {(byte) 0xFF}));
    assertThat(page.minUtf8()).isEqualTo("a");
    assertThat(page.maxUtf8()).isNull();
  }

  /** The one page of a string column holding {@code values}. */
  private static FloecatConnector.ParquetPageIndexEntry pageOf(Path dir, Binary... values)
      throws IOException {
    MessageType schema =
        MessageTypeParser.parseMessageType("message m { required binary s (STRING); }");
    Path file = dir.resolve("f.parquet");
    try (var writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(schema).build()) {
      SimpleGroupFactory groups = new SimpleGroupFactory(schema);
      for (Binary value : values) {
        writer.write(groups.newGroup().append("s", value));
      }
    }
    return new ParquetPageIndexReader(path -> new LocalInputFile(Path.of(path)))
        .readEntries(file.toString())
        .getFirst();
  }

  private static final class ExtensionlessLookupReached extends RuntimeException {
    private ExtensionlessLookupReached(String path) {
      super(path);
    }
  }
}
