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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

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

  private static final class ExtensionlessLookupReached extends RuntimeException {
    private ExtensionlessLookupReached(String path) {
      super(path);
    }
  }
}
