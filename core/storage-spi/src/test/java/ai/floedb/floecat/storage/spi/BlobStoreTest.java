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

package ai.floedb.floecat.storage.spi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BlobStoreTest {

  @Test
  void defaultBatchReadOmitsEveryMissingShape() {
    BlobStore store =
        store(Map.of("/present", "value".getBytes(StandardCharsets.UTF_8), "/null", new byte[0]));

    Map<String, byte[]> result = store.getBatch(List.of("/present", "/absent", "/null"));

    assertEquals(1, result.size());
    assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), result.get("/present"));
  }

  @Test
  void defaultBatchReadPropagatesNonAbsenceFailures() {
    BlobStore store = store(Map.of());

    assertThrows(IllegalStateException.class, () -> store.getBatch(List.of("/broken")));
  }

  private static BlobStore store(Map<String, byte[]> values) {
    return new BlobStore() {
      @Override
      public byte[] get(String uri) {
        if ("/broken".equals(uri)) {
          throw new IllegalStateException("broken store");
        }
        if ("/null".equals(uri)) {
          return null;
        }
        byte[] value = values.get(uri);
        if (value == null) {
          throw new StorageNotFoundException("missing: " + uri);
        }
        return value;
      }

      @Override
      public void put(String uri, byte[] bytes, String contentType) {}

      @Override
      public Optional<BlobHeader> head(String uri) {
        return Optional.empty();
      }

      @Override
      public boolean delete(String uri) {
        return false;
      }

      @Override
      public int deletePrefix(String prefix) {
        return 0;
      }

      @Override
      public Page list(String prefix, int limit, String pageToken) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
