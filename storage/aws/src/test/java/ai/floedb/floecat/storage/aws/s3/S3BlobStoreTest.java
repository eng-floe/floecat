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

package ai.floedb.floecat.storage.aws.s3;

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

class S3BlobStoreTest {

  @Test
  void mapsRuntimeClosedPoolFailureToRetryableForHead() {
    S3BlobStore store = closedPoolStore();

    assertThrows(StorageAbortRetryableException.class, () -> store.head("key"));
  }

  @Test
  void mapsRuntimeClosedPoolFailureToRetryableForGet() {
    S3BlobStore store = closedPoolStore();

    assertThrows(StorageAbortRetryableException.class, () -> store.get("key"));
  }

  @Test
  void mapsRuntimeClosedPoolFailureToRetryableForPut() {
    S3BlobStore store = closedPoolStore();

    assertThrows(
        StorageAbortRetryableException.class,
        () ->
            store.put(
                "key", "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8), "text/plain"));
  }

  @Test
  void mapsRuntimeClosedPoolFailureToRetryableForList() {
    S3BlobStore store = closedPoolStore();

    assertThrows(StorageAbortRetryableException.class, () -> store.list("prefix", 10, ""));
  }

  @Test
  void mapsRuntimeClosedPoolFailureToRetryableForDelete() {
    S3BlobStore store = closedPoolStore();

    assertThrows(StorageAbortRetryableException.class, () -> store.delete("key"));
  }

  @Test
  void mapsRuntimeClosedPoolFailureToRetryableForDeletePrefix() {
    S3BlobStore store = closedPoolStore();

    assertThrows(StorageAbortRetryableException.class, () -> store.deletePrefix("prefix"));
  }

  private static S3BlobStore closedPoolStore() {
    return new S3BlobStore(
        new S3BlobStore.S3Caller() {
          @Override
          public <T> T call(Function<S3Client, T> operation) {
            throw new IllegalStateException("Connection pool shut down");
          }
        },
        Optional.of("bucket"));
  }
}
