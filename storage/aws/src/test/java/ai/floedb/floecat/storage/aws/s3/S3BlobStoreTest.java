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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

class S3BlobStoreTest {

  @Test
  void headExposesTheS3VersionId() {
    var client =
        new FakeS3Client() {
          @Override
          public HeadObjectResponse headObject(HeadObjectRequest request) {
            return HeadObjectResponse.builder()
                .versionId("v9")
                .contentLength(3L)
                .lastModified(Instant.EPOCH)
                .build();
          }
        };
    S3BlobStore store = new S3BlobStore(client, Optional.of("bucket"));

    assertEquals("v9", store.head("/k").orElseThrow().getVersionId());
  }

  @Test
  void headMapsAMissingVersionIdToEmpty() {
    var client =
        new FakeS3Client() {
          @Override
          public HeadObjectResponse headObject(HeadObjectRequest request) {
            // Unversioned bucket: S3 returns no versionId.
            return HeadObjectResponse.builder()
                .contentLength(3L)
                .lastModified(Instant.EPOCH)
                .build();
          }
        };
    S3BlobStore store = new S3BlobStore(client, Optional.of("bucket"));

    assertEquals("", store.head("/k").orElseThrow().getVersionId());
  }

  @Test
  void versionTargetedDeleteNamesExactlyTheObservedVersion() {
    List<DeleteObjectRequest> captured = new ArrayList<>();
    var client =
        new FakeS3Client() {
          @Override
          public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
            captured.add(request);
            return DeleteObjectResponse.builder().build();
          }
        };
    S3BlobStore store = new S3BlobStore(client, Optional.of("bucket"));

    assertTrue(store.delete("/k", "v123"));

    assertEquals(1, captured.size());
    assertEquals("k", captured.get(0).key());
    assertEquals("v123", captured.get(0).versionId());
  }

  @Test
  void aBlankVersionIdIsRejectedNotDegradedToUnconditional() {
    List<DeleteObjectRequest> captured = new ArrayList<>();
    var client =
        new FakeS3Client() {
          @Override
          public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
            captured.add(request);
            return DeleteObjectResponse.builder().build();
          }
        };
    S3BlobStore store = new S3BlobStore(client, Optional.of("bucket"));

    assertThrows(IllegalArgumentException.class, () -> store.delete("/k", ""));
    assertEquals(0, captured.size(), "no delete request was sent");
  }

  @Test
  void versionedDeletesAreSupportedOnlyWhileBucketVersioningIsEnabled() {
    assertTrue(versioningStore(BucketVersioningStatus.ENABLED).supportsVersionedDeletes());
    // Suspended buckets overwrite the "null" version in place — a targeted delete of it could
    // destroy a concurrent re-write, so it must report unsupported (fail closed).
    assertFalse(versioningStore(BucketVersioningStatus.SUSPENDED).supportsVersionedDeletes());
    assertFalse(versioningStore(null).supportsVersionedDeletes());
  }

  @Test
  void versioningStatusLookupFailureFailsClosed() {
    var client =
        new FakeS3Client() {
          @Override
          public GetBucketVersioningResponse getBucketVersioning(
              GetBucketVersioningRequest request) {
            throw new IllegalStateException("s3:GetBucketVersioning denied");
          }
        };
    S3BlobStore store = new S3BlobStore(client, Optional.of("bucket"));

    assertFalse(store.supportsVersionedDeletes());
  }

  @Test
  void versioningStatusIsCachedAcrossCalls() {
    int[] calls = {0};
    var client =
        new FakeS3Client() {
          @Override
          public GetBucketVersioningResponse getBucketVersioning(
              GetBucketVersioningRequest request) {
            calls[0]++;
            return GetBucketVersioningResponse.builder()
                .status(BucketVersioningStatus.ENABLED)
                .build();
          }
        };
    S3BlobStore store = new S3BlobStore(client, Optional.of("bucket"));

    assertTrue(store.supportsVersionedDeletes());
    assertTrue(store.supportsVersionedDeletes());
    assertEquals(1, calls[0], "the versioning status is cached, not re-fetched per call");
  }

  private static S3BlobStore versioningStore(BucketVersioningStatus status) {
    var client =
        new FakeS3Client() {
          @Override
          public GetBucketVersioningResponse getBucketVersioning(
              GetBucketVersioningRequest request) {
            var b = GetBucketVersioningResponse.builder();
            if (status != null) {
              b.status(status);
            }
            return b.build();
          }
        };
    return new S3BlobStore(client, Optional.of("bucket"));
  }

  @Test
  void mapsRuntimeClosedPoolFailureToRetryableForVersionDelete() {
    S3BlobStore store = closedPoolStore();

    assertThrows(StorageAbortRetryableException.class, () -> store.delete("key", "v1"));
  }

  /** All S3Client operations are codegen defaults throwing UnsupportedOperationException. */
  private abstract static class FakeS3Client implements S3Client {
    @Override
    public String serviceName() {
      return "s3";
    }

    @Override
    public void close() {}
  }

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
