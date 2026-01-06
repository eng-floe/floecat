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

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageConflictException;
import ai.floedb.floecat.storage.errors.StorageCorruptionException;
import ai.floedb.floecat.storage.errors.StorageException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.errors.StoragePreconditionFailedException;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Singleton
@IfBuildProperty(name = "floecat.blob", stringValue = "s3")
public class S3BlobStore implements BlobStore {

  private static final String META_SHA256 = "floecat-sha256";
  private static final String META_CREATED_AT = "floecat-created-at";

  private final S3Client s3;
  private final String bucket;

  @Inject
  public S3BlobStore(
      S3Client s3, @ConfigProperty(name = "floecat.blob.s3.bucket") Optional<String> bucketOpt) {
    this.s3 = s3;
    this.bucket = bucketOpt.orElseGet(() -> System.getProperty("floecat.blob.s3.bucket", ""));
    if (this.bucket.isBlank()) {
      throw new IllegalStateException("S3 bucket not configured: floecat.blob.s3.bucket");
    }
  }

  @Override
  public Optional<BlobHeader> head(String key) {
    final String k = normalize(key);
    try {
      HeadObjectResponse r =
          s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(k).build());

      String metaSha = (r.metadata() != null) ? r.metadata().get(META_SHA256) : null;

      long createdAtMs =
          Optional.ofNullable(r.metadata() != null ? r.metadata().get(META_CREATED_AT) : null)
              .map(Long::parseLong)
              .orElseGet(
                  () ->
                      r.lastModified() != null
                          ? r.lastModified().toEpochMilli()
                          : System.currentTimeMillis());
      long lastModifiedMs =
          (r.lastModified() != null) ? r.lastModified().toEpochMilli() : createdAtMs;

      long storedBytes = r.contentLength();

      BlobHeader.Builder hb =
          BlobHeader.newBuilder()
              .setSchemaVersion("v1")
              .setEtag(metaSha == null ? "" : metaSha)
              .setCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(createdAtMs))
              .setLastModifiedAt(com.google.protobuf.util.Timestamps.fromMillis(lastModifiedMs))
              .setContentLength((int) Math.min(Integer.MAX_VALUE, storedBytes));

      return Optional.of(hb.build());
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return Optional.empty();
      }

      throw mapAndWrap("HEAD", k, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("HEAD", k, e.getMessage()));
    }
  }

  @Override
  public byte[] get(String key) {
    final String k = normalize(key);
    try {
      return s3.getObject(
              GetObjectRequest.builder().bucket(bucket).key(k).build(),
              ResponseTransformer.toBytes())
          .asByteArray();
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        throw new StorageNotFoundException(msg("GET", k, "not found"));
      }
      throw mapAndWrap("GET", k, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("GET", k, e.getMessage()));
    }
  }

  @Override
  public void put(String key, byte[] bytes, String contentType) {
    final String k = normalize(key);
    try {
      Long createdAtMs = null;
      try {
        HeadObjectResponse prev =
            s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(k).build());
        createdAtMs =
            Optional.ofNullable(prev.metadata().get(META_CREATED_AT))
                .map(Long::parseLong)
                .orElse(null);
      } catch (S3Exception e) {
        if (e.statusCode() != 404) {
          throw e;
        }
      }

      if (createdAtMs == null) {
        createdAtMs = System.currentTimeMillis();
      }

      String checksum = computeSha256Base64(bytes);
      String ct =
          (contentType == null || contentType.isBlank()) ? "application/octet-stream" : contentType;

      Map<String, String> meta = new HashMap<>();
      meta.put(META_SHA256, checksum);
      meta.put(META_CREATED_AT, Long.toString(createdAtMs));

      PutObjectRequest req =
          PutObjectRequest.builder().bucket(bucket).key(k).contentType(ct).metadata(meta).build();

      s3.putObject(req, RequestBody.fromBytes(bytes));
    } catch (S3Exception e) {
      throw mapAndWrap("PUT", k, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("PUT", k, e.getMessage()));
    }
  }

  private static final class PageImpl implements BlobStore.Page {
    private final List<String> keys;
    private final String next;

    PageImpl(List<String> keys, String next) {
      this.keys = keys;
      this.next = next;
    }

    @Override
    public List<String> keys() {
      return keys;
    }

    @Override
    public String nextToken() {
      return next;
    }
  }

  @Override
  public BlobStore.Page list(String prefix, int limit, String pageToken) {
    final String p = normalize(prefix);
    final int lim = Math.max(1, limit);

    try {
      ListObjectsV2Request.Builder b =
          ListObjectsV2Request.builder().bucket(bucket).prefix(p).maxKeys(lim);

      if (pageToken != null && !pageToken.isBlank()) {
        b = b.continuationToken(pageToken);
      }

      ListObjectsV2Response resp = s3.listObjectsV2(b.build());

      var keys = resp.contents().stream().map(o -> o.key()).toList();

      String next = resp.isTruncated() ? resp.nextContinuationToken() : "";

      return new PageImpl(keys, next);

    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return new PageImpl(List.of(), "");
      }
      throw mapAndWrap("LIST", p, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("LIST", p, e.getMessage()));
    }
  }

  @Override
  public boolean delete(String key) {
    final String k = normalize(key);
    try {
      s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(k).build());
      return true;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return true;
      }
      throw mapAndWrap("DELETE", k, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("DELETE", k, e.getMessage()));
    }
  }

  @Override
  public void deletePrefix(String prefix) {
    final String p = normalize(prefix);
    String ct = null;

    try {
      do {
        var req =
            ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(p)
                .maxKeys(1000)
                .continuationToken(ct)
                .build();

        var resp = s3.listObjectsV2(req);

        if (!resp.contents().isEmpty()) {
          var objs = resp.contents();
          for (int i = 0; i < objs.size(); i += 1000) {
            var slice = objs.subList(i, Math.min(i + 1000, objs.size()));
            var dels =
                slice.stream().map(o -> ObjectIdentifier.builder().key(o.key()).build()).toList();
            s3.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(dels)));
          }
        }

        ct = resp.isTruncated() ? resp.nextContinuationToken() : null;

      } while (ct != null);

      if (p.endsWith("/")) {
        try {
          s3.deleteObject(b -> b.bucket(bucket).key(p));
        } catch (Throwable ignore) {
        }
      }

    } catch (S3Exception e) {
      throw mapAndWrap("DELETE_PREFIX", p, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("DELETE_PREFIX", p, e.getMessage()));
    }
  }

  private static String normalize(String key) {
    return key.startsWith("/") ? key.substring(1) : key;
  }

  private static String computeSha256Base64(byte[] data) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      return Base64.getEncoder().encodeToString(md.digest(data));
    } catch (Exception e) {
      throw new StorageCorruptionException("SHA-256 computation failed", e);
    }
  }

  private static String msg(String op, String key, String detail) {
    return "s3 " + op + " failed for key=" + key + (detail == null ? "" : " : " + detail);
  }

  private static StorageException mapAndWrap(String op, String key, S3Exception e) {
    int sc = e.statusCode();
    String detail =
        e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage();

    if (sc == 404) {
      return new StorageNotFoundException(msg(op, key, "not found"));
    }

    if (sc == 409) {
      return new StorageConflictException(msg(op, key, detail));
    }

    if (sc == 412) {
      return new StoragePreconditionFailedException(msg(op, key, detail));
    }

    if (sc >= 500) {
      return new StorageAbortRetryableException(msg(op, key, detail));
    }

    return new StorageException(msg(op, key, detail), e);
  }
}
