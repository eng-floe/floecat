package ai.floedb.metacat.storage.aws.s3;

import ai.floedb.metacat.common.rpc.BlobHeader;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.errors.StorageAbortRetryableException;
import ai.floedb.metacat.storage.errors.StorageConflictException;
import ai.floedb.metacat.storage.errors.StorageCorruptionException;
import ai.floedb.metacat.storage.errors.StorageException;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import ai.floedb.metacat.storage.errors.StoragePreconditionFailedException;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.security.MessageDigest;
import java.util.Base64;
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
@IfBuildProperty(name = "metacat.blob", stringValue = "s3")
public class S3BlobStore implements BlobStore {

  private static final String META_SHA256 = "metacat-sha256";

  private final S3Client s3;
  private final String bucket;

  @Inject
  public S3BlobStore(
      S3Client s3, @ConfigProperty(name = "metacat.blob.s3.bucket") Optional<String> bucketOpt) {
    this.s3 = s3;
    this.bucket = bucketOpt.orElseGet(() -> System.getProperty("metacat.blob.s3.bucket", ""));
    if (this.bucket.isBlank()) {
      throw new IllegalStateException("S3 bucket not configured: metacat.blob.s3.bucket");
    }
  }

  @Override
  public Optional<BlobHeader> head(String key) {
    final String k = normalize(key);
    try {
      HeadObjectResponse response =
          s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(k).build());

      String metaSha = response.metadata() != null ? response.metadata().get(META_SHA256) : null;
      if (metaSha == null || metaSha.isBlank()) {
        return Optional.empty();
      }

      return Optional.of(BlobHeader.newBuilder().setEtag(metaSha).build());
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
      String checksum = computeSha256Base64(bytes);
      PutObjectRequest.Builder b =
          PutObjectRequest.builder().bucket(bucket).key(k).metadata(Map.of(META_SHA256, checksum));
      if (contentType != null && !contentType.isBlank()) {
        b = b.contentType(contentType);
      }

      s3.putObject(b.build(), RequestBody.fromBytes(bytes));
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
