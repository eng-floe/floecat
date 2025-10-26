package ai.floedb.metacat.storage.aws.s3;

import ai.floedb.metacat.common.rpc.BlobHeader;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Singleton
@IfBuildProperty(name = "metacat.blob", stringValue = "s3")
public class S3BlobStore implements BlobStore {

  private final S3Client s3;
  private final String bucket;

  @Inject
  public S3BlobStore(
      S3Client s3, @ConfigProperty(name = "metacat.s3.bucket") Optional<String> bucketOpt) {
    this.s3 = s3;
    this.bucket = bucketOpt.orElseGet(() -> System.getProperty("metacat.s3.bucket", ""));
    if (this.bucket.isBlank()) {
      throw new IllegalStateException("S3 bucket not configured: metacat.s3.bucket");
    }
  }

  @Override
  public Optional<BlobHeader> head(String key) {
    try {
      HeadObjectResponse r =
          s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());

      BlobHeader.Builder b = BlobHeader.newBuilder();
      if (r.eTag() != null) {
        b.setEtag(r.eTag());
      }
      return Optional.of(b.build());

    } catch (S3Exception e) {
      if (e.statusCode() == 404) return Optional.empty();
      throw e;
    }
  }

  @Override
  public byte[] get(String key) {
    try {
      return s3.getObject(
              GetObjectRequest.builder().bucket(bucket).key(key).build(),
              ResponseTransformer.toBytes())
          .asByteArray();
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        throw new StorageNotFoundException("blob not found: " + key);
      }
      throw e;
    }
  }

  @Override
  public void put(String key, byte[] bytes, String contentType) {
    PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(bucket).key(key);
    if (contentType != null && !contentType.isBlank()) {
      b = b.contentType(contentType);
    }
    s3.putObject(b.build(), RequestBody.fromBytes(bytes));
  }

  @Override
  public boolean delete(String key) {
    try {
      s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) return false;
      throw e;
    }
  }
}
