package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public final class TestS3Fixtures {
  private static final Path MODULE_RELATIVE = Path.of("protocol-gateway", "common-test");
  private static final Path MODULE_ROOT = resolveModuleRoot();
  private static final Path FIXTURE_ROOT =
      MODULE_ROOT.resolve(Path.of("src", "main", "resources", "iceberg-fixtures"));
  private static final Path SIMPLE_ROOT = FIXTURE_ROOT.resolve("simple");
  private static final Path TARGET_ROOT = MODULE_ROOT.resolve(Path.of("target", "test-fake-s3"));
  private static final Path STAGE_ROOT = TARGET_ROOT.resolve("staged-fixtures");
  private static final String BUCKET = "yb-iceberg-tpcds";
  private static final String PREFIX = "trino_test";
  private static final String USE_AWS_FIXTURES_PROP = "floecat.fixtures.use-aws-s3";
  private static final String STAGE_BUCKET = "staged-fixtures";

  private record FixtureSet(String name, Path sourceRoot, String bucket, String prefix) {}

  private static final FixtureSet SIMPLE_SET =
      new FixtureSet("simple", SIMPLE_ROOT, BUCKET, PREFIX);
  private static final FixtureSet COMPLEX_SET =
      new FixtureSet("complex", FIXTURE_ROOT.resolve("complex"), "floecat", "sales/us/trino_types");
  private static final List<FixtureSet> FIXTURE_SETS = List.of(SIMPLE_SET, COMPLEX_SET);

  private TestS3Fixtures() {}

  public static boolean useAwsFixtures() {
    return Boolean.parseBoolean(System.getProperty(USE_AWS_FIXTURES_PROP, "false"));
  }

  public static String bucketUri(String relativePath) {
    String suffix = relativePath == null ? "" : relativePath.replaceFirst("^/", "");
    return "s3://" + BUCKET + "/" + PREFIX + (suffix.isEmpty() ? "" : "/" + suffix);
  }

  private static Path bucketPath(String bucket) {
    return TARGET_ROOT.resolve(bucket);
  }

  public static Path bucketPath() {
    return bucketPath(BUCKET);
  }

  public static Path prefixPath() {
    return bucketPath().resolve(PREFIX);
  }

  public static Path stageBucketPath() {
    return STAGE_ROOT;
  }

  public static void seedFixtures() {
    if (useAwsFixtures()) {
      seedFixturesToS3();
    } else {
      seedFixturesLocal();
    }
  }

  public static void seedFixturesOnce() {
    if (useAwsFixtures()) {
      seedFixturesToS3();
      return;
    }
    System.setProperty("fs.floecat.test-root", TARGET_ROOT.toAbsolutePath().toString());
    try {
      if (!needsReseed()) {
        return;
      }
      seedFixturesLocal();
    } catch (IOException e) {
      throw new RuntimeException("Failed to seed fixture bucket", e);
    }
  }

  private static boolean needsReseed() throws IOException {
    for (FixtureSet set : FIXTURE_SETS) {
      Path targetRoot = bucketPath(set.bucket).resolve(Path.of(set.prefix));
      if (!Files.exists(targetRoot.resolve("metadata"))
          || !Files.exists(targetRoot.resolve("data"))) {
        return true;
      }
      try (var stream = Files.walk(set.sourceRoot)) {
        boolean diff =
            stream
                .filter(Files::isRegularFile)
                .anyMatch(
                    source -> {
                      Path relative = set.sourceRoot.relativize(source);
                      Path target = targetRoot.resolve(relative);
                      try {
                        return !Files.isRegularFile(target)
                            || Files.size(source) != Files.size(target);
                      } catch (IOException e) {
                        throw new UncheckedIOException(e);
                      }
                    });
        if (diff) {
          return true;
        }
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }
    return false;
  }

  public static void seedStageTable(String namespace, String table) {
    if (useAwsFixtures()) {
      seedStageTableS3(namespace, table);
      return;
    }
    Path stageRoot = stageBucketPath().resolve(Path.of(namespace, table));
    Path parent = stageRoot.getParent();
    if (parent == null) {
      throw new IllegalStateException("Stage root has no parent: " + stageRoot);
    }

    Path tmp = parent.resolve(table + ".__tmp__" + UUID.randomUUID().toString().replace("-", ""));

    try {
      if (Files.exists(tmp)) {
        deleteRecursive(tmp);
      }

      Path tmpMetadata = tmp.resolve("metadata");
      Path tmpData = tmp.resolve("data");
      Files.createDirectories(tmpMetadata);
      Files.createDirectories(tmpData);

      copyRecursive(SIMPLE_ROOT.resolve("metadata"), tmpMetadata);
      copyRecursive(SIMPLE_ROOT.resolve("data"), tmpData);

      Path backup =
          parent.resolve(table + ".__old__" + UUID.randomUUID().toString().replace("-", ""));
      boolean hadExisting = Files.exists(stageRoot);
      if (hadExisting) {
        try {
          Files.move(stageRoot, backup, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
          Files.move(stageRoot, backup);
        }
      }

      try {
        try {
          Files.move(tmp, stageRoot, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
          Files.move(tmp, stageRoot);
        }
      } catch (IOException moveNewFailed) {
        if (hadExisting && Files.exists(backup) && !Files.exists(stageRoot)) {
          try {
            Files.move(backup, stageRoot);
          } catch (IOException ignored) {
          }
        }
        throw moveNewFailed;
      }

      if (Files.exists(backup)) {
        deleteRecursive(backup);
      }
    } catch (IOException e) {
      try {
        if (Files.exists(tmp)) {
          deleteRecursive(tmp);
        }
      } catch (IOException ignored) {
      }
      throw new UncheckedIOException("Failed to seed staged table " + namespace + "." + table, e);
    }
  }

  public static Path fixturePath(String relativePath) {
    return FIXTURE_ROOT.resolve(Path.of(relativePath));
  }

  public static String stageTableUri(String namespace, String table, String relativePath) {
    String normalized = relativePath == null ? "" : relativePath.replaceFirst("^/", "");
    String suffix = normalized.isEmpty() ? "" : "/" + normalized;
    return "s3://" + STAGE_BUCKET + "/" + namespace + "/" + table + suffix;
  }

  private static void seedFixturesLocal() {
    System.setProperty("fs.floecat.test-root", TARGET_ROOT.toAbsolutePath().toString());
    var buckets = new LinkedHashSet<Path>();
    for (FixtureSet set : FIXTURE_SETS) {
      buckets.add(bucketPath(set.bucket));
    }
    try {
      for (Path bucket : buckets) {
        if (Files.exists(bucket)) {
          deleteRecursive(bucket);
        }
      }
      for (FixtureSet set : FIXTURE_SETS) {
        Path target = bucketPath(set.bucket).resolve(Path.of(set.prefix));
        Files.createDirectories(target);
        copyRecursive(set.sourceRoot, target);
      }
      Files.createDirectories(stageBucketPath());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to seed fixture bucket", e);
    }
  }

  private static void copyRecursive(Path source, Path dest) throws IOException {
    if (!Files.exists(source)) {
      return;
    }
    try (var stream = Files.walk(source)) {
      stream.forEach(
          path -> {
            try {
              Path relative = source.relativize(path);
              Path target = dest.resolve(relative);
              if (Files.isDirectory(path)) {
                Files.createDirectories(target);
              } else {
                Files.createDirectories(target.getParent());
                Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  private static void deleteRecursive(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (var stream = Files.walk(path)) {
      stream
          .sorted((a, b) -> b.compareTo(a))
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  private static void seedFixturesToS3() {
    try (S3Client s3 = buildS3Client()) {
      for (FixtureSet set : FIXTURE_SETS) {
        ensureBucketExists(s3, set.bucket);
        deletePrefix(s3, set.bucket, normalizeKey(set.prefix));
        uploadDirectoryToS3(s3, set.sourceRoot, set.bucket, normalizeKey(set.prefix));
      }
      ensureBucketExists(s3, STAGE_BUCKET);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to upload fixtures to S3", e);
    }
  }

  private static void seedStageTableS3(String namespace, String table) {
    String basePrefix = namespace + "/" + table;
    try (S3Client s3 = buildS3Client()) {
      ensureBucketExists(s3, STAGE_BUCKET);
      deletePrefix(s3, STAGE_BUCKET, normalizeKey(basePrefix));
      uploadDirectoryToS3(
          s3, SIMPLE_ROOT.resolve("metadata"), STAGE_BUCKET, normalizeKey(basePrefix + "/metadata"));
      uploadDirectoryToS3(
          s3, SIMPLE_ROOT.resolve("data"), STAGE_BUCKET, normalizeKey(basePrefix + "/data"));
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to seed staged table %s.%s".formatted(namespace, table), e);
    }
  }

  private static void uploadDirectoryToS3(
      S3Client s3, Path sourceRoot, String bucket, String prefix) throws IOException {
    if (!Files.exists(sourceRoot)) {
      return;
    }
    try (var stream = Files.walk(sourceRoot)) {
      stream
          .filter(Files::isRegularFile)
          .forEach(
              source -> {
                Path relative = sourceRoot.relativize(source);
                String key =
                    (prefix.isEmpty() ? "" : prefix)
                        + relative.toString().replace(File.separatorChar, '/');
                PutObjectRequest request =
                    PutObjectRequest.builder().bucket(bucket).key(key).build();
                s3.putObject(request, source);
              });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  private static void deletePrefix(S3Client s3, String bucket, String prefix) {
    String normalized = prefix == null ? "" : prefix;
    ListObjectsV2Request request =
      ListObjectsV2Request.builder().bucket(bucket).prefix(normalized).build();
    ListObjectsV2Response response = s3.listObjectsV2(request);
    while (true) {
      response
          .contents()
          .forEach(
              object ->
                  s3.deleteObject(
                      DeleteObjectRequest.builder()
                          .bucket(bucket)
                          .key(object.key())
                          .build()));
      if (!response.isTruncated()) {
        break;
      }
      response =
          s3.listObjectsV2(
              ListObjectsV2Request.builder()
                  .bucket(bucket)
                  .prefix(normalized)
                  .continuationToken(response.nextContinuationToken())
                  .build());
    }
  }

  private static void ensureBucketExists(S3Client s3, String bucket) {
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ignored) {
    } catch (RuntimeException e) {
      if (!(e instanceof NoSuchBucketException)) {
        throw e;
      }
    }
  }

  private static String normalizeKey(String prefix) {
    if (prefix == null || prefix.isBlank()) {
      return "";
    }
    String trimmed = prefix.replaceFirst("^/+", "").replaceFirst("/+$", "");
    return trimmed.isEmpty() ? "" : trimmed + "/";
  }

  private static S3Client buildS3Client() {
    Region region = Region.of(resolveFileIoProperty("s3.region", "us-east-1"));
    var builder = S3Client.builder().region(region);
    String endpoint = resolveFileIoProperty("s3.endpoint", null);
    if (endpoint != null && !endpoint.isBlank()) {
      builder.endpointOverride(URI.create(endpoint));
    }
    boolean pathStyle =
        Boolean.parseBoolean(resolveFileIoProperty("s3.path-style-access", "false"));
    builder.serviceConfiguration(
        S3Configuration.builder().pathStyleAccessEnabled(pathStyle).build());
    builder.credentialsProvider(resolveCredentials());
    return builder.build();
  }

  private static AwsCredentialsProvider resolveCredentials() {
    String accessKey = resolveFileIoProperty("s3.access-key-id", null);
    String secretKey = resolveFileIoProperty("s3.secret-access-key", null);
    if (accessKey != null && secretKey != null) {
      return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    }
    return DefaultCredentialsProvider.create();
  }

  private static String resolveFileIoProperty(String overrideKey, String defaultValue) {
    String override = System.getProperty("floecat.fileio.override." + overrideKey);
    if (override != null && !override.isBlank()) {
      return override;
    }
    return defaultValue;
  }

  public static Map<String, String> fileIoProperties(String localRoot) {
    if (useAwsFixtures()) {
      return awsFileIoProperties();
    }
    return Map.of(
        "io-impl", InMemoryS3FileIO.class.getName(), "fs.floecat.test-root", localRoot);
  }

  public static Map<String, String> awsFileIoProperties() {
    Map<String, String> props = new LinkedHashMap<>();
    addOverrideIfPresent(props, "io-impl", resolveFileIoProperty("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"));
    addOverrideIfPresent(props, "s3.endpoint", resolveFileIoProperty("s3.endpoint", null));
    addOverrideIfPresent(props, "s3.region", resolveFileIoProperty("s3.region", null));
    addOverrideIfPresent(props, "s3.access-key-id", resolveFileIoProperty("s3.access-key-id", null));
    addOverrideIfPresent(props, "s3.secret-access-key", resolveFileIoProperty("s3.secret-access-key", null));
    addOverrideIfPresent(props, "s3.session-token", resolveFileIoProperty("s3.session-token", null));
    addOverrideIfPresent(props, "s3.path-style-access", resolveFileIoProperty("s3.path-style-access", null));
    return props;
  }

  private static void addOverrideIfPresent(Map<String, String> target, String key, String value) {
    if (key == null || value == null || value.isBlank()) {
      return;
    }
    target.put(key, value);
  }

  public static boolean objectExistsInS3(String uri) {
    if (!useAwsFixtures()) {
      throw new IllegalStateException("AWS fixture mode disabled");
    }
    S3Location location = parseS3Location(uri);
    try (S3Client s3 = buildS3Client()) {
      s3.headObject(
          HeadObjectRequest.builder()
              .bucket(location.bucket())
              .key(location.key())
              .build());
      return true;
    } catch (NoSuchKeyException | NoSuchBucketException e) {
      return false;
    }
  }

  private static S3Location parseS3Location(String uri) {
    URI parsed = URI.create(uri);
    if (!"s3".equalsIgnoreCase(parsed.getScheme())) {
      throw new IllegalArgumentException("Expected s3:// URI: " + uri);
    }
    String bucket = parsed.getHost();
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("Missing bucket in " + uri);
    }
    String key = Optional.ofNullable(parsed.getPath()).orElse("");
    key = key.replaceFirst("^/", "");
    return new S3Location(bucket, key);
  }

  private record S3Location(String bucket, String key) {}

  private static Path resolveModuleRoot() {
    Path cwd = Path.of("").toAbsolutePath();
    while (cwd != null) {
      if (cwd.endsWith(MODULE_RELATIVE)) {
        return cwd;
      }
      Path candidate = cwd.resolve(MODULE_RELATIVE);
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
      cwd = cwd.getParent();
    }
    throw new IllegalStateException(
        "Unable to locate module root for " + MODULE_RELATIVE.toString());
  }
}
