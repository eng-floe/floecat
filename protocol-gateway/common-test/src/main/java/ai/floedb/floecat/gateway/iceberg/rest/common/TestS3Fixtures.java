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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Logger;
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
  private static final Logger LOG = Logger.getLogger(TestS3Fixtures.class.getName());
  private static final Path MODULE_RELATIVE = Path.of("protocol-gateway", "common-test");
  private static final Path FIXTURE_ROOT = resolveFixtureRoot("iceberg-fixtures");
  private static final Path SIMPLE_ROOT = FIXTURE_ROOT.resolve("simple");
  private static final Path TARGET_ROOT = resolveTargetRoot();
  private static final Path STAGE_ROOT = TARGET_ROOT.resolve("staged-fixtures");
  private static final String BUCKET = "yb-iceberg-tpcds";
  private static final String PREFIX = "trino_test";
  private static final String USE_AWS_FIXTURES_PROP = "floecat.fixtures.use-aws-s3";
  private static final String FORCE_RESEED_PROP = "floecat.fixtures.force-reseed";
  private static final String SEEDED_MARKER_PROP = "floecat.fixtures.iceberg.seeded";
  private static final String STAGE_BUCKET = "staged-fixtures";
  private static final int MAX_DELETE_PREFIX_PAGES = 10_000;
  private static final Duration S3_VERIFY_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration S3_VERIFY_POLL_INTERVAL = Duration.ofMillis(500);
  private static volatile boolean seeded;
  private static final Object SEED_LOCK = new Object();

  private record FixtureSet(String name, Path sourceRoot, String bucket, String prefix) {}

  private static final FixtureSet SIMPLE_SET =
      new FixtureSet("simple", SIMPLE_ROOT, BUCKET, PREFIX);
  private static final FixtureSet COMPLEX_SET =
      new FixtureSet("complex", FIXTURE_ROOT.resolve("complex"), "floecat", "sales/us/trino_types");
  private static final List<FixtureSet> FIXTURE_SETS = List.of(SIMPLE_SET, COMPLEX_SET);

  private TestS3Fixtures() {}

  public static boolean useAwsFixtures() {
    return Boolean.parseBoolean(
        resolveProperty(USE_AWS_FIXTURES_PROP, "FLOECAT_FIXTURES_USE_AWS_S3", "false"));
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
    if (!Boolean.getBoolean(FORCE_RESEED_PROP)
        && (seeded || Boolean.getBoolean(SEEDED_MARKER_PROP))) {
      return;
    }
    synchronized (SEED_LOCK) {
      if (!Boolean.getBoolean(FORCE_RESEED_PROP)
          && (seeded || Boolean.getBoolean(SEEDED_MARKER_PROP))) {
        return;
      }
      LOG.info("Seeding Iceberg test fixtures");
      if (useAwsFixtures()) {
        seedFixturesToS3();
        verifyS3FixturesReady();
      } else {
        System.setProperty("fs.floecat.test-root", TARGET_ROOT.toAbsolutePath().toString());
        try {
          if (!needsReseed() && !Boolean.getBoolean(FORCE_RESEED_PROP)) {
            seeded = true;
            return;
          }
          seedFixturesLocal();
        } catch (IOException e) {
          throw new RuntimeException("Failed to seed fixture bucket", e);
        }
      }
      seeded = true;
      System.setProperty(SEEDED_MARKER_PROP, Boolean.TRUE.toString());
      LOG.info("Iceberg test fixture seeding completed");
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
          s3,
          SIMPLE_ROOT.resolve("metadata"),
          STAGE_BUCKET,
          normalizeKey(basePrefix + "/metadata"));
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
    String continuationToken = null;
    String previousContinuationToken = null;
    int pages = 0;
    while (pages < MAX_DELETE_PREFIX_PAGES) {
      ListObjectsV2Response response =
          s3.listObjectsV2(
              ListObjectsV2Request.builder()
                  .bucket(bucket)
                  .prefix(normalized)
                  .continuationToken(continuationToken)
                  .build());
      response
          .contents()
          .forEach(
              object ->
                  s3.deleteObject(
                      DeleteObjectRequest.builder().bucket(bucket).key(object.key()).build()));
      if (!response.isTruncated()) {
        return;
      }
      previousContinuationToken = continuationToken;
      continuationToken = response.nextContinuationToken();
      if (continuationToken == null || continuationToken.equals(previousContinuationToken)) {
        throw new IllegalStateException(
            "S3 pagination stalled while deleting prefix "
                + bucket
                + "/"
                + normalized
                + " token="
                + continuationToken);
      }
      pages++;
      if (pages % 100 == 0) {
        LOG.info(
            "Deleting fixture prefix still in progress for "
                + bucket
                + "/"
                + normalized
                + " pages="
                + pages);
      }
    }
    throw new IllegalStateException(
        "Exceeded max pagination pages while deleting prefix "
            + bucket
            + "/"
            + normalized
            + " pages="
            + pages);
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
    String override = resolveAwsOverride(overrideKey, null);
    if (override != null && !override.isBlank()) {
      return override;
    }
    return defaultValue;
  }

  private static String resolveProperty(String propertyKey, String envKey, String defaultValue) {
    String fromProperty = System.getProperty(propertyKey);
    if (fromProperty != null && !fromProperty.isBlank()) {
      return fromProperty;
    }
    String fromEnv = System.getenv(envKey);
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv;
    }
    return defaultValue;
  }

  private static String resolveAwsOverride(String overrideKey, String defaultValue) {
    String fromProperty = System.getProperty("floecat.fixture.aws." + overrideKey);
    if (fromProperty != null && !fromProperty.isBlank()) {
      return fromProperty;
    }
    String normalizedKey =
        overrideKey.replace('.', '_').replace('-', '_').toUpperCase(java.util.Locale.ROOT);
    String fromEnv = System.getenv("FLOECAT_FIXTURE_AWS_" + normalizedKey);
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv;
    }
    // Backward-compatible env alias used in docker/env.localstack
    if ("s3.path-style-access".equals(overrideKey)) {
      String alias = System.getenv("FLOECAT_FIXTURE_AWS_S3_PATH_STYLE");
      if (alias != null && !alias.isBlank()) {
        return alias;
      }
    }
    return defaultValue;
  }

  public static Map<String, String> fileIoProperties(String localRoot) {
    if (useAwsFixtures()) {
      return awsFileIoProperties();
    }
    return Map.of("io-impl", InMemoryS3FileIO.class.getName(), "fs.floecat.test-root", localRoot);
  }

  public static Map<String, String> awsFileIoProperties() {
    Map<String, String> props = new LinkedHashMap<>();
    addOverrideIfPresent(
        props, "io-impl", resolveFileIoProperty("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"));
    addOverrideIfPresent(props, "s3.endpoint", resolveFileIoProperty("s3.endpoint", null));
    addOverrideIfPresent(props, "s3.region", resolveFileIoProperty("s3.region", null));
    addOverrideIfPresent(
        props, "s3.access-key-id", resolveFileIoProperty("s3.access-key-id", null));
    addOverrideIfPresent(
        props, "s3.secret-access-key", resolveFileIoProperty("s3.secret-access-key", null));
    addOverrideIfPresent(
        props, "s3.session-token", resolveFileIoProperty("s3.session-token", null));
    addOverrideIfPresent(
        props, "s3.path-style-access", resolveFileIoProperty("s3.path-style-access", null));
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
          HeadObjectRequest.builder().bucket(location.bucket()).key(location.key()).build());
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

  private static void verifyS3FixturesReady() {
    Instant deadline = Instant.now().plus(S3_VERIFY_TIMEOUT);
    try (S3Client s3 = buildS3Client()) {
      while (Instant.now().isBefore(deadline)) {
        if (allFixtureMarkersExist(s3)) {
          return;
        }
        try {
          Thread.sleep(S3_VERIFY_POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for fixture readiness", e);
        }
      }
    }
    throw new IllegalStateException(
        "Timed out waiting for S3 fixture readiness after " + S3_VERIFY_TIMEOUT.toSeconds() + "s");
  }

  private static boolean allFixtureMarkersExist(S3Client s3) {
    for (FixtureSet set : FIXTURE_SETS) {
      String markerPrefix = normalizeKey(set.prefix) + "metadata/";
      ListObjectsV2Response response =
          s3.listObjectsV2(
              ListObjectsV2Request.builder()
                  .bucket(set.bucket)
                  .prefix(markerPrefix)
                  .maxKeys(1)
                  .build());
      if (response.contents().isEmpty()) {
        return false;
      }
    }
    // Ensure stage bucket API is responsive and bucket exists.
    s3.listObjectsV2(ListObjectsV2Request.builder().bucket(STAGE_BUCKET).maxKeys(1).build());
    return true;
  }

  private static Optional<Path> findModuleRoot() {
    Path cwd = Path.of("").toAbsolutePath();
    while (cwd != null) {
      if (cwd.endsWith(MODULE_RELATIVE)) {
        return Optional.of(cwd);
      }
      Path candidate = cwd.resolve(MODULE_RELATIVE);
      if (Files.isDirectory(candidate)) {
        return Optional.of(candidate);
      }
      cwd = cwd.getParent();
    }
    return Optional.empty();
  }

  private static Path resolveTargetRoot() {
    Optional<Path> moduleRoot = findModuleRoot();
    if (moduleRoot.isPresent()) {
      return moduleRoot.get().resolve(Path.of("target", "test-fake-s3"));
    }
    try {
      return Files.createTempDirectory("floecat-test-s3");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create temp fixture root", e);
    }
  }

  private static Path resolveFixtureRoot(String resourceRoot) {
    Optional<Path> moduleRoot = findModuleRoot();
    if (moduleRoot.isPresent()) {
      Path candidate = moduleRoot.get().resolve(Path.of("src", "main", "resources", resourceRoot));
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
    }

    URL url = TestS3Fixtures.class.getClassLoader().getResource(resourceRoot);
    if (url == null) {
      throw new IllegalStateException("Unable to locate fixture resources: " + resourceRoot);
    }
    if ("file".equals(url.getProtocol())) {
      try {
        return Path.of(url.toURI());
      } catch (Exception e) {
        throw new IllegalStateException("Failed to resolve fixture resources", e);
      }
    }
    if ("jar".equals(url.getProtocol())) {
      return extractFromJar(resourceRoot, url);
    }
    return extractFromJar(resourceRoot, url);
  }

  private static Path extractFromJar(String resourceRoot, URL url) {
    Path tempRoot;
    try {
      tempRoot = Files.createTempDirectory("floecat-fixtures");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create fixture temp directory", e);
    }
    Path targetRoot = tempRoot.resolve(resourceRoot);

    try (JarFile jarFile = openJarFile(url)) {
      Enumeration<JarEntry> entries = jarFile.entries();
      String prefix = resourceRoot.endsWith("/") ? resourceRoot : resourceRoot + "/";
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        String name = entry.getName();
        if (!name.startsWith(prefix)) {
          continue;
        }
        Path target = safeResolve(tempRoot, name);
        if (entry.isDirectory()) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          try (var stream = jarFile.getInputStream(entry)) {
            Files.copy(stream, target, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to extract fixture resources", e);
    }
    return targetRoot;
  }

  private static Path safeResolve(Path root, String entryName) {
    Path target = root.resolve(entryName).normalize();
    if (!target.startsWith(root)) {
      throw new IllegalStateException("Blocked invalid jar entry: " + entryName);
    }
    return target;
  }

  private static JarFile openJarFile(URL url) throws IOException {
    if ("jar".equals(url.getProtocol())) {
      JarURLConnection connection = (JarURLConnection) url.openConnection();
      return connection.getJarFile();
    }
    URL codeSource = TestS3Fixtures.class.getProtectionDomain().getCodeSource().getLocation();
    try {
      return new JarFile(Path.of(codeSource.toURI()).toFile());
    } catch (Exception e) {
      throw new IOException("Failed to resolve fixture jar path", e);
    }
  }
}
