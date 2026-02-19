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
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public final class TestDeltaFixtures {
  private static final Path MODULE_RELATIVE = Path.of("protocol-gateway", "common-test");
  private static final Path FIXTURE_ROOT = resolveFixtureRoot("delta-fixtures");
  private static final Path CALL_CENTER_ROOT = FIXTURE_ROOT.resolve("call_center");

  private static final Path TARGET_ROOT = resolveTargetRoot();
  private static final String USE_AWS_FIXTURES_PROP = "floecat.fixtures.use-aws-s3";

  private static final String BUCKET = "floecat-delta";
  private static final String PREFIX = "call_center";

  private TestDeltaFixtures() {}

  public static boolean useAwsFixtures() {
    return Boolean.parseBoolean(System.getProperty(USE_AWS_FIXTURES_PROP, "false"));
  }

  public static String tableUri() {
    return "s3://" + BUCKET + "/" + PREFIX;
  }

  public static Map<String, String> s3Options() {
    Map<String, String> props = new LinkedHashMap<>();
    addIfPresent(props, "s3.endpoint", System.getProperty("floecat.fixture.aws.s3.endpoint"));
    addIfPresent(props, "s3.region", System.getProperty("floecat.fixture.aws.s3.region"));
    addIfPresent(
        props, "s3.access-key-id", System.getProperty("floecat.fixture.aws.s3.access-key-id"));
    addIfPresent(
        props,
        "s3.secret-access-key",
        System.getProperty("floecat.fixture.aws.s3.secret-access-key"));
    addIfPresent(
        props, "s3.session-token", System.getProperty("floecat.fixture.aws.s3.session-token"));
    addIfPresent(
        props,
        "s3.path-style-access",
        System.getProperty("floecat.fixture.aws.s3.path-style-access"));
    return props;
  }

  public static void seedFixturesOnce() {
    if (useAwsFixtures()) {
      seedFixturesToS3();
      return;
    }
    seedFixturesLocal();
  }

  private static void seedFixturesLocal() {
    System.setProperty("fs.floecat.test-root", TARGET_ROOT.toAbsolutePath().toString());
    Path targetRoot = TARGET_ROOT.resolve(Path.of(BUCKET, PREFIX));
    try {
      if (Files.exists(targetRoot)) {
        deleteRecursive(targetRoot);
      }

      Path parent = targetRoot.getParent();
      if (parent == null) {
        throw new IllegalStateException("Local fixture root has no parent: " + targetRoot);
      }

      Path tmp = parent.resolve(PREFIX + ".__tmp__");
      if (Files.exists(tmp)) {
        deleteRecursive(tmp);
      }

      Files.createDirectories(tmp);
      copyRecursive(CALL_CENTER_ROOT, tmp);

      try {
        Files.move(tmp, targetRoot, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tmp, targetRoot);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to seed local delta fixtures", e);
    }
  }

  private static void seedFixturesToS3() {
    try (S3Client s3 = buildS3Client()) {
      ensureBucketExists(s3, BUCKET);
      deletePrefix(s3, BUCKET, normalizeKey(PREFIX));
      uploadDirectoryToS3(s3, CALL_CENTER_ROOT, BUCKET, normalizeKey(PREFIX));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to upload delta fixtures to S3", e);
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
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();
    ListObjectsV2Response response = s3.listObjectsV2(request);
    while (true) {
      response
          .contents()
          .forEach(
              object ->
                  s3.deleteObject(
                      DeleteObjectRequest.builder().bucket(bucket).key(object.key()).build()));
      if (!response.isTruncated()) {
        break;
      }
      response =
          s3.listObjectsV2(
              ListObjectsV2Request.builder()
                  .bucket(bucket)
                  .prefix(prefix)
                  .continuationToken(response.nextContinuationToken())
                  .build());
    }
  }

  private static void ensureBucketExists(S3Client s3, String bucket) {
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ignored) {
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
    Region region = Region.of(resolveProperty("s3.region", "us-east-1"));
    var builder = S3Client.builder().region(region);
    String endpoint = resolveProperty("s3.endpoint", null);
    if (endpoint != null && !endpoint.isBlank()) {
      builder.endpointOverride(URI.create(endpoint));
    }
    boolean pathStyle = Boolean.parseBoolean(resolveProperty("s3.path-style-access", "false"));
    builder.serviceConfiguration(
        S3Configuration.builder().pathStyleAccessEnabled(pathStyle).build());
    builder.credentialsProvider(resolveCredentials());
    return builder.build();
  }

  private static AwsCredentialsProvider resolveCredentials() {
    String accessKey = resolveProperty("s3.access-key-id", null);
    String secretKey = resolveProperty("s3.secret-access-key", null);
    if (accessKey != null && secretKey != null) {
      return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    }
    return DefaultCredentialsProvider.create();
  }

  private static String resolveProperty(String overrideKey, String defaultValue) {
    String override = System.getProperty("floecat.fixture.aws." + overrideKey);
    if (override != null && !override.isBlank()) {
      return override;
    }
    return defaultValue;
  }

  private static void addIfPresent(Map<String, String> target, String key, String value) {
    if (value == null || value.isBlank()) {
      return;
    }
    target.put(key, value);
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

    URL url = TestDeltaFixtures.class.getClassLoader().getResource(resourceRoot);
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
    URL codeSource = TestDeltaFixtures.class.getProtectionDomain().getCodeSource().getLocation();
    try {
      return new JarFile(Path.of(codeSource.toURI()).toFile());
    } catch (Exception e) {
      throw new IOException("Failed to resolve fixture jar path", e);
    }
  }
}
