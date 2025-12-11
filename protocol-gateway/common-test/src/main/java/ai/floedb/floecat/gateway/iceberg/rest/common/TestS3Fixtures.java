package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class TestS3Fixtures {
  private static final Path MODULE_RELATIVE = Path.of("protocol-gateway", "iceberg-rest");
  private static final Path MODULE_ROOT = resolveModuleRoot();
  private static final Path FIXTURE_ROOT =
      MODULE_ROOT.resolve(Path.of("src", "test", "resources", "iceberg-fixtures"));
  private static final Path TARGET_ROOT = MODULE_ROOT.resolve(Path.of("target", "test-fake-s3"));
  private static final Path STAGE_ROOT = TARGET_ROOT.resolve("staged-fixtures");
  private static final String BUCKET = "yb-iceberg-tpcds";
  private static final String PREFIX = "trino_test";

  private TestS3Fixtures() {}

  public static String bucketUri(String relativePath) {
    String suffix = relativePath == null ? "" : relativePath.replaceFirst("^/", "");
    return "s3://" + BUCKET + "/" + PREFIX + (suffix.isEmpty() ? "" : "/" + suffix);
  }

  public static Path bucketPath() {
    return TARGET_ROOT.resolve(BUCKET);
  }

  public static Path prefixPath() {
    return bucketPath().resolve(PREFIX);
  }

  public static Path stageBucketPath() {
    return STAGE_ROOT;
  }

  public static void seedFixtures() {
    Path bucket = bucketPath();
    try {
      if (Files.exists(bucket)) {
        deleteRecursive(bucket);
      }
      Path prefixRoot = prefixPath();
      Files.createDirectories(prefixRoot);
      copyRecursive(FIXTURE_ROOT, prefixRoot);
      Files.createDirectories(stageBucketPath());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to seed fixture bucket", e);
    }
  }

  public static void seedStageTable(String namespace, String table) {
    Path stageRoot = stageBucketPath().resolve(Path.of(namespace, table));
    try {
      if (Files.exists(stageRoot)) {
        deleteRecursive(stageRoot);
      }
      Path stageMetadata = stageRoot.resolve("metadata");
      Path stageData = stageRoot.resolve("data");
      Files.createDirectories(stageMetadata);
      Files.createDirectories(stageData);
      copyRecursive(FIXTURE_ROOT.resolve("metadata"), stageMetadata);
      copyRecursive(FIXTURE_ROOT.resolve("data"), stageData);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to seed staged table " + namespace + "." + table, e);
    }
  }

  public static String stageTableUri(String namespace, String table, String relativePath) {
    String normalized = relativePath == null ? "" : relativePath.replaceFirst("^/", "");
    String suffix = normalized.isEmpty() ? "" : "/" + normalized;
    return "s3://staged-fixtures/" + namespace + "/" + table + suffix;
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

  private static Path resolveModuleRoot() {
    Path cwd = Path.of("").toAbsolutePath();
    if (cwd.endsWith(MODULE_RELATIVE)) {
      return cwd;
    }
    return cwd.resolve(MODULE_RELATIVE);
  }
}
