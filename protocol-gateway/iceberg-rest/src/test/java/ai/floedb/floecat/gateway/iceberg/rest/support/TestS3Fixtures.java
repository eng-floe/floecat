package ai.floedb.floecat.gateway.iceberg.rest.support;

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
  private static final String BUCKET = "test-bucket";

  private TestS3Fixtures() {}

  public static String bucketUri(String relativePath) {
    return "s3://" + BUCKET + "/" + relativePath;
  }

  public static Path bucketPath() {
    return TARGET_ROOT.resolve(BUCKET);
  }

  public static void seedFixtures() {
    Path bucket = bucketPath();
    try {
      if (Files.exists(bucket)) {
        deleteRecursive(bucket);
      }
      Files.createDirectories(bucket);
      copyRecursive(FIXTURE_ROOT, bucket);
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

  private static Path resolveModuleRoot() {
    Path cwd = Path.of("").toAbsolutePath();
    if (cwd.endsWith(MODULE_RELATIVE)) {
      return cwd;
    }
    return cwd.resolve(MODULE_RELATIVE);
  }
}
