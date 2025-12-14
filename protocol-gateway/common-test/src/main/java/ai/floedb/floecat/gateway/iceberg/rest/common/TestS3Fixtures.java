package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

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

  private record FixtureSet(String name, Path sourceRoot, String bucket, String prefix) {}

  private static final FixtureSet SIMPLE_SET =
      new FixtureSet("simple", SIMPLE_ROOT, BUCKET, PREFIX);
  private static final FixtureSet COMPLEX_SET =
      new FixtureSet("complex", FIXTURE_ROOT.resolve("complex"), "floecat", "sales/us/trino_types");
  private static final List<FixtureSet> FIXTURE_SETS = List.of(SIMPLE_SET, COMPLEX_SET);

  private TestS3Fixtures() {}

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

  public static void seedFixturesOnce() {
    System.setProperty("fs.floecat.test-root", TARGET_ROOT.toAbsolutePath().toString());
    try {
      if (!needsReseed()) {
        return;
      }
      seedFixtures();
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
