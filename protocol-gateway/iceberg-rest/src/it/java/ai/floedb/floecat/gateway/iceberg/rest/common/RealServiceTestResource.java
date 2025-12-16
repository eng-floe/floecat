package ai.floedb.floecat.gateway.iceberg.rest.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class RealServiceTestResource implements QuarkusTestResourceLifecycleManager {
  private static final String TEST_GRPC_PORT_PROPERTY = "floecat.test.upstream-grpc-port";
  private static final String TEST_S3_ROOT =
      TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString();
  private static final String LOOPBACK_HOST = "127.0.0.1";
  private static final String DEFAULT_ACCOUNT = "5eaa9cd5-7d08-3750-9457-cfe800b0b9d2";
  private static final String AUTH_HEADER_VALUE = "Bearer integration-test";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private Process serviceProcess;
  private int httpPort;
  private int managementPort;

  @Override
  public Map<String, String> start() {
    try {
      ensureServiceBuilt();

      httpPort = findFreePort();
      managementPort = findFreePort();
      System.setProperty(TEST_GRPC_PORT_PROPERTY, Integer.toString(httpPort));

      Path runnerJar = serviceRunnerJar();

      Path logDir = Path.of("target");
      Files.createDirectories(logDir);

      Path logFile =
          logDir.resolve(
              "floecat-service-it-"
                  + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())
                  + ".log");

      List<String> extraClasspath = selectItClasspathEntries();
      if (!extraClasspath.isEmpty()) {
        System.out.printf(
            "RealServiceTestResource adding IT classpath entries to service: %s%n",
            extraClasspath);
      }

      List<String> command = new ArrayList<>();
      command.add(javaBin());
      command.add("-Dquarkus.http.host=" + LOOPBACK_HOST);
      command.add("-Dquarkus.http.port=" + httpPort);
      command.add("-Dquarkus.grpc.server.use-separate-server=false");
      command.add("-Dquarkus.grpc.server.host=" + LOOPBACK_HOST);
      command.add("-Dquarkus.grpc.server.port=" + httpPort);
      command.add("-Dquarkus.grpc.server.plain-text=true");
      command.add("-Dquarkus.grpc.clients.floecat.host=" + LOOPBACK_HOST);
      command.add("-Dquarkus.grpc.clients.floecat.port=" + httpPort);
      command.add("-Dquarkus.grpc.clients.floecat.plain-text=true");
      command.add("-Dquarkus.management.enabled=true");
      command.add("-Dquarkus.management.host=" + LOOPBACK_HOST);
      command.add("-Dquarkus.management.port=" + managementPort);
      command.add("-Dfloecat.seed.mode=fake");
      command.add("-Dquarkus.profile=test");
      command.add("-cp");
      command.add(serviceClasspath(runnerJar, extraClasspath));
      command.add("io.quarkus.bootstrap.runner.QuarkusEntryPoint");

      System.out.printf(
          "RealServiceTestResource launching Floecat service http/grpc port=%d management=%d"
              + " log=%s%n",
          httpPort, managementPort, logFile);

      serviceProcess =
          new ProcessBuilder(command)
              .directory(repoRoot().resolve("service").toFile())
              .redirectErrorStream(true)
              .start();
      startLogRelay(serviceProcess, logFile);

      waitForPortOpen(Duration.ofSeconds(60));
      System.out.printf(
          "RealServiceTestResource Floecat service ready http/grpc port=%d management=%d%n",
          httpPort, managementPort);

    } catch (IOException e) {
      throw new RuntimeException("Failed to start Floecat service", e);
    }

    return Map.of(
        "floecat.gateway.upstream-target",
        LOOPBACK_HOST + ":" + httpPort,
        "floecat.gateway.upstream-plaintext",
        "true",
        "floecat.gateway.metadata-file-io",
        InMemoryS3FileIO.class.getName(),
        "floecat.gateway.metadata-file-io-root",
        TEST_S3_ROOT,
        "floecat.gateway.connector-integration-enabled",
        "true",
        TEST_GRPC_PORT_PROPERTY,
        Integer.toString(httpPort));
  }

  @Override
  public void stop() {
    System.clearProperty(TEST_GRPC_PORT_PROPERTY);
    if (serviceProcess != null) {
      serviceProcess.destroy();
      try {
        if (!serviceProcess.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
          serviceProcess.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void ensureServiceBuilt() {
    if (Files.exists(serviceRunnerJar())) {
      return;
    }
    ProcessBuilder builder =
        new ProcessBuilder("mvn", "-pl", "service", "-am", "package", "-DskipTests");
    builder.directory(repoRoot().toFile());
    builder.inheritIO();
    try {
      Process mvn = builder.start();
      int exit = mvn.waitFor();
      if (exit != 0) {
        throw new IllegalStateException("Failed to build service module. Exit code=" + exit);
      }
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Unable to build service module", e);
    }
  }

  private void waitForPortOpen(Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (!serviceProcess.isAlive()) {
        throw new IllegalStateException("Service process exited prematurely");
      }
      try (var socket = new java.net.Socket()) {
        socket.connect(new java.net.InetSocketAddress("127.0.0.1", httpPort), 500);
        return;
      } catch (IOException ignored) {
        // retry
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for service startup", e);
      }
    }
    throw new IllegalStateException("Timed out waiting for Floecat service to become healthy");
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private void startLogRelay(Process process, Path logFile) {
    Thread relay =
        new Thread(
            () -> {
              try (BufferedReader reader =
                      new BufferedReader(
                          new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
                  BufferedWriter writer =
                      Files.newBufferedWriter(
                          logFile,
                          StandardOpenOption.CREATE,
                          StandardOpenOption.APPEND,
                          StandardOpenOption.WRITE)) {
                String line;
                while ((line = reader.readLine()) != null) {
                  writer.write(line);
                  writer.newLine();
                  writer.flush();
                  System.out.printf("[FloecatService] %s%n", line);
                }
              } catch (IOException e) {
                System.err.printf("Failed to relay Floecat service logs: %s%n", e.getMessage());
              }
            },
            "FloecatServiceLogRelay");
    relay.setDaemon(true);
    relay.start();
  }

  private static Path repoRoot() {
    Path moduleRoot = Path.of("").toAbsolutePath();
    Path parent = moduleRoot.getParent();
    Path grandParent = parent != null ? parent.getParent() : null;
    return grandParent == null ? moduleRoot : grandParent;
  }

  private static Path serviceRunnerJar() {
    return repoRoot().resolve("service/target/quarkus-app/quarkus-run.jar");
  }

  private static String serviceClasspath(Path runnerJar, List<String> extraEntries) {
    List<String> entries = new ArrayList<>();
    entries.add(runnerJar.toAbsolutePath().toString());
    Path quarkusAppDir = runnerJar.getParent();
    addJarEntries(quarkusAppDir.resolve("app"), entries);
    addJarEntries(quarkusAppDir.resolve("lib").resolve("boot"), entries);
    addJarEntries(quarkusAppDir.resolve("lib").resolve("main"), entries);
    entries.addAll(extraEntries);

    StringJoiner joiner = new StringJoiner(File.pathSeparator);
    for (String entry : entries) {
      joiner.add(entry);
    }
    return joiner.toString();
  }

  private static void addJarEntries(Path dir, List<String> entries) {
    if (dir == null || !Files.isDirectory(dir)) {
      return;
    }
    try (var stream = Files.list(dir)) {
      stream
          .filter(path -> path.toString().endsWith(".jar"))
          .map(path -> path.toAbsolutePath().toString())
          .forEach(entries::add);
    } catch (IOException e) {
      throw new RuntimeException("Failed to scan service classpath directory " + dir, e);
    }
  }

  private static List<String> selectItClasspathEntries() {
    List<String> selected = new ArrayList<>();
    Path moduleRoot = Path.of("").toAbsolutePath();
    addIfExists(moduleRoot.resolve("target/test-classes"), selected);
    addIfExists(moduleRoot.resolve("target/classes"), selected);

    String classpath = System.getProperty("java.class.path");
    if (classpath == null || classpath.isBlank()) {
      return selected;
    }

    for (String entry : classpath.split(File.pathSeparator)) {
      if (entry.contains("parquet") || entry.contains("hadoop")) {
        selected.add(entry);
      }
    }
    return selected;
  }

  private static void addIfExists(Path path, List<String> entries) {
    if (Files.exists(path)) {
      entries.add(path.toAbsolutePath().toString());
    }
  }

  private static String javaBin() {
    String javaHome = System.getProperty("java.home");
    Path bin = Path.of(javaHome, "bin", "java");
    if (!Files.exists(bin)) {
      bin = Path.of(javaHome, "bin", "java.exe");
    }
    return bin.toString();
  }
}
