package ai.floedb.floecat.gateway.iceberg.rest.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
  private static final String[] FORWARDED_PROPS = {
    "floecat.kv",
    "floecat.kv.table",
    "floecat.kv.auto-create",
    "floecat.kv.ttl-enabled",
    "floecat.blob",
    "floecat.blob.s3.bucket",
    "floecat.fixtures.use-aws-s3",
    "aws.region",
    "aws.accessKeyId",
    "aws.secretAccessKey",
    "aws.dynamodb.endpoint-override",
    "aws.s3.endpoint-override",
    "aws.s3.force-path-style"
  };
  private static final String[] FORWARDED_ENVS = {
    "AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION"
  };
  private static final String[] DEBUG_AWS_ENVS = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_PROFILE",
    "AWS_ENDPOINT_URL",
    "AWS_S3_ENDPOINT",
    "LOCALSTACK_ENDPOINT"
  };
  private static final String CHECKSUM_REQUEST_PROP = "aws.requestChecksumCalculation";
  private static final String CHECKSUM_RESPONSE_PROP = "aws.responseChecksumValidation";
  private static final String CHECKSUM_DEFAULT = "when_required";
  private static final Map<String, String> DEFAULT_SERVICE_PROPS =
      Map.of("floecat.kv", "memory", "floecat.blob", "memory");
  private static final String BUILD_PROPS_FILE = ".real-service-build.properties";
  private static final String[] BUILD_PROP_NAMES = {"floecat.kv", "floecat.blob", "floecat.kv.auto-create"};

  @Override
  public Map<String, String> start() {
    try {
      applyDefaultServiceProperties();
      logForwardedState();
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
      command.add("--add-opens");
      command.add("java.base/java.lang=ALL-UNNAMED");
      addForwardedProperties(command);
      addChecksumProperties(command);
      command.add("-cp");
      command.add(serviceClasspath(runnerJar, extraClasspath));
      command.add("io.quarkus.bootstrap.runner.QuarkusEntryPoint");

      System.out.printf(
          "RealServiceTestResource launching Floecat service http/grpc port=%d management=%d"
              + " log=%s%n",
          httpPort, managementPort, logFile);

      ProcessBuilder builder =
          new ProcessBuilder(command)
              .directory(repoRoot().resolve("service").toFile())
              .redirectErrorStream(true);
      addForwardedEnv(builder.environment());
      addChecksumEnv(builder.environment());
      serviceProcess = builder.start();
      startLogRelay(serviceProcess, logFile);

      waitForPortOpen(Duration.ofSeconds(60));
      System.out.printf(
          "RealServiceTestResource Floecat service ready http/grpc port=%d management=%d%n",
          httpPort, managementPort);

    } catch (IOException e) {
      throw new RuntimeException("Failed to start Floecat service", e);
    }

    Map<String, String> testConfig = new LinkedHashMap<>();
    testConfig.put("floecat.gateway.upstream-target", LOOPBACK_HOST + ":" + httpPort);
    testConfig.put("floecat.gateway.upstream-plaintext", "true");
    testConfig.put("floecat.gateway.connector-integration-enabled", "true");
    testConfig.put(TEST_GRPC_PORT_PROPERTY, Integer.toString(httpPort));
    if (!TestS3Fixtures.useAwsFixtures()) {
      testConfig.put("floecat.gateway.metadata-file-io", InMemoryS3FileIO.class.getName());
      testConfig.put("floecat.gateway.metadata-file-io-root", TEST_S3_ROOT);
    }
    return testConfig;
  }

  private static void addForwardedProperties(List<String> command) {
    for (String name : FORWARDED_PROPS) {
      String value = System.getProperty(name);
      if (value != null && !value.isBlank()) {
        command.add("-D" + name + "=" + value);
      }
    }
  }

  private static void addForwardedEnv(Map<String, String> env) {
    for (String name : FORWARDED_ENVS) {
      String value = System.getenv(name);
      if (value != null && !value.isBlank()) {
        env.put(name, value);
      }
    }
  }

  private static void addChecksumProperties(List<String> command) {
    String request =
        firstNonBlank(System.getProperty(CHECKSUM_REQUEST_PROP), System.getenv("AWS_REQUEST_CHECKSUM_CALCULATION"));
    if (request == null || request.isBlank()) {
      request = CHECKSUM_DEFAULT;
    }
    String response =
        firstNonBlank(System.getProperty(CHECKSUM_RESPONSE_PROP), System.getenv("AWS_RESPONSE_CHECKSUM_VALIDATION"));
    if (response == null || response.isBlank()) {
      response = CHECKSUM_DEFAULT;
    }
    command.add("-D" + CHECKSUM_REQUEST_PROP + "=" + request);
    command.add("-D" + CHECKSUM_RESPONSE_PROP + "=" + response);
  }

  private void logForwardedState() {
    System.out.printf(
        "RealServiceTestResource forwarded system properties: %s%n",
        maskSensitive(collectProperties(FORWARDED_PROPS)));
    System.out.printf(
        "RealServiceTestResource forwarded env vars: %s%n",
        maskSensitive(collectEnv(FORWARDED_ENVS)));
    System.out.printf(
        "RealServiceTestResource AWS env vars: %s%n",
        maskSensitive(collectEnv(DEBUG_AWS_ENVS)));
  }

  private Map<String, String> collectProperties(String[] names) {
    Map<String, String> collected = new LinkedHashMap<>();
    for (String name : names) {
      String value = System.getProperty(name);
      if (value != null && !value.isBlank()) {
        collected.put(name, value);
      }
    }
    return collected;
  }

  private Map<String, String> collectEnv(String[] names) {
    Map<String, String> collected = new LinkedHashMap<>();
    for (String name : names) {
      String value = System.getenv(name);
      if (value != null && !value.isBlank()) {
        collected.put(name, value);
      }
    }
    return collected;
  }

  private Map<String, String> maskSensitive(Map<String, String> source) {
    if (source.isEmpty()) {
      return source;
    }
    Map<String, String> masked = new LinkedHashMap<>();
    source.forEach((key, value) -> masked.put(key, maskValue(key, value)));
    return masked;
  }

  private String maskValue(String key, String value) {
    if (value == null) {
      return null;
    }
    String lowerKey = key == null ? "" : key.toLowerCase();
    if (lowerKey.contains("secret") || lowerKey.contains("accesskey") || lowerKey.contains("token")) {
      return redact(value);
    }
    return value;
  }

  private String redact(String value) {
    int length = value.length();
    if (length <= 4) {
      return "***";
    }
    String prefix = value.substring(0, Math.min(2, length));
    String suffix = value.substring(length - Math.min(2, length));
    return prefix + "***" + suffix;
  }

  private static void addChecksumEnv(Map<String, String> env) {
    env.putIfAbsent("AWS_REQUEST_CHECKSUM_CALCULATION", CHECKSUM_DEFAULT);
    env.putIfAbsent("AWS_RESPONSE_CHECKSUM_VALIDATION", CHECKSUM_DEFAULT);
  }

  private static String firstNonBlank(String a, String b) {
    if (a != null && !a.isBlank()) {
      return a;
    }
    if (b != null && !b.isBlank()) {
      return b;
    }
    return null;
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
    Map<String, String> desiredProps = collectBuildProperties();
    if (Files.exists(serviceRunnerJar()) && buildPropsMatch(desiredProps)) {
      return;
    }
    List<String> command = new ArrayList<>();
    command.add("mvn");
    command.add("-pl");
    command.add("service");
    command.add("-am");
    command.add("package");
    command.add("-DskipTests");
    for (var entry : desiredProps.entrySet()) {
      command.add("-D" + entry.getKey() + "=" + entry.getValue());
    }
    System.out.printf(
        "RealServiceTestResource building service runner with props: %s%n", desiredProps);
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.directory(repoRoot().toFile());
    builder.inheritIO();
    try {
      Process mvn = builder.start();
      int exit = mvn.waitFor();
      if (exit != 0) {
        throw new IllegalStateException("Failed to build service module. Exit code=" + exit);
      }
      storeBuildProperties(desiredProps);
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

  private static void applyDefaultServiceProperties() {
    for (var entry : DEFAULT_SERVICE_PROPS.entrySet()) {
      String current = System.getProperty(entry.getKey());
      if (current == null || current.isBlank()) {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  private static Map<String, String> collectBuildProperties() {
    Map<String, String> props = new LinkedHashMap<>();
    for (String name : BUILD_PROP_NAMES) {
      String value = System.getProperty(name);
      if (value != null && !value.isBlank()) {
        props.put(name, value);
      }
    }
    return props;
  }

  private static boolean buildPropsMatch(Map<String, String> desired) {
    Map<String, String> existing = readBuildProperties();
    return Objects.equals(existing, desired);
  }

  private static Map<String, String> readBuildProperties() {
    Path file = serviceBuildPropsFile();
    if (!Files.isRegularFile(file)) {
      return Map.of();
    }
    Properties props = new Properties();
    try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      props.load(reader);
    } catch (IOException e) {
      System.err.printf(
          "RealServiceTestResource failed to read %s: %s%n", file, e.getMessage());
      return Map.of();
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (String name : BUILD_PROP_NAMES) {
      String value = props.getProperty(name);
      if (value != null && !value.isBlank()) {
        result.put(name, value);
      }
    }
    return result;
  }

  private static void storeBuildProperties(Map<String, String> props) {
    Path file = serviceBuildPropsFile();
    try {
      Files.createDirectories(file.getParent());
      Properties toWrite = new Properties();
      for (var entry : props.entrySet()) {
        toWrite.setProperty(entry.getKey(), entry.getValue());
      }
      try (Writer writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
        toWrite.store(writer, "RealServiceTestResource build properties");
      }
    } catch (IOException e) {
      System.err.printf(
          "RealServiceTestResource failed to store %s: %s%n", file, e.getMessage());
    }
  }

  private static Path serviceBuildPropsFile() {
    Path dir = serviceRunnerJar().getParent();
    if (dir == null) {
      dir = repoRoot().resolve("service").resolve("target");
    }
    return dir.resolve(BUILD_PROPS_FILE);
  }
}
