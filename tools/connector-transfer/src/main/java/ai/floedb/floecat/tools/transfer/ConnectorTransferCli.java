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

package ai.floedb.floecat.tools.transfer;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.ConnectorTransferBundle;
import ai.floedb.floecat.connector.rpc.ConnectorTransferEntry;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.ExportConnectorRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.rpc.ValidateConnectorRequest;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

@CommandLine.Command(
    name = "floecat-connector-transfer",
    mixinStandardHelpOptions = true,
    version = "floecat-connector-transfer 1",
    description = "Export and import complete Floecat connector definitions.",
    subcommands = {
      ConnectorTransferCli.ExportCommand.class,
      ConnectorTransferCli.ImportCommand.class,
      ConnectorTransferCli.InspectCommand.class
    })
public final class ConnectorTransferCli implements Runnable {
  @CommandLine.Option(names = "--host", description = "gRPC host (default: ${DEFAULT-VALUE})")
  String host = env("FLOECAT_GRPC_HOST", "localhost");

  @CommandLine.Option(names = "--port", description = "gRPC port (default: ${DEFAULT-VALUE})")
  int port = envInt("FLOECAT_GRPC_PORT", 9100);

  @CommandLine.Option(names = "--tls", description = "Use TLS for the gRPC connection")
  boolean tls;

  @CommandLine.Option(names = "--token", description = "Bearer token (or FLOECAT_TOKEN)")
  String token = env("FLOECAT_TOKEN", "");

  @CommandLine.Option(
      names = "--session-token",
      description = "Session token (or FLOECAT_SESSION_TOKEN)")
  String sessionToken = env("FLOECAT_SESSION_TOKEN", "");

  @CommandLine.Option(
      names = "--account-id",
      description = "Account header value (or FLOECAT_ACCOUNT)")
  String accountId = env("FLOECAT_ACCOUNT", "");

  public static void main(String[] args) {
    int exit = new CommandLine(new ConnectorTransferCli()).execute(args);
    System.exit(exit);
  }

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  private Client client() {
    return new Client(host, port, tls, token, sessionToken, accountId);
  }

  @CommandLine.Command(
      name = "export",
      mixinStandardHelpOptions = true,
      description = "Export connector definitions and credentials to a protected zip archive.")
  static final class ExportCommand implements Callable<Integer> {
    @CommandLine.ParentCommand ConnectorTransferCli parent;

    @CommandLine.Parameters(index = "0", description = "Output archive path")
    Path output;

    @CommandLine.Option(
        names = "--connector",
        description = "Connector display name or ID to include; repeatable (default: all)")
    List<String> requested = new ArrayList<>();

    @CommandLine.Option(names = "--force", description = "Replace an existing output archive")
    boolean force;

    @CommandLine.Option(
        names = "--plaintext-secrets",
        required = true,
        description = "Acknowledge that the archive contains plaintext connector credentials")
    boolean plaintextSecrets;

    @Override
    public Integer call() throws Exception {
      if (!plaintextSecrets) {
        throw new IllegalArgumentException("--plaintext-secrets acknowledgement is required");
      }
      try (var client = parent.client()) {
        List<Connector> connectors = client.listConnectors();
        connectors = select(connectors, requested);
        if (connectors.isEmpty()) throw new IllegalArgumentException("no connectors selected");

        var bundle =
            ConnectorTransferBundle.newBuilder()
                .setFormatVersion(ConnectorArchive.FORMAT_VERSION)
                .setExportedAt(now());

        for (Connector listed : connectors) {
          var exported =
              client.connectors.exportConnector(
                  ExportConnectorRequest.newBuilder()
                      .setConnectorId(listed.getResourceId())
                      .setIncludeCredentials(true)
                      .build());
          var credentials =
              exported.hasCredentials()
                  ? java.util.Optional.of(exported.getCredentials())
                  : java.util.Optional.<ai.floedb.floecat.connector.rpc.AuthCredentials>empty();
          ConnectorSpec portable =
              PortableConnectorSpecs.from(exported.getConnector(), client.directory);
          var entry =
              ConnectorTransferEntry.newBuilder()
                  .setConnector(exported.getConnector())
                  .setPortableSpec(portable);
          credentials.ifPresent(entry::setCredentials);
          bundle.addEntries(entry);
        }
        String sourceAccount = parent.accountId;
        if (sourceAccount == null || sourceAccount.isBlank()) {
          sourceAccount = bundle.getEntries(0).getConnector().getResourceId().getAccountId();
        }
        bundle.setSourceAccountId(sourceAccount);
        ConnectorArchive.write(output, bundle.build(), force);
        System.out.printf("Exported %d connector(s) to %s%n", connectors.size(), output);
        return 0;
      }
    }
  }

  enum ConflictMode {
    FAIL,
    SKIP,
    REPLACE
  }

  @CommandLine.Command(
      name = "import",
      mixinStandardHelpOptions = true,
      description = "Import connector definitions through the Floecat gRPC API.")
  static final class ImportCommand implements Callable<Integer> {
    @CommandLine.ParentCommand ConnectorTransferCli parent;

    @CommandLine.Parameters(index = "0", description = "Input archive path")
    Path input;

    @CommandLine.Option(
        names = "--conflict",
        defaultValue = "FAIL",
        description =
            "Existing-name behavior: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    ConflictMode conflictMode;

    @CommandLine.Option(
        names = "--dry-run",
        description = "Validate every portable specification without mutating connectors")
    boolean dryRun;

    @Override
    public Integer call() throws Exception {
      ConnectorTransferBundle bundle = ConnectorArchive.read(input);
      try (var client = parent.client()) {
        Map<String, Connector> existing = new HashMap<>();
        for (Connector connector : client.listConnectors()) {
          existing.put(connector.getDisplayName(), connector);
        }

        int imported = 0;
        int skipped = 0;
        for (var entry : bundle.getEntriesList()) {
          ConnectorSpec spec = specWithCredentials(entry);
          if (spec.getState() == ConnectorState.CS_DELETING) {
            throw new IllegalArgumentException(
                "cannot import connector in deleting state: " + spec.getDisplayName());
          }
          var validation =
              client.connectors.validateConnector(
                  ValidateConnectorRequest.newBuilder().setSpec(spec).build());
          if (!validation.getOk()) {
            throw new IllegalArgumentException(
                "connector validation failed for "
                    + spec.getDisplayName()
                    + ": "
                    + validation.getSummary());
          }
          if (dryRun) {
            System.out.printf("VALID %s: %s%n", spec.getDisplayName(), validation.getSummary());
            continue;
          }

          Connector current = existing.get(spec.getDisplayName());
          String replacementId = "";
          if (current != null) {
            switch (conflictMode) {
              case FAIL ->
                  throw new IllegalArgumentException(
                      "connector already exists: " + spec.getDisplayName());
              case SKIP -> {
                System.out.println("Skipped existing connector " + spec.getDisplayName());
                skipped++;
                continue;
              }
              case REPLACE -> {
                replacementId = current.getResourceId().getId();
                client.connectors.deleteConnector(
                    DeleteConnectorRequest.newBuilder()
                        .setConnectorId(current.getResourceId())
                        .build());
              }
            }
          }

          var created =
              client
                  .connectors
                  .createConnector(
                      CreateConnectorRequest.newBuilder()
                          .setSpec(spec)
                          .setIdempotency(
                              IdempotencyKey.newBuilder()
                                  .setKey(idempotencyKey(bundle, entry, replacementId)))
                          .build())
                  .getConnector();
          restoreState(client.connectors, created, spec.getState());
          System.out.printf(
              "%s -> %s%n",
              entry.getConnector().getResourceId().getId(), created.getResourceId().getId());
          imported++;
        }
        if (dryRun) {
          System.out.printf(
              "Validated %d connector(s); no changes made%n", bundle.getEntriesCount());
        } else {
          System.out.printf("Imported %d connector(s); skipped %d%n", imported, skipped);
        }
        return 0;
      }
    }
  }

  @CommandLine.Command(
      name = "inspect",
      mixinStandardHelpOptions = true,
      description = "Print the contents of a connector archive without exposing secret values.")
  static final class InspectCommand implements Callable<Integer> {
    @CommandLine.ParentCommand ConnectorTransferCli parent;

    @CommandLine.Parameters(index = "0", description = "Archive path")
    Path input;

    @Override
    public Integer call() throws Exception {
      ConnectorTransferBundle bundle = ConnectorArchive.read(input);
      System.out.printf(
          "format=%d source_account=%s connectors=%d%n",
          bundle.getFormatVersion(), bundle.getSourceAccountId(), bundle.getEntriesCount());
      for (var entry : bundle.getEntriesList()) {
        System.out.printf(
            "%s  %s  %s  credentials=%s%n",
            entry.getConnector().getResourceId().getId(),
            entry.getPortableSpec().getKind().name(),
            entry.getPortableSpec().getDisplayName(),
            entry.hasCredentials() ? "present" : "absent");
      }
      return 0;
    }
  }

  private static void restoreState(
      ConnectorsGrpc.ConnectorsBlockingStub connectors,
      Connector created,
      ConnectorState requestedState) {
    if (requestedState == ConnectorState.CS_UNSPECIFIED
        || requestedState == ConnectorState.CS_ACTIVE) return;
    connectors.updateConnector(
        UpdateConnectorRequest.newBuilder()
            .setConnectorId(created.getResourceId())
            .setSpec(ConnectorSpec.newBuilder().setState(requestedState))
            .setUpdateMask(FieldMask.newBuilder().addPaths("state"))
            .build());
  }

  private static ConnectorSpec specWithCredentials(ConnectorTransferEntry entry) {
    if (!entry.hasCredentials()) return entry.getPortableSpec();
    var spec = entry.getPortableSpec().toBuilder();
    var auth =
        entry.getPortableSpec().hasAuth()
            ? entry.getPortableSpec().getAuth().toBuilder()
            : ai.floedb.floecat.connector.rpc.AuthConfig.newBuilder();
    auth.setCredentials(entry.getCredentials());
    return spec.setAuth(auth).build();
  }

  private static List<Connector> select(List<Connector> all, List<String> requested) {
    all = all.stream().sorted(Comparator.comparing(Connector::getDisplayName)).toList();
    if (requested == null || requested.isEmpty()) return all;
    Set<String> wanted = new HashSet<>(requested);
    List<Connector> selected =
        all.stream()
            .filter(
                connector ->
                    wanted.contains(connector.getDisplayName())
                        || wanted.contains(connector.getResourceId().getId()))
            .toList();
    Set<String> found = new HashSet<>();
    for (Connector connector : selected) {
      found.add(connector.getDisplayName());
      found.add(connector.getResourceId().getId());
    }
    List<String> missing =
        wanted.stream().filter(value -> !found.contains(value)).sorted().toList();
    if (!missing.isEmpty()) {
      throw new IllegalArgumentException("unknown connector(s): " + String.join(", ", missing));
    }
    return selected;
  }

  private static String idempotencyKey(
      ConnectorTransferBundle bundle, ConnectorTransferEntry entry, String replacementId)
      throws Exception {
    String source =
        bundle.getSourceAccountId()
            + "\n"
            + entry.getConnector().getResourceId().getId()
            + "\n"
            + entry.getPortableSpec().getDisplayName()
            + "\n"
            + replacementId;
    byte[] digest =
        MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8));
    return "connector-transfer-" + HexFormat.of().formatHex(digest);
  }

  private static Timestamp now() {
    Instant now = Instant.now();
    return Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
  }

  private static String env(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value.trim();
  }

  private static int envInt(String name, int fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
  }

  private static final class Client implements AutoCloseable {
    private final ManagedChannel managedChannel;
    final ConnectorsGrpc.ConnectorsBlockingStub connectors;
    final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

    Client(
        String host, int port, boolean tls, String token, String sessionToken, String accountId) {
      var builder = ManagedChannelBuilder.forAddress(host, port);
      if (tls) builder.useTransportSecurity();
      else builder.usePlaintext();
      managedChannel = builder.build();
      Channel channel =
          ClientInterceptors.intercept(
              managedChannel, new GrpcHeaders(token, sessionToken, accountId));
      connectors = ConnectorsGrpc.newBlockingStub(channel);
      directory = DirectoryServiceGrpc.newBlockingStub(channel);
    }

    List<Connector> listConnectors() {
      var output = new ArrayList<Connector>();
      String token = "";
      do {
        var response =
            connectors.listConnectors(
                ListConnectorsRequest.newBuilder()
                    .setPage(PageRequest.newBuilder().setPageSize(100).setPageToken(token).build())
                    .build());
        output.addAll(response.getConnectorsList());
        token = response.hasPage() ? response.getPage().getNextPageToken() : "";
      } while (!token.isBlank());
      return output;
    }

    @Override
    public void close() throws InterruptedException {
      managedChannel.shutdown();
      if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
        managedChannel.shutdownNow();
      }
    }
  }
}
