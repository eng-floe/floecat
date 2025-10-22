package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

public final class IcebergConnector implements MetacatConnector {
  private final String connectorId;
  private final RESTCatalog catalog;

  private IcebergConnector(String connectorId, RESTCatalog catalog) {
    this.connectorId = connectorId;
    this.catalog = catalog;
  }

  public static MetacatConnector create(
      String uri,
      Map<String, String> options,
      String authScheme,
      Map<String, String> authProps,
      Map<String, String> headerHints) {
    Objects.requireNonNull(uri, "uri");
    Map<String, String> props = new HashMap<>(options == null ? Map.of() : options);
    props.put("type", "rest");
    props.put("uri", uri);

    String scheme = authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT);
    switch (scheme) {
      case "aws-sigv4" -> {
        String signingName = authProps.getOrDefault("signing-name", "glue");
        String signingRegion =
            authProps.getOrDefault("signing-region", props.getOrDefault("region", "us-east-1"));
        props.put("rest.auth.type", "sigv4");
        props.put("rest.signing-name", signingName);
        props.put("rest.signing-region", signingRegion);
        props.putIfAbsent("s3.region", signingRegion);
      }
      case "oauth2" -> {
        String token =
            Objects.requireNonNull(authProps.get("token"), "authProps.token required for oauth2");
        props.put("token", token);
      }
      case "none" -> {}
      default -> throw new IllegalArgumentException("Unsupported auth scheme: " + authScheme);
    }

    if (headerHints != null) {
      headerHints.forEach((k, v) -> props.put("header." + k, v));
    }

    props.putIfAbsent("rest.client.user-agent", "metacat-connector-iceberg");

    RESTCatalog cat = new RESTCatalog();
    cat.initialize("metacat-iceberg", Collections.unmodifiableMap(props));
    return new IcebergConnector("iceberg-rest", cat);
  }

  @Override
  public String id() {
    return connectorId;
  }

  @Override
  public ConnectorFormat format() {
    return ConnectorFormat.CF_ICEBERG;
  }

  @Override
  public List<String> listNamespaces() {
    return catalog.listNamespaces().stream()
        .map(Namespace::toString)
        .sorted()
        .collect(Collectors.toList());
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    Namespace ns = Namespace.of(namespaceFq.split("\\."));
    return catalog.listTables(ns).stream()
        .sorted(Comparator.comparing(TableIdentifier::name))
        .map(TableIdentifier::name)
        .collect(Collectors.toList());
  }

  @Override
  public UpstreamTable describe(String namespaceFq, String tableName) {
    Namespace namespace = Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    Table table = catalog.loadTable(tableId);
    Schema schema = table.schema();
    String schemaJson = SchemaParser.toJson(schema);
    List<String> partitionKeys = table.spec().fields().stream().map(f -> f.name()).toList();

    var snap = table.currentSnapshot();
    Optional<Long> snapshotId = Optional.ofNullable(snap).map(s -> s.snapshotId());
    Optional<Long> snapshotTs = Optional.ofNullable(snap).map(s -> s.timestampMillis());

    return new UpstreamTable(
        namespaceFq,
        tableName,
        table.location(),
        schemaJson,
        snapshotId,
        snapshotTs,
        table.properties(),
        partitionKeys);
  }

  @Override
  public boolean supportsTableStats() {
    return false;
  }

  @Override
  public void close() {
    try {
      catalog.close();
    } catch (Exception ignore) {
      // ignore
    }
  }
}
