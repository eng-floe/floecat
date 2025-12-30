package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.GenericStatsEngine;
import ai.floedb.floecat.connector.common.ProtoStatsBuilder;
import ai.floedb.floecat.connector.common.StatsEngine;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.floecat.connector.common.ndv.SamplingNdvProvider;
import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.storage.spi.io.RuntimeFileIoOverrides;
import ai.floedb.floecat.types.LogicalType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.parquet.io.InputFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public final class UnityDeltaConnector implements FloecatConnector {

  private static final ObjectMapper M = new ObjectMapper();

  private final String connectorId;
  private final UcHttp ucHttp;
  private final SqlStmtClient sql;
  private final Engine engine;
  private final Function<String, InputFile> parquetInput;
  private final boolean ndvEnabled;
  private final double ndvSampleFraction;
  private final long ndvMaxFiles;
  private final String tableRootOverride;

  private UnityDeltaConnector(
      String connectorId,
      UcHttp ucHttp,
      SqlStmtClient sql,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      String tableRootOverride) {
    this.connectorId = connectorId;
    this.ucHttp = ucHttp;
    this.sql = sql;
    this.engine = engine;
    this.parquetInput = parquetInput;
    this.ndvEnabled = ndvEnabled;
    this.ndvSampleFraction = ndvSampleFraction;
    this.ndvMaxFiles = ndvMaxFiles;
    this.tableRootOverride = tableRootOverride;
  }

  public static FloecatConnector create(
      String uri, Map<String, String> options, AuthProvider authProvider) {
    Objects.requireNonNull(uri, "Unity base uri");
    String host = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
    Map<String, String> effectiveOptions =
        options == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options);
    if (Boolean.parseBoolean(System.getProperty("floecat.connector.fileio.overrides", "true"))) {
      RuntimeFileIoOverrides.mergeInto(effectiveOptions);
    }

    int connectMs = Integer.parseInt(effectiveOptions.getOrDefault("http.connect.ms", "10000"));
    int readMs = Integer.parseInt(effectiveOptions.getOrDefault("http.read.ms", "60000"));
    String warehouse = effectiveOptions.getOrDefault("databricks.sql.warehouse_id", "");

    var region =
        Region.of(
            resolveOption(
                effectiveOptions,
                "s3.region",
                "aws.region",
                "floecat.fileio.override.s3.region",
                "us-east-1"));

    String localRoot = effectiveOptions.getOrDefault("fs.floecat.test-root", "");
    var uc = new UcHttp(host, connectMs, readMs, authProvider);
    var sql = warehouse.isBlank() ? null : new SqlStmtClient(host, authProvider, warehouse, readMs);

    final Engine engine;
    final Function<String, InputFile> inputFn;

    if (localRoot != null && !localRoot.isBlank()) {
      var root = java.nio.file.Path.of(localRoot).toAbsolutePath();
      engine = DefaultEngine.create(new LocalFileSystemClient(root));
      inputFn = p -> new ParquetLocalInputFile(root, p);
    } else {
      boolean pathStyle =
          Boolean.parseBoolean(
              resolveOption(
                  effectiveOptions,
                  "s3.path-style-access",
                  "floecat.fileio.override.s3.path-style-access",
                  "false"));

      var s3Builder =
          S3Client.builder()
              .region(region)
              .serviceConfiguration(
                  S3Configuration.builder().pathStyleAccessEnabled(pathStyle).build())
              .credentialsProvider(resolveCredentials(effectiveOptions));

      String endpoint =
          resolveOption(
              effectiveOptions, "s3.endpoint", "floecat.fileio.override.s3.endpoint", null);
      if (endpoint != null && !endpoint.isBlank()) {
        s3Builder.endpointOverride(URI.create(endpoint));
      }

      var s3 = s3Builder.build();
      engine = DefaultEngine.create(new S3V2FileSystemClient(s3));
      inputFn = p -> new ParquetS3V2InputFile(s3, p);
    }

    boolean ndvEnabled =
        Boolean.parseBoolean(effectiveOptions.getOrDefault("stats.ndv.enabled", "false"));

    double ndvSampleFraction = 1.0;
    try {
      ndvSampleFraction =
          Double.parseDouble(effectiveOptions.getOrDefault("stats.ndv.sample_fraction", "1.0"));
      if (ndvSampleFraction <= 0.0 || ndvSampleFraction > 1.0) {
        ndvSampleFraction = 1.0;
      }
    } catch (NumberFormatException ignore) {
    }

    long ndvMaxFiles = 0L;
    try {
      ndvMaxFiles = Long.parseLong(effectiveOptions.getOrDefault("stats.ndv.max_files", "0"));
      if (ndvMaxFiles < 0) ndvMaxFiles = 0;
    } catch (NumberFormatException ignore) {
    }

    String tableRootOverride = effectiveOptions.getOrDefault("delta.table-root", "");

    return new UnityDeltaConnector(
        "delta-unity",
        uc,
        sql,
        engine,
        inputFn,
        ndvEnabled,
        ndvSampleFraction,
        ndvMaxFiles,
        tableRootOverride);
  }

  private static AwsCredentialsProvider resolveCredentials(Map<String, String> options) {
    String access =
        resolveOption(
            options, "s3.access-key-id", "floecat.fileio.override.s3.access-key-id", null);
    String secret =
        resolveOption(
            options, "s3.secret-access-key", "floecat.fileio.override.s3.secret-access-key", null);
    String token =
        resolveOption(
            options, "s3.session-token", "floecat.fileio.override.s3.session-token", null);

    if (access != null && !access.isBlank() && secret != null && !secret.isBlank()) {
      AwsCredentials creds =
          (token != null && !token.isBlank())
              ? AwsSessionCredentials.create(access, secret, token)
              : AwsBasicCredentials.create(access, secret);
      return StaticCredentialsProvider.create(creds);
    }
    return DefaultCredentialsProvider.builder().build();
  }

  private static String resolveOption(
      Map<String, String> options, String key, String sysProp, String defaultValue) {
    if (options != null) {
      String opt = options.get(key);
      if (opt != null && !opt.isBlank()) {
        return opt;
      }
    }
    if (sysProp != null && !sysProp.isBlank()) {
      String prop = System.getProperty(sysProp);
      if (prop != null && !prop.isBlank()) {
        return prop;
      }
    }
    return defaultValue;
  }

  private static String resolveOption(
      Map<String, String> options,
      String key,
      String fallbackKey,
      String sysProp,
      String defaultValue) {
    if (options != null) {
      String opt = options.get(key);
      if (opt != null && !opt.isBlank()) {
        return opt;
      }
      String fallback = options.get(fallbackKey);
      if (fallback != null && !fallback.isBlank()) {
        return fallback;
      }
    }
    if (sysProp != null && !sysProp.isBlank()) {
      String prop = System.getProperty(sysProp);
      if (prop != null && !prop.isBlank()) {
        return prop;
      }
    }
    return defaultValue;
  }

  @Override
  public String id() {
    return connectorId;
  }

  @Override
  public ConnectorFormat format() {
    return ConnectorFormat.CF_DELTA;
  }

  @Override
  public List<String> listNamespaces() {
    try {
      var cats = M.readTree(ucHttp.get("/api/2.1/unity-catalog/catalogs").body()).path("catalogs");
      List<String> out = new ArrayList<>();
      for (var c : cats) {
        String catalogName = c.path("name").asText();
        var schemas =
            M.readTree(
                    ucHttp
                        .get(
                            "/api/2.1/unity-catalog/schemas?catalog_name="
                                + UcBaseSupport.url(catalogName))
                        .body())
                .path("schemas");
        for (var s : schemas) {
          out.add(catalogName + "." + s.path("name").asText());
        }
      }
      out.sort(String::compareTo);
      return out;
    } catch (Exception e) {
      throw new RuntimeException("listNamespaces failed", e);
    }
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    int dot = namespaceFq.indexOf('.');
    if (dot < 0) {
      return List.of();
    }

    String catalog = namespaceFq.substring(0, dot);
    String schema = namespaceFq.substring(dot + 1);
    try {
      var tables =
          M.readTree(
                  ucHttp
                      .get(
                          "/api/2.1/unity-catalog/tables?catalog_name="
                              + UcBaseSupport.url(catalog)
                              + "&schema_name="
                              + UcBaseSupport.url(schema))
                      .body())
              .path("tables");
      List<String> out = new ArrayList<>();
      for (var t : tables) {
        String fmt = t.path("data_source_format").asText("");
        if ("DELTA".equalsIgnoreCase(fmt)) {
          out.add(t.path("name").asText());
        }
      }
      out.sort(String::compareTo);
      return out;
    } catch (Exception e) {
      throw new RuntimeException("listTables failed", e);
    }
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    if (tableRootOverride != null && !tableRootOverride.isBlank()) {
      return describeFromDelta(tableRootOverride, namespaceFq, tableName);
    }

    try {
      String full = namespaceFq + "." + tableName;
      var meta =
          M.readTree(ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full)).body());

      var fields = M.createArrayNode();
      for (var c : meta.path("columns")) {
        var n = M.createObjectNode();
        n.put("name", c.path("name").asText());
        n.put("type", c.path("type_text").asText(c.path("type_name").asText()));
        n.put("nullable", c.path("nullable").asBoolean(true));
        var md = M.createObjectNode();
        if (!c.path("comment").isMissingNode()) {
          md.put("comment", c.path("comment").asText());
        }
        n.set("metadata", md);
        fields.add(n);
      }
      var schemaNode = M.createObjectNode();
      schemaNode.put("type", "struct");
      schemaNode.set("fields", fields);

      Map<String, String> props = new LinkedHashMap<>();
      putIfPresent(props, meta, "table_type");
      putIfPresent(props, meta, "data_source_format");
      putIfPresent(props, meta, "storage_location");

      String location = meta.path("storage_location").asText(null);
      return new TableDescriptor(
          namespaceFq, tableName, location, schemaNode.toString(), List.of(), props);
    } catch (Exception e) {
      throw new RuntimeException("describe failed", e);
    }
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns) {

    final String tableRoot = storageLocation(namespaceFq, tableName);

    final Table table = Table.forPath(engine, tableRoot);
    final Snapshot snapshot = table.getLatestSnapshot(engine);

    final long version = snapshot.getVersion();
    final long createdMs = snapshot.getTimestamp(engine);
    final long parent = Math.max(0L, version - 1L);

    final StructType kernelSchema = snapshot.getSchema();
    final Map<String, LogicalType> nameToType = DeltaTypeMapper.deltaTypeMap(kernelSchema);
    final String schemaJson = kernelSchema.toJson();
    final PartitionSpecInfo partitionSpec = toPartitionSpecInfo(snapshot);

    final Set<String> includeNames =
        (includeColumns == null || includeColumns.isEmpty())
            ? new LinkedHashSet<>(nameToType.keySet())
            : includeColumns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));

    EngineOut engineOut = runEngine(tableRoot, version, includeNames, nameToType);
    if (engineOut.hasInlineDeletionVectors()) {
      throw new UnsupportedOperationException(
          "Delta table uses inline deletion vectors; not supported for snapshot " + version);
    }
    var result = engineOut.result();
    var logicalTypes = engineOut.logicalTypes();

    var tStats =
        ProtoStatsBuilder.toTableStats(
            destinationTableId, version, createdMs, TableFormat.TF_DELTA, result);

    var positions = new LinkedHashMap<String, Integer>();
    var fields = kernelSchema.fields();
    for (int i = 0; i < fields.size(); i++) {
      positions.put(fields.get(i).getName(), i + 1);
    }

    var cStats =
        ProtoStatsBuilder.toColumnStats(
            destinationTableId,
            version,
            TableFormat.TF_DELTA,
            result.columns(),
            name -> name,
            positions::get,
            name -> {
              var lt = logicalTypes.get(name);
              return (lt != null) ? lt : nameToType.get(name);
            },
            createdMs,
            result.totalRowCount());

    var fileStats =
        ProtoStatsBuilder.toFileColumnStats(
            destinationTableId,
            version,
            TableFormat.TF_DELTA,
            result.files(),
            name -> name,
            positions::get,
            name -> {
              var lt = logicalTypes.get(name);
              return (lt != null) ? lt : nameToType.get(name);
            },
            createdMs);

    for (DeletionVectorDescriptor dv : engineOut.deletionVectors()) {
      String dvPath =
          (dv.isOnDisk() && dv.getPathOrInlineDv() != null) ? dv.getPathOrInlineDv() : "";
      long rowCount = dv.getCardinality();
      long sizeBytes = dv.getSizeInBytes();
      fileStats.add(
          FileColumnStats.newBuilder()
              .setTableId(destinationTableId)
              .setSnapshotId(version)
              .setFilePath(dvPath)
              .setRowCount(rowCount)
              .setSizeBytes(sizeBytes)
              .setFileContent(FileContent.FC_POSITION_DELETES)
              .build());
    }

    return List.of(
        new SnapshotBundle(
            version,
            parent,
            createdMs,
            tStats,
            cStats,
            fileStats,
            schemaJson,
            partitionSpec,
            0L,
            null,
            Map.of(),
            0,
            Map.of()));
  }

  @Override
  public void close() {}

  private String storageLocation(String namespaceFq, String tableName) {
    if (tableRootOverride != null && !tableRootOverride.isBlank()) {
      return tableRootOverride;
    }
    try {
      String full = namespaceFq + "." + tableName;
      var meta =
          M.readTree(ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full)).body());
      String loc = meta.path("storage_location").asText(null);
      if (loc == null || loc.isBlank()) {
        throw new IllegalStateException("Table has no storage_location: " + full);
      }

      return loc;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to resolve storage_location for " + namespaceFq + "." + tableName, e);
    }
  }

  private TableDescriptor describeFromDelta(
      String tableRoot, String namespaceFq, String tableName) {
    try {
      Table table = Table.forPath(engine, tableRoot);
      Snapshot snapshot = table.getLatestSnapshot(engine);
      StructType kernelSchema = snapshot.getSchema();

      var fields = M.createArrayNode();
      var kernelFields = kernelSchema.fields();
      for (int i = 0; i < kernelFields.size(); i++) {
        var c = kernelFields.get(i);
        var n = M.createObjectNode();
        n.put("name", c.getName());
        n.put("type", c.getDataType().toString());
        n.put("nullable", c.isNullable());
        n.set("metadata", M.createObjectNode());
        fields.add(n);
      }
      var schemaNode = M.createObjectNode();
      schemaNode.put("type", "struct");
      schemaNode.set("fields", fields);

      Map<String, String> props = new LinkedHashMap<>();
      props.put("data_source_format", "DELTA");
      props.put("storage_location", tableRoot);

      return new TableDescriptor(
          namespaceFq, tableName, tableRoot, schemaNode.toString(), List.of(), props);
    } catch (Exception e) {
      throw new RuntimeException("describe failed", e);
    }
  }

  private static void putIfPresent(Map<String, String> props, JsonNode n, String field) {
    if (!n.path(field).isMissingNode()) {
      props.put(field, n.path(field).asText());
    }
  }

  private record EngineOut(
      StatsEngine.Result<String> result,
      Map<String, LogicalType> logicalTypes,
      boolean hasDeletionVectors,
      boolean hasInlineDeletionVectors,
      List<DeletionVectorDescriptor> deletionVectors) {}

  private EngineOut runEngine(
      String tableRoot,
      long version,
      Set<String> includeNames,
      Map<String, LogicalType> nameToType) {

    NdvProvider bootstrap = null;

    NdvProvider ndvProvider = null;

    if (ndvEnabled) {
      NdvProvider base = new ParquetNdvProvider(parquetInput);
      if (ndvSampleFraction < 1.0 || ndvMaxFiles > 0) {
        base = new SamplingNdvProvider(base, ndvSampleFraction, ndvMaxFiles);
      }

      ndvProvider = base;
    }

    try (var planner =
        new DeltaPlanner(
            this.engine,
            this.parquetInput,
            tableRoot,
            version,
            includeNames,
            nameToType,
            ndvProvider,
            true)) {

      var columnNames = planner.columnNamesByKey();
      var logicalTypes = planner.logicalTypesByKey();

      var engine =
          new GenericStatsEngine<>(planner, ndvProvider, bootstrap, columnNames, logicalTypes);

      var result = engine.compute();
      return new EngineOut(
          result,
          logicalTypes,
          planner.hasDeletionVectors(),
          planner.hasInlineDeletionVectors(),
          planner.deletionVectors());
    } catch (Exception e) {
      throw new RuntimeException("Delta stats compute failed (version " + version + ")", e);
    }
  }

  private Snapshot resolveSnapshot(Table table, long snapshotId, long asOfTime) {
    if (snapshotId > 0) {
      return table.getSnapshotAsOfVersion(engine, snapshotId);
    }
    if (asOfTime > 0) {
      return table.getSnapshotAsOfTimestamp(engine, asOfTime);
    }

    return table.getLatestSnapshot(engine);
  }

  private static PartitionSpecInfo toPartitionSpecInfo(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    var partitionCols = snapshot.getPartitionColumnNames();
    if (partitionCols == null || partitionCols.isEmpty()) {
      return null;
    }
    PartitionSpecInfo.Builder builder =
        PartitionSpecInfo.newBuilder().setSpecId(0).setSpecName("delta");
    int order = 0;
    for (String column : partitionCols) {
      builder.addFields(
          ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
              .setFieldId(++order)
              .setName(column)
              .setTransform("identity")
              .build());
    }
    return builder.build();
  }
}
