package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
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
import ai.floedb.floecat.types.LogicalType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.types.StructType;
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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

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

  private UnityDeltaConnector(
      String connectorId,
      UcHttp ucHttp,
      SqlStmtClient sql,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    this.connectorId = connectorId;
    this.ucHttp = ucHttp;
    this.sql = sql;
    this.engine = engine;
    this.parquetInput = parquetInput;
    this.ndvEnabled = ndvEnabled;
    this.ndvSampleFraction = ndvSampleFraction;
    this.ndvMaxFiles = ndvMaxFiles;
  }

  public static FloecatConnector create(
      String uri, Map<String, String> options, AuthProvider authProvider) {
    Objects.requireNonNull(uri, "Unity base uri");
    String host = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
    int connectMs = Integer.parseInt(options.getOrDefault("http.connect.ms", "10000"));
    int readMs = Integer.parseInt(options.getOrDefault("http.read.ms", "60000"));
    String warehouse = options.getOrDefault("databricks.sql.warehouse_id", "");

    var region =
        Region.of(
            options.getOrDefault("s3.region", options.getOrDefault("aws.region", "us-east-1")));

    var s3 =
        S3Client.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .build();

    var engine = DefaultEngine.create(new S3V2FileSystemClient(s3));

    var uc = new UcHttp(host, connectMs, readMs, authProvider);
    var sql = warehouse.isBlank() ? null : new SqlStmtClient(host, authProvider, warehouse, readMs);

    Function<String, InputFile> inputFn = p -> new ParquetS3V2InputFile(s3, p);

    boolean ndvEnabled = Boolean.parseBoolean(options.getOrDefault("stats.ndv.enabled", "false"));

    double ndvSampleFraction = 1.0;
    try {
      ndvSampleFraction =
          Double.parseDouble(options.getOrDefault("stats.ndv.sample_fraction", "1.0"));
      if (ndvSampleFraction <= 0.0 || ndvSampleFraction > 1.0) {
        ndvSampleFraction = 1.0;
      }
    } catch (NumberFormatException ignore) {
    }

    long ndvMaxFiles = 0L;
    try {
      ndvMaxFiles = Long.parseLong(options.getOrDefault("stats.ndv.max_files", "0"));
      if (ndvMaxFiles < 0) ndvMaxFiles = 0;
    } catch (NumberFormatException ignore) {
    }

    return new UnityDeltaConnector(
        "delta-unity", uc, sql, engine, inputFn, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
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
            nameToType::get,
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
            nameToType::get,
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
            0));
  }

  @Override
  public void close() {}

  private String storageLocation(String namespaceFq, String tableName) {
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

  private static void putIfPresent(Map<String, String> props, JsonNode n, String field) {
    if (!n.path(field).isMissingNode()) {
      props.put(field, n.path(field).asText());
    }
  }

  private record EngineOut(
      StatsEngine.Result<String> result,
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

      var engine =
          new GenericStatsEngine<>(
              planner,
              ndvProvider,
              bootstrap,
              planner.columnNamesByKey(),
              planner.logicalTypesByKey());

      var result = engine.compute();
      return new EngineOut(
          result,
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
