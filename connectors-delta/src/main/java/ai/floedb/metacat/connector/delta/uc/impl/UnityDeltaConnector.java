package ai.floedb.metacat.connector.delta.uc.impl;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.common.GenericStatsEngine;
import ai.floedb.metacat.connector.common.ProtoStatsBuilder;
import ai.floedb.metacat.connector.common.StatsEngine;
import ai.floedb.metacat.connector.common.ndv.FilteringNdvProvider;
import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.metacat.connector.spi.AuthProvider;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import ai.floedb.metacat.planning.rpc.FileContent;
import ai.floedb.metacat.planning.rpc.PlanFile;
import ai.floedb.metacat.types.LogicalType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
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

public final class UnityDeltaConnector implements MetacatConnector {

  private static final ObjectMapper M = new ObjectMapper();

  private final String connectorId;
  private final UcHttp ucHttp;
  private final SqlStmtClient sql;
  private final Engine engine;
  private final Function<String, InputFile> parquetInput;
  private final S3Client s3Client;

  private UnityDeltaConnector(
      String connectorId,
      UcHttp ucHttp,
      SqlStmtClient sql,
      Engine engine,
      S3Client s3Client,
      java.util.function.Function<String, org.apache.parquet.io.InputFile> parquetInput) {
    this.connectorId = connectorId;
    this.ucHttp = ucHttp;
    this.sql = sql;
    this.engine = engine;
    this.s3Client = s3Client;
    this.parquetInput = parquetInput;
  }

  public static MetacatConnector create(
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

    var s3Client = s3;

    var engine = DefaultEngine.create(new S3V2FileSystemClient(s3));

    var uc = new UcHttp(host, connectMs, readMs, authProvider);
    var sql = warehouse.isBlank() ? null : new SqlStmtClient(host, authProvider, warehouse, readMs);

    var inputFn =
        (Function<String, InputFile>)
            (p -> {
              String s3uri = p.startsWith("s3a://") ? "s3://" + p.substring(6) : p;
              return new ParquetS3V2InputFile(s3, s3uri);
            });

    return new UnityDeltaConnector("delta-unity", uc, sql, engine, s3Client, inputFn);
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
        for (var s : schemas) out.add(catalogName + "." + s.path("name").asText());
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

    final Set<String> includeNames =
        (includeColumns == null || includeColumns.isEmpty())
            ? new LinkedHashSet<>(nameToType.keySet())
            : includeColumns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));

    EngineOut engineOut = runEngine(tableRoot, version, includeNames, nameToType);
    var result = engineOut.result();

    var tStats =
        ProtoStatsBuilder.toTableStats(
            destinationTableId, version, createdMs, TableFormat.TF_DELTA, result);

    var cStats =
        ProtoStatsBuilder.toColumnStats(
            destinationTableId,
            version,
            TableFormat.TF_DELTA,
            result.columns(),
            name -> name,
            nameToType::get,
            createdMs,
            result.totalRowCount());

    var normCols = normalizeColumnStats(cStats, kernelSchema);
    return List.of(new SnapshotBundle(version, parent, createdMs, tStats, normCols));
  }

  @Override
  public PlanBundle plan(String namespaceFq, String tableName, long snapshotId, long asOfTime) {
    final String tableRoot = storageLocation(namespaceFq, tableName);
    final Table table = Table.forPath(engine, tableRoot);

    final Snapshot snap =
        snapshotId > 0
            ? table.getSnapshotAsOfVersion(engine, snapshotId)
            : (asOfTime > 0
                ? table.getSnapshotAsOfTimestamp(engine, asOfTime)
                : table.getLatestSnapshot(engine));
    final long version = snap.getVersion();

    final DeltaFileEnumerator files = new DeltaFileEnumerator(engine, tableRoot, version);

    final List<PlanFile> data = new ArrayList<>();
    for (var f : files.dataFiles()) {
      data.add(
          PlanFile.newBuilder()
              .setFilePath(f.path())
              .setFileFormat("PARQUET")
              .setFileSizeInBytes(f.sizeBytes())
              .setRecordCount(f.rowCount())
              .setFileContent(FileContent.DATA)
              .build());
    }
    final List<PlanFile> deletes = new ArrayList<>();
    for (var df : files.deleteFiles()) {
      deletes.add(
          PlanFile.newBuilder()
              .setFilePath(df.path())
              .setFileFormat("PARQUET")
              .setFileSizeInBytes(df.sizeBytes())
              .setRecordCount(df.rowCount())
              .setFileContent(
                  df.isEquality() ? FileContent.EQUALITY_DELETES : FileContent.POSITION_DELETES)
              .build());
    }
    return new PlanBundle(data, deletes);
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

      return normalizeS3(loc);
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

  private static String normalizeS3(String uri) {
    if (uri == null) {
      return null;
    }

    if (uri.startsWith("s3://")) {
      return "s3a://" + uri.substring(5);
    }

    return uri;
  }

  private record EngineOut(StatsEngine.Result<String> result) {}

  private EngineOut runEngine(
      String tableRoot,
      long version,
      Set<String> includeNames,
      Map<String, LogicalType> nameToType) {

    NdvProvider bootstrap = null;

    var parquetNdv = new ParquetNdvProvider(parquetInput);
    var perFileNdv = FilteringNdvProvider.bySuffix(Set.of(".parquet", ".parq"), parquetNdv);

    try (var planner =
        new DeltaPlanner(
            this.engine,
            this.s3Client,
            tableRoot,
            version,
            includeNames,
            nameToType,
            perFileNdv,
            true)) {

      var engine =
          new GenericStatsEngine<>(
              planner,
              parquetNdv,
              bootstrap,
              planner.columnNamesByKey(),
              planner.logicalTypesByKey());

      var result = engine.compute();
      return new EngineOut(result);
    } catch (Exception e) {
      throw new RuntimeException("Delta stats compute failed (version " + version + ")", e);
    }
  }

  private static List<ColumnStats> normalizeColumnStats(
      List<ColumnStats> in, StructType kernelSchema) {
    var pos = new LinkedHashMap<String, Integer>();
    var fields = kernelSchema.fields();
    for (int i = 0; i < fields.size(); i++) {
      pos.put(fields.get(i).getName(), i + 1);
    }

    var byName = new LinkedHashMap<String, ColumnStats>();
    for (var c : in) {
      String name = c.getColumnName();
      String newCid = Integer.toString(pos.get(name));
      var normalized = c.toBuilder().setColumnId(newCid).build();
      byName.put(name, normalized);
    }

    return new ArrayList<>(byName.values());
  }
}
