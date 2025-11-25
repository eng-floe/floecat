package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.FileContent;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.common.GenericStatsEngine;
import ai.floedb.metacat.connector.common.ProtoStatsBuilder;
import ai.floedb.metacat.connector.common.StatsEngine;
import ai.floedb.metacat.connector.common.ndv.ColumnNdv;
import ai.floedb.metacat.connector.common.ndv.FilteringNdvProvider;
import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.metacat.connector.common.ndv.SamplingNdvProvider;
import ai.floedb.metacat.connector.common.ndv.StaticOnceNdvProvider;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import ai.floedb.metacat.planning.rpc.PlanFile;
import ai.floedb.metacat.types.LogicalType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

public final class IcebergConnector implements MetacatConnector {
  private final String connectorId;
  private final RESTCatalog catalog;
  private final GlueIcebergFilter glueFilter;
  private final boolean ndvEnabled;
  private final double ndvSampleFraction;
  private final long ndvMaxFiles;

  private IcebergConnector(
      String connectorId,
      RESTCatalog catalog,
      GlueIcebergFilter filter,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    this.connectorId = connectorId;
    this.catalog = catalog;
    this.glueFilter = filter;
    this.ndvEnabled = ndvEnabled;
    this.ndvSampleFraction = ndvSampleFraction;
    this.ndvMaxFiles = ndvMaxFiles;
  }

  public static MetacatConnector create(
      String uri,
      Map<String, String> options,
      String authScheme,
      Map<String, String> authProps,
      Map<String, String> headerHints) {

    Objects.requireNonNull(uri, "uri");

    Map<String, String> props = new HashMap<>();
    props.put("type", "rest");
    props.put("uri", uri);

    Map<String, String> opts = (options == null) ? Collections.emptyMap() : options;
    if (!opts.isEmpty()) {
      for (var e : opts.entrySet()) {
        String k = e.getKey();
        if (k.startsWith("rest.") || k.startsWith("s3.") || k.equals("io-impl")) {
          props.put(k, e.getValue());
        }
      }
    }

    String scheme = (authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT));
    switch (scheme) {
      case "aws-sigv4" -> {
        String signingName = authProps.getOrDefault("signing-name", "glue");
        String signingRegion =
            authProps.getOrDefault(
                "signing-region",
                props.getOrDefault(
                    "rest.signing-region", props.getOrDefault("s3.region", "us-east-1")));
        props.put("rest.auth.type", "sigv4");
        props.put("rest.signing-name", signingName);
        props.put("rest.signing-region", signingRegion);

        props.putIfAbsent("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
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

    var glue =
        GlueClient.builder()
            .region(Region.of(props.getOrDefault("s3.region", "us-east-1")))
            .build();

    RESTCatalog cat = new RESTCatalog();
    cat.initialize("metacat-iceberg", Collections.unmodifiableMap(props));

    boolean ndvEnabled = parseNdvEnabled(options);
    double ndvSampleFraction = parseNdvSampleFraction(options);

    long ndvMaxFiles = 0L;
    if (options != null) {
      try {
        ndvMaxFiles = Long.parseLong(options.getOrDefault("stats.ndv.max_files", "0"));
        if (ndvMaxFiles < 0) {
          ndvMaxFiles = 0;
        }
      } catch (NumberFormatException ignore) {
      }
    }

    return new IcebergConnector(
        "iceberg-rest",
        cat,
        new GlueIcebergFilter(glue),
        ndvEnabled,
        ndvSampleFraction,
        ndvMaxFiles);
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
        .filter(glueFilter::databaseHasIceberg)
        .sorted()
        .toList();
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    return glueFilter.icebergTables(namespaceFq);
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    Namespace namespace = Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    Table table = catalog.loadTable(tableId);
    Schema schema = table.schema();
    String schemaJson = SchemaParser.toJson(schema);
    List<String> partitionKeys = table.spec().fields().stream().map(f -> f.name()).toList();

    return new TableDescriptor(
        namespaceFq, tableName, table.location(), schemaJson, partitionKeys, table.properties());
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns) {
    Namespace ns = Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tid = TableIdentifier.of(ns, tableName);
    Table table = catalog.loadTable(tid);

    final Set<Integer> includeIds =
        (includeColumns == null || includeColumns.isEmpty())
            ? table.schema().columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(Collectors.toCollection(LinkedHashSet::new))
            : resolveFieldIdsNested(table.schema(), includeColumns);

    List<SnapshotBundle> out = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      long parentId = snapshot.parentId() != null ? snapshot.parentId().longValue() : 0;
      long createdMs = snapshot.timestampMillis();

      EngineOut engineOutput = runEngine(table, snapshotId, includeIds);

      var tStats =
          ProtoStatsBuilder.toTableStats(
              destinationTableId,
              snapshotId,
              createdMs,
              TableFormat.TF_ICEBERG,
              engineOutput.result());
      var cStats =
          ProtoStatsBuilder.toColumnStats(
              destinationTableId,
              snapshotId,
              TableFormat.TF_ICEBERG,
              engineOutput.result().columns(),
              id -> {
                var f = table.schema().findField((Integer) id);
                return f == null ? "" : f.name();
              },
              id -> {
                var f = table.schema().findField((Integer) id);
                return f == null ? null : IcebergTypeMapper.toLogical(f.type());
              },
              createdMs,
              engineOutput.result().totalRowCount());
      var fileStats =
          ProtoStatsBuilder.toFileColumnStats(
              destinationTableId,
              snapshotId,
              TableFormat.TF_ICEBERG,
              engineOutput.result().files(),
              id -> {
                var f = table.schema().findField((Integer) id);
                return f == null ? "" : f.name();
              },
              id -> {
                var f = table.schema().findField((Integer) id);
                return f == null ? null : IcebergTypeMapper.toLogical(f.type());
              },
              createdMs);

      List<FileColumnStats> deleteStats = new ArrayList<>();
      TableScan scan = table.newScan().useSnapshot(snapshotId);
      try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
        for (FileScanTask task : tasks) {
          for (var df : task.deletes()) {
            deleteStats.add(
                FileColumnStats.newBuilder()
                    .setTableId(destinationTableId)
                    .setSnapshotId(snapshotId)
                    .setFilePath(df.location())
                    .setRowCount(df.recordCount())
                    .setSizeBytes(df.fileSizeInBytes())
                    .setFileContent(
                        df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
                            ? FileContent.FC_EQUALITY_DELETES
                            : FileContent.FC_POSITION_DELETES)
                    .build());
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to enumerate delete files for snapshot " + snapshotId, e);
      }

      List<FileColumnStats> allFiles = new ArrayList<>(fileStats);
      allFiles.addAll(deleteStats);

      out.add(new SnapshotBundle(snapshotId, parentId, createdMs, tStats, cStats, allFiles));
    }
    return out;
  }

  @Override
  public PlanBundle plan(String namespaceFq, String tableName, long snapshotId, long asOfTime) {
    Namespace ns = Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tid = TableIdentifier.of(ns, tableName);
    Table table = catalog.loadTable(tid);

    TableScan scan = table.newScan().includeColumnStats();
    if (snapshotId > 0) {
      scan.useSnapshot(snapshotId);
    } else {
      scan.asOfTime(asOfTime);
    }

    Snapshot snap = table.snapshot(snapshotId);
    int schemaId = (snap != null) ? snap.schemaId() : table.schema().schemaId();
    Schema schema = Optional.ofNullable(table.schemas().get(schemaId)).orElse(table.schema());
    var schemaColumns = schema.columns();

    PlanBundle result = new PlanBundle(new ArrayList<>(), new ArrayList<>());
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        {
          DataFile df = task.file();
          var pf =
              PlanFile.newBuilder()
                  .setFilePath(df.location())
                  .setFileFormat(df.format().name())
                  .setFileSizeInBytes(df.fileSizeInBytes())
                  .setRecordCount(df.recordCount())
                  .setFileContent(FileContent.FC_DATA);
          result.dataFiles().add(pf.build());
        }

        for (var df : task.deletes()) {
          var pf =
              PlanFile.newBuilder()
                  .setFilePath(df.location())
                  .setFileFormat(df.format().name())
                  .setFileSizeInBytes(df.fileSizeInBytes())
                  .setRecordCount(df.recordCount())
                  .setFileContent(
                      df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
                          ? FileContent.FC_EQUALITY_DELETES
                          : FileContent.FC_POSITION_DELETES);
          result.deleteFiles().add(pf.build());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Iceberg planning failed (snapshot " + snapshotId + ")", e);
    }

    return result;
  }

  @Override
  public void close() {
    try {
      catalog.close();
    } catch (Exception ignore) {
    }
  }

  private record EngineOut(StatsEngine.Result<Integer> result) {}

  private EngineOut runEngine(Table table, long snapshotId, Set<Integer> colIds) {
    try (var planner = new IcebergPlanner(table, snapshotId, colIds, null)) {

      Map<Integer, String> colNames = planner.columnNamesByKey();
      Map<Integer, LogicalType> logicalTypes = planner.logicalTypesByKey();

      if (!ndvEnabled) {
        NdvProvider none = null;
        StatsEngine<Integer> engine =
            new GenericStatsEngine<>(planner, none, null, colNames, logicalTypes);
        var result = engine.compute();
        return new EngineOut(result);
      }

      Map<String, ColumnNdv> puffinMap = null;
      try {
        puffinMap = PuffinNdvProvider.readPuffinNdvWithSketches(table, snapshotId, colNames::get);
      } catch (IOException ioe) {
      }

      NdvProvider bootstrap =
          (puffinMap == null || puffinMap.isEmpty()) ? null : new StaticOnceNdvProvider(puffinMap);

      final int totalCols = colNames.size();
      int missing;
      if (bootstrap != null) {
        final Map<String, ColumnNdv> m = puffinMap;
        int miss = 0;
        for (String columnName : colNames.values()) {
          ColumnNdv ndv = m.get(columnName);
          boolean hasData =
              ndv != null
                  && (ndv.approx != null || (ndv.sketches != null && !ndv.sketches.isEmpty()));
          if (!hasData) miss++;
        }
        missing = miss;
      } else {
        missing = totalCols;
      }

      NdvProvider perFileNdv = null;
      if (missing > 0) {
        var parquetNdv = ParquetNdvProvider.forIcebergIO(path -> table.io().newInputFile(path));
        NdvProvider base = FilteringNdvProvider.bySuffix(Set.of(".parquet", ".parq"), parquetNdv);

        if (ndvSampleFraction < 1.0 || ndvMaxFiles > 0) {
          base = new SamplingNdvProvider(base, ndvSampleFraction, ndvMaxFiles);
        }

        perFileNdv = base;
      }

      var engine = new GenericStatsEngine<>(planner, perFileNdv, bootstrap, colNames, logicalTypes);

      var result = engine.compute();
      return new EngineOut(result);
    } catch (Exception e) {
      throw new RuntimeException("Stats compute failed for snapshot " + snapshotId, e);
    }
  }

  private static Set<Integer> resolveFieldIdsNested(Schema schema, Set<String> selectors) {
    Map<String, Integer> byPath = new LinkedHashMap<>();
    for (Types.NestedField top : schema.columns()) {
      collectNested(top, "", byPath);
    }

    Set<Integer> out = new LinkedHashSet<>();
    for (String sel : selectors) {
      if (sel == null || sel.isBlank()) {
        continue;
      }

      String s = sel.trim();
      if (s.startsWith("#")) {
        out.add(Integer.parseInt(s.substring(1)));
      } else {
        Integer id = byPath.get(s);
        if (id == null) {
          throw new IllegalArgumentException("Unknown column selector: " + s);
        }
        out.add(id);
      }
    }
    return out;
  }

  private static void collectNested(Types.NestedField f, String prefix, Map<String, Integer> out) {
    final String name = prefix.isEmpty() ? f.name() : prefix + "." + f.name();
    final Type t = f.type();

    out.put(name, f.fieldId());

    if (t.isStructType()) {
      for (Types.NestedField child : t.asStructType().fields()) {
        collectNested(child, name, out);
      }
    } else if (t.isListType()) {
      Types.ListType lt = t.asListType();
      Types.NestedField elem = lt.fields().get(0);
      collectNested(elem, name, out);
    } else if (t.isMapType()) {
      Types.MapType mt = t.asMapType();
      Types.NestedField key = mt.fields().get(0);
      Types.NestedField val = mt.fields().get(1);
      collectNested(key, name, out);
      collectNested(val, name, out);
    }
  }

  private static boolean parseNdvEnabled(Map<String, String> options) {
    if (options != null) {
      String v = options.getOrDefault("stats.ndv.enabled", "true");
      return !v.equalsIgnoreCase("false");
    }
    return false;
  }

  private static double parseNdvSampleFraction(Map<String, String> options) {
    if (options == null) {
      return 0.0d;
    }

    String raw = options.get("stats.ndv.sample_fraction");
    if (raw == null || raw.isBlank()) {
      return 1.0d;
    }
    try {
      double f = Double.parseDouble(raw.trim());
      if (f <= 0.0d) {
        return 0.0d;
      }
      if (f > 1.0d) {
        return 1.0d;
      }
      return f;
    } catch (NumberFormatException nfe) {
      return 1.0d;
    }
  }

  final class NdvOnlyResult<K> implements StatsEngine.Result<K> {
    private final long rows;
    private final long bytes;
    private final long files;
    private final Map<K, StatsEngine.ColumnAgg> cols;

    NdvOnlyResult(long rows, long bytes, long files, Map<K, StatsEngine.ColumnAgg> cols) {
      this.rows = rows;
      this.bytes = bytes;
      this.files = files;
      this.cols = cols;
    }

    @Override
    public long totalRowCount() {
      return rows;
    }

    @Override
    public long totalSizeBytes() {
      return bytes;
    }

    @Override
    public long fileCount() {
      return files;
    }

    @Override
    public Map<K, StatsEngine.ColumnAgg> columns() {
      return cols;
    }

    static <K> StatsEngine.ColumnAgg ndvOnly(ColumnNdv ndv) {
      return new StatsEngine.ColumnAgg() {
        @Override
        public Long ndvExact() {
          return null;
        }

        @Override
        public ColumnNdv ndv() {
          return ndv;
        }

        @Override
        public Long valueCount() {
          return null;
        }

        @Override
        public Long nullCount() {
          return null;
        }

        @Override
        public Long nanCount() {
          return null;
        }

        @Override
        public Object min() {
          return null;
        }

        @Override
        public Object max() {
          return null;
        }
      };
    }
  }
}
